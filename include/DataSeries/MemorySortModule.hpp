// -*-C++-*-
// TODO-tomer: ask Eric how he wants this copyright header to read for your
// stuff. (or make Jay follow up.)
/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A module which buffers all the extents from the upstream module
    and sorts all the records in memory. getExtent returns new extents.
*/

#ifndef __DATASERIES_MEMORYSORTMODULE_H
#define __DATASERIES_MEMORYSORTMODULE_H

#include <vector>
#include <algorithm>
#include <memory>

#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/format.hpp>

#include <Lintel/PriorityQueue.hpp>
#include <Lintel/LintelLog.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/Field.hpp>
#include <DataSeries/GeneralField.hpp>

template <typename FieldType, typename FieldComparator>
class MemorySortModule : public DataSeriesModule {
public:
    /** Constructs a new @c MemorySortModule that will sort all the records based on the field
        named @param fieldName\. A sorting functor must also be provided.
        \param upstreamModule  The upstream @c DataSeriesModule from which this module requests
                               extents.
        \param fieldName       The name of the field on which the records will be sorted.
        \param fieldComparator The comparison (less-than) function for comparing two fields.
        \param extentSizeLimit The maximum size of the extents returned by getExtent. A value
                               of 0 indicates that a single extent should be returned. */
    MemorySortModule(DataSeriesModule &upstreamModule,
                     const std::string &fieldName,
                     const FieldComparator &fieldComparator,
                     size_t extentSizeLimit = 0);

    virtual ~MemorySortModule();

    /** Returns the next @c Extent according to the sorted order. Note that the first call to
        getExtent will be significantly slower than the rest, because it triggers the sorting process. */
    virtual Extent *getExtent();

    /** Resets the object so that it can be used again if the upstream module has data. */
    void reset();

private:
    /** An internal class that wraps a single extent and allows it to be sorted. Sorting the
        extent simply involves sorting the positions variable. The purpose of the iterator
        variable is to iterate over the positions vector. */
    class SortedExtent {
    public:
        boost::shared_ptr<Extent> extent;
        std::vector<const void*> positions; // the positions in sorted order
                                            // (positions.size() == # of records in this extent)
        std::vector<const void*>::iterator iterator;

        void resetIterator();
    };

    /** A data class to hold data that is needed by an AbstractComparator. This cannot
        be stored directly in AbstractComparator because std::sort copies the comparator
        (once for each item!), resulting in a performance hit. */
    class ComparatorData {
    public:
        ComparatorData(const std::string &fieldName, FieldComparator &fieldComparator);
        FieldComparator &fieldComparator;
        ExtentSeries seriesLhs;
        ExtentSeries seriesRhs;
        FieldType fieldLhs;
        FieldType fieldRhs;
    };

    /** A base class for comparators that wrap a user-specified FieldComparator. */
    class AbstractComparator {
    public:
        AbstractComparator(ComparatorData &data);
        void setExtent(Extent *extent); // so we can use relocate later

    protected:
        ComparatorData &data;
    };

    /** An internal class for comparing fields based on pointers to their records. */
    class PositionComparator : public AbstractComparator {
    public:
        PositionComparator(ComparatorData &data);
        bool operator()(const void *positionLhs, const void *positionRhs);
    };

    /** An internal class for comparing sorted extents based on pointers to their records. */
    class SortedExtentComparator : public AbstractComparator {
    public:
        SortedExtentComparator(ComparatorData &data);
        bool operator()(SortedExtent *sortedExtentLhs, SortedExtent *sortedExtentRhs);
    };

    void retrieveExtents();
    void sortExtents();
    void sortExtent(SortedExtent &sortedExtent);
    Extent* createNextExtent();

    std::vector<boost::shared_ptr<SortedExtent> > sortedExtents;

    bool initialized; // starts as false, becomes true after first call to upstreamModule.getExtent
    DataSeriesModule &upstreamModule;
    std::string fieldName;
    FieldComparator fieldComparator;
    size_t extentSizeLimit;

    ComparatorData comparatorData;

    PositionComparator positionComparator;

    // some members to help with the merging
    SortedExtentComparator sortedExtentComparator;
    PriorityQueue <SortedExtent*, SortedExtentComparator> sortedExtentQueue;

    struct timeval startSortTime;
    struct timeval endSortTime;
    struct timeval endMergeTime;
};


template <typename FieldType, typename FieldComparator>
MemorySortModule<FieldType, FieldComparator>::
MemorySortModule(DataSeriesModule &upstreamModule,
                 const std::string &fieldName,
                 const FieldComparator &fieldComparator,
                 size_t extentSizeLimit)
    : initialized(false),
      upstreamModule(upstreamModule), fieldName(fieldName), fieldComparator(fieldComparator),
      extentSizeLimit(extentSizeLimit),
      comparatorData(fieldName, this->fieldComparator),
      positionComparator(comparatorData),
      sortedExtentComparator(comparatorData),
      sortedExtentQueue(sortedExtentComparator) {
}

template <typename FieldType, typename FieldComparator>
MemorySortModule<FieldType, FieldComparator>::
~MemorySortModule() {
}

template <typename FieldType, typename FieldComparator>
Extent *MemorySortModule<FieldType, FieldComparator>::
getExtent() {
    // retrieve the extents and sort them if this is the first call to getExtent
    if (!initialized) {
        retrieveExtents();
        if (sortedExtents.size() > 0) {
            sortExtents();
        }
        initialized = true;
    }

    // pull some more records from the priority queue and constructs an extent
    return createNextExtent();
}

template <typename FieldType, typename FieldComparator>
void MemorySortModule<FieldType, FieldComparator>::
reset() {
    sortedExtents.clear();
    sortedExtentQueue.clear();
    initialized = false;
}

template <typename FieldType, typename FieldComparator>
void MemorySortModule<FieldType, FieldComparator>::
retrieveExtents() {
    Extent *extent = NULL;
    while ((extent = upstreamModule.getExtent()) != NULL) {
        boost::shared_ptr<SortedExtent> sortedExtent(new SortedExtent);
        sortedExtent->extent.reset(extent);
        sortedExtents.push_back(sortedExtent);

        INVARIANT(&extent->getType() == &sortedExtents[0]->extent->getType(),
                  "all extents must be of the same type");
    }
}

template <typename FieldType, typename FieldComparator>
void MemorySortModule<FieldType, FieldComparator>::
sortExtents() {
    // the next two calls initialize the extent series types so we can use the
    // relocate method later instead of setExtent and setCurPos
    sortedExtentComparator.setExtent(sortedExtents[0]->extent.get());


    gettimeofday(&startSortTime, NULL);

    LintelLogDebug("memorysortmodule",
            boost::format("Starting sort at time %d.%d") %
            startSortTime.tv_sec % startSortTime.tv_usec);

    BOOST_FOREACH(boost::shared_ptr<SortedExtent> &sortedExtent, sortedExtents) {
        sortExtent(*sortedExtent);
        sortedExtent->resetIterator();
        if (sortedExtent->positions.size() > 0) {
            DEBUG_SINVARIANT(sortedExtent->iterator != sortedExtent->positions.end());
            sortedExtentQueue.push(sortedExtent.get());
        }
    }

    LintelLogDebug("memorysortmodule",
            boost::format("Added %d sorted extents to the priority queue") %
            sortedExtentQueue.size());

    gettimeofday(&endSortTime, NULL);

    LintelLogDebug("memorysortmodule",
            boost::format("Starting merge at time %d.%d") %
            endSortTime.tv_sec % endSortTime.tv_usec);
}

template <typename FieldType, typename FieldComparator>
void MemorySortModule<FieldType, FieldComparator>::
sortExtent(SortedExtent &sortedExtent) {
    // start by initializing the positions array so that it holds the pointer to each fixed record
    ExtentSeries series(sortedExtent.extent.get());
    sortedExtent.positions.reserve(sortedExtent.extent->getRecordCount());
    for (; series.more(); series.next()) {
        sortedExtent.positions.push_back(series.getCurPos());
    }

    positionComparator.setExtent(sortedExtent.extent.get());

    // sort the positions using our custom comparator and STL's sort (the role of the custom
    // comparator is to translate a comparison of void*-based positions to a comparison of fields)
    std::sort(sortedExtent.positions.begin(), sortedExtent.positions.end(), positionComparator);
}

template <typename FieldType, typename FieldComparator>
Extent* MemorySortModule<FieldType, FieldComparator>::
createNextExtent() {
    if (sortedExtentQueue.empty()) {
        gettimeofday(&endMergeTime, NULL);

        LintelLogDebug("memorysortmodule",
                boost::format("Finished merge/copy at time %d.%d") %
                endMergeTime.tv_sec % endMergeTime.tv_usec);
        LintelLogDebug("memorysortmodule",
                boost::format("The overall memory sort took %lf seconds") %
                ((endMergeTime.tv_sec - startSortTime.tv_sec) +
                    (double)(endMergeTime.tv_usec - startSortTime.tv_usec) / 1000000));
        return NULL;
    }

    SortedExtent *sortedExtent = sortedExtentQueue.top();
    Extent *destinationExtent = new Extent(sortedExtent->extent->getType());

    ExtentSeries destinationSeries(destinationExtent);
    ExtentSeries sourceSeries(destinationExtent); // initialize so we can use relocate later
    ExtentRecordCopy recordCopier(sourceSeries, destinationSeries);

    size_t recordCount = 0;

    do {
        sourceSeries.relocate(sortedExtent->extent.get(), *sortedExtent->iterator);
        destinationSeries.newRecord();
        recordCopier.copyRecord();

        ++recordCount;
        ++(sortedExtent->iterator);

        if (sortedExtent->iterator == sortedExtent->positions.end()) {
            // we have consumed all of the records in this sorted extent
            --(sortedExtent->iterator); // we are not allowed to change the element before popping
            sortedExtentQueue.pop();

            if (sortedExtentQueue.empty()) {
                break; // there are no more records left in any sorted extent
            }
        } else {
            // there are more records left in this extent
            sortedExtentQueue.replaceTop(sortedExtent); // reinsert the sorted extent
        }

        sortedExtent = sortedExtentQueue.top();

        // keep going as long as we haven't reached the maxmium extent size (user-specified)
    } while (extentSizeLimit == 0 || destinationExtent->size() < extentSizeLimit);

    LintelLogDebug("memorysortmodule",
            boost::format("Added %d records to a new extent") %
            recordCount);

    return destinationExtent;
}

template <typename FieldType, typename FieldComparator>
void MemorySortModule<FieldType, FieldComparator>::
SortedExtent::resetIterator() {
    iterator = positions.begin();
}

template <typename FieldType, typename FieldComparator>
MemorySortModule<FieldType, FieldComparator>::
PositionComparator::PositionComparator(ComparatorData &data)
    : AbstractComparator(data) {
}

template <typename FieldType, typename FieldComparator>
bool MemorySortModule<FieldType, FieldComparator>::
PositionComparator::operator()(const void *positionLhs, const void *positionRhs) {
    this->data.seriesLhs.setCurPos(positionLhs);
    this->data.seriesRhs.setCurPos(positionRhs);
    return this->data.fieldComparator(this->data.fieldLhs, this->data.fieldRhs);
}

template <typename FieldType, typename FieldComparator>
MemorySortModule<FieldType, FieldComparator>::
SortedExtentComparator::SortedExtentComparator(ComparatorData &data)
    : AbstractComparator(data) {
}

template <typename FieldType, typename FieldComparator>
bool MemorySortModule<FieldType, FieldComparator>::
SortedExtentComparator::operator()(SortedExtent *sortedExtentLhs, SortedExtent *sortedExtentRhs) {
    // sortedExtentLhs->iterator and sortedExtentRhs->iterator are valid entries
    DEBUG_SINVARIANT(sortedExtentLhs->iterator != sortedExtentLhs->positions.end());
    DEBUG_SINVARIANT(sortedExtentRhs->iterator != sortedExtentRhs->positions.end());

    this->data.seriesLhs.relocate(sortedExtentLhs->extent.get(), *sortedExtentLhs->iterator);
    this->data.seriesRhs.relocate(sortedExtentRhs->extent.get(), *sortedExtentRhs->iterator);

    // swap fieldRhs and fieldLhs because compareSortedExtents == "less important" and
    // fieldComparator == "less than"
    return this->data.fieldComparator(this->data.fieldRhs, this->data.fieldLhs);
}

template <typename FieldType, typename FieldComparator>
MemorySortModule<FieldType, FieldComparator>::
AbstractComparator::AbstractComparator(ComparatorData &data)
    : data(data) {
}

template <typename FieldType, typename FieldComparator>
void MemorySortModule<FieldType, FieldComparator>::
AbstractComparator::setExtent(Extent *extent) {
    data.seriesLhs.setExtent(extent);
    data.seriesRhs.setExtent(extent);
}

template <typename FieldType, typename FieldComparator>
MemorySortModule<FieldType, FieldComparator>::
ComparatorData::ComparatorData(const std::string &fieldName,
                               FieldComparator &fieldComparator)
    : fieldComparator(fieldComparator),
      fieldLhs(seriesLhs, fieldName), fieldRhs(seriesRhs, fieldName) {
}

#endif
