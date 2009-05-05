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

#include <boost/function.hpp>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/format.hpp>

#include <Lintel/PriorityQueue.hpp>
#include <Lintel/LintelLog.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/Field.hpp>
#include <DataSeries/GeneralField.hpp>

template <typename FieldType>
class MemorySortModule : public DataSeriesModule {
public:
    // TODO-tomer: figure out if the following comment is correct.
    // FieldComparator effectively uses a function pointer. It may be a
    // performance bottleneck in the future. I.e., a fully templatized
    // implementation should be faster.
    typedef boost::function<bool (const FieldType&, const FieldType&)> FieldComparator;

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
    // TODO-tomer: add comment to document this private class
    class SortedExtent {
    public:
        boost::shared_ptr<Extent> extent;
        std::vector<const void*> positions; // the positions in sorted order
                                            // (positions.size() == # of records in this extent)
        std::vector<const void*>::iterator iterator;
    };

    /** An internal class for comparing fields based on pointers to their records. */
    class PositionComparator {
    public:
        PositionComparator(const std::string &fieldName, const FieldComparator &fieldComparator);
        ~PositionComparator();
        void setExtent(Extent *extent);
        bool operator()(const void *position0, const void *position1);

    private:
        const FieldComparator &fieldComparator;
        // TODO-tomer: 0 = _lhs, 1 = _rhs? same with private members below.
        boost::shared_ptr<ExtentSeries> series0;
        boost::shared_ptr<ExtentSeries> series1;
        boost::shared_ptr<FieldType> field0;
        boost::shared_ptr<FieldType> field1;
    };

    typedef boost::function<bool (SortedExtent*, SortedExtent*)> SortedExtentComparator;

    void retrieveExtents();
    void sortExtents();
    void sortExtent(SortedExtent &sortedExtent);
    bool compareSortedExtents(SortedExtent *sortedExtent0, SortedExtent *sortedExtent1);
    Extent* createNextExtent();

    std::vector<boost::shared_ptr<SortedExtent> > sortedExtents;

    bool initialized; // starts as false, becomes true after first call to upstreamModule.getExtent
    DataSeriesModule &upstreamModule;
    std::string fieldName;
    FieldComparator fieldComparator;
    size_t extentSizeLimit;

    // some members to help with the merging
    PriorityQueue <SortedExtent*, SortedExtentComparator> sortedExtentQueue;
    ExtentSeries series0;
    ExtentSeries series1;
    FieldType field0;
    FieldType field1;
};


template <typename FieldType>
MemorySortModule<FieldType>::MemorySortModule(DataSeriesModule &upstreamModule,
                                   const std::string &fieldName,
                                   const FieldComparator &fieldComparator,
                                   size_t extentSizeLimit)
    : initialized(false),
      upstreamModule(upstreamModule), fieldName(fieldName), fieldComparator(fieldComparator),
      extentSizeLimit(extentSizeLimit),
      sortedExtentQueue(boost::bind(&MemorySortModule::compareSortedExtents, this, _1, _2)),
      field0(series0, fieldName), field1(series1, fieldName) {
}

template <typename FieldType>
MemorySortModule<FieldType>::~MemorySortModule() {
}

template <typename FieldType>
Extent *MemorySortModule<FieldType>::getExtent() {
    // retrieve the extents and sort them if this is the first call to getExtent
    if (!initialized) {
        retrieveExtents();
        sortExtents();
        initialized = true;
    }

    // pull some more records from the priority queue and constructs an extent
    return createNextExtent();
}

template <typename FieldType>
void MemorySortModule<FieldType>::reset() {
    sortedExtents.clear();
    sortedExtentQueue.clear();
    initialized = false;
}

template <typename FieldType>
void MemorySortModule<FieldType>::retrieveExtents() {
    Extent *extent = NULL;
    while ((extent = upstreamModule.getExtent()) != NULL) {
        boost::shared_ptr<SortedExtent> sortedExtent(new SortedExtent);
        sortedExtent->extent.reset(extent);
        sortedExtents.push_back(sortedExtent);
    }
}

template <typename FieldType>
void MemorySortModule<FieldType>::sortExtents() {
    BOOST_FOREACH(boost::shared_ptr<SortedExtent> &sortedExtent, sortedExtents) {
        sortExtent(*sortedExtent);
        // TODO-tomer: this next line seems like it ought to either be part of
        // construction or a side effect of sortExtent. OR, it should be a
        // method on sortedExtent.
        sortedExtent->iterator = sortedExtent->positions.begin();
        sortedExtentQueue.push(sortedExtent.get());
    }

    LintelLogDebug("memorysortmodule", boost::format("Added %d sorted extents to the priority queue") % sortedExtentQueue.size());
}

template <typename FieldType>
void MemorySortModule<FieldType>::sortExtent(SortedExtent &sortedExtent) {
    // start by initializing the positions array so that it holds the pointer to each fixed record
    ExtentSeries series(sortedExtent.extent.get());
    sortedExtent.positions.reserve(sortedExtent.extent->getRecordCount());
    for (; series.more(); series.next()) {
        sortedExtent.positions.push_back(series.getCurPos());
    }

    // sort the positions using our custom comparator and STL's sort (the role of the custom
    // comparator is to translate a comparison of void*-based positions to a comparison of fields)
    PositionComparator comparator(fieldName, fieldComparator);
    comparator.setExtent(sortedExtent.extent.get());
    std::sort(sortedExtent.positions.begin(), sortedExtent.positions.end(), comparator);
}

template <typename FieldType>
bool MemorySortModule<FieldType>::compareSortedExtents(SortedExtent *sortedExtent0, SortedExtent *sortedExtent1) {
    // if there are no more records in one of the sorted extents, then the other is more important
    if (sortedExtent0->iterator == sortedExtent0->positions.end()) return true;
    if (sortedExtent1->iterator == sortedExtent1->positions.end()) return false;

    // TODO-tomer: use DEBUG_INVARIANT rather than the below comment
    // (sortedExtent0->iterator != end()). 
    // sortedExtent0->iterator and sortedExtent1->iterator are valid entries

    series0.setExtent(sortedExtent0->extent.get());
    series0.setCurPos(*sortedExtent0->iterator);

    series1.setExtent(sortedExtent1->extent.get());
    series1.setCurPos(*sortedExtent1->iterator);

    // sense of fieldComparator is inverted compared to normal: so, field1 then field0.
    return fieldComparator(field1, field0);
}

template <typename FieldType>
Extent* MemorySortModule<FieldType>::createNextExtent() {
    if (sortedExtentQueue.empty()) {
        return NULL;
    }

    SortedExtent *sortedExtent = sortedExtentQueue.top();
    if (sortedExtent->iterator == sortedExtent->positions.end()) {
        return NULL;
    }

    Extent *destinationExtent = new Extent(sortedExtent->extent->getType());

    ExtentSeries destinationSeries(destinationExtent);
    ExtentSeries sourceSeries;
    ExtentRecordCopy recordCopier(sourceSeries, destinationSeries);

    size_t recordCount = 0;

    // TODO-tomer: retry a do while(!extentSizeLimit && records left) loop.
    while (true) {
        INVARIANT(&sortedExtent->extent->getType() == &destinationExtent->getType(),
                "all extents must be of the same type");
        sourceSeries.setExtent(sortedExtent->extent.get());
        //It may be possible to extend ExtentRecordCopy so that it "knows" to
        //use sortedExtent->iterator rather than explicitly invoking
        //setCurPos. But, it may also not be possible... This is a performance
        //improvement possibility.
        sourceSeries.setCurPos(*sortedExtent->iterator);
        destinationSeries.newRecord();
        recordCopier.copyRecord();
        ++recordCount;

        ++(sortedExtent->iterator);
        sortedExtentQueue.replaceTop(sortedExtent); // reinsert the sorted extent

        // have we crossed the maximum extent size
        if (extentSizeLimit != 0 && destinationExtent->size() >= extentSizeLimit) {
            break;
        }

        // check if there are any records left
        sortedExtent = sortedExtentQueue.top();
        if (sortedExtent->iterator == sortedExtent->positions.end()) {
            break;
        }
    }

    return destinationExtent;
}

template <typename FieldType>
MemorySortModule<FieldType>::PositionComparator::PositionComparator(const std::string &fieldName,
                                                         const FieldComparator &fieldComparator)
    : fieldComparator(fieldComparator),
      series0(new ExtentSeries()), series1(new ExtentSeries()),
      field0(new FieldType(*series0, fieldName)), field1(new FieldType(*series1, fieldName)) {
}

template <typename FieldType>
MemorySortModule<FieldType>::PositionComparator::~PositionComparator() {
}

template <typename FieldType>
void MemorySortModule<FieldType>::PositionComparator::setExtent(Extent *extent) {
    series0->setExtent(extent);
    series1->setExtent(extent);
}

template <typename FieldType>
bool MemorySortModule<FieldType>::PositionComparator::operator()(const void *position0, const void *position1) {
    series0->setCurPos(position0);
    series1->setCurPos(position1);
    return fieldComparator(*field0, *field1);
}

#endif
