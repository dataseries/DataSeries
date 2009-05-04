// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A module which sorts all the extents from the upstream module. If the sorting cannot be completed
    in memory, an external sort is used.
*/

#ifndef __DATASERIES_SORTMODULE_H
#define __DATASERIES_SORTMODULE_H

#include <string>
#include <vector>
#include <deque>

#include <boost/shared_ptr.hpp>
#include <boost/format.hpp>
#include <boost/foreach.hpp>

#include <Lintel/PriorityQueue.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/MemorySortModule.hpp>

template <typename FieldType>
class SortModule : public DataSeriesModule {
public:
    typedef boost::function<bool (const FieldType&, const FieldType&)> FieldComparator;

    /** Constructs a new @c SortModule that will sort all the records based on the field
        named @param fieldName\. A sorting functor must also be provided.
        \param upstreamModule  The upstream @c DataSeriesModule from which this module requests
                               extents.
        \param fieldName       The name of the field on which the records will be sorted.
        \param fieldComparator The comparison (less-than) function for comparing two fields.
        \param extentSizeLimit The maximum size of the extents returned by getExtent. A value
                               of 0 indicates that a single extent should be returned.
        \param memoryLimit     The maximum amount of memory to use. Since SortModule has a 100%
                               memory overhead, about 1/2 of @param memoryLimit will be used for
                               data from the upstream module.
        \param tempFilePrefix  In case an external (two-phase) sort is required, @c SortModule will
                               create temporary DataSeries files. The files will be named by appending
                               an incrementing integer to the specified @param tempFilePrefix\. */
    SortModule(DataSeriesModule &upstreamModule,
               const std::string &fieldName,
               const FieldComparator &fieldComparator,
               size_t extentSizeLimit,
               size_t memoryLimit,
               const std::string &tempFilePrefix);

    virtual ~SortModule();

    /** Returns the next @c Extent according to the sorted order. Note that the first call to
        getExtent will be significantly slower than the rest, because it triggers the sorting process. */
    virtual Extent *getExtent();

private:
    class FeederModule : public DataSeriesModule {
    public:
        Extent *getExtent();
        void addExtent(Extent *extent);
        std::deque<Extent*> extents;
    };

    class SortedInputFile {
    public:
        SortedInputFile(const std::string &file, const std::string &extentType);

        std::string file;
        TypeIndexModule inputModule;
        boost::shared_ptr<Extent> extent; // the extent that we're currently reading from
        const void *position; // where are we in the current extent?
    };

    typedef boost::function
            <bool (SortedInputFile*, SortedInputFile*)>
            SortedInputFileComparator;

    bool retrieveExtents(); // fills up the feeder and returns false if we're out of extents
    void createSortedFiles();
    void prepareSortedInputFiles();
    Extent *createNextExtent();
    bool compareSortedInputFiles(SortedInputFile *sortedInputFile0,
                                 SortedInputFile *sortedInputFile1);

    bool initialized;
    bool external; // is this an external/two-phase sort?

    DataSeriesModule &upstreamModule;
    std::string fieldName;
    FieldComparator fieldComparator;
    size_t extentSizeLimit;
    size_t memoryLimit;
    std::string tempFilePrefix;

    size_t bufferLimit;

    FeederModule feederModule;
    MemorySortModule<FieldType> memorySortModule;

    std::vector<boost::shared_ptr<SortedInputFile> > sortedInputFiles;
    PriorityQueue<SortedInputFile*, SortedInputFileComparator> sortedInputFileQueue;

    ExtentSeries series0;
    ExtentSeries series1;
    FieldType field0;
    FieldType field1;
};

template <typename FieldType>
SortModule<FieldType>::SortModule(DataSeriesModule &upstreamModule,
                       const std::string &fieldName,
                       const FieldComparator &fieldComparator,
                       size_t extentSizeLimit,
                       size_t memoryLimit,
                       const std::string &tempFilePrefix)
    : initialized(false), external(false),
      upstreamModule(upstreamModule), fieldName(fieldName), fieldComparator(fieldComparator),
      extentSizeLimit(extentSizeLimit), memoryLimit(memoryLimit), tempFilePrefix(tempFilePrefix),
      bufferLimit(memoryLimit / 2),
      memorySortModule(feederModule, fieldName, fieldComparator, extentSizeLimit),
      sortedInputFileQueue(boost::bind(&SortModule::compareSortedInputFiles, this, _1, _2)),
      field0(series0, fieldName), field1(series1, fieldName) {
}

template <typename FieldType>
SortModule<FieldType>::~SortModule() {
}

template <typename FieldType>
Extent *SortModule<FieldType>::getExtent() {
    if (!initialized) {
        external = retrieveExtents();
        if (external) {
            createSortedFiles();
            prepareSortedInputFiles();
        }
        initialized = true;
    }

    // for external sort we need to merge from the files; for memory sort we just return an extent
    return external ? createNextExtent() : memorySortModule.getExtent();
}

template <typename FieldType>
bool SortModule<FieldType>::retrieveExtents() {
    size_t totalSize = 0;
    Extent *extent = upstreamModule.getExtent();
    if (extent == NULL) return false;

    size_t firstSize = extent->size();
    totalSize = firstSize;
    feederModule.addExtent(extent);

    while (totalSize + firstSize <= bufferLimit) {
        // we have space to read another extent (assuming they are all the same size)
        extent = upstreamModule.getExtent();
        if (extent == NULL) return false;
        totalSize += extent->size();
        feederModule.addExtent(extent);
    }

    LintelLogDebug("sortmodule", boost::format("Filled up the buffer with %d bytes") % totalSize);

    return true; // and do not delete extent (we're passing it as-is to memorySortModule)
}

template <typename FieldType>
void SortModule<FieldType>::createSortedFiles() {
    int i = 0;
    bool lastFile = false; // we need more than one file (although special case at end of function)

    while (true) {
        // read the first extent
        Extent *extent = memorySortModule.getExtent();
        INVARIANT(extent != NULL, "why are we making an empty file?");

        // create a new input file entry
        boost::shared_ptr<SortedInputFile> sortedInputFile(new SortedInputFile(
                tempFilePrefix + (boost::format("%d") % i++).str(),
                extent->getType().getName()));
        sortedInputFiles.push_back(sortedInputFile);

        // create the sink
        boost::shared_ptr<DataSeriesSink> sink(new DataSeriesSink(
                sortedInputFile->file,
                Extent::compress_none,
                0));

        LintelLogDebug("sortmodule",
                boost::format("Created a temporary file for the external sort: %s") %
                sortedInputFile->file);

        // write the type library
        ExtentTypeLibrary library;
        library.registerType(extent->getType());
        sink->writeExtentLibrary(library);

        // write the first extent
        sink->writeExtent(*extent, NULL);
        delete extent;

        // read and write remaining extents
        while ((extent = memorySortModule.getExtent()) != NULL) {
            sink->writeExtent(*extent, NULL);
            delete extent;
        }

        // close the sink
        sink->close();

        if (lastFile) break;

        // re-fill the feeder
        lastFile = !retrieveExtents();
        if (feederModule.extents.size() == 0) {
            SINVARIANT(lastFile);
            break; // having zero extents in the "last file" is a special case - no need for that file
        }

        memorySortModule.reset();
    }
}

template <typename FieldType>
void SortModule<FieldType>::prepareSortedInputFiles() {
    // create the input modules and read/store the first extent from each one
    BOOST_FOREACH(boost::shared_ptr<SortedInputFile> &sortedInputFile, sortedInputFiles) {
        sortedInputFile->extent.reset(sortedInputFile->inputModule.getExtent());
        INVARIANT(sortedInputFile->extent.get() != NULL, "why do we have an empty file?");

        ExtentSeries series(sortedInputFile->extent.get());
        sortedInputFile->position = series.getCurPos();

        sortedInputFileQueue.push(sortedInputFile.get()); // add the file to our priority queue
    }
}

template <typename FieldType>
Extent *SortModule<FieldType>::createNextExtent() {
    if (sortedInputFileQueue.empty()) return NULL;

    SortedInputFile *sortedInputFile = sortedInputFileQueue.top();
    INVARIANT(sortedInputFile->extent != NULL, "each file must have at least one extent"
            " and we are never returning finished input files to the queue");

    Extent *destinationExtent = new Extent(sortedInputFile->extent->getType());

    ExtentSeries destinationSeries(destinationExtent);
    ExtentSeries sourceSeries;
    ExtentRecordCopy recordCopier(sourceSeries, destinationSeries);

    size_t recordCount = 0;

    // each iteration of this loop adds a single record to the destination extent
    while (true) {
        sourceSeries.setExtent(sortedInputFile->extent.get());
        sourceSeries.setCurPos(sortedInputFile->position);
        destinationSeries.newRecord();
        recordCopier.copyRecord();
        ++recordCount;

        sourceSeries.next();

        if (!sourceSeries.more()) {
            Extent *nextExtent = NULL;

            do { // skip over any empty extents
                nextExtent = sortedInputFile->inputModule.getExtent();
                if (nextExtent == NULL) break; // this input file is done
                sourceSeries.setExtent(nextExtent);
            } while (!sourceSeries.more());

            if (nextExtent == NULL) { // no more records so pop it out
                sortedInputFileQueue.pop();
                sortedInputFile->extent.reset(); // be nice and clean up!
                sortedInputFile->position = NULL;
            } else { // more records available in a new extent
                sortedInputFile->extent.reset(nextExtent);
                sortedInputFile->position = sourceSeries.getCurPos();
                sortedInputFileQueue.replaceTop(sortedInputFile);
            }
        } else { // more records available in the current extent
            sortedInputFile->position = sourceSeries.getCurPos();
            sortedInputFileQueue.replaceTop(sortedInputFile);
        }

        // have we crossed the maximum extent size
        if (extentSizeLimit != 0 && destinationExtent->size() >= extentSizeLimit) break;

        // check if there are any records left
        if (sortedInputFileQueue.empty()) break;

        sortedInputFile = sortedInputFileQueue.top();
    }

    return destinationExtent;
}

template <typename FieldType>
bool SortModule<FieldType>::compareSortedInputFiles(SortedInputFile *sortedInputFile0,
                                         SortedInputFile *sortedInputFile1) {
    series0.setExtent(sortedInputFile0->extent.get());
    series0.setCurPos(sortedInputFile0->position);

    series1.setExtent(sortedInputFile1->extent.get());
    series1.setCurPos(sortedInputFile1->position);

    return !fieldComparator(field0, field1);
}

template <typename FieldType>
void SortModule<FieldType>::FeederModule::addExtent(Extent *extent) {
    extents.push_back(extent);
}

template <typename FieldType>
Extent* SortModule<FieldType>::FeederModule::getExtent() {
    if (extents.empty()) return NULL;
    Extent *extent = extents.front();
    extents.pop_front();
    return extent;
}

template <typename FieldType>
SortModule<FieldType>::SortedInputFile::SortedInputFile(const std::string &file, const std::string &extentType)
    : file(file), inputModule(extentType) {
    inputModule.addSource(file);
}

#endif
