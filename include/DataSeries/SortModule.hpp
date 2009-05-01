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

#include <boost/shared_ptr.hpp>
#include <boost/format.hpp>
#include <boost/foreach.hpp>

#include <Lintel/PriorityQueue.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/MemorySortModule.hpp>

class SortModule : public DataSeriesModule {
public:
    typedef boost::function<bool (const Variable32Field&, const Variable32Field&)> FieldComparator;

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
    typedef std::vector<boost::shared_ptr<Extent> > ExtentVector;

    class ThrottlerModule : public DataSeriesModule {
    public:
        ThrottlerModule(DataSeriesModule &upstreamModule, size_t memoryLimit);
        void reset();
        bool full();
        Extent *getExtent();

    private:
        size_t firstSize;
        size_t totalSize;

        DataSeriesModule &upstreamModule;
        size_t memoryLimit;
    };

    class SortedInputFile {
    public:
        SortedInputFile(const std::string &file);

        TypeIndexModule inputModule;
        boost::shared_ptr<Extent> extent; // the extent that we're currently reading from
        const void *position; // where are we in the current extent?
    };

    typedef boost::function
            <bool (boost::shared_ptr<SortedInputFile>, boost::shared_ptr<SortedInputFile>)>
            SortedInputFileComparator;

    void createSortedFiles(Extent *extent); // extent is the first extent (we already popped it)
    Extent *createNextExtent();
    void prepareSortedInputFiles();
    bool compareSortedInputFiles(boost::shared_ptr<SortedInputFile> &sortedInputFile0,
                                 boost::shared_ptr<SortedInputFile> &sortedInputFile1);

    bool initialized;
    bool external; // is this an external/two-phase sort?
    size_t lastTempFileSuffix;

    DataSeriesModule &upstreamModule;
    std::string fieldName;
    FieldComparator fieldComparator;
    size_t extentSizeLimit;
    size_t memoryLimit;
    std::string tempFilePrefix;

    size_t bufferLimit;

    ExtentVector extents;
    ThrottlerModule throttlerModule;
    MemorySortModule<Variable32Field> memorySortModule;

    PriorityQueue<boost::shared_ptr<SortedInputFile>, SortedInputFileComparator> sortedInputFileQueue;
    ExtentSeries series0;
    ExtentSeries series1;
    Variable32Field field0;
    Variable32Field field1;
};

SortModule::SortModule(DataSeriesModule &upstreamModule,
                       const std::string &fieldName,
                       const FieldComparator &fieldComparator,
                       size_t extentSizeLimit,
                       size_t memoryLimit,
                       const std::string &tempFilePrefix)
    : initialized(false), external(false), lastTempFileSuffix(),
      upstreamModule(upstreamModule), fieldName(fieldName), fieldComparator(fieldComparator),
      extentSizeLimit(extentSizeLimit), memoryLimit(memoryLimit), tempFilePrefix(tempFilePrefix),
      bufferLimit(memoryLimit / 2),
      throttlerModule(upstreamModule, bufferLimit),
      memorySortModule(throttlerModule, fieldName, fieldComparator, extentSizeLimit),
      sortedInputFileQueue(boost::bind(&SortModule::compareSortedInputFiles, this, _1, _2)),
      field0(series0, fieldName), field1(series1, fieldName) {
}

SortModule::~SortModule() {
}

Extent *SortModule::getExtent() {
    if (!initialized) {
        Extent *firstExtent = memorySortModule.getExtent();
        external = throttlerModule.full();
        initialized = true;
        if (external) {
            createSortedFiles(firstExtent); // we already took off the first extent so we have to pass it
            prepareSortedInputFiles();
        } else {
            return firstExtent;
        }
    }

    // this is just an in-memory sort
    if (!external) return memorySortModule.getExtent();

    // this is an external sort so we have to create the extent by merging from the files
    return createNextExtent();
}

void SortModule::createSortedFiles(Extent *extent) {
    lastTempFileSuffix = 0;

    INVARIANT(extent != NULL, "why are we trying to do an external sort if the first extent is NULL?");
    ExtentTypeLibrary library;
    library.registerType(extent->getType());

    boost::shared_ptr<DataSeriesSink> sink(new DataSeriesSink(
            tempFilePrefix + (boost::format("%d") % lastTempFileSuffix).str(),
            Extent::compress_none,
            0));
    sink->writeExtentLibrary(library);

    while (true) {
        sink->writeExtent(*extent, NULL);

        // get the next extent in this batch?
        extent = memorySortModule.getExtent();

        // no more extents in this batch? (we'll either start a new batch or finish)
        if (extent == NULL) {
            memorySortModule.reset();
            sink->close(); // not really needed because of sink's destructor

            // get the first extent in the new batch
            extent = memorySortModule.getExtent();

            // if it's still NULL (after the reset) then no more data is available and we're done
            if (extent == NULL) break;

            ++lastTempFileSuffix;

            sink.reset(new DataSeriesSink(
                    tempFilePrefix + (boost::format("%d") % lastTempFileSuffix).str(),
                    Extent::compress_none,
                    0));
            sink->writeExtentLibrary(library);
        }
    }
}

Extent *SortModule::createNextExtent() {
    if (sortedInputFileQueue.empty()) return NULL;

    boost::shared_ptr<SortedInputFile> sortedInputFile = sortedInputFileQueue.top();
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

        // skip to the next record in this input file, and push the file back into the priority
        // queue if we can find one
        while (!sourceSeries.more()) {
            Extent *nextExtent = sortedInputFile->inputModule.getExtent();
            sortedInputFile->extent.reset(nextExtent);

            if (nextExtent == NULL) { // this input file is done
                sortedInputFile->position = NULL; // not really needed
                break;
            }

            // reset the position
            sourceSeries.setExtent(nextExtent);
            sortedInputFile->position = sourceSeries.getCurPos();
        }

        if (sortedInputFile->extent.get() == NULL) {
            sortedInputFileQueue.pop();
        } else {
            sortedInputFileQueue.replaceTop(sortedInputFile);
        }

        // have we crossed the maximum extent size
        if (extentSizeLimit != 0 && destinationExtent->size() >= extentSizeLimit) break;

        // check if there are any records left
        if (sortedInputFileQueue.empty()) break;
    }

    LintelLogDebug("sortmodule", boost::format("Added %d records to the extent") % recordCount);

    return destinationExtent;
}

void SortModule::prepareSortedInputFiles() {
    // create the input modules and read/store the first extent from each one
    for (size_t tempFileSuffix = 0; tempFileSuffix < lastTempFileSuffix; ++tempFileSuffix) {
        std::string file(tempFilePrefix + (boost::format("%d") % tempFileSuffix).str());
        boost::shared_ptr<SortedInputFile> sortedInputFile(new SortedInputFile(file));
        sortedInputFile->extent.reset(sortedInputFile->inputModule.getExtent());

        ExtentSeries series(sortedInputFile->extent.get());
        sortedInputFile->position = series.getCurPos();

        sortedInputFileQueue.push(sortedInputFile); // add the file to our priority queue
    }
}

bool SortModule::compareSortedInputFiles(boost::shared_ptr<SortedInputFile> &sortedInputFile0,
                                         boost::shared_ptr<SortedInputFile> &sortedInputFile1) {
    series0.setExtent(sortedInputFile0->extent.get());
    series0.setCurPos(sortedInputFile0->position);

    series1.setExtent(sortedInputFile1->extent.get());
    series1.setCurPos(sortedInputFile1->position);

    return !fieldComparator(field0, field1);
}

SortModule::ThrottlerModule::ThrottlerModule(DataSeriesModule &upstreamModule, size_t memoryLimit)
    : firstSize(0), totalSize(0),
      upstreamModule(upstreamModule), memoryLimit(memoryLimit) {
}

void SortModule::ThrottlerModule::reset() {
    totalSize = 0;
}

bool SortModule::ThrottlerModule::full() {
    return totalSize > 0 && totalSize + firstSize > memoryLimit ;
}

Extent* SortModule::ThrottlerModule::getExtent() {
    if (firstSize == 0) {
        Extent *extent = upstreamModule.getExtent();
        totalSize = firstSize = extent->size();
        return extent;
    }

    // if we returned at least one extent in this batch, and the next extent will push us beyond
    // the memory limit, then don't fetch another extent
    if (full()) return NULL;

    Extent *extent = upstreamModule.getExtent();
    totalSize += extent->size();
    return extent;
}

SortModule::SortedInputFile::SortedInputFile(const std::string &file) {
    inputModule.addSource(file);
}

#endif
