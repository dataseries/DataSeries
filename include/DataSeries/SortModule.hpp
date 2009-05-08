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

// TODO-tomer: Document that this will do in memory or out-of-core
// sort. Consider writing intermediate file to dataseries or not? I.e., is
// there a simpler file format?

/** A class that sorts input of any size. It uses an external (ie, two-phase) sort when there
    is too much data to do it in memory (the amount of available memory is an input parameter). */
template <typename FieldType, typename FieldComparator>
class SortModule : public DataSeriesModule {
public:
    /** Constructs a new @c SortModule that will sort all the records based on the field
        named @param fieldName\. A sorting functor must also be provided.
        \param upstreamModule  The upstream @c DataSeriesModule from which this module requests
                               extents.
        \param fieldName       The name of the field on which the records will be sorted.
        \param fieldComparator The comparison (less-than) function for comparing two fields.
        \param extentSizeLimit The maximum size of the extents returned by getExtent. A value
                               of 0 indicates that a single extent should be returned.
        \param memoryLimit     The maximum amount of memory to use.
        \param tempFilePrefix  In case an external (two-phase) sort is required, @c SortModule will
                               create temporary DataSeries files. The files will be named by appending
                               an incrementing integer to the specified @param tempFilePrefix\. */
    SortModule(DataSeriesModule &upstreamModule,
               const std::string &fieldName,
               const FieldComparator &fieldComparator,
               size_t extentSizeLimit = 1 * 1000000, // 1 MB
               size_t memoryLimit = 2000 * 1000000, // 2 GB
               // TODO-tomer: ask Eric if tempFilePrefix is "good enough" for
               // merging. Other options include environment variables, program options,
               // use of default values, and methods that create a unique file etc.
               const std::string &tempFilePrefix = "/tmp/externalsort");

    virtual ~SortModule();

    /** Returns the next @c Extent according to the sorted order. Note that the first call to
        getExtent will be significantly slower than the rest, because it triggers the sorting process. */
    virtual Extent *getExtent();

private:
    // Wrap deque to offer getExtent interface for use in memory sort module.
    class FeederModule : public DataSeriesModule {
    public:
        Extent *getExtent();
        void addExtent(Extent *extent);
        std::deque<Extent*> extents;
    };

    class SortedMergeFile {
    public:
        SortedMergeFile(const std::string &file, const std::string &extentType);
        void next();
        std::string fileName; // TODO-tomer: fileName.
        TypeIndexModule inputModule;
        boost::shared_ptr<Extent> extent; // the extent that we're currently reading from
        const void *position; // where are we in the current extent?
    };

    typedef boost::function <bool (SortedMergeFile*, SortedMergeFile*)> SortedMergeFileComparator;

    bool retrieveExtents(); // fills up the feeder and returns false if we're out of extents
    void createSortedFiles();
    void prepareSortedMergeFiles();
    Extent *createNextExtent();
    bool compareSortedMergeFiles(SortedMergeFile *sortedMergeFileLhs,
                                 SortedMergeFile *sortedMergeFileRhs);

    bool initialized;
    bool external; // automatically set to true when upstream module provides more data than
                   // we can store in memory

    DataSeriesModule &upstreamModule;
    std::string fieldName;
    FieldComparator fieldComparator;
    size_t extentSizeLimit;
    size_t memoryLimit;
    std::string tempFilePrefix;

    size_t bufferLimit;

    FeederModule feederModule;
    MemorySortModule<FieldType, FieldComparator> memorySortModule;

    std::vector<boost::shared_ptr<SortedMergeFile> > sortedMergeFiles;
    PriorityQueue<SortedMergeFile*, SortedMergeFileComparator> sortedMergeFileQueue;

    ExtentSeries seriesLhs;
    ExtentSeries seriesRhs;
    FieldType fieldLhs;
    FieldType fieldRhs;
};

template <typename FieldType, typename FieldComparator>
SortModule<FieldType, FieldComparator>::
SortModule(DataSeriesModule &upstreamModule,
           const std::string &fieldName,
           const FieldComparator &fieldComparator,
           size_t extentSizeLimit,
           size_t memoryLimit,
           const std::string &tempFilePrefix)
    : initialized(false), external(false),
      upstreamModule(upstreamModule), fieldName(fieldName), fieldComparator(fieldComparator),
      extentSizeLimit(extentSizeLimit), memoryLimit(memoryLimit), tempFilePrefix(tempFilePrefix),
      bufferLimit(memoryLimit * 4 / 5), // this module has some overhead (we just assume 20%)
      memorySortModule(feederModule, fieldName, fieldComparator, extentSizeLimit),
      sortedMergeFileQueue(boost::bind(&SortModule::compareSortedMergeFiles, this, _1, _2)),
      fieldLhs(seriesLhs, fieldName), fieldRhs(seriesRhs, fieldName) {
}

template <typename FieldType, typename FieldComparator>
SortModule<FieldType, FieldComparator>::
~SortModule() {
}

template <typename FieldType, typename FieldComparator>
Extent *SortModule<FieldType, FieldComparator>::
getExtent() {
    if (!initialized) {
        external = retrieveExtents();
        if (external) {
            createSortedFiles();
            prepareSortedMergeFiles();
        }
        initialized = true;
    }

    // for external sort we need to merge from the files; for memory sort we just return an extent
    return external ? createNextExtent() : memorySortModule.getExtent();
}

template <typename FieldType, typename FieldComparator>
bool SortModule<FieldType, FieldComparator>::
retrieveExtents() {
    size_t totalSize = 0;
    Extent *extent = upstreamModule.getExtent();
    if (extent == NULL) {
        return false;
    }

    size_t firstSize = extent->size();
    totalSize = firstSize;
    feederModule.addExtent(extent);

    while (totalSize + firstSize <= bufferLimit) {
        // we have space to read another extent (assuming they are all the same size)
        extent = upstreamModule.getExtent();
        if (extent == NULL) {
            return false;
        }
        totalSize += extent->size();
        feederModule.addExtent(extent);
    }

    LintelLogDebug("sortmodule", boost::format("Filled up the buffer with %d bytes") % totalSize);

    return true; // and do not delete extent (we're passing it as-is to memorySortModule)
}

template <typename FieldType, typename FieldComparator>
void SortModule<FieldType, FieldComparator>::
createSortedFiles() {
    int i = 0;
    bool lastFile = false; // we need more than one file (although special case at end of function)

    // one iteration for each batch/file (exits when we are out of data)
    while (true) {
        // read the first extent
        Extent *extent = memorySortModule.getExtent();
        INVARIANT(extent != NULL, "why are we making an empty file?");

        // create a new input file entry
        boost::shared_ptr<SortedMergeFile> sortedMergeFile(new SortedMergeFile(
                tempFilePrefix + (boost::format("%d") % i++).str(),
                extent->getType().getName()));
        sortedMergeFiles.push_back(sortedMergeFile);

        // create the sink
        boost::shared_ptr<DataSeriesSink> sink(new DataSeriesSink(
                sortedMergeFile->fileName,
                Extent::compress_none,
                0));

        LintelLogDebug("sortmodule",
                boost::format("Created a temporary file for the external sort: %s") %
                sortedMergeFile->fileName);

        // write the type library
        ExtentTypeLibrary library;
        library.registerType(extent->getType());
        sink->writeExtentLibrary(library);

        do {
            sink->writeExtent(*extent, NULL);
            delete extent;
            extent = memorySortModule.getExtent();
        } while (extent != NULL);

        // close the sink
        sink->close();

        if (lastFile) { // we know there's no more data
            break;
        }

        // re-fill the feeder
        lastFile = !retrieveExtents();
        if (feederModule.extents.size() == 0) { // we thought there might be more data but there isn't
            SINVARIANT(lastFile);
            break; // having zero extents in the "last file" is a special case - no need for that file
        }

        memorySortModule.reset();
    }
}

template <typename FieldType, typename FieldComparator>
void SortModule<FieldType, FieldComparator>::
prepareSortedMergeFiles() {
    // create the input modules and read/store the first extent from each one
    BOOST_FOREACH(boost::shared_ptr<SortedMergeFile> &sortedMergeFile, sortedMergeFiles) {
        sortedMergeFile->extent.reset(sortedMergeFile->inputModule.getExtent());
        INVARIANT(sortedMergeFile->extent.get() != NULL, "why do we have an empty file?");

        ExtentSeries series(sortedMergeFile->extent.get());
        sortedMergeFile->position = series.getCurPos();

        sortedMergeFileQueue.push(sortedMergeFile.get()); // add the file to our priority queue
    }
}

template <typename FieldType, typename FieldComparator>
Extent *SortModule<FieldType, FieldComparator>::
createNextExtent() {
    if (sortedMergeFileQueue.empty()) return NULL;

    SortedMergeFile *sortedMergeFile = sortedMergeFileQueue.top();
    INVARIANT(sortedMergeFile->extent != NULL, "each file must have at least one extent"
            " and we are never returning finished input files to the queue");

    Extent *destinationExtent = new Extent(sortedMergeFile->extent->getType());

    ExtentSeries destinationSeries(destinationExtent);
    ExtentSeries sourceSeries(destinationExtent); // just so we can use relocate later
    ExtentRecordCopy recordCopier(sourceSeries, destinationSeries);

    size_t recordCount = 0;

    // each iteration of this loop adds a single record to the destination extent
    do {
        sortedMergeFile = sortedMergeFileQueue.top();

        sourceSeries.relocate(sortedMergeFile->extent.get(), sortedMergeFile->position);
        destinationSeries.newRecord();
        recordCopier.copyRecord();
        ++recordCount;

        sourceSeries.next();

        if (sourceSeries.more()) { // more records available in the current extent
            sortedMergeFile->position = sourceSeries.getCurPos();
            sortedMergeFileQueue.replaceTop(sortedMergeFile);
        } else {
            Extent *nextExtent = NULL;

            do { // skip over any empty extents
                nextExtent = sortedMergeFile->inputModule.getExtent();
                if (nextExtent == NULL) {
                    break; // this input file is done
                }
                sourceSeries.setExtent(nextExtent);
            } while (!sourceSeries.more());

            if (nextExtent == NULL) { // no more records so pop it out
                sortedMergeFileQueue.pop();
                sortedMergeFile->extent.reset(); // be nice and clean up!
                sortedMergeFile->position = NULL;
            } else { // more records available in a new extent
                sortedMergeFile->extent.reset(nextExtent);
                sortedMergeFile->position = sourceSeries.getCurPos();
                sortedMergeFileQueue.replaceTop(sortedMergeFile);
            }
        }
    } while (!sortedMergeFileQueue.empty() &&
             (extentSizeLimit == 0 || destinationExtent->size() < extentSizeLimit));

    return destinationExtent;
}

template <typename FieldType, typename FieldComparator>
bool SortModule<FieldType, FieldComparator>::
compareSortedMergeFiles(SortedMergeFile *sortedMergeFileLhs,
                        SortedMergeFile *sortedMergeFileRhs) {
    seriesLhs.setExtent(sortedMergeFileLhs->extent.get());
    seriesLhs.setCurPos(sortedMergeFileLhs->position);

    seriesRhs.setExtent(sortedMergeFileRhs->extent.get());
    seriesRhs.setCurPos(sortedMergeFileRhs->position);

    // swap fieldRhs and fieldLhs because compareSortedExtents == "less important" and
    // fieldComparator == "less than"
    return fieldComparator(fieldRhs, fieldLhs);
}

template <typename FieldType, typename FieldComparator>
void SortModule<FieldType, FieldComparator>::
FeederModule::addExtent(Extent *extent) {
    extents.push_back(extent);
}

template <typename FieldType, typename FieldComparator>
Extent* SortModule<FieldType, FieldComparator>::
FeederModule::getExtent() {
    if (extents.empty()) return NULL;
    Extent *extent = extents.front();
    extents.pop_front();
    return extent;
}

template <typename FieldType, typename FieldComparator>
SortModule<FieldType, FieldComparator>::
SortedMergeFile::SortedMergeFile(const std::string &fileName, const std::string &extentType)
    : fileName(fileName), inputModule(extentType) {
    inputModule.addSource(fileName);
}

template <typename FieldType, typename FieldComparator>
void SortModule<FieldType, FieldComparator>::
SortedMergeFile::next() {

}

#endif
