// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A module that filters records based on a given matching function. This module utilizes
    all of the CPU cores on the machine.
*/

#ifndef __DATASERIES_PARALLELGREPMODULE_H
#define __DATASERIES_PARALLELGREPMODULE_H

#include <string>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/ParallelFilterModule.hpp>

template <typename FieldType, typename FieldMatcher>
class ParallelGrepModule : public ParallelFilterModule {
public:
    ParallelGrepModule(DataSeriesModule &upstreamModule,
                       const std::string &fieldName,
                       const FieldMatcher &fieldMatcher,
                       size_t extentSizeLimit = 1 * 1000000, // 1 MB
                       int threadCount = 0,
                       size_t downstreamQueueLimit = 0);

    virtual ~ParallelGrepModule() {}

protected:
    /** Read extents from the upstream module and product exactly one extent. Note: This function
        must be thread-safe!! */
    virtual Extent* createNextExtent(ThreadLocalStorage *tls);

    virtual ThreadLocalStorage* createTls();

private:
    std::string fieldName;
    FieldMatcher fieldMatcher;
    int lastThreadId;

    class Data : public ThreadLocalStorage {
    public:
        Data(const std::string &fieldName, int threadId)
            : threadId(threadId), field(sourceSeries, fieldName),
              recordCopier(sourceSeries, destinationSeries) {}

        int threadId;
        ExtentSeries sourceSeries;
        FieldType field;
        ExtentSeries destinationSeries;
        ExtentRecordCopy recordCopier;
    };
};

template <typename FieldType, typename FieldMatcher>
ParallelGrepModule<FieldType, FieldMatcher>::ParallelGrepModule(
        DataSeriesModule &upstreamModule,
        const std::string &fieldName,
        const FieldMatcher &fieldMatcher,
        size_t extentSizeLimit,
        int threadCount,
        size_t downstreamQueueLimit)
    : ParallelFilterModule(upstreamModule, extentSizeLimit, threadCount, downstreamQueueLimit),
      fieldName(fieldName), fieldMatcher(fieldMatcher), lastThreadId(0) {
}

template <typename FieldType, typename FieldMatcher>
ParallelFilterModule::ThreadLocalStorage* ParallelGrepModule<FieldType, FieldMatcher>::createTls() {
    // Add an identifier to the thread-local storage, so that we can identify the threads for
    // debugging and just for fun.
    upstreamModuleMutex.lock();
    int currentThreadId = lastThreadId;
    ++lastThreadId;
    upstreamModuleMutex.unlock();

    return new Data(fieldName, currentThreadId);
}

template <typename FieldType, typename FieldMatcher>
Extent* ParallelGrepModule<FieldType, FieldMatcher>::createNextExtent(ThreadLocalStorage *tls) {
    Data *data = reinterpret_cast<Data*>(tls);

    Extent *destinationExtent = NULL;
    bool matchFound = false;

    upstreamModuleMutex.lock();
    std::auto_ptr<Extent> sourceExtent(upstreamModule.getExtent());
    upstreamModuleMutex.unlock();

    while (sourceExtent.get() != NULL) {
        data->sourceSeries.setExtent(sourceExtent.get());

        for (; data->sourceSeries.more(); data->sourceSeries.next()) {
            if (fieldMatcher(data->field)) {
                if (!matchFound) { // the first match for the user's call to getExtent
                    destinationExtent = new Extent(sourceExtent->getType());
                    data->destinationSeries.setExtent(destinationExtent);
                    matchFound = true;
                }

                data->destinationSeries.newRecord();
                data->recordCopier.copyRecord();
            }
        }

        if (destinationExtent != NULL && destinationExtent->size() > extentSizeLimit) {
            break;
        }

        upstreamModuleMutex.lock();
        sourceExtent.reset(upstreamModule.getExtent()); // the next extent
        upstreamModuleMutex.unlock();
    }

    return destinationExtent;
}

#endif
