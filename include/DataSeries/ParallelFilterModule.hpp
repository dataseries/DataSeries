// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    An abstract class for creating modules that processes extents in parallel. This class
    should only be used for modules that do not care about extent ordering.
*/

#ifndef __DATASERIES_PARALLELFILTERMODULE_H
#define __DATASERIES_PARALLELFILTERMODULE_H

#include <deque>
#include <vector>

#include <boost/shared_ptr.hpp>

#include <Lintel/PThread.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/Extent.hpp>

class ParallelFilterModule : public DataSeriesModule {
public:
    /** Create a new ParallelFilterModule that processes extents in parallel.
        \param upstreamModule  The upstream @c DataSeriesModule from which this module requests
                               extents.
        \param extentSizeLimit The maximum size of the extents returned by getExtent. A value
                               of 0 indicates that a single extent should be returned.
        \param threadCount     The number of threads to create. If not specified, this is
                               automatically set to the number of CPU cores available.
        \param downstreamQueueLimit The maximum amount of processed data to buffer. The worker
                                    threads will stop processing upstream data if this limit is
                                    crossed. */
    ParallelFilterModule(DataSeriesModule &upstreamModule,
                         size_t extentSizeLimit = 1 * 1000000, // 1 MB
                         int threadCount = 0,
                         size_t downstreamQueueLimit = 0);
    virtual ~ParallelFilterModule();

    Extent* getExtent();

protected:
    class ThreadLocalStorage {
    public:
        ThreadLocalStorage() {}
        virtual ~ThreadLocalStorage() {}
    };

    /** Create an extent by grabbing one or more extents from the upstream module (under
        the lock) and returning an extent. */
    virtual Extent* createNextExtent(ThreadLocalStorage *tls) = 0;

    /** Create an thread-specific object that will be stored in the thread-local storage and
        passed to createNextExtent in each call. */
    virtual ThreadLocalStorage* createTls();

    size_t extentSizeLimit;

    DataSeriesModule &upstreamModule;
    PThreadMutex upstreamModuleMutex;

private:
    friend class FilterThread;
    typedef boost::shared_ptr<PThread> PThreadPtr;

    bool upstreamEmpty; // Have we received NULL from the upstream module?

    int threadCount;
    int activeThreadCount;
    size_t downstreamQueueLimit;

    PThreadMutex downstreamQueueMutex;
    PThreadCond downstreamQueueNotEmptyCond;
    PThreadCond downstreamQueueNotFullCond;

    std::vector<PThreadPtr> filterThreads;
    std::deque<Extent*> downstreamQueue;
    size_t downstreamQueueSize;


    boost::thread_specific_ptr<ThreadLocalStorage> tls;

    void startFilterThread();
    void waitDownstreamQueueNotFull();
    void appendDownstreamQueue(Extent *extent);

    bool downstreamQueueFull();

    void createThreads();
};

#endif

