// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    An abstract class for creating modules that processes extents in parallel. This class
    should only be used for modules that do not care about extent ordering.

    ***** WARNING WARNING WARNING *****
    This interface is completely subject to change, it should be
    considered alpha quality at best, the interface is highly
    changeable based on further experience.  One desirable improvement
    is to allow the use of threads so that we can take advantage of SMPs
    ***** WARNING WARNING WARNING *****
*/

#ifndef __DATASERIES_PARALLELFILTERMODULE_H
#define __DATASERIES_PARALLELFILTERMODULE_H

#include <deque>
#include <vector>

#include <boost/shared_ptr.hpp>

#include <Lintel/PThread.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/Extent.hpp>

// TODO-tomer: rename to UnorderedParallelProcessingModule 
class ParallelFilterModule : public DataSeriesModule {
public:
    /** Create a new ParallelFilterModule that processes extents in parallel.
        \param upstreamModule  The upstream @c DataSeriesModule from which this module requests
                               extents.
        \param extentSizeLimit The maximum size of the extents returned by getExtent. A value
                               of 0 indicates that a single extent should be returned.
        \param threadCount     The number of threads to create. -1 = # CPU cores, 0 = no threads,
	                       work will only be done during calls to getExtent(), > 0 = # threads.
			       TODO-tomer: make the above docs correct.

        \param downstreamQueueLimit The maximum number of bytes of processed data to buffer. 
	                            The worker threads will stop processing upstream data if this 
				    limit is crossed. */
    ParallelFilterModule(DataSeriesModule &upstreamModule,
                         size_t extentSizeLimit = 1024 * 1024, 
                         int threadCount = 0,
                         size_t downstreamQueueLimit = 0);
    virtual ~ParallelFilterModule();

    Extent *getExtent();

protected:
#if 1
    class ThreadLocalStorage {
    public:
        ThreadLocalStorage() {}
        virtual ~ThreadLocalStorage() {}
    };
#endif

    /** Create an extent by grabbing one or more extents from the upstream module (under
        the lock) and returning an extent. */
    virtual Extent *createNextExtent(ThreadLocalStorage *tls) = 0;

#if 1
    /** Create an thread-specific object that will be stored in the thread-local storage and
        passed to createNextExtent in each call. */
    virtual ThreadLocalStorage *createTls();
#else
    virtual void initWorkerThread(); // called once after each pthread is created
#endif

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
    // maybe use lintel deque; g++ one no longer has horrifically bad
    // performance, so this is less of an issue.
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

