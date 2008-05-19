// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A module that creates it's own thread and prefetches extents from an 
    upstream source
*/

#ifndef __PREFETCH_BUFFER_MODULE_H
#define __PREFETCH_BUFFER_MODULE_H

#include <Lintel/PThread.H>
#include <Lintel/Deque.H>

#include <DataSeries/DataSeriesModule.H>
#include <DataSeries/IndexSourceModule.H>

class PrefetchBufferModule : public DataSeriesModule {
public:
    // may exceed maxextentmemory by one extent.
    PrefetchBufferModule(DataSeriesModule &source, 
			 unsigned maxextentmemory = 32*1024*1024);
    virtual ~PrefetchBufferModule();

    virtual Extent *getExtent(); // will start prefetching if it hasn't already
    void startPrefetching();

    void prefetcherThread();
private:
    DataSeriesModule &source;
    pthread_t prefetch_thread;
    Deque<Extent *> buffer;
    bool source_done, start_prefetching, abort_prefetching;
    unsigned cur_used_memory, max_used_memory;
    PThreadMutex mutex;
    PThreadCond cond;
};

#endif
