// -*-C++-*-
/*
  (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

  See the file named COPYING for license details
*/

#ifndef __PREFETCH_BUFFER_MODULE_H
#define __PREFETCH_BUFFER_MODULE_H

#include <Lintel/PThread.hpp>
#include <Lintel/Deque.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/IndexSourceModule.hpp>

/** \brief A module that can be used to run multiple analyses in parallel
    on the same data.

    It creates its own thread and prefetches
    extents from an upstream source.
*/
class PrefetchBufferModule : public DataSeriesModule {
  public:
    /** \arg source The Module to get Extents from.

        \arg maxextentmemory The maximum size of the queue in bytes.
        The queue may exceed maxextentmemory by one extent.  When
        the queue is full, we stop getting Extents from source until
        the queue is small enough again.
    */
    PrefetchBufferModule(DataSeriesModule &source, 
                         unsigned maxextentmemory = 32*1024*1024);
    virtual ~PrefetchBufferModule();

    /** this function should return exactly the same
        sequence of Extents as the source module passed to
        the constructor.  If the queue is empty this
        will block until an Extent is available.  If we haven't
        already started  prefetching we will start now. */
    virtual Extent::Ptr getSharedExtent();
    /** Launch the worker thread that gets Extents from the source. */
    void startPrefetching();

    /// \cond INTERNAL_ONLY
    void prefetcherThread();
    /// \endcond
  private:
    DataSeriesModule &source;
    pthread_t prefetch_thread;
    Deque<Extent::Ptr> buffer;
    bool source_done, start_prefetching, abort_prefetching;
    unsigned cur_used_memory, max_used_memory;
    PThreadMutex mutex;
    PThreadCond cond;
};

#endif
