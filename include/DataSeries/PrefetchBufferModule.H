/* -*-C++-*-
*******************************************************************************
*
* File:         PrefetchBufferModule.H
* RCS:          $Header: /mount/cello/cvs/DataSeries/include/DataSeries/PrefetchBufferModule.H,v 1.1 2003/10/27 03:46:32 anderse Exp $
* Description:  A module that creates it's own thread and prefetches extents from an upstream source
* Author:       Eric Anderson
* Created:      Mon Sep 29 20:15:51 2003
* Modified:     Sun Oct 12 16:33:09 2003 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#ifndef __PREFETCH_BUFFER_MODULE_H
#define __PREFETCH_BUFFER_MODULE_H

#include <PThread.H>
#include <Deque.H>

#include <DataSeriesModule.H>
#include <IndexSourceModule.H>

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
