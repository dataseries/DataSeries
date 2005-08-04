/* -*-C++-*-
*******************************************************************************
*
* File:         PrefetchBufferModule.C
* RCS:          $Header: /mount/cello/cvs/DataSeries/src/module/PrefetchBufferModule.C,v 1.3 2004/09/28 05:08:32 anderse Exp $
* Description:  implementation
* Author:       Eric Anderson
* Created:      Mon Sep 29 20:24:07 2003
* Modified:     Wed Sep 15 15:58:19 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#include <PrefetchBufferModule.H>

/** Note: we special case the code when we are compiling in profile mode because
    profiling doesn't tend to work very well when we have multiple threads */

static void *
pthreadfn(void *arg)
{
    PrefetchBufferModule *pbm = (PrefetchBufferModule *)arg;
    pbm->prefetcherThread();
    return NULL;
}

PrefetchBufferModule::PrefetchBufferModule(DataSeriesModule &_source, unsigned maxextentmemory)
    : source(_source), source_done(false), start_prefetching(false), abort_prefetching(false),
    cur_used_memory(0), max_used_memory(maxextentmemory)
{
#ifdef COMPILE_PROFILE
    prefetch_thread = 0;
#else
    AssertAlways(pthread_create(&prefetch_thread, NULL, pthreadfn, this)==0,
		 ("Pthread create failed??"));
#endif
    AssertAlways(max_used_memory > 0,("can't have 0 max used memory\n"));
}

PrefetchBufferModule::~PrefetchBufferModule()
{
#ifdef COMPILE_PROFILE
    // nothing to do, no actual prefetching
#else
    mutex.lock();
    abort_prefetching = true;
    cond.signal();
    mutex.unlock();
    AssertAlways(pthread_join(prefetch_thread, NULL) == 0,
		 ("pthread_join failed.\n"));
    while (buffer.empty() == false) {
	delete buffer.front();
	buffer.pop_front();
    }
#endif
}

Extent *
PrefetchBufferModule::getExtent()
{
#ifdef COMPILE_PROFILE
    return source.getExtent();
#else
    Extent *ret;
    mutex.lock();
    while (true) {
	AssertAlways(abort_prefetching == false,("bad"));
	if (buffer.empty() == false) {
	    ret = buffer.front();
	    buffer.pop_front();
	    cur_used_memory -= ret->extentsize();
	    break;
	} else if (source_done) {
	    ret = NULL;
	    break;
	} else {
	    start_prefetching = true;
	    cond.signal();
	    cond.wait(mutex);
	}
    }
    mutex.unlock();
    return ret;
#endif
}

void
PrefetchBufferModule::startPrefetching()
{
#ifdef COMPILE_PROFILE
    fprintf(stderr,"warning, not enabling prefetching, running in profiling mode\n");
#else
    mutex.lock();
    start_prefetching = true;
    cond.signal();
    mutex.unlock();
#endif
}

void
PrefetchBufferModule::prefetcherThread()
{
#ifdef COMPILE_PROFILE
    AssertFatal(("should not have created a prefetcher thread in profiling mode"));
#endif
    mutex.lock();
    while (start_prefetching == false) {
	if (abort_prefetching)
	    break;
	cond.wait(mutex);
    }
    while (abort_prefetching == false) {
	if (cur_used_memory < max_used_memory) {
	    mutex.unlock();
	    Extent *e = source.getExtent();
	    mutex.lock();
	    if (e == NULL) {
		source_done = true;
		cond.signal();
		break;
	    }
	    buffer.push_back(e);
	    cond.signal();
	    cur_used_memory += e->extentsize();
	} else {
	    AssertAlways(buffer.empty() == false,("bad"));
	    cond.wait(mutex);
	}
    }
    mutex.unlock();
}

