// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation
*/

#include <DataSeries/PrefetchBufferModule.hpp>

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
    INVARIANT(pthread_create(&prefetch_thread, NULL, pthreadfn, this)==0,
	      "Pthread create failed??");
#endif
    INVARIANT(max_used_memory > 0, "can't have 0 max used memory");
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
    INVARIANT(pthread_join(prefetch_thread, NULL) == 0, 
	      "pthread_join failed.");
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
	SINVARIANT(abort_prefetching == false);
	if (buffer.empty() == false) {
	    ret = buffer.front();
	    buffer.pop_front();
	    cur_used_memory -= ret->size();
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
    FATAL_ERROR("should not have created a prefetcher thread in profiling mode");
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
	    cur_used_memory += e->size();
	} else {
	    SINVARIANT(buffer.empty() == false);
	    cond.wait(mutex);
	}
    }
    mutex.unlock();
}

