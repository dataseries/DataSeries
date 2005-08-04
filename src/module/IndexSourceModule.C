/* -*-C++-*-
*******************************************************************************
*
* File:         IndexSourceModule.C
* RCS:          $Header: /mount/cello/cvs/DataSeries/src/module/IndexSourceModule.C,v 1.7 2005/02/15 01:18:36 anderse Exp $
* Description:  implementation
* Author:       Eric Anderson
* Created:      Sun Aug 17 16:23:07 2003
* Modified:     Wed Jan 19 18:51:41 2005 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>

#include <IndexSourceModule.H>

#ifndef __HP_aCC
using namespace std;
#endif

static void *
pthreadfn(void *arg)
{
    IndexSourceModule *ism = (IndexSourceModule *)arg;
    ism->prefetchThread();
    return NULL;
}

IndexSourceModule::IndexSourceModule(SourceList *sources)
    : SourceModule(sources), 
      getting_extent(false),
      prefetch(NULL)
{
}

IndexSourceModule::~IndexSourceModule()
{
    if (prefetch != NULL) {
	prefetch->mutex.lock();
	prefetch->abort_prefetching = true;
	prefetch->cond.signal();
	prefetch->mutex.unlock();
	AssertAlways(pthread_join(prefetch->prefetch_thread, NULL) == 0,
		     ("pthread_join failed.\n"));
	while (prefetch->buffers.empty() == false) {
	    delete prefetch->buffers.front();
	    prefetch->buffers.pop_front();
	}
	delete prefetch;
	prefetch = NULL;
    }
}

void
IndexSourceModule::startPrefetching(unsigned prefetch_max_memory)
{
    AssertAlways(prefetch == NULL,("invalid to start prefetching twice.\n"));
    AssertAlways(prefetch_max_memory > 0,("pmm == 0"));
    prefetch = new prefetchInfo(prefetch_max_memory);
    AssertAlways(pthread_create(&prefetch->prefetch_thread, NULL, 
				pthreadfn, this)==0,
		 ("Pthread create failed??"));
}

static inline double 
timediff(struct timeval &end,struct timeval &start)
{
    return end.tv_sec - start.tv_sec + (end.tv_usec - start.tv_usec) * 1e-6;
}

Extent *
IndexSourceModule::getExtent()
{
    AssertAlways(getting_extent == false,("incorrect re-entrancy detected\n"));
    getting_extent = true;
    Extent *ret = getExtentPrefetch();
    getting_extent = false;
    return ret;
}

Extent *
IndexSourceModule::getExtentPrefetch()
{
    if (prefetch == NULL) {
	startPrefetching();
    }
    AssertAlways(prefetch != NULL,("internal error"));
    prefetch->mutex.lock();
    while(prefetch->source_done == false &&
	  prefetch->buffers.empty()) {
	++prefetch->wait_for_disk;
	prefetch->cond.wait(prefetch->mutex);
    }
    if (prefetch->buffers.empty() && prefetch->source_done) {
	prefetch->mutex.unlock();
	return NULL;
    }
    ++prefetch->nextents;
    compressedPrefetch *buf = prefetch->buffers.front();
    prefetch->buffers.pop_front();
    prefetch->cur_memory -= buf->bytes.size();
    AssertAlways(prefetch->cur_memory >= 0,
		 ("internal %d %d",
		  prefetch->cur_memory,prefetch->max_memory));
    prefetch->cond.signal();
    prefetch->mutex.unlock();
    struct rusage rusage_start;
    AssertAlways(getrusage(RUSAGE_SELF,&rusage_start)==0,
		 ("getrusage failed: %s\n",strerror(errno)));
    Extent *ret = new Extent(*buf->library,buf->bytes,buf->need_bitflip);
    struct rusage rusage_end;
    AssertAlways(getrusage(RUSAGE_SELF,&rusage_end)==0,
		 ("getrusage failed: %s\n",strerror(errno)));
    AssertAlways(ret->type->name == buf->uncompressed_type,
		 ("index error?! %s != %s\n",ret->type->name.c_str(),
		  buf->uncompressed_type.c_str()));
    prefetch->mutex.lock();
    total_compressed_bytes += buf->bytes.size();
    total_uncompressed_bytes += ret->extentsize();
    decode_time += timediff(rusage_end.ru_utime,rusage_start.ru_utime) +
	timediff(rusage_end.ru_stime,rusage_start.ru_stime);
    prefetch->mutex.unlock();
    // could move this check into another virtual function, but it can't be in
    // the common parent class
//    AssertAlways(ExtentType::prefixmatch(ret->type->name,type_prefix),
//		   ("whoa, got wrong extent type?!\n"));
    delete buf;
    return ret;
}

void
IndexSourceModule::resetPos()
{
    AssertAlways(prefetch != NULL,("internal"));
    prefetch->mutex.lock();
    AssertAlways(prefetch->reset_flag == false,("internal"));
    prefetch->reset_flag = true;
    prefetch->cond.signal();
    while(prefetch->reset_flag) {
	prefetch->cond.wait(prefetch->mutex);
    }
    prefetch->mutex.unlock();
}

double 
IndexSourceModule::waitFraction()
{
    if (prefetch == NULL) {
	return 0; // never got anything
    } else {
	prefetch->mutex.lock();
	double ret = prefetch->nextents > 0 ? prefetch->wait_for_disk / (double)prefetch->nextents : 0;
	prefetch->mutex.unlock();
	return ret;
    }
}

void
IndexSourceModule::prefetchThread()
{
    struct rusage rusage_start;
    AssertAlways(getrusage(RUSAGE_SELF,&rusage_start)==0,
		 ("getrusage failed: %s\n",strerror(errno)));
    prefetch->mutex.lock();
    unsigned start_sourcelist_size = sourcelist->sources.size();
    while(prefetch->abort_prefetching == false) {
	AssertAlways(start_sourcelist_size == sourcelist->sources.size(),
		     ("invalid change to sourcelist size after start of prefetching\n"));
	if (prefetch->cur_memory >= prefetch->max_memory ||
	    prefetch->source_done) {
	    prefetch->cond.wait(prefetch->mutex);
	}
	if (prefetch->abort_prefetching) {
	    break;
	}
	if (prefetch->reset_flag) {
	    while(prefetch->buffers.empty() == false) {
		prefetch->cur_memory -= 
		    prefetch->buffers.front()->bytes.size();
		AssertAlways(prefetch->cur_memory >= 0,("internal"));
		delete prefetch->buffers.front();
		prefetch->buffers.pop_front();
	    }
	    AssertAlways(prefetch->cur_memory == 0,("internal"));
	    lockedResetModule();
	    prefetch->source_done = false;
	    prefetch->reset_flag = false;
	    prefetch->cond.signal();
	}
	if (prefetch->cur_memory < prefetch->max_memory) {
	    compressedPrefetch *p = lockedGetCompressedExtent();
	    if (p == NULL) {
		prefetch->source_done = true;
	    } else {
		prefetch->cur_memory += p->bytes.size();
		prefetch->buffers.push_back(p);
	    }
	    prefetch->cond.signal();
	}
    }
    struct rusage rusage_end;
    AssertAlways(getrusage(RUSAGE_SELF,&rusage_end)==0,
		 ("getrusage failed: %s\n",strerror(errno)));
    decode_time += timediff(rusage_end.ru_utime,rusage_start.ru_utime) +
	timediff(rusage_end.ru_stime,rusage_start.ru_stime);
    prefetch->mutex.unlock();
}

IndexSourceModule::compressedPrefetch *
IndexSourceModule::getCompressed(DataSeriesSource *dss,
				 off64_t offset,
				 const string &uncompressed_type)
{
    prefetch->mutex.unlock();
    compressedPrefetch *p = new compressedPrefetch;
    bool ok = dss->preadCompressed(offset,p->bytes);
    AssertAlways(ok,("whoa, shouldn't have hit eof!\n"));
    p->library = &dss->mylibrary;
    p->need_bitflip = dss->needBitflip();
    p->dataseries_type = dss->dataseriesType();
    p->uncompressed_type = uncompressed_type;
    prefetch->mutex.lock();
    return p;
}
