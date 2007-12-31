// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation
*/

#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>

#include <Lintel/PThread.H>
#include <DataSeries/IndexSourceModule.H>

#ifndef __HP_aCC
using namespace std;
#endif

class IndexSourceModuleCompressedPrefetchThread : public PThread {
public:
    IndexSourceModuleCompressedPrefetchThread(IndexSourceModule &_ism)
	: ism(_ism) { }

    virtual ~IndexSourceModuleCompressedPrefetchThread() { }

    virtual void *run() {
	ism.compressedPrefetchThread();
	return NULL;
    }
    IndexSourceModule &ism;
};

IndexSourceModule::IndexSourceModule()
    : getting_extent(false), prefetch(NULL)
{
}

IndexSourceModule::~IndexSourceModule()
{
    if (prefetch != NULL) {
	prefetch->mutex.lock();
	prefetch->abort_prefetching = true;
	prefetch->cond.signal();
	prefetch->mutex.unlock();
	prefetch->compressed_prefetch_thread->join();
	while (prefetch->compressed.empty() == false) {
	    delete prefetch->compressed.front();
	    prefetch->compressed.pop_front();
	}
	delete prefetch;
	prefetch = NULL;
    }
}

void
IndexSourceModule::startPrefetching(unsigned prefetch_max_compressed,
				    unsigned)
{
    INVARIANT(prefetch == NULL,"invalid to start prefetching twice.");
    INVARIANT(prefetch_max_compressed > 0,"pmm == 0");
    prefetch = new PrefetchInfo(prefetch_max_compressed);
    prefetch->compressed_prefetch_thread = 
	new IndexSourceModuleCompressedPrefetchThread(*this);
    prefetch->compressed_prefetch_thread->start();
}

static inline double 
timediff(struct timeval &end,struct timeval &start)
{
    return end.tv_sec - start.tv_sec + (end.tv_usec - start.tv_usec) * 1e-6;
}

Extent *
IndexSourceModule::getExtent()
{
    INVARIANT(getting_extent == false,"incorrect re-entrancy detected");
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
    INVARIANT(prefetch != NULL,"internal error");
    prefetch->mutex.lock();
    while(prefetch->source_done == false &&
	  prefetch->compressed.empty()) {
	++prefetch->wait_for_disk;
	prefetch->cond.wait(prefetch->mutex);
    }
    if (prefetch->compressed.empty() && prefetch->source_done) {
	prefetch->mutex.unlock();
	return NULL;
    }
    ++prefetch->nextents;
    PrefetchExtent *buf = prefetch->compressed.front();
    prefetch->compressed.pop_front();
    prefetch->cur_memory -= buf->bytes.size();
    AssertAlways(prefetch->cur_memory >= 0,
		 ("internal %d %d",
		  prefetch->cur_memory,prefetch->max_memory));
    prefetch->cond.signal();
    prefetch->mutex.unlock();

    Extent *ret = new Extent(*buf->type,buf->bytes,buf->need_bitflip);
    INVARIANT(ret->type.getName() == buf->uncompressed_type,
	      boost::format("index error?! %s != %s\n")
	      % ret->type.getName() % buf->uncompressed_type);
    prefetch->mutex.lock();
    total_compressed_bytes += buf->bytes.size();
    total_uncompressed_bytes += ret->extentsize();
    prefetch->mutex.unlock();

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
IndexSourceModule::compressedPrefetchThread()
{
    prefetch->mutex.lock();
    while(prefetch->abort_prefetching == false) {
	if (prefetch->cur_memory >= prefetch->max_memory ||
	    prefetch->source_done) {
	    prefetch->cond.wait(prefetch->mutex);
	}
	if (prefetch->abort_prefetching) {
	    break;
	}
	if (prefetch->reset_flag) {
	    while(prefetch->compressed.empty() == false) {
		prefetch->cur_memory -= 
		    prefetch->compressed.front()->bytes.size();
		INVARIANT(prefetch->cur_memory >= 0,"internal");
		delete prefetch->compressed.front();
		prefetch->compressed.pop_front();
	    }
	    INVARIANT(prefetch->cur_memory == 0,"internal");
	    lockedResetModule();
	    prefetch->source_done = false;
	    prefetch->reset_flag = false;
	    prefetch->cond.signal();
	}
	if (prefetch->cur_memory < prefetch->max_memory) {
	    PrefetchExtent *p = lockedGetCompressedExtent();
	    if (p == NULL) {
		prefetch->source_done = true;
	    } else {
		prefetch->cur_memory += p->bytes.size();
		prefetch->compressed.push_back(p);
	    }
	    prefetch->cond.signal();
	}
    }
    prefetch->mutex.unlock();
}

IndexSourceModule::PrefetchExtent *
IndexSourceModule::readCompressed(DataSeriesSource *dss,
				  off64_t offset,
				  const string &uncompressed_type)
{
    prefetch->mutex.unlock();
    PrefetchExtent *p = new PrefetchExtent;
    bool ok = dss->preadCompressed(offset,p->bytes);
    INVARIANT(ok,"whoa, shouldn't have hit eof!");
    p->type = dss->mylibrary.getTypeByName(Extent::getPackedExtentType(p->bytes));
    p->need_bitflip = dss->needBitflip();
    p->uncompressed_type = uncompressed_type;
    prefetch->mutex.lock();
    return p;
}
