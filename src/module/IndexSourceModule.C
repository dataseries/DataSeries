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

class IndexSourceModuleUnpackThread : public PThread {
public:
    IndexSourceModuleUnpackThread(IndexSourceModule &_ism)
	: ism(_ism) { }

    virtual ~IndexSourceModuleUnpackThread() { }

    virtual void *run() {
	ism.unpackThread();
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
	prefetch->compressed_cond.broadcast();
	prefetch->unpack_cond.broadcast();
	prefetch->ready_cond.broadcast();
	prefetch->mutex.unlock();
	prefetch->compressed_prefetch_thread->join();
	for(vector<PThread *>::iterator i = prefetch->unpack_threads.begin();
	    i != prefetch->unpack_threads.end(); ++i) {
	    (**i).join();
	}
	while (prefetch->compressed.empty() == false) {
	    delete prefetch->compressed.getFront();
	}
	while (prefetch->unpacked.empty() == false) {
	    delete prefetch->unpacked.getFront();
	}
	delete prefetch;
	prefetch = NULL;
    }
}

void
IndexSourceModule::startPrefetching(unsigned prefetch_max_compressed,
				    unsigned prefetch_max_unpacked,
				    int n_unpack_threads)
{
    INVARIANT(prefetch == NULL,"invalid to start prefetching twice.");
    SINVARIANT(prefetch_max_compressed > 0);
    SINVARIANT(prefetch_max_unpacked > 0);
    
    PrefetchInfo *tmp = new PrefetchInfo(prefetch_max_compressed,
					 prefetch_max_unpacked);
    prefetch = tmp;
    prefetch->compressed_prefetch_thread = 
	new IndexSourceModuleCompressedPrefetchThread(*this);
    prefetch->compressed_prefetch_thread->start();

    unsigned unpack_count = PThreadMisc::getNCpus();
    // TODO: Add support (and test) for 0 unpack threads which should
    // disable all of the prefetching.
    if (n_unpack_threads != -1) {
	SINVARIANT(n_unpack_threads > 0);
	unpack_count = static_cast<unsigned>(n_unpack_threads);
    }

    INVARIANT(unpack_count > 0, "?");
    prefetch->unpack_threads.reserve(unpack_count);
    for(unsigned i = 0; i < unpack_count; ++i) {
	PThread *p = new IndexSourceModuleUnpackThread(*this);
	p->start();
	prefetch->unpack_threads.push_back(p);
    }
    INVARIANT(prefetch == tmp, "two simulataneous calls to startPrefetching??");
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

    if (prefetch == NULL) {
	startPrefetching();
    }
    SINVARIANT(prefetch != NULL);
    prefetch->mutex.lock();
    while(!prefetch->allDone() &&
	  !prefetch->unpackedReady()) {
	++prefetch->stats.consumer;
	prefetch->unpack_cond.broadcast();
	prefetch->ready_cond.wait(prefetch->mutex);
    }
    if (prefetch->allDone()) {
	prefetch->mutex.unlock();
	getting_extent = false;
	return NULL;
    }
    ++prefetch->stats.nextents;
    SINVARIANT(!prefetch->unpacked.empty());
    PrefetchExtent *buf = prefetch->unpacked.getFront();
    SINVARIANT(buf->bytes.empty() && buf->unpacked != NULL);
    prefetch->unpacked.subtract(buf->unpacked->size());
    if (!prefetch->compressed.empty() &&
	prefetch->unpacked.can_add(prefetch->compressed.front())) {
	prefetch->unpack_cond.signal();
    } else {
	++prefetch->stats.skip_unpack_signal;
    }
    prefetch->mutex.unlock();

    Extent *ret = buf->unpacked;
    delete buf;

    getting_extent = false;
    return ret;
}

void
IndexSourceModule::resetPos()
{
    SINVARIANT(prefetch != NULL);
    prefetch->mutex.lock();
    SINVARIANT(prefetch->reset_flag == false);
    INVARIANT(prefetch->compressed.empty() && prefetch->unpacked.empty(), 
	      "reset with anything remaining not implemented");

    prefetch->reset_flag = true;
    prefetch->compressed_cond.signal();
    while(prefetch->reset_flag) {
	prefetch->compressed_cond.wait(prefetch->mutex);
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
	double ret = 0;
	if (prefetch->stats.nextents > 0) {
	    ret = prefetch->stats.consumer
		/ static_cast<double>(prefetch->stats.nextents);
	}
	prefetch->mutex.unlock();
	return ret;
    }
}

bool
IndexSourceModule::getWaitStats(WaitStats &stats)
{
    if (prefetch == NULL) {
	return false;
    }
    prefetch->mutex.lock();
    stats = prefetch->stats;
    prefetch->mutex.unlock();
    SINVARIANT(stats.active_unpack_stats.count() == 0 ||
	       stats.active_unpack_stats.min() > 0);
    return true;
}

void
IndexSourceModule::compressedPrefetchThread()
{
    prefetch->mutex.lock();
    while(true) {
	if (prefetch->abort_prefetching) {
	    break;
	} else if (prefetch->reset_flag) {
	    SINVARIANT(prefetch->compressed.empty() &&
		       prefetch->compressed.cur == 0);
		      
	    lockedResetModule();
	    prefetch->source_done = false;
	    prefetch->reset_flag = false;
	    prefetch->compressed_cond.signal();
	} else if (!prefetch->source_done && prefetch->compressed.can_add(0)) {
	    PrefetchExtent *p = lockedGetCompressedExtent();
	    if (p == NULL) {
		prefetch->source_done = true;
		prefetch->ready_cond.signal();
	    } else {
		prefetch->compressed.add(p, p->bytes.size());
		if (prefetch->unpacked.can_add(prefetch->compressed.front())) {
		    prefetch->unpack_cond.signal();
		} else {
		    ++prefetch->stats.skip_unpack_signal;
		}
	    }
	} else {
	    prefetch->compressed_cond.wait(prefetch->mutex);
	}
    }
    prefetch->mutex.unlock();
}

void
IndexSourceModule::unpackThread()
{
    prefetch->mutex.lock();
    ++prefetch->stats.active_unpackers;
    while(prefetch->abort_prefetching == false) {
	if(prefetch->compressed.data.empty() || 
	   !prefetch->unpacked.can_add(prefetch->compressed.front())) {
	    --prefetch->stats.active_unpackers;
	    if (prefetch->compressed.data.empty()) {
		++prefetch->stats.unpack_no_upstream;
	    } else {
		++prefetch->stats.unpack_downstream_full;
	    }
	    prefetch->unpack_cond.wait(prefetch->mutex);
	    ++prefetch->stats.active_unpackers;
	}
	
	if (prefetch->abort_prefetching) {
	    break;
	}

	prefetch->stats.lockedUpdateActive();
	if (!prefetch->compressed.data.empty() &&
	    prefetch->unpacked.can_add(prefetch->compressed.front())) {
	    PrefetchExtent *pe = prefetch->compressed.getFront();
	    prefetch->compressed.subtract(pe->bytes.size());
	    uint32_t unpacked_size 
		= Extent::unpackedSize(pe->bytes, pe->need_bitflip,
				       *pe->type);
	    prefetch->unpacked.add(pe, unpacked_size);
	    prefetch->compressed_cond.signal();
	    bool should_yield; 
	    if (prefetch->unpackedReady()) {
		// For small extents, almost equivalent to just having the
		// next condition, but not equivalent with large extents.
		// really want a directed yield here to the consumer.
		++prefetch->stats.unpack_yield_ready;
		should_yield = true;
	    } else if (prefetch->unpacked.data.size() > 2*prefetch->unpack_threads.size()) {
		++prefetch->stats.unpack_yield_front;
		// The front of the queue isn't done, but we have a lot of 
		// things in the queue, this means whatever thread is working
		// on that element has been preempted.
		// really want a directed yield to the thread processing the
		// first extent.
		should_yield = true;
	    } else {
		should_yield = false;
	    }
	    prefetch->mutex.unlock();
	    if (should_yield) {
		sched_yield();
	    }
	    Extent *e = new Extent(*pe->type, pe->bytes, pe->need_bitflip);
	    SINVARIANT(e->type.getName() == pe->uncompressed_type);
	    SINVARIANT(e->size() == unpacked_size);
	    prefetch->mutex.lock();
	    SINVARIANT(pe->unpacked == NULL && pe->bytes.size() > 0);
	    total_compressed_bytes += pe->bytes.size();
	    total_uncompressed_bytes += e->size();
	    pe->bytes.clear();
	    pe->unpacked = e;
	    SINVARIANT(!prefetch->unpacked.empty());
	    if (prefetch->unpackedReady()) {
		prefetch->ready_cond.signal();
	    }
	}
    }
    --prefetch->stats.active_unpackers;
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
