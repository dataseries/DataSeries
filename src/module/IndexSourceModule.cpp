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

#include <Lintel/LintelLog.hpp>
#include <Lintel/PThread.hpp>

#include <DataSeries/IndexSourceModule.hpp>

using namespace std;
using boost::format;

class IndexSourceModuleCompressedPrefetchThread : public PThread {
public:
    IndexSourceModuleCompressedPrefetchThread(IndexSourceModule &_ism)
	: ism(_ism) { 
	setStackSize(256*1024); // shouldn't need much
    }

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
	: ism(_ism) { 
	setStackSize(256*1024); // shouldn't need much
    }

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

IndexSourceModule::~IndexSourceModule() {
    INVARIANT(prefetch == NULL || isClosed(),
              "Must either have never read data or be done reading data");
    delete prefetch;
    prefetch = NULL;
}

void
IndexSourceModule::startPrefetching(unsigned prefetch_max_compressed,
				    unsigned prefetch_max_unpacked,
				    int n_unpack_threads)
{
    INVARIANT(prefetch == NULL, "invalid to start prefetching twice without closing.");
    SINVARIANT(prefetch_max_compressed > 0);
    SINVARIANT(prefetch_max_unpacked > 0);

    PrefetchInfo *tmp = new PrefetchInfo(prefetch_max_compressed,
					 prefetch_max_unpacked);
    tmp->mutex.lock();
    prefetch = tmp;

    unsigned unpack_count;
    if (n_unpack_threads == -1) {
        unpack_count = PThreadMisc::getNCpus();
    } else {
        // TODO: Add support (and test) for 0 unpack threads which should
        // disable all of the prefetching.
	SINVARIANT(n_unpack_threads > 0);
	unpack_count = static_cast<unsigned>(n_unpack_threads);
    } 

    INVARIANT(unpack_count > 0, "?");
    tmp->unpack_threads.resize(unpack_count);
    INVARIANT(prefetch == tmp, "two simulataneous calls to startPrefetching??");
    lockedStartThreads();
    tmp->mutex.unlock();
    INVARIANT(prefetch == tmp, "two simulataneous calls to startPrefetching??");
}

void IndexSourceModule::lockedStartThreads() {
    prefetch->compressed_prefetch_thread = 
	new IndexSourceModuleCompressedPrefetchThread(*this);
    prefetch->compressed_prefetch_thread->start();
    for(unsigned i = 0; i < prefetch->unpack_threads.size(); ++i) {
	prefetch->unpack_threads[i] = new IndexSourceModuleUnpackThread(*this);
	prefetch->unpack_threads[i]->start();
    }
}

static inline double 
timediff(struct timeval &end,struct timeval &start)
{
    return end.tv_sec - start.tv_sec + (end.tv_usec - start.tv_usec) * 1e-6;
}

Extent::Ptr IndexSourceModule::getSharedExtent() {
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
        close();
	getting_extent = false;
	return Extent::Ptr();
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

    Extent::Ptr ret = buf->unpacked;
    delete buf;

    SINVARIANT(ret->extent_source != Extent::in_memory_str &&
	       ret->extent_source_offset > 0);
    getting_extent = false;

    LintelLogDebug("IndexSourceModule", format("return extent %s:%d type %s")
		   % ret->extent_source % ret->extent_source_offset % ret->getType().getName());
    return ret;
}

void IndexSourceModule::resetPos() {
    SINVARIANT(prefetch != NULL);
    close();
    prefetch->mutex.lock();
    SINVARIANT(lockedIsClosed());
    lockedResetModule();
    prefetch->source_done = false;
    SINVARIANT(prefetch->abort_prefetching == 0);
    lockedStartThreads();
}

void IndexSourceModule::close() {
    if (prefetch == NULL) { // never opened, or already closed.
        return;
    }

    PThreadScopedLock lock(prefetch->mutex);
    if (lockedIsClosed()) {
        return;
    }
    if (prefetch->abort_prefetching == 0) {
        //                          me + compressed_prefetch + unpackers
        prefetch->abort_prefetching = 2 + prefetch->unpack_threads.size();
        prefetch->compressed_cond.broadcast();
        prefetch->unpack_cond.broadcast();
        prefetch->ready_cond.broadcast();

        while (prefetch->abort_prefetching > 1) {
            prefetch->compressed_cond.wait(prefetch->mutex);
        }

	prefetch->compressed_prefetch_thread->join();
        delete prefetch->compressed_prefetch_thread;
        prefetch->compressed_prefetch_thread = NULL;
	for(vector<PThread *>::iterator i = prefetch->unpack_threads.begin();
	    i != prefetch->unpack_threads.end(); ++i) {
	    (**i).join();
            delete *i;
            *i = NULL;
	}
	while (prefetch->compressed.empty() == false) {
	    delete prefetch->compressed.getFront();
	}
	while (prefetch->unpacked.empty() == false) {
	    delete prefetch->unpacked.getFront();
	}
        SINVARIANT(prefetch->abort_prefetching == 1);
        prefetch->abort_prefetching = 0;
        prefetch->compressed_cond.broadcast();
    }

    // multiple threads can call close() at the same time; all of them wait
    while (prefetch->abort_prefetching > 0) {
        prefetch->compressed_cond.wait(prefetch->mutex);
    }
}

bool IndexSourceModule::isClosed() {
    if (prefetch == NULL) { 
        return false; // not yet started; implicitly open
    }

    PThreadScopedLock lock(prefetch->mutex);
    return lockedIsClosed();
}

bool IndexSourceModule::lockedIsClosed() {
    if (prefetch->compressed_prefetch_thread != NULL) {
        return false;
    } 

    for (vector<PThread *>::iterator i = prefetch->unpack_threads.begin(); 
         i != prefetch->unpack_threads.end(); ++i) {
        if (*i != NULL) {
            return false;
        }
    }
    return true;
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

void IndexSourceModule::compressedPrefetchThread() {
    prefetch->mutex.lock();
    while (prefetch->abort_prefetching == 0) {
        if (!prefetch->source_done && prefetch->compressed.can_add(0)) {
	    PrefetchExtent *p = lockedGetCompressedExtent();
	    if (p == NULL) {
		prefetch->source_done = true;
		prefetch->ready_cond.signal();
	    } else {
		SINVARIANT(p->extent_source != Extent::in_memory_str &&
			   p->extent_source_offset > 0);
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
    SINVARIANT(prefetch->abort_prefetching > 0);
    --prefetch->abort_prefetching;
    prefetch->compressed_cond.broadcast();
    prefetch->mutex.unlock();
}

void IndexSourceModule::unpackThread() {
    prefetch->mutex.lock();
    ++prefetch->stats.active_unpackers;
    while (prefetch->abort_prefetching == 0) {
	if (prefetch->compressed.data.empty() || 
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
	
	if (prefetch->abort_prefetching > 0) {
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
            Extent::Ptr e(new Extent(*pe->type, pe->bytes, pe->need_bitflip));
	    e->extent_source = pe->extent_source;
	    e->extent_source_offset = pe->extent_source_offset;
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
    SINVARIANT(prefetch->abort_prefetching > 0);
    --prefetch->abort_prefetching;
    prefetch->compressed_cond.broadcast();
    prefetch->mutex.unlock();
}

IndexSourceModule::PrefetchExtent *
IndexSourceModule::readCompressed(DataSeriesSource *dss,
				  off64_t offset,
				  const string &uncompressed_type)
{
    prefetch->mutex.unlock();
    PrefetchExtent *p = new PrefetchExtent;
    p->extent_source = dss->getFilename();
    p->extent_source_offset = offset;
    bool ok = dss->preadCompressed(offset,p->bytes);
    INVARIANT(ok,"whoa, shouldn't have hit eof!");
    p->type = dss->getLibrary().getTypeByName(Extent::getPackedExtentType(p->bytes));
    p->need_bitflip = dss->needBitflip();
    p->uncompressed_type = uncompressed_type;
    prefetch->mutex.lock();
    return p;
}
