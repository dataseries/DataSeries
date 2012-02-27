// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A module which tries to use an index to source a subset of the extents
*/

#ifndef __DATASERIES_INDEXSOURCEMODULE_H
#define __DATASERIES_INDEXSOURCEMODULE_H

#include <Lintel/PThread.hpp>
#include <Lintel/Deque.hpp>
#include <Lintel/Stats.hpp>

#include <DataSeries/DataSeriesModule.hpp>

/** \brief Base class for source modules that select a subset of the
           \link Extent Extents \endlink in collection of files via an index file.

 * There are at least two different forms of indexing for reading from
 * DataSeries files.  However they share enough operations that when
 * the time came to make the second form of indexing, this class was
 * re-done to just be the common bits so that the different forms of
 * accessing indexes could be given names that made it more clear what
 * type of index use is made by the sub-class.
 *
 * Added sources will get deleted when this module is deleted; the
 * standard SourceModule deletes them faster, but this module lets you
 * repeatedly scan over files, so can't afford to delete the sources
 * immediately.  */

// TODO: pass back the filename that was being read when doing
// getExtentPrefetch so that we can give a better error message than
// just "index error?! %s != %s"

// TODO: have some sort of way of sharing the various possible input
// files, see dsextentindex.C for a case where this matters, as well
// as nfsdsanalysis.

// TODO: this module (and all the sub-classes) probably should be rewritten.
// Because we inherit for the function to get the next extent, we have to have
// the close() function and the destructor can't properly cleanup after the
// function since another thread might be trying to be in the pure virtual
// function while we are destroying.  If we rewrite this to have a subclass
// which does the locked extent operations, then we can properly cleanup.
// Moreover, once we do a rewrite, we can use the modern lintel locking support
// (locking in here is a bit weird), and we can probably share the
// decompression threads and prefetching threads across multiple new-style
// index source modules which would reduce the number of threads and give us
// better control over the priority of decompression, and the memory used
// during it (effectively a global pool rather than a per-type pool).  While
// making those changes, also fixing the code so that we have parallel
// compressed reading would be a good idea.  With large disk subsystem
// machines, and the really many cores we have now, it is not difficult to need
// to have multiple threads reading in extents in order to read them fast
// enough to keep the pipeline full.

class IndexSourceModule : public SourceModule {
public:
    IndexSourceModule();
    virtual ~IndexSourceModule();

    virtual Extent::Ptr getSharedExtent();

    /** call this to start prefetching; if you don't call it, it will
	be automatically called when you call getExtent; Max
	compressed may slightly overrun because we don't know the size
	of compressed extents until we read them. nthreads == -1 ==>
	use # cpus */
    virtual void startPrefetching(unsigned prefetch_max_compressed = 8 * 1024 * 1024,
				  unsigned prefetch_max_unpacked = 32 * 1024 * 1024,
				  int n_unpack_threads = -1);
    /** call this to start the index source module over again from the 
	beginning */
    virtual void resetPos();

    /** close() will stop all prefetching, and will remove any prefetched
        extents.  It is automatically called when the last extent is removed
        from the module (i.e. before getExtent() returns null).  It *must* be
        called before a call to a destructor in the most derived subclass so
        that the virtual function calls to prefetch extents are not accessing
        dangling pointers. */
    void close();

    /** isClosed() will return whether the module is currently closed */
    bool isClosed();

    /** what fraction of the extents did we wait for, either disk or
	unpacking? */
    double waitFraction();

    struct WaitStats {
	uint64_t nextents, consumer, compressed_downstream_full;
	uint64_t unpack_no_upstream, unpack_downstream_full;
	uint64_t unpack_yield_front, unpack_yield_ready;
	uint64_t skip_unpack_signal;

	Stats active_unpack_stats;
	int active_unpackers;
	void lockedUpdateActive() {
	    active_unpack_stats.add(active_unpackers);
	}
	WaitStats()
	    : nextents(0), consumer(0), compressed_downstream_full(0),
	      unpack_no_upstream(0), unpack_downstream_full(0),
	      unpack_yield_front(0), unpack_yield_ready(0),
	      skip_unpack_signal(0), active_unpackers(0)
	{ }
    };

    /** returns true if there were wait statistics, false otherwise */
    bool getWaitStats(WaitStats &stats);

    /** use readCompressed() to create this structure, it will unlock
	the mutex while doing the work to get the compressed data */
    struct PrefetchExtent {
	Extent::ByteArray bytes;
	const ExtentType *type;
        Extent::Ptr unpacked;
	bool need_bitflip;
	std::string uncompressed_type, extent_source;
	int64_t extent_source_offset;
	PrefetchExtent() 
	    : type(NULL), unpacked(), need_bitflip(false), extent_source_offset(-1) { }
    };

protected:
    bool startedPrefetching() { return prefetch != NULL; }

    /** utility function to read compressed data, it will unlock and relock
        the mutex associated with prefetching */
    PrefetchExtent *readCompressed(DataSeriesSource *dss,
				 off64_t offset, 
				 const std::string &uncompressed_type);

    /** function that is called from the prefetch thread to restart; parent
	will clear out any remaining data */
    virtual void lockedResetModule() = 0;

    /** return NULL when no more extents */
    // TODO: consider re-writing this as bool nextOffset(offset, type,
    // size?)  size would be nice so that we could be more accurate
    // about memory usage, but isn't possible for the type index
    // module without extending the extent type index, and the min/max
    // index module without extending the indexer.
    virtual PrefetchExtent *lockedGetCompressedExtent() = 0;

private:
    bool lockedIsClosed();
    void lockedStartThreads();

    friend class IndexSourceModuleCompressedPrefetchThread;
    friend class IndexSourceModuleUnpackThread;
    void compressedPrefetchThread();
    void unpackThread();

    bool getting_extent;

    struct Queue {
	Queue(unsigned _limit) : cur(0), limit(_limit) { }
	unsigned cur, limit; // limit is max or target
	Deque<PrefetchExtent *> data;
	bool can_add(uint32_t amount) {
	    return cur == 0 || cur + amount < limit;
	}
	bool can_add(int amount) {
	    SINVARIANT(amount >= 0);
	    return can_add(static_cast<uint32_t>(amount));
	}
	bool can_add(PrefetchExtent *pe) {
	    return can_add(Extent::unpackedSize(pe->bytes, pe->need_bitflip,
						*pe->type));
	}
	bool empty() { 
	    return data.empty();
	}
	void add(PrefetchExtent *pe, unsigned size) {
	    cur += size;
	    data.push_back(pe);
	}
	void subtract(unsigned size) {
	    SINVARIANT(cur >= size);
	    cur -= size;
	}
	PrefetchExtent *front() {
	    return data.front();
	}
	PrefetchExtent *getFront() {
	    PrefetchExtent *ret = data.front();
	    data.pop_front();
	    return ret;
	}
    };

    // TODO: move this entire class into the parent; I suspect it used to be
    // available to subclasses, but that turned out not to be useful.  Once
    // moved into the parent class, the locking can be fixed up.

    struct PrefetchInfo {
	Queue compressed, unpacked;
	WaitStats stats;
	PThread *compressed_prefetch_thread;
	std::vector<PThread *> unpack_threads;
	PThreadMutex mutex;
	PThreadCond compressed_cond, unpack_cond, ready_cond;
	bool source_done;
	uint32_t abort_prefetching; // number of threads remaining to abort 

	PrefetchInfo(unsigned cmm, unsigned tum) 
	    : compressed(cmm), unpacked(tum), source_done(false), abort_prefetching(0)
        { }

	bool allDone() {
	    return source_done && compressed.empty() && unpacked.empty();
	}

	bool unpackedReady() {
	    return unpacked.empty() == false && unpacked.front()->unpacked != NULL;
	}
    };

    PrefetchInfo *prefetch; // NULL until startPrefetching() is called
};
    

#endif
