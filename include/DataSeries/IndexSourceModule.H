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

#include <Lintel/PThread.H>
#include <Lintel/Deque.H>

#include <DataSeries/DataSeriesModule.H>

/** \brief Base class for source modules that use indicies

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

class IndexSourceModule : public SourceModule {
public:
    IndexSourceModule();
    virtual ~IndexSourceModule();

    virtual Extent *getExtent();

    /** call this to start prefetching; if you don't call it, it will
	be automatically called when you call getExtent; Max compressed
        may slightly overrun because we don't know the size of compressed
	extents until we read them. */
    virtual void startPrefetching(unsigned prefetch_max_compressed = 8 * 1024 * 1024,
				  unsigned prefetch_max_unpacked = 32 * 1024 * 1024);
    /** call this to start the index source module over again from the 
	beginning */
    virtual void resetPos();

    /** what fraction of the extents did we wait for, either disk or
	unpacking? */
    double waitFraction();

    /** use readCompressed() to create this structure, it will unlock
	the mutex while doing the work to get the compressed data */
    struct PrefetchExtent {
	Extent::ByteArray bytes;
	const ExtentType *type;
	Extent *unpacked;
	bool need_bitflip;
	std::string uncompressed_type;
	PrefetchExtent() 
	    : type(NULL), unpacked(NULL), need_bitflip(false) { }
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

    struct PrefetchInfo {
	Queue compressed, unpacked;
	int wait_for_extent, nextents;
	PThread *compressed_prefetch_thread;
	std::vector<PThread *> unpack_threads;
	PThreadMutex mutex;
	PThreadCond compressed_cond, unpack_cond, ready_cond;
	bool reset_flag; // for when user calls resetpos
	bool source_done;
	bool abort_prefetching; 
	PrefetchInfo(unsigned cmm, unsigned tum) 
	    : compressed(cmm), unpacked(tum), wait_for_extent(0), nextents(0), 
	      reset_flag(false), source_done(false), 
	      abort_prefetching(false) { }
	bool allDone() {
	    return source_done &&
		compressed.empty() && unpacked.empty();
	}
	bool unpackedReady() {
	    return unpacked.empty() == false
		&& unpacked.front()->unpacked != NULL;
	}
    };
    PrefetchInfo *prefetch;
};
    

#endif
