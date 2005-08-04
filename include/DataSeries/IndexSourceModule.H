/* -*-C++-*-
*******************************************************************************
*
* File:         IndexSourceModule.H
* RCS:          $Header: /mount/cello/cvs/DataSeries/include/DataSeries/IndexSourceModule.H,v 1.6 2005/02/15 01:18:34 anderse Exp $
* Description:  A module which tries to use an index to source a subset of the extents
* Author:       Eric Anderson
* Created:      Sun Aug 17 16:17:48 2003
* Modified:     Wed Jan 19 18:51:26 2005 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#ifndef __DATASERIES_INDEXSOURCEMODULE_H
#define __DATASERIES_INDEXSOURCEMODULE_H

#include <PThread.H>
#include <Deque.H>

#include <DataSeriesModule.H>

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

// TODO: pass back the filename that was being read when doing getExtentPrefetch
// so that we can give a better error message than just "index error?! %s != %s"

class IndexSourceModule : public SourceModule {
public:
    IndexSourceModule(SourceList *sources = NULL);
    virtual ~IndexSourceModule();

    virtual Extent *getExtent();

    /** call this to start prefetching; if you don't call it, it will be
	automatically called when you call getExtent */
    void startPrefetching(unsigned prefetch_max_memory = 8 * 1024 * 1024);
    /** call this to start the index source module over again from the 
	beginning */
    void resetPos();

    /** what fraction of the extents did we wait for? */
    double waitFraction();

    /** public interface to start the prefetch thread, public only to allow
	it to be called from a C function */
    void prefetchThread();

    /** use getCompressed() to create this structure, it will unlock
	the mutex while doing the work to get the compressed data */
    struct compressedPrefetch {
	Extent::ByteArray bytes;
	ExtentTypeLibrary *library;
	bool need_bitflip;
	const ExtentType *dataseries_type;
	std::string uncompressed_type;
	compressedPrefetch() : library(NULL), 
	    need_bitflip(false), dataseries_type(NULL) { }
    };

protected:
    bool startedPrefetching() { return prefetch != NULL; }

    /** utility function to read compressed data, it will unlock and relock
        the mutex associated with prefetching */
    compressedPrefetch *getCompressed(DataSeriesSource *dss,
				      off64_t offset, 
				      const std::string &uncompressed_type);

    /** function that is called from the prefetch thread to restart; parent
	will clear out any remaining data */
    virtual void lockedResetModule() = 0;

    /** return NULL when no more extents */
    virtual compressedPrefetch *lockedGetCompressedExtent() = 0;

private:
    Extent *getExtentPrefetch();

    bool getting_extent;

    struct prefetchInfo {
	int max_memory, cur_memory;
	int wait_for_disk, nextents;
	pthread_t prefetch_thread;
	PThreadMutex mutex;
	PThreadCond cond;
	Deque<compressedPrefetch *> buffers;
	bool reset_flag; // for when user calls resetpos
	bool source_done;
	bool abort_prefetching; 
	prefetchInfo(int mm) 
	    : max_memory(mm), cur_memory(0), wait_for_disk(0), nextents(0), 
	    reset_flag(false), source_done(false), abort_prefetching(false) { }
    };
    prefetchInfo *prefetch;
};
    

#endif
