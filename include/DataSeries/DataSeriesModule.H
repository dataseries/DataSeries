/* -*-C++-*-
*******************************************************************************
*
* File:         DataSeriesModule.H
* RCS:          $Header: /mount/cello/cvs/DataSeries/include/DataSeries/DataSeriesModule.H,v 1.8 2004/09/28 05:08:32 anderse Exp $
* Description:  first attempt at trying to build a modular structure for
                the data series work
* Author:       Eric Anderson
* Created:      Mon Aug  4 22:56:42 2003
* Modified:     Sun Sep 26 21:39:25 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

// ***** WARNING WARNING WARNING *****
// This interface is completely subject to change, it should be
// considered alpha quality at best, the interface is highly
// changeable based on further experience.  One desirable improvement
// is to allow the use of threads so that we can take advantage of SMPs
// ***** WARNING WARNING WARNING *****

#ifndef __DATASERIES_MODULE_H
#define __DATASERIES_MODULE_H

#include <PThread.H>

#include <Extent.H>
#include <DataSeriesFile.H>

class DataSeriesModule {
public:
    virtual Extent *getExtent() = 0;
    static void getAndDelete(DataSeriesModule &from); // get all the extents from module and delete them.
    virtual ~DataSeriesModule();
};

// Module for returning all the extents from a series of files
class SourceModule : public DataSeriesModule {
public:
    // BUG: This is now somewhat multi-threaded safe, but two
    // SourceModules using the same SourceList will each see a subset
    // of the extents.  IndexSourceModules will do the right thing.

    // Eventually, we should replace most of this with some sort of
    // shared DataSource buffer cache, as that's really what we end up
    // doing, the sourcelist worked fine for this module, became more
    // iffy with the TypeIndexModule, and was completely ignored by
    // the MinMaxIndexModule because it wanted to generate orders
    // differently

    struct SourceList {
	struct sourceinfo {
	    DataSeriesSource *dss;
	    int use_count; // actively using the dss file descriptor
	    sourceinfo(DataSeriesSource *b) : dss(b), use_count(0) {}
	};
	// sources are deleted when refcount falls to zero, and a
	// SourceModule is deleted
	std::vector<sourceinfo> sources;
	int refcount; // to the sourcelist overall
	int nactive, target_active; // number of sources with an
        // active data series source
	PThreadMutex mutex; // protects above + dss; you don't have to
        // hold the lock while using a source if you have marked the in_use
        // for it.

	// The following 4 do the locking for you.
	void addReference();
	void removeReference(); // will delete itself if refcount goes to 0
	void addSource(DataSeriesSource *source);
	void addSource(const std::string &filename);
	void useSource(unsigned sourcenum);
	void unuseSource(unsigned sourcenum);
	SourceList() : refcount(0), nactive(0), target_active(32) {};
	~SourceList(); // only should be called when refcount == 0
    private:
	void lockedCloseInactive();
    };
    // by default, no shared sources, if you pass one in, it should
    // have been dynamically allocated, and one of the SourceModules
    // will delete it when there are no more references
    SourceModule(SourceList *sources = NULL); 

    virtual ~SourceModule();
    void addSource(const std::string &filename) {
	sourcelist->addSource(filename); 
    }
    void addSource(DataSeriesSource *source) {
	sourcelist->addSource(source); 
    }
    void addSource(DataSeriesSource &source) {
	sourcelist->addSource(&source);
    }
    DataSeriesSource *curSource();
    virtual Extent *getExtent();
    bool haveSources();
    SourceList *getSourceList() { return sourcelist; }

    /** statistics on the source module; some of these may remain 0 if
        the module doesn't, or is unable to calculate the statistic */
    long long total_uncompressed_bytes, total_compressed_bytes;
    double decode_time;
protected:
    SourceList *sourcelist;
    int cur_source;
};

// Module for filtering out any extents not matching type_prefix
class FilterModule : public DataSeriesModule {
public:
    FilterModule(DataSeriesModule &_from, const std::string &_type_prefix);
    virtual ~FilterModule();
    virtual Extent *getExtent();
    void setPrefix(const std::string &prefix) {
	type_prefix = prefix;
    }
private:
    DataSeriesModule &from;
    std::string type_prefix;
};

class OutputModule {
public:
    // you are still responsible for closing the sink if necessary;
    // this module just helps with making records of close the the
    // right size.  If you use pack_unique for your variable data and
    // have lots of duplicates than you may want to increase the
    // target extent size.
    OutputModule(DataSeriesSink &sink, ExtentSeries &series,
		 const ExtentType *outputtype, int target_extent_size);

    // you should call writeExtentLibrary before calling this too
    // many times or it will try to write an extent.
    void newRecord();
    void flushExtent(); // force current extent out, you can continue writing.
private:
    DataSeriesSink &sink;
    ExtentSeries &series;
    const ExtentType *outputtype;
    int target_extent_size;
    Extent *cur_extent;
};

#endif
