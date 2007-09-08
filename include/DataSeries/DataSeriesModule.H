// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
     first attempt at trying to build a modular structure for data series
*/

// ***** WARNING WARNING WARNING *****
// This interface is completely subject to change, it should be
// considered alpha quality at best, the interface is highly
// changeable based on further experience.  One desirable improvement
// is to allow the use of threads so that we can take advantage of SMPs
// ***** WARNING WARNING WARNING *****

#ifndef __DATASERIES_MODULE_H
#define __DATASERIES_MODULE_H

#include <Lintel/PThread.H>

#include <DataSeries/Extent.H>
#include <DataSeries/DataSeriesFile.H>

class DataSeriesModule {
public:
    virtual Extent *getExtent() = 0;
    static void getAndDelete(DataSeriesModule &from); // get all the extents from module and delete them.
    virtual ~DataSeriesModule();
};

// Module for keeping statistics about reading from files, otherwise
// just empty now.
class SourceModule : public DataSeriesModule {
public:
    // Eventually, we should add some sort of shared DataSource buffer
    // cache, in the current design if we have multiple modules
    // accessing the same file, they will end up separately reading in
    // the same indexing information.  It might also be nice to keep
    // the index extent at the end of each file cached.  Need to make
    // sure that you don't accidentally re-add the need to open all of
    // the files at startup, that is very slow if there are thousands
    // of files.

    SourceModule(); 

    virtual ~SourceModule();

    /** statistics on the source module; some of these may remain 0 if
        the actual module doesn't, or is unable to calculate the
        statistic */
    long long total_uncompressed_bytes, total_compressed_bytes;
    double decode_time;
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
    ~OutputModule();

    // you should call writeExtentLibrary before calling this too
    // many times or it will try to write an extent.
    void newRecord();
    void flushExtent(); // force current extent out, you can continue writing.

    void finishWriting() {
	sink.finishWriting();
    }
    DataSeriesSink::Stats getStats() {
	PThreadAutoLocker lock(DataSeriesSink::Stats::getMutex());
	DataSeriesSink::Stats ret = stats;
	return ret;
    }

    void printStats(std::ostream &to);
    const int target_extent_size;
    const ExtentType *outputtype;

    size_t curExtentSize() {
	return cur_extent->fixeddata.size() + cur_extent->variabledata.size();
    }
private:
    DataSeriesSink::Stats stats;
    
    DataSeriesSink &sink;
    ExtentSeries &series;
    Extent *cur_extent;
};

#endif
