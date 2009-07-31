// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
     first attempt at trying to build a modular structure for data series
*/

#ifndef DATASERIES_MODULE_H
#define DATASERIES_MODULE_H

#include <boost/utility.hpp>

#include <Lintel/PThread.hpp>

#include <DataSeries/Extent.hpp>
#include <DataSeries/DataSeriesFile.hpp>

/** \brief Abstract base class for analysis.

    The main entry point for processing is getAndDelete.  Each
    derived class should to its processing in getExtent.  Derived
    classes are expected to be chained together, giving a structure like:

\verbatim

            +----------------+
            | SequenceModule |<>-----+-----------------+------------------+
            +----------------+       |                 |                  |
                   |                 |                 |                  |
                             +-------------+  +----------------+  +---------------+   +------------------+
 getAndDelete()    |         | Some Module |->| Another Module |->| Source Module |<>-| DataSeriesSource |
----------------->+-+        +-------------+  +----------------+  +---------------+   +------------------+
                  | |                |                 |                  |                      |
              /-->+ |  getExtent()                                                                
             |    | |-------------->+-+  getExtent()   |                  |                      |
             |    | |               | |-------------->+-+   getExtent()                           
             |    | |               | |               | |--------------->+-+    preadExtent()    |
             |    | |               | |               | |                | |------------------->+-+
             |    | |               | |               | |                | |                    | |
loop until   |    | |               | |               | |                | |<-------------------+-+
getExtent()  |    | |               | |               | |<---------------+-+
returns null |    | |               | |               | |
             |    | |               | |               | | Process the Extent
             |    | |               | |<--------------+-+
             |    | |               | |
             |    | |               | | Process the Extent
             |    | |<--------------+-+
              \--<+-+

\endverbatim
    
    */
class DataSeriesModule : boost::noncopyable {
public:
    /** Returns an Extent which ought to have been allocated with
        global new. It is the caller's responsibility to delete
        it. Each call to this function should return a different
        @c Extent. Derived classes should return a null pointer
        to indicate the end of the sequence of Extents. */
    virtual Extent *getExtent() = 0;
    /** get all the extents from module and delete them. */
    void getAndDelete();
    /// \cond INTERNAL_ONLY
    // TODO: deprecate static getAndDelete.
    static void getAndDelete(DataSeriesModule &from) {
	from.getAndDelete();
    }
    /// \endcond
    virtual ~DataSeriesModule();
};

/** \brief Base class for source modules that keeps statistics about
    reading from files. Otherwise it is just empty for now. */
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
    // TODO: deprecate decode_time, it's pretty much useless given that
    // all the ways that I can find to get resource usage work poorly.
    // getrusage is per-process, /proc is slow, clock_gettime can go
    // backwards on RHEL4.
    double decode_time;
};

/** \brief Module for filtering out any extents not matching type_prefix */
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

/** \brief Splits a sequence of records into "bite-sized" Extents.

    Since the unit of processing in DataSeries is a single
    @c Extent, this provides a way to keep Extents from getting
    too big to fit in memory. */
class OutputModule {
public:
    // TODO: Replace constructor with OutputModule(DataSeriesSink
    // &sink, const ExtentType &outputtype, uint32_t
    // target_extent_size = 0) auto-infer tes if it is 0 same as with
    // commonargs, then make the extentseries a real class member.

    // you are still responsible for closing the sink if necessary;
    // this module just helps with making records of close the the
    // right size.  If you use pack_unique for your variable data and
    // have lots of duplicates than you may want to increase the
    // target extent size.
    OutputModule(DataSeriesSink &sink, ExtentSeries &series,
		 const ExtentType *outputtype, int target_extent_size);
    /** Calls close unless you have already called close manually. */
    ~OutputModule();

    // you should call writeExtentLibrary before calling this too
    // many times or it will try to write an extent.
    void newRecord();
    /** force current extent out, you can continue writing. */
    void flushExtent();
    /** Force current @c Extent out.  After calling close, it is illegal
        to call newRecord, flushExtent or close.  Note that this does
        not close the underlying @c DataSeriesSink. */
    void close();

    /** Returns cumulative statistics on the Extents written.  As
        with @c DataSeriesSink::getStats, this is likely to be unreliable
        before you close the sink or call @c DataSeriesSink::flushPending. */
    DataSeriesSink::Stats getStats();

    uint32_t getTargetExtentSize() {
	return target_extent_size;
    }

    void setTargetExtentSize(uint32_t bytes) {
	// setting this will only have an effect after the next call to
	// newRecord.
	target_extent_size = bytes;
    }

    void printStats(std::ostream &to);
    const ExtentType *outputtype;

    size_t curExtentSize() {
	return cur_extent->fixeddata.size() + cur_extent->variabledata.size();
    }

    ExtentSeries &getSeries() {
	return series;
    }

    const DataSeriesSink &getSink() { 
	return sink; 
    }
private:
    uint32_t target_extent_size; 

    DataSeriesSink::Stats stats;
    
    DataSeriesSink &sink;
    ExtentSeries &series;
    Extent *cur_extent;
};

#endif
