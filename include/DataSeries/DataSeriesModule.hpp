// -*-C++-*-
/*
   (c) Copyright 2003-2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
     first attempt at trying to build a modular structure for data series
*/

// TODO: figure out how to cleanly support multi-threaded modules; they are now necessary
// to get sufficient performance on a single machine.

#ifndef DATASERIES_MODULE_H
#define DATASERIES_MODULE_H

#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>

#include <Lintel/PThread.hpp>

#include <DataSeries/DataSeriesSource.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentSeries.hpp>
#include <DataSeries/IExtentSink.hpp>

// TODO: remove the below (or decide it's the right thing to do after moving the specific modules
// into their own header/cpp files); lots of programs seem to rely on the implicit include of field
// and sink from including this header.
#include <DataSeries/ExtentField.hpp>
#include <DataSeries/DataSeriesSink.hpp>

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
    typedef boost::shared_ptr<DataSeriesModule> Ptr;

    virtual ~DataSeriesModule();

    /** Returns an Extent which ought to have been allocated with global new. It is the caller's
        responsibility to delete it. Each call to this function should return a different @c
        Extent. Derived classes should return a null pointer to indicate the end of the sequence of
        Extents. NOTE: This function is being deprecated in preference to getSharedExtent. */
    virtual Extent *getExtent() DS_RAW_EXTENT_PTR_DEPRECATED;

    /** Returns a new Extent that has been allocated with global new, and may be read-shared with
        other modules, therefore callers should not modify the extent without either a) making a
        copy of it, or b) verifying that it is not shared.  The Ptr may be null, which indicates
        the end of the sequence of Extents. */
    virtual Extent::Ptr getSharedExtent();

    /** get all the extents from module and delete them via the getExtent() interface. */
    void getAndDelete() DS_RAW_EXTENT_PTR_DEPRECATED;

    /** get all the extents from module and delete them via the getExtentShared() interface. */
    void getAndDeleteShared();
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
    uint64_t total_uncompressed_bytes, total_compressed_bytes;
};

// TODO: deprecate this module after 2012-09-01, replace with TypeFilterModule, and then rename
// TypeFilterModule back to filtermodule, or if it's sufficiently compatible,
// just replace.

/** \brief Module for filtering out any extents not matching type_prefix */
class FilterModule : public DataSeriesModule {
public:
    FilterModule(DataSeriesModule &_from, const std::string &_type_prefix) FUNC_DEPRECATED;
    virtual ~FilterModule();
    virtual Extent::Ptr getSharedExtent();
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
    // TODO: remove after 2012-02-01
    OutputModule(dataseries::IExtentSink &sink, ExtentSeries &series,
		 const ExtentType *outputtype, int target_extent_size) FUNC_DEPRECATED;

    OutputModule(dataseries::IExtentSink &sink, ExtentSeries &series,
		 const ExtentType &outputtype, int target_extent_size);

    OutputModule(dataseries::IExtentSink &sink, ExtentSeries &series,
		 const ExtentType::Ptr outputtype, int target_extent_size);

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
    dataseries::IExtentSink::Stats getStats();

    uint32_t getTargetExtentSize() {
	return target_extent_size;
    }

    void setTargetExtentSize(uint32_t bytes) {
	// setting this will only have an effect after the next call to
	// newRecord.
	target_extent_size = bytes;
    }

    void printStats(std::ostream &to);
    const ExtentType &outputtype;

    size_t curExtentSize() {
	return cur_extent->fixeddata.size() + cur_extent->variabledata.size();
    }

    ExtentSeries &getSeries() {
	return series;
    }

    const dataseries::IExtentSink &getSink() { 
	return sink; 
    }
private:
    uint32_t target_extent_size; 

    dataseries::IExtentSink::Stats stats;
    
    dataseries::IExtentSink &sink;
    ExtentSeries &series;
    Extent::Ptr cur_extent;
};

#endif
