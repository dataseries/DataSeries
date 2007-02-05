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

#include <DataSeries/DataSeriesModule.H>

DataSeriesModule::~DataSeriesModule()
{
}

void
DataSeriesModule::getAndDelete(DataSeriesModule &from)
{
    while(true) {
	Extent *e = from.getExtent();
	if (e == NULL) return;
	delete e;
    }
}


SourceModule::SourceModule()
    : total_uncompressed_bytes(0), 
    total_compressed_bytes(0), decode_time(0)
{
}

SourceModule::~SourceModule()
{
}

static inline double 
timediff(struct timeval &end,struct timeval &start)
{
    return end.tv_sec - start.tv_sec + (end.tv_usec - start.tv_usec) * 1e-6;
}

FilterModule::FilterModule(DataSeriesModule &_from, 
			   const std::string &_type_prefix)
    : from(_from), type_prefix(_type_prefix)
{
}

FilterModule::~FilterModule()
{
}

Extent *
FilterModule::getExtent()
{
    while(true) {
	Extent *e = from.getExtent();
	if (e == NULL)
	    return NULL;
	if (ExtentType::prefixmatch(e->type->name,type_prefix))
	    return e;
	delete e;
    }
}

OutputModule::OutputModule(DataSeriesSink &_sink, ExtentSeries &_series,
			   const ExtentType *_outputtype, int _target_extent_size)
    : extents(0), compress_none(0), compress_lzo(0), compress_gzip(0), 
      compress_bz2(0), compress_lzf(0),
      unpacked_size(0), unpacked_fixed(0), unpacked_variable(0), 
      packed_size(0), pack_time(0),
      sink(_sink), series(_series), outputtype(_outputtype),
      target_extent_size(_target_extent_size)
{
    AssertAlways(series.curExtent() == NULL,
		 ("usage error, outputmodule series extent started with an extent\n"));
    cur_extent = new Extent(outputtype);
    series.setExtent(cur_extent);
}

OutputModule::~OutputModule()
{
    flushExtent();
}


void
OutputModule::newRecord()
{
    AssertAlways(series.curExtent() == cur_extent,
		 ("usage error, someone else changed the series extent\n"));
    if ((int)(cur_extent->extentsize() + outputtype->fixedrecordsize()) > target_extent_size) {
	flushExtent();
    }
    series.newRecord();
}

void
OutputModule::flushExtent()
{
    if (cur_extent->fixeddata.size() > 0) {
	
        int old_extents;
	int old_compress_none, old_compress_lzo, old_compress_gzip, old_compress_bz2, old_compress_lzf;
	long long old_unpacked_size, old_unpacked_fixed, old_unpacked_variable, old_packed_size;
	double old_pack_time;

        old_extents           = sink.extents           ;
	old_compress_none     = sink.compress_none     ;
	old_compress_lzo      = sink.compress_lzo      ;
	old_compress_gzip     = sink.compress_gzip     ;
	old_compress_bz2      = sink.compress_bz2      ;
	old_compress_lzf      = sink.compress_lzf      ;
	old_unpacked_size     = sink.unpacked_size     ;
	old_unpacked_fixed    = sink.unpacked_fixed    ;
	old_unpacked_variable = sink.unpacked_variable ;
	old_packed_size       = sink.packed_size       ;
	old_pack_time         = sink.pack_time         ;

	sink.writeExtent(cur_extent);
	cur_extent->clear();

        extents           += sink.extents           - old_extents           ;
	compress_none     += sink.compress_none     - old_compress_none     ;
	compress_lzo      += sink.compress_lzo      - old_compress_lzo      ;
	compress_gzip     += sink.compress_gzip     - old_compress_gzip     ;
	compress_bz2      += sink.compress_bz2      - old_compress_bz2      ;
	compress_lzf      += sink.compress_lzf      - old_compress_lzf      ;
	unpacked_size     += sink.unpacked_size     - old_unpacked_size     ;
	unpacked_fixed    += sink.unpacked_fixed    - old_unpacked_fixed    ;
	unpacked_variable += sink.unpacked_variable - old_unpacked_variable ;
	packed_size       += sink.packed_size       - old_packed_size       ;
	pack_time         += sink.pack_time         - old_pack_time         ;
    }
}

void
OutputModule::printStats(std::ostream &to)
{
    to << boost::format("  wrote %d extents of type %s")
       % extents % outputtype->name 
       << std::endl;
    to << boost::format("  compression (none,lzo,gzip,bz2,lzf): (%d,%d,%d,%d,%d)")
	% compress_none % compress_lzo % compress_gzip % compress_bz2 % compress_lzf
       << std::endl;
    to << boost::format("  unpacked: %d = %d (fixed) + %d (variable)")
	% unpacked_size % unpacked_fixed % unpacked_variable
       << std::endl;
    to << boost::format("  packed size: %d; pack time: %.3f")
	% packed_size % pack_time
       << std::endl;
}
