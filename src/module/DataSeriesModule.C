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

#include <DataSeriesModule.H>

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
    : sink(_sink), series(_series), outputtype(_outputtype),
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
	sink.writeExtent(cur_extent);
	cur_extent->clear();
    }
    series.newRecord();
}

void
OutputModule::flushExtent()
{
    if (cur_extent->fixeddata.size() > 0) {
	sink.writeExtent(cur_extent);
	cur_extent->clear();
    }
}

