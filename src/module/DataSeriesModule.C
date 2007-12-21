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
DataSeriesModule::getAndDelete()
{
    while(true) {
	Extent *e = getExtent();
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
	if (ExtentType::prefixmatch(e->type.getName(), type_prefix))
	    return e;
	delete e;
    }
}

OutputModule::OutputModule(DataSeriesSink &_sink, ExtentSeries &_series,
			   const ExtentType *_outputtype, int _target_extent_size)
    : outputtype(_outputtype),
      target_extent_size(_target_extent_size),
      sink(_sink), series(_series)
{
    INVARIANT(outputtype != NULL, "can't create output module without type");
    INVARIANT(series.curExtent() == NULL,
	      "series specified for output module already had an extent");
    cur_extent = new Extent(*outputtype);
    series.setExtent(cur_extent);
}

OutputModule::~OutputModule()
{
    if (cur_extent != NULL) {
	close();
    }
    sink.removeStatsUpdate(&stats);
}

void
OutputModule::newRecord()
{
    INVARIANT(series.curExtent() == cur_extent,
	      "usage error, someone else changed the series extent");
    INVARIANT(cur_extent != NULL, "called newRecord() after close()");
    if ((int)(cur_extent->extentsize() + outputtype->fixedrecordsize()) > target_extent_size) {
	double fixedsize = cur_extent->fixeddata.size();
	double variablesize = cur_extent->variabledata.size();
	double sumsize = fixedsize + variablesize;
	double fixedfrac = fixedsize / sumsize;
	double variablefrac = variablesize / sumsize;
	flushExtent();
	double inflate_size = 1.1 * target_extent_size; // a little extra
	size_t fixed = static_cast<size_t>(inflate_size * fixedfrac);
	cur_extent->fixeddata.reserve(fixed);
	size_t variable = static_cast<size_t>(inflate_size * variablefrac);
	cur_extent->variabledata.reserve(variable);
    }
    series.newRecord();
}

void
OutputModule::flushExtent()
{
    INVARIANT(cur_extent != NULL, "??");
    if (cur_extent->fixeddata.size() > 0) {
	stats.unpacked_variable_raw += cur_extent->variabledata.size();

	sink.writeExtent(*cur_extent, &stats);
	cur_extent->clear();
    }
}

void
OutputModule::close()
{
    Extent *old_extent = cur_extent;
    cur_extent = NULL;
    series.clearExtent();

    if (old_extent->fixeddata.size() > 0) {
	stats.unpacked_variable_raw += old_extent->variabledata.size();

	sink.writeExtent(*old_extent, &stats);
    }
    delete old_extent;
}


void
OutputModule::printStats(std::ostream &to)
{
    getStats().printText(to, outputtype->name);
}
