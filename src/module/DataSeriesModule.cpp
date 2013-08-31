// -*-C++-*-
/*
  (c) Copyright 2003-2012, Hewlett-Packard Development Company, LP

  See the file named COPYING for license details
*/

/** @file
    implementation
*/

#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>

#define DS_RAW_EXTENT_PTR_DEPRECATED /* allowed */
#define DSM_VAR_DEPRECATED /* allowed */
#include <DataSeries/DataSeriesModule.hpp>

namespace dataseries { namespace hack {
        Extent *releaseExtentSharedPtr(boost::shared_ptr<Extent> &p, Extent *e);
        size_t extentSharedPtrSize();
    }}

using namespace dataseries;

DataSeriesModule::~DataSeriesModule() { }

Extent *DataSeriesModule::getExtent() {
    Extent::Ptr e(getSharedExtent());
    if (e == NULL) {
        return NULL;
    } else {
        SINVARIANT(e->extent_source_offset != -2);
        SINVARIANT(dataseries::hack::extentSharedPtrSize() == sizeof(e));
        Extent *ret = dataseries::hack::releaseExtentSharedPtr(e, e.get());
        SINVARIANT(e == NULL && ret->extent_source_offset != -2);
        return ret;
    }
}

Extent::Ptr DataSeriesModule::getSharedExtent() {
    Extent *e = getExtent();
    if (e == NULL) {
        return Extent::Ptr();
    } else {
        SINVARIANT(e->extent_source_offset != -2);
    }
    try {
        Extent::Ptr p = e->shared_from_this();
        FATAL_ERROR(boost::format("Extent %p is not supposed to be a shared pointer but is")
                    % static_cast<void *>(this));
    } catch (std::exception &) {
        // ok
    }
    Extent::Ptr ret(e);
    return ret;
}

void DataSeriesModule::getAndDelete() {
    while (true) {
        Extent *e = getExtent();
        if (e == NULL) return;
        delete e;
    }
}

void DataSeriesModule::getAndDeleteShared() {
    Extent::Ptr e;
    do {
        e = getSharedExtent();
    } while (e != NULL);
}

SourceModule::SourceModule()
        : total_uncompressed_bytes(0), total_compressed_bytes(0)
{ }

SourceModule::~SourceModule() { }

static inline double timediff(struct timeval &end, struct timeval &start) {
    return end.tv_sec - start.tv_sec + (end.tv_usec - start.tv_usec) * 1e-6;
}

FilterModule::FilterModule(DataSeriesModule &_from, 
                           const std::string &_type_prefix)
        : from(_from), type_prefix(_type_prefix)
{ }

FilterModule::~FilterModule() { }

Extent::Ptr FilterModule::getSharedExtent() {
    while (true) {
        Extent::Ptr e = from.getSharedExtent();
        if (e == NULL || prefixequal(e->type->getName(), type_prefix)) {
            return e;
        }
    }
}

OutputModule::OutputModule(IExtentSink &sink, ExtentSeries &series,
                           const ExtentType *in_outputtype, 
                           int target_extent_size)
        : outputtype(*in_outputtype),
          target_extent_size(target_extent_size),
          sink(sink), series(series)
{
    SINVARIANT(&series != NULL);
    INVARIANT(&outputtype != NULL, "can't create output module without type");
    INVARIANT(!series.hasExtent(),
              "series specified for output module already had an extent");
    series.setType(outputtype.shared_from_this());
    series.newExtent();
    cur_extent = series.getSharedExtent();
}

OutputModule::OutputModule(IExtentSink &sink, ExtentSeries &series,
                           const ExtentType &in_outputtype, 
                           int target_extent_size)
        : outputtype(in_outputtype),
          target_extent_size(target_extent_size),
          sink(sink), series(series)
{
    SINVARIANT(&series != NULL);
    INVARIANT(&outputtype != NULL, "can't create output module without type");
    INVARIANT(!series.hasExtent(),
              "series specified for output module already had an extent");
    series.setType(outputtype.shared_from_this());
    series.newExtent();
    cur_extent = series.getSharedExtent();
}

OutputModule::OutputModule(IExtentSink &sink, ExtentSeries &series,
                           const ExtentType::Ptr in_outputtype, 
                           int target_extent_size)
        : outputtype(*in_outputtype), target_extent_size(target_extent_size),
          sink(sink), series(series)
{
    SINVARIANT(&series != NULL);
    INVARIANT(&outputtype != NULL, "can't create output module without type");
    INVARIANT(!series.hasExtent(),
              "series specified for output module already had an extent");
    series.setType(in_outputtype);
    series.newExtent();
    cur_extent = series.getSharedExtent();
}

OutputModule::~OutputModule() {
    if (cur_extent != NULL) {
        close();
    }
    sink.removeStatsUpdate(&stats);
}

void OutputModule::newRecord() {
    INVARIANT(series.hasExtent() && cur_extent != NULL, "called newRecord() after close()");
    INVARIANT(series.getSharedExtent() == cur_extent,
              "usage error, someone else changed the series extent");
    if ((cur_extent->size() + outputtype.fixedrecordsize()) > target_extent_size) {
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

void OutputModule::flushExtent() {
    INVARIANT(cur_extent != NULL, "??");
    if (cur_extent->fixeddata.size() > 0) {
        sink.writeExtent(*cur_extent, &stats);
        cur_extent->clear();
    }
}

void OutputModule::close() {
    Extent::Ptr old_extent = cur_extent;
    cur_extent.reset();
    series.clearExtent();

    if (old_extent->fixeddata.size() > 0) {
        sink.writeExtent(*old_extent, &stats);
    }
}

IExtentSink::Stats OutputModule::getStats() {
    return sink.getStats(&stats);
}

void OutputModule::printStats(std::ostream &to) {
    getStats().printText(to, outputtype.getName());
}

ExtentType::Ptr OutputModule::getOutputType() {
    return outputtype.shared_from_this();
}
