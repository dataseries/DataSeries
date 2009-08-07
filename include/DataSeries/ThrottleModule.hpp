#ifndef DATASERIES_THROTTLEMODULE_H
#define DATASERIES_THROTTLEMODULE_H

#include <Lintel/Clock.hpp>
#include <Lintel/LintelLog.hpp>

#include <DataSeries/DataSeriesModule.hpp>

class ThrottleModule : public DataSeriesModule {
public:
    ThrottleModule(DataSeriesModule &upstream_module, uint64_t size_limit);
    Extent *getExtent();
    bool limitReached();
    void reset();

private:
    DataSeriesModule &upstream_module;
    uint64_t size_limit;
    uint64_t total_size;
    uint64_t extent_size;

    Clock::Tfrac start_clock;
    Clock::Tfrac stop_clock;
};

ThrottleModule::ThrottleModule(DataSeriesModule &upstream_module, uint64_t size_limit)
    : upstream_module(upstream_module), size_limit(size_limit), total_size(0), extent_size(0) {
    LintelLogDebug("ThrottleModule", boost::format("Size limit set to %s.") % size_limit);
    reset();
}

Extent *ThrottleModule::getExtent() {
    if (limitReached()) {
        stop_clock = Clock::todTfrac();
        double throughput = (double)total_size / Clock::TfracToDouble(stop_clock - start_clock) / (1 << 20);
        LintelLogDebug("ThrottleModule",
                       boost::format("Full buffer (%s bytes @ %s MB/s).") % total_size % throughput);
        return NULL;
    }
    Extent *extent = upstream_module.getExtent();
    if (extent != NULL) {
        if (extent_size == 0) {
            extent_size = extent->size();
        }
        total_size += extent->size();
    } else {
        double throughput = (double)total_size / Clock::TfracToDouble(stop_clock - start_clock) / (1 << 20);
        LintelLogDebug("ThrottleModule",
                       boost::format("Partially full buffer (%s bytes @ %s MB/s).") % total_size % throughput);
    }
    return extent;
}

void ThrottleModule::reset() {
    total_size = 0;
    extent_size = 0;
    start_clock = Clock::todTfrac();
}

bool ThrottleModule::limitReached() {
    return total_size + extent_size > size_limit;
}

#endif
