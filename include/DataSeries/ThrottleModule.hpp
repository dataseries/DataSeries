#ifndef DATASERIES_THROTTLEMODULE_H
#define DATASERIES_THROTTLEMODULE_H

#include <Lintel/LintelLog.hpp>

#include <DataSeries/DataSeriesModule.hpp>

class ThrottleModule : public DataSeriesModule {
public:
    ThrottleModule(DataSeriesModule &upstream_module, size_t size_limit);
    Extent *getExtent();
    bool limitReached();
    void reset();

private:
    DataSeriesModule &upstream_module;
    size_t size_limit;
    size_t total_size;
    size_t extent_size;
};

ThrottleModule::ThrottleModule(DataSeriesModule &upstream_module, size_t size_limit)
    : upstream_module(upstream_module), size_limit(size_limit), total_size(0), extent_size(0) { }

Extent *ThrottleModule::getExtent() {
    if (limitReached()) {
        LintelLogDebug("ThrottleModule",
                       boost::format("Full buffer (%s bytes).") % total_size);
        return NULL;
    }
    Extent *extent = upstream_module.getExtent();
    if (extent != NULL) {
        if (extent_size == 0) {
            extent_size = extent->size();
        }
        total_size += extent->size();
    } else {
        LintelLogDebug("ThrottleModule",
                       boost::format("Partially full buffer (%s bytes).") % total_size);
    }
    return extent;
}

void ThrottleModule::reset() {
    total_size = 0;
    extent_size = 0;
}

bool ThrottleModule::limitReached() {
    return total_size + extent_size > size_limit;
}

#endif
