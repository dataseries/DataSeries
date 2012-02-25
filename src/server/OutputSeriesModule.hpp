#ifndef DATASERIES_OUTPUTSERIESMODULE_HPP
#define DATASERIES_OUTPUTSERIESMODULE_HPP

#include <DataSeries/DataSeriesModule.hpp>

class OutputSeriesModule : public DataSeriesModule {
public:
    typedef boost::shared_ptr<OutputSeriesModule> OSMPtr;

    ExtentSeries output_series;

    Extent::Ptr returnOutputSeries() {
        Extent::Ptr ret = output_series.getSharedExtent();
        output_series.clearExtent();
        return ret;
    }

};

#endif
