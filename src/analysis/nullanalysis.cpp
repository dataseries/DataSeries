/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <string>
#include <algorithm>
#include <memory>
#include <iostream>

#include <boost/format.hpp>
#include <boost/foreach.hpp>

#include <Lintel/LintelLog.hpp>
#include <Lintel/ProgramOptions.hpp>
#include <Lintel/Clock.hpp>

#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentWriter.hpp>
#include <DataSeries/MemorySortModule.hpp>
#include <DataSeries/SortModule.hpp>
#include <DataSeries/ParallelSortModule.hpp>
#include <DataSeries/ParallelRadixSortModule.hpp>
#include <DataSeries/MemoryRadixSortModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/ThrottleModule.hpp>

using namespace std;

int main(int argc, char *argv[]) {
    LintelLog::parseEnv();

    Clock::Tfrac start_clock = Clock::todTfrac();
    TypeIndexModule input_module(argv[1]);
    input_module.addSource(argv[2]);
    uint64_t count = 0;
    uint64_t size = 0;
    uint64_t extent_size = 0;
    uint64_t size_limit = 1;
    size_limit = size_limit << 30;
    ThrottleModule throttle_module(input_module, size_limit);
    vector<Extent*> extents;
    extents.reserve(20000);
    while (true) {
        Extent *extent = throttle_module.getExtent();
        if (extent == NULL) {
            if (throttle_module.limitReached()) {
                cout << (boost::format("%s extents in buffer.") % extents.size()) << endl;
                BOOST_FOREACH(Extent *extent, extents) {
                    delete extent;
                }
                extents.resize(0);
                extents.reserve(20000);
                throttle_module.reset();
                continue;
            }
            break;
        }
        //delete extent;
        extents.push_back(extent);
        //cout << (boost::format("Read an extent of %s bytes") % extent->size()) << endl;
        if (extent_size != extent->size()) {
            extent_size = extent->size();
            cout << (boost::format("New extent size: %s") % extent_size) << endl;
        }
        size += extent_size;
        ++count;
    }
    Clock::Tfrac stop_clock = Clock::todTfrac();
    double seconds = Clock::TfracToDouble(stop_clock - start_clock);
    size /= (1 << 20);
    double throughput = (double)size / seconds;
    cout << (boost::format("Read %s extents (%s MB, %s seconds, %s MB/s) from '%s'.") % count % size % seconds % throughput % argv[2]) << endl;
    return 0;
}
