//
// (c) Copyright 2009, Hewlett-Packard Development Company, LP
//
//  See the file named COPYING for license details
//
// test program for SortedIndexModule
#include <iostream>
#include <string>

#include <boost/scoped_ptr.hpp>
#include <boost/foreach.hpp>

#include <Lintel/AssertBoost.hpp>

#include <DataSeries/SortedIndexModule.hpp>

struct myfield {
    std::string name;
    Int64Field field;
};

void doSearch(const SortedIndexModule<int64_t,Int64Field>::Index &index, int64_t val) {    
    SortedIndexModule<int64_t,Int64Field> sim(index, val);
    ExtentSeries series;
    Int64Field packet_at(series, "packet-at");
    Int64Field record_id(series, "record-id");
    std::cout << val << ": ";
    while (true) {
        boost::shared_ptr<Extent> e(sim.getSharedExtent());
        if (!e) {
            break;
        }
        series.setExtent(e);
        for (; series.morerecords(); ++series) {
            if (packet_at.val() == val) {
                std::cout << record_id.val() << " ";
            }
        }
    }
    std::cout << "\n";
}

void doSetSearch(const SortedIndexModule<int64_t,Int64Field>::Index &index, 
                 const std::vector<int64_t> &vals) {
    SortedIndexModule<int64_t,Int64Field> sim(index, vals);

    ExtentSeries series;
    Int64Field packet_at(series, "packet-at");
    Int64Field record_id(series, "record-id");
    while (true) {
        boost::shared_ptr<Extent> e(sim.getSharedExtent());
        if (!e) {
            break;
        }
        series.setExtent(e);
        for (; series.morerecords(); ++series) {
            BOOST_FOREACH(const int64_t val, vals) {
                if (packet_at.val() == val) {
                    std::cout << val << ":\t" << record_id.val() << "\n";
                }
            }
        }
    }
}

void doRangeSearch(const SortedIndexModule<int64_t,Int64Field>::Index &index, 
                   int64_t min, int64_t max) {
    SortedIndexModule<int64_t,Int64Field> sim(index, min, max);

    ExtentSeries series;
    Int64Field packet_at(series, "packet-at");
    Int64Field record_id(series, "record-id");
    while (true) {
        boost::shared_ptr<Extent> e(sim.getSharedExtent());
        if (!e) {
            break;
        }
        series.setExtent(e);
        for (; series.morerecords(); ++series) {
            if (packet_at.val() >= min && packet_at.val() <= max) {
                std::cout << packet_at.val() << ":\t" << record_id.val() << "\n";
            }
        }
    }
}

int main(int argc, char *argv[]) {
    SortedIndexModule<int64_t,Int64Field>::Index 
            index("sortedindex.ds", "NFS trace: common", "packet-at");

    // These are in the first extent
    doSearch(index, 1063931188266052000LL);
    doSearch(index, 1063931188271036000LL);
    doSearch(index, 1063931188607278000LL);
    // these are in middle extents
    doSearch(index, 1063931190602259000LL);
    doSearch(index, 1063931191880891000LL);
    doSearch(index, 1063931192724801000LL);
    // these are in the end extent
    doSearch(index, 1063931192803710000LL);
    doSearch(index, 1063931192806206000LL);
    // these don't exist
    doSearch(index, 1063931190284050000LL);
    doSearch(index, 1063931191880891001LL);

    // set up the same searches in a set
    std::vector<int64_t> set_values;
    set_values.resize(10);
    set_values[0] = 1063931188266052000LL;
    set_values[1] = 1063931188271036000LL;
    set_values[2] = 1063931188607278000LL;
    set_values[3] = 1063931190602259000LL;
    set_values[4] = 1063931191880891000LL;
    set_values[5] = 1063931192724801000LL;
    set_values[6] = 1063931192803710000LL;
    set_values[7] = 1063931192806206000LL;
    set_values[8] = 1063931190284050000LL;
    set_values[9] = 1063931191880891001LL;
    doSetSearch(index, set_values);

    // set up range searches
    // These are in the first extent
    doRangeSearch(index, 1063931188266052000LL, 1063931188266052000LL);
    doRangeSearch(index, 1063931188271035999LL, 1063931188271036001LL);
    doRangeSearch(index, 1063931188607278000LL, 1063931188607278001LL);
    // these are in middle extents
    doRangeSearch(index, 1063931190602258999LL, 1063931190602259000LL);
    doRangeSearch(index, 1063931191880891000LL, 1063931191880891000LL);
    doRangeSearch(index, 1063931192724801000LL, 1063931192724801000LL);
    // these are in the end extent
    doRangeSearch(index, 1063931192803710000LL, 1063931192803710000LL);
    doRangeSearch(index, 1063931192806206000LL, 1063931192806206000LL);
    // these don't exist
    doRangeSearch(index, 1063931190284050000LL, 1063931190284050000LL);
    doRangeSearch(index, 1063931191880891001LL, 1063931191880891001LL);

    // using an unsorted index is an error
    AssertBoostFnBefore(AssertBoostThrowExceptionFn);
    try {
        SortedIndexModule<int32_t,Int32Field>::Index uindex("unsortedindex.ds", "NFS trace: common", "source");
    } catch (AssertBoostException &e) {
        std::cout << "Error: " << e.msg.substr(e.msg.find("nfs.set6.20k.ds")) << "\n";
    }

    return 0;
}
