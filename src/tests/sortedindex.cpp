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

void doSearch(SortedIndexModule &index, int64_t val) {
    GeneralValue search_val;
    search_val.setInt64(val);
    index.search(search_val);
    ExtentSeries series;
    Int64Field packet_at(series, "packet-at");
    Int64Field record_id(series, "record-id");
    std::cout << val << ": ";
    while (true) {
	boost::scoped_ptr<Extent> e(index.getExtent());
	if (!e) {
	    break;
	}
	series.setExtent(e.get());
	for (; series.pos.morerecords(); ++series.pos) {
	    if (packet_at.val() == val) {
		std::cout << record_id.val() << " ";
	    }
	}
    }
    std::cout << "\n";
}

void doSetSearch(SortedIndexModule &index, std::vector<GeneralValue> vals) {
    index.searchSet(vals);

    ExtentSeries series;
    Int64Field packet_at(series, "packet-at");
    Int64Field record_id(series, "record-id");
    while(true) {
	boost::scoped_ptr<Extent> e(index.getExtent());
	if (!e) {
	    break;
	}
	series.setExtent(e.get());
	for (; series.pos.morerecords(); ++series.pos) {
            BOOST_FOREACH(GeneralValue &val, vals) {
                if (packet_at.val() == val.valInt64()) {
                    std::cout << val.valInt64() << ":\t" << record_id.val() << "\n";
                }
            }
	}
    }
}

void doRangeSearch(SortedIndexModule &index, int64_t min, int64_t max) {
    GeneralValue min_val, max_val;
    min_val.setInt64(min);
    max_val.setInt64(max);

    index.searchRange(min_val, max_val);

    ExtentSeries series;
    Int64Field packet_at(series, "packet-at");
    Int64Field record_id(series, "record-id");
    while(true) {
	boost::scoped_ptr<Extent> e(index.getExtent());
	if (!e) {
	    break;
	}
	series.setExtent(e.get());
	for (; series.pos.morerecords(); ++series.pos) {
            if (packet_at.val() >= min && packet_at.val() <= max) {
                std::cout << packet_at.val() << ":\t" << record_id.val() << "\n";
            }
	}
    }
}

int main(int argc, char *argv[]) {
    SortedIndexModule index("sortedindex.ds", "NFS trace: common", "packet-at");
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
    std::vector<GeneralValue> set_values;
    set_values.resize(10);
    set_values[0].setInt64(1063931188266052000LL);
    set_values[1].setInt64(1063931188271036000LL);
    set_values[2].setInt64(1063931188607278000LL);
    set_values[3].setInt64(1063931190602259000LL);
    set_values[4].setInt64(1063931191880891000LL);
    set_values[5].setInt64(1063931192724801000LL);
    set_values[6].setInt64(1063931192803710000LL);
    set_values[7].setInt64(1063931192806206000LL);
    set_values[8].setInt64(1063931190284050000LL);
    set_values[9].setInt64(1063931191880891001LL);
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
	SortedIndexModule uindex("unsortedindex.ds", "NFS trace: common", "source");
    } catch (AssertBoostException &e) {
	std::cout << "Error: " << e.msg.substr(e.msg.find("nfs.set6.20k.ds")) << "\n";
    }

    return 0;
}
