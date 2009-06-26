//
// (c) Copyright 2009, Hewlett-Packard Development Company, LP
//
//  See the file named COPYING for license details
//
// test program for SortedIndexModule
#include <iostream>
#include <string>

#include <boost/scoped_ptr.hpp>

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
    
    // using an unsorted index is an error
    AssertBoostFnBefore(AssertBoostThrowExceptionFn);
    try {
	SortedIndexModule uindex("unsortedindex.ds", "NFS trace: common", "source");
    } catch (AssertBoostException &e) {
	std::cout << "Error: " << e.msg.substr(e.msg.find("nfs.set6.20k.ds")) << "\n";
    }

    return 0;
}
