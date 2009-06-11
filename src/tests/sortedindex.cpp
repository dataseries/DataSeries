//
// (c) Copyright 2009, Hewlett-Packard Development Company, LP
//
//  See the file named COPYING for license details
//
// test program for SortedIndexModule
#include <iostream>

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
	Extent *e = index.getExtent();
	if (e == NULL) {
	    break;
	}
	series.setExtent(e);
	for (; series.pos.morerecords(); ++series.pos) {
	    if (packet_at.val() == val) {
		std::cout << record_id.val() << " ";
	    }
	}
	delete e;
    }
    std::cout << "\n";
}

int main(int argc, char *argv[]) {
    SortedIndexModule index("sortedindex.ds", "NFS trace: common", "packet-at");
    // TODO-aveitch: pick one at the beginning, middle, end, and non-existent
    doSearch(index, 1063931188268856000LL);
    doSearch(index, 1063931191484179000LL);

    // TODO-aveitch: add a test for unsorted
    return 0;
}
