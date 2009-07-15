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

#include <DataSeries/SortedIndex.hpp>

#include <DataSeries/ExtentVectorModule.hpp>
#include <DataSeries/RowAnalysisModule.hpp>

class SearchID : public RowAnalysisModule {
private:
    Int64Field packet_at;
    Int64Field record_id;
    uint64_t _id;
    
public:
    SearchID(DataSeriesModule *source, uint64_t id)
	: RowAnalysisModule(*source, ExtentSeries::typeExact),
	  packet_at(series, "packet-at"), 
	  record_id(series, "record-id"), _id(id)
    { }

    virtual void processRow()
    {
	if(packet_at.val() == _id) {
	    std::cout << record_id.val() << " ";
	}
    }
};

void doSearch(SortedIndex &index, int64_t val) {
    GeneralValue search_val;
    search_val.setInt64(val);
    std::vector<SortedIndex::IndexEntry*> *extents = index.search(search_val);
    std::cout << val << ": ";
    ExtentVectorModule evm(index.getFileNames(), extents, index.getIndexType());
    SearchID search(&evm, val);
    search.getAndDelete();
    std::cout << "\n";
}

int main(int argc, char *argv[]) {
    SortedIndex index("sortedindex.ds", "NFS trace: common", "packet-at");
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
	SortedIndex uindex("unsortedindex.ds", "NFS trace: common", "source");
    } catch (AssertBoostException &e) {
	std::cout << "Error: " << e.msg.substr(e.msg.find("nfs.set6.20k.ds")) << "\n";
    }

    return 0;
}
