// -*-C++-*-
/*
   (c) Copyright 2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    SRT-specfic statistic calculation, for purpose of the DataSeries paper
    Special cased to match with what c-store can do.

srtstat2 bytes device_number bytes logical_volume_number bytes machine_id -- 

dsselect --compress-lzo Trace::BlockIO::SRT::V7 bytes,machine_id,device_number,driver_type,thread_id,queue_length,pid,logical_volume_number ~/hourly-all.lzo.ds ~/select-all.lzo.ds

*/

#include <string>

#include <boost/format.hpp>

#include <Lintel/Stats.H>
#include <Lintel/HashMap.H>
#include <Lintel/AssertBoost.H>

#include <DataSeries/RowAnalysisModule.H>
#include <DataSeries/TypeIndexModule.H>
#include <DataSeries/PrefetchBufferModule.H>
#include <DataSeries/SequenceModule.H>

using namespace std;

struct HashMap_hashintfast {
    unsigned operator()(const int _a) const {
	return _a;
    }
};

class IntGroupByInt : public RowAnalysisModule {
public:
    IntGroupByInt(DataSeriesModule &source, 
		  const string &_value_field, const string &_groupby_field)
	: RowAnalysisModule(source), 
	  value_field(_value_field),
	  groupby_field(_groupby_field),
	  value(series, value_field),
	  groupby(series, groupby_field)
    {
    }

    typedef HashMap<ExtentType::int32, Stats *, HashMap_hashintfast> mytableT;

    virtual ~IntGroupByInt() { }
    
    virtual void processRow() {
	Stats *stat = mystats[groupby.val()];
	if (stat == NULL) {
	    stat = new Stats();
	    mystats[groupby.val()] = stat;
	}
	stat->add(value.val());
    }

    virtual void printResult() {
	cout << boost::format("%s, count(*), mean(%s), stddev, min, max\n")
	    % groupby_field % value_field;
	for(mytableT::iterator i = mystats.begin(); 
	    i != mystats.end(); ++i) {
	    cout << boost::format("%u, %d, %.6g, %.6g, %.6g, %.6g\n") 
		% static_cast<uint32_t>(i->first) % i->second->count() 
		% i->second->mean() % i->second->stddev()
		% i->second->min() % i->second->max();
	}
    }

    mytableT mystats;
    string value_field, groupby_field;
    Int32Field value, groupby;
};

int
main(int argc, char *argv[]) 
{
    TypeIndexModule source("Trace::BlockIO::SRT");
    PrefetchBufferModule *prefetch = new PrefetchBufferModule(source,64*1024*1024);

    SequenceModule seq(prefetch);
    int argpos = 1;
    for(argpos = 1; argpos < argc; ) {
	if (strcmp(argv[argpos],"--") == 0) {
	    ++argpos;
	    break;
	}
	INVARIANT(argpos + 2 < argc, "not enough args");
	seq.addModule(new IntGroupByInt(seq.tail(), argv[argpos], argv[argpos+1]));
	argpos += 2;
    }
    INVARIANT(argpos < argc, "missing files??");

    for(; argpos<argc; ++argpos) {
	source.addSource(argv[argpos]);
    }
    DataSeriesModule::getAndDelete(seq);
    
    RowAnalysisModule::printAllResults(seq,1);

    printf("extents: %.2f MB -> %.2f MB in %.2f secs decode time\n",
	   (double)(source.total_compressed_bytes)/(1024.0*1024),
	   (double)(source.total_uncompressed_bytes)/(1024.0*1024),
	   source.decode_time);
    printf("                   common\n");
    printf("MB compressed:   %8.2f\n",
	   (double)source.total_compressed_bytes/(1024.0*1024));
    printf("MB uncompressed: %8.2f\n",
	   (double)source.total_uncompressed_bytes/(1024.0*1024));
    printf("decode seconds:  %8.2f\n",
	   source.decode_time);
    printf("wait fraction :  %8.2f\n",
	   source.waitFraction());
    
    return 0;
}
