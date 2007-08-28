// -*-C++-*-
/*
   (c) Copyright 2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    SRT-specfic statistic calculation, for purpose of the DataSeries paper
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

class LatencyGroupByInt : public RowAnalysisModule {
public:
    LatencyGroupByInt(DataSeriesModule &source, 
		      const string &_start_field, const string &_end_field, const string &_groupby_field)
	: RowAnalysisModule(source), 
	  start_field(_start_field),
	  end_field(_end_field),
	  groupby_field(_groupby_field),
	  start_time(series, start_field, DoubleField::flag_allownonzerobase), 
	  end_time(series, end_field, DoubleField::flag_allownonzerobase), 
	  groupby(series, groupby_field, DoubleField::flag_allownonzerobase)
    {
    }

    typedef HashMap<ExtentType::int32, Stats *, HashMap_hashintfast> mytableT;

    virtual ~LatencyGroupByInt() { }
    
    virtual void processRow() {
	Stats *stat = mystats[groupby.val()];
	if (stat == NULL) {
	    stat = new Stats();
	    mystats[groupby.val()] = stat;
	}
	stat->add(end_time.val() - start_time.val());
    }

    virtual void printResult() {
	cout << boost::format("%s, count(*), mean(%s - %s), stddev, min, max")
	    % groupby_field % end_field % start_field << endl;
	for(mytableT::iterator i = mystats.begin(); 
	    i != mystats.end(); ++i) {
	    cout << boost::format("%d, %d, %.6g, %.6g, %.6g, %.6g") 
		% i->first % i->second->count() % i->second->mean() % i->second->stddev()
		% i->second->min() % i->second->max() 
		 << endl;
	}
    }

    mytableT mystats;
    string start_field, end_field, groupby_field;
    DoubleField start_time, end_time;
    Int32Field groupby;
};

int
main(int argc, char *argv[]) 
{
    //   TypeIndexModule source("Trace::BlockIO::SRT");
    TypeIndexModule source("I/O trace: SRT-V7");
    PrefetchBufferModule *prefetch = new PrefetchBufferModule(source,64*1024*1024);

    SequenceModule seq(prefetch);
    while(1) {
	int opt = getopt(argc, argv, "123456789");
	if (opt == -1) break;
	switch(opt) 
	    {
	    case '1': 
		seq.addModule(new LatencyGroupByInt(seq.tail(),"enter_driver","leave_driver","device_number")); 
		break;
	    case '2': 
		seq.addModule(new LatencyGroupByInt(seq.tail(),"enter_driver","leave_driver","logical_volume_number")); 
		break;
	    case '3': 
		seq.addModule(new LatencyGroupByInt(seq.tail(),"enter_driver","leave_driver","bytes")); 
		break;

	    case '4': 
		seq.addModule(new LatencyGroupByInt(seq.tail(),"enter_driver","return_to_driver","device_number")); 
		break;
	    case '5': 
		seq.addModule(new LatencyGroupByInt(seq.tail(),"enter_driver","return_to_driver","logical_volume_number")); 
		break;
	    case '6': 
		seq.addModule(new LatencyGroupByInt(seq.tail(),"enter_driver","return_to_driver","bytes")); 
		break;

	    case '7': 
		seq.addModule(new LatencyGroupByInt(seq.tail(),"leave_driver","return_to_driver","device_number")); 
		break;
	    case '8': 
		seq.addModule(new LatencyGroupByInt(seq.tail(),"leave_driver","return_to_driver","logical_volume_number")); 
		break;
	    case '9': 
		seq.addModule(new LatencyGroupByInt(seq.tail(),"leave_driver","return_to_driver","bytes")); 
		break;
	    default: INVARIANT(false, "bad");
	    }
    }

    for(int i=optind; i<argc; ++i) {
	source.addSource(argv[i]);
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
