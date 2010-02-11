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

/* Performance notes:
   2 dual core Intel 3GhZ Xeon processors (4 cores total), 5GB memory

(LINTEL_STATS)
time srtstat2 bytes device_number -- select-bytes-dev.none.ds  >/tmp/f
real    0m8.523s user    0m9.509s sys     0m2.512s
real    0m8.568s user    0m9.573s sys     0m2.532s
real    0m8.418s user    0m9.501s sys     0m2.744s

(DIRECT_STATS)
time srtstat2 bytes device_number -- select-bytes-dev.none.ds  >/tmp/f
real    0m5.554s user    0m6.444s sys     0m2.680s
real    0m5.609s user    0m6.268s sys     0m2.624s
real    0m5.477s user    0m6.500s sys     0m2.764s

time srtstat2 bytes device_number -- select-bytes-dev.lzo.ds  >/tmp/f
real    0m5.428s user    0m7.020s sys     0m1.188s
real    0m5.438s user    0m7.100s sys     0m1.056s
real    0m5.407s user    0m6.980s sys     0m1.188s

*/

#include <string>
#include <malloc.h>

#include <boost/format.hpp>

#include <Lintel/Stats.hpp>
#include <Lintel/HashMap.hpp>
#include <Lintel/AssertBoost.hpp>

#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/PrefetchBufferModule.hpp>
#include <DataSeries/SequenceModule.hpp>

using namespace std;

struct HashMap_hashintfast {
    unsigned operator()(const int _a) const {
	return _a;
    }
};

#define DIRECT_STATS 1
#define LINTEL_STATS 0

int foo;
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

    struct GStats {
#if DIRECT_STATS
      double sum;
      double count;
#endif
#if LINTEL_STATS
      Stats *stat;
#endif
      GStats() {
#if DIRECT_STATS
	sum = count = 0;
#endif
#if LINTEL_STATS
	stat = new Stats();
	// ++foo; INVARIANT(foo < 100000, "bad");
#endif
      }
      void add(double v) {
#if DIRECT_STATS
	sum += v;
	count += 1;
#endif
#if LINTEL_STATS
	stat->add(v);
#endif
      }
    };
    typedef HashMap<ExtentType::int32, GStats *, HashMap_hashintfast> mytableT;

    virtual ~IntGroupByInt() { }
    
    virtual void processRow() {
	GStats **statp = mystats.lookup(groupby.val());
	GStats *stat;
	if (statp == NULL) {
	    stat = new GStats();
	    mystats[groupby.val()] = stat;
	} else {
	  stat = *statp;
	}
	stat->add(value.val());
    }

    virtual Extent *getExtent() {
      Extent *e = source.getExtent();

      if (e == NULL) {
	completeProcessing();
	return NULL;
      }
      series.setExtent(e);

      if (!prepared) {
	prepareForProcessing();
	prepared = true;
      }
      for(;series.pos.morerecords();++series.pos) {
	processRow();
      }
      return e;
    }

    virtual void printResult() {
#if DIRECT_STATS
	cout << boost::format("%s, mean(%s)\n")
	    % groupby_field % value_field;
	for(mytableT::iterator i = mystats.begin(); 
	    i != mystats.end(); ++i) {
	    cout << boost::format("%d, %.15g\n") 
	      % static_cast<int32_t>(i->first) // % i->second->count
	      % (i->second->sum / i->second->count);
	}
#endif

#if LINTEL_STATS
	cout << boost::format("%s, count(*), mean(%s), stddev, min, max\n")
	    % groupby_field % value_field;
	for(mytableT::iterator i = mystats.begin(); 
	    i != mystats.end(); ++i) {
	    cout << boost::format("%d, %d, %.6g, %.6g, %.6g, %.6g\n") 
		% static_cast<int32_t>(i->first) % i->second->stat->count() 
		% i->second->stat->mean() % i->second->stat->stddev()
		% i->second->stat->min() % i->second->stat->max();
	}
#endif
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

    mallopt(M_MMAP_THRESHOLD, 1024 * 1024);
    mallopt(M_TRIM_THRESHOLD, 1024 * 1024);
    mallopt(M_MMAP_MAX, 0);
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
    seq.getAndDelete();
    
    RowAnalysisModule::printAllResults(seq);

    printf("extents: %.2f MB -> %.2f MB\n",
	   (double)(source.total_compressed_bytes)/(1024.0*1024),
	   (double)(source.total_uncompressed_bytes)/(1024.0*1024));
    printf("                   common\n");
    printf("MB compressed:   %8.2f\n",
	   (double)source.total_compressed_bytes/(1024.0*1024));
    printf("MB uncompressed: %8.2f\n",
	   (double)source.total_uncompressed_bytes/(1024.0*1024));
    printf("wait fraction :  %8.2f\n",
	   source.waitFraction());
    
    return 0;
}
