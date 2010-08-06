// TODO-elie : use program options, use proper tyeps (int32_t, etc.),
// come up with a better name, and put invariants in the right places

#include <string>

#include <stdio.h>

#include <boost/format.hpp>

#include <Lintel/LintelLog.hpp>
#include <Lintel/RotatingHashMap.hpp>
#include <Lintel/Stats.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/commonargs.hpp>


using namespace std;
using boost::format;

const int64_t WINLIMIT = 100000;
const int64_t STEPSIZE = 1000;

// "<ExtentType namespace=\"ssd.hopl.hp.com\" name=\"Genread\" version=\"1.0\" "
// "   <field type=\"int32\" name=\"timedelta\" />\n"
// "   <field type=\"int32\" name=\"bytes\" />\n"

class Flow {
public:    
    Flow(DataSeriesModule &source) :
	upstreamModule(source),
	timedelta(series, "timedelta"),
	bytes(series, "bytes"),
	sourceExtent(NULL),
	min_time(-1), cur_time(0), max_time(-1), rotateLimit(0)
    {
	// TODO-elie : paramaterize
	winLimit = WINLIMIT;
	stepSize = STEPSIZE;
	// Process the batch on construction.
	getMore(winLimit+stepSize*1000);
    }
    
    int32_t getBytes(int64_t window) {
	SINVARIANT(window % stepSize == 0);
	if (window > rotateLimit) {
	    rotateLimit = max(cur_time, rotateLimit+winLimit+stepSize*1000);
	    windows.rotate();
	}
	if (max_time != 0 && cur_time < window+winLimit) {
	    if (getMore(cur_time+winLimit+stepSize*1000)<0) {
		fprintf(stderr, "max is %d (seconds?)", (int32_t)(max_time/1000000));
	    }
	}
	return windows[window];
    }
    
    int64_t getMax() {
	return max_time;
    }

private:
    int64_t getMore(int64_t limit) {
	if (sourceExtent == NULL) {
	    sourceExtent = upstreamModule.getExtent();
	    if (sourceExtent != NULL) {
		series.start(sourceExtent);
		if (series.more() && min_time==-1) {
		    min_time = timedelta.val();
		}
	    }
	}
	while (sourceExtent != NULL) {
	    if (!series.more()) {
		delete sourceExtent;
		sourceExtent = upstreamModule.getExtent();
		if (sourceExtent != NULL) {
		    series.start(sourceExtent);		    
		    if (series.more() && min_time==-1) {
			min_time = timedelta.val();
		    }		
		}
		continue;
	    }

	    // These will return int32 of the current val's.
	    cur_time += timedelta.val();
	    // TODO-elie : if you want to print gaps over 200ms, here
	    // is the place
	    int64_t win = (cur_time - winLimit);
	    win = win - (win % stepSize) + stepSize;
	    for (; win < cur_time; win += stepSize) {
		windows[win] += bytes.val();
	    }
	    series.next();
	    if (cur_time > limit) {
		return cur_time;
	    }
	}
	max_time = cur_time;
	return -cur_time;
    }

    DataSeriesModule &upstreamModule;
    ExtentSeries series;

    Int32Field timedelta;
    Int32Field bytes;
    
    Extent * sourceExtent;

    int64_t min_time;
    int64_t cur_time;
    int64_t max_time;
    int64_t rotateLimit;

    // Lazy circular buffer.  todo-joe :  at some point; use a true circular
    // buffer, so we're not doing all these hash lookups.  lintel
    // Deque with the at operation is probably the answer.
    RotatingHashMap<int64_t, int32_t> windows;
    int32_t winLimit, stepSize;
};

int main(int argc, char ** argv) {
    SINVARIANT(argc==2);

    int32_t numNodes = atoi(argv[1]);
    Flow *** flows;
    TypeIndexModule *** tim;
    flows = new Flow **[numNodes];
    tim = new TypeIndexModule **[numNodes];
    for(int32_t i = 0; i<numNodes; ++i) {
	flows[i] = new Flow *[numNodes];
	tim[i] = new TypeIndexModule *[numNodes];
    }

    // Open all of the ds files.
    for(int32_t i = 0; i<numNodes; ++i) {
	for(int32_t j = i+1; j<numNodes; ++j) {
	    tim[i][j] = new TypeIndexModule("Genread");
	    tim[j][i] = new TypeIndexModule("Genread");	    
	    // TODO-elie : check that the i/j and j/i ordering with c
	    // and s is correct.  I think this implies [x][y] is x
	    // sending to y
	    tim[i][j]->addSource
		((format("%d.%d.c.ds") % i % j).str());
	    tim[j][i]->addSource
		((format("%d.%d.s.ds") % i % j).str());
	    flows[i][j] = new Flow(*tim[i][j]);
	    flows[j][i] = new Flow(*tim[j][i]);
	}
    }
    
    int64_t window = 0;
    bool hadMore;
    do {
	hadMore = false;
	Stats stat;
	for(int32_t i = 0; i<numNodes; ++i) {
	    for(int32_t j = 0; j<numNodes; ++j) {
		if (i==j) {
		    continue;
		}
		int32_t bytes = flows[i][j]->getBytes(window);
		// So, what do we want to do?  For now, we can print the
		// throughput, and calulate the std. dev of throughput
		// Total bytes sent can easily be updated at this point, but
		// not gonna do so just yet.

		// TODO-elie : don't know what units you're using for
		// time, so you get it in bytes for this window.
		printf("%d ", bytes);
		stat.add(bytes);

		if(!hadMore && flows[i][j]->getMax()<0) {
		    hadMore = true;
		}
	    }
	}
	printf("%f\n", stat.variance());

	window+=STEPSIZE;
    }while(hadMore);
    
    //Eh, not gonna bother cleaning up.
    // TODO-elie : ...

}
/*
// TODO-elie : check return value of calls to fopen 
int32_t convertFile(string fileName, string outName) {
    FILE *inf = fopen(fileName.c_str(), "r");

    DataSeriesSink sink(outName, Extent::compress_gz, 7);
    ExtentTypeLibrary library;
    const ExtentType *extent_type = library.registerType(text_xml);
    ExtentSeries series;
    Int32Field f_time(series, "timedelta");
    Int32Field f_amount(series, "bytes");
    series.setType(*extent_type);
    OutputModule *outmodule = new OutputModule(sink, series, extent_type,
					       1024 * 1024);
    sink.writeExtentLibrary(library);

    int32_t time, amount;
    int32_t res;
    
    res = fscanf(inf, "%d %d", &time, &amount);
    while(res == 0) {
	fscanf(inf, "%*s");
	res = fscanf(inf, "%d %d", &time, &amount);	
	if(res==EOF) {
	    return 0;
	}
    }
    
    do {
	outmodule->newRecord();
	f_time.set(time);
	f_amount.set(amount);
	res = fscanf(inf, "%d %d", &time, &amount);		
    } while(res==2);
    fclose(inf);
    delete outmodule;
    sink.close();
    return 0;
}

*/
