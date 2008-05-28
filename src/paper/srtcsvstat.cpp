// -*-C++-*-
/*
   (c) Copyright 2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file

    SRT-specfic statistic calculation over a DS converted CSV file,
    for purpose of the DataSeries paper.  This is suitably
    optimized. According to valgrind, we're spending about 40% of the
    instructions in strtod, 7% in strtol, 14% in fgets, 17% in the
    loop finding the ,'s.  Could probably optimize a little more by
    doing our own fgets.  Could use mmap if we were reading from an
    uncompressed file and were very careful about scanning through the
    file to handle files bigger than namespace.  Might be able to use
    mmap with compressed files with the same problem of carefulness.
*/

#include <string>
#include <inttypes.h>

#include <boost/format.hpp>

#include <Lintel/Stats.hpp>
#include <Lintel/HashMap.hpp>
#include <Lintel/AssertBoost.hpp>

using namespace std;

struct HashMap_hashintfast {
    unsigned operator()(const int _a) const {
	return _a;
    }
};

struct csvline {
    vector<char *> fields;
    vector<double> asdouble;
    vector<int> asint;
    vector<int> converted_flags;
    
    csvline() { 
	fields.resize(42);
	asdouble.resize(42);
	asint.resize(42);
	converted_flags.resize(42);
    }

    void set(char *line) {
	unsigned i = 0;
	char * startbit = line;
	while(1) {
	    while (*line != ',' && *line != '\0') {
		++line;
	    }
	    INVARIANT(i < 42, "bad");
	    fields[i] = startbit;
	    converted_flags[i] = 0;
	    ++i;
	    if (*line == '\0') {
		break;
	    }
	    *line = '\0';
	    ++line;
	    startbit = line;
	}
	INVARIANT(i==42, boost::format("bad %d '%s'") % i % fields[0]);
    }
    
    double doubleVal(int field) {
	if ((converted_flags[field] & doubleFlag) == 0) {
	    asdouble[field] = atof(fields[field]);
	    converted_flags[field] |= doubleFlag;
	}
	return asdouble[field];
    }

    int intVal(int field) {
	if ((converted_flags[field] & intFlag) == 0) {
	    asint[field] = atoi(fields[field]);
	    converted_flags[field] |= intFlag;
	}
	return asint[field];
    }

    static const int doubleFlag = 2;
    static const int intFlag = 4;
};

class LatencyGroupByInt {
public:
    LatencyGroupByInt(int _start_field, int _end_field, int _groupby_field)
	: start_field(_start_field),
	  end_field(_end_field),
	  groupby_field(_groupby_field)
    {
    }

    typedef HashMap<int32_t, Stats *, HashMap_hashintfast> mytableT;

    virtual ~LatencyGroupByInt() { }
    
    virtual void processRow(csvline &line) {
	Stats *stat = mystats[line.intVal(groupby_field)];
	if (stat == NULL) {
	    stat = new Stats();
	    mystats[line.intVal(groupby_field)] = stat;
	}
	stat->add(line.doubleVal(end_field) - line.doubleVal(start_field));
    }

    virtual void printResult() {
	cout << boost::format("#%d, count(*), mean(#%d - #%d), stddev, min, max")
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
    int start_field, end_field, groupby_field;
};

int
main(int argc, char *argv[]) 
{
    static const int enter_driver = 0;
    static const int leave_driver = 1;
    static const int return_to_driver = 2;
    static const int device_number = 5;
    static const int logical_volume_number = 10;
    static const int bytes = 3;

    vector<LatencyGroupByInt *> mods;
    while(1) {
	int opt = getopt(argc, argv, "123456789");
	if (opt == -1) break;
	switch(opt) 
	    {
	    case '1': 
		mods.push_back(new LatencyGroupByInt(enter_driver,leave_driver,device_number)); 
		break;
	    case '2': 
		mods.push_back(new LatencyGroupByInt(enter_driver,leave_driver,logical_volume_number)); 
		break;
	    case '3': 
		mods.push_back(new LatencyGroupByInt(enter_driver,leave_driver,bytes)); 
		break;

	    case '4': 
		mods.push_back(new LatencyGroupByInt(enter_driver,return_to_driver,device_number)); 
		break;
	    case '5': 
		mods.push_back(new LatencyGroupByInt(enter_driver,return_to_driver,logical_volume_number)); 
		break;
	    case '6': 
		mods.push_back(new LatencyGroupByInt(enter_driver,return_to_driver,bytes)); 
		break;

	    case '7': 
		mods.push_back(new LatencyGroupByInt(leave_driver,return_to_driver,device_number)); 
		break;
	    case '8': 
		mods.push_back(new LatencyGroupByInt(leave_driver,return_to_driver,logical_volume_number)); 
		break;
	    case '9': 
		mods.push_back(new LatencyGroupByInt(leave_driver,return_to_driver,bytes)); 
		break;
	    default: INVARIANT(false, "bad");
	    }
    }

    csvline line;

    // using cin.getline() is a performance disaster, running through
    // hourly-0000x.csv takes 15 seconds, mostly in getc() which is
    // apparently how getline() gets the line.  fgets() drops the time
    // to about 2 seconds.

    while(!feof(stdin)) {
	char buf[8192];
	buf[0] = '\0';
	fgets(buf, 8192, stdin);
	if (feof(stdin))
	    break;
	INVARIANT(buf[0] != '\0', "bad");
	line.set(buf);

	for(unsigned i=0;i<mods.size();++i) {
	    mods[i]->processRow(line);
	}
    }
	
    for(unsigned i=0;i<mods.size();++i) {
	mods[i]->printResult();
    }

    return 0;
}
