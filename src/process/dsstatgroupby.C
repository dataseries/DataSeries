// -*-C++-*-
/*
   (c) Copyright 2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Calculate some derived value from a dataseries and calculate a statistic over it.
*/

#include <boost/format.hpp>

#include <DataSeries/DSStatGroupByModule.H>
#include <DataSeries/TypeIndexModule.H>
#include <DataSeries/PrefetchBufferModule.H>
#include <DataSeries/SequenceModule.H>

using namespace std;

void 
usage(const std::string &program_name, const std::string &error)
{
    cerr << error << endl 
	 << "Usage: " << program_name 
	 << " <stat-type> <extent-type-prefix> (<expr> <group-by>)* -- file..."
	 << endl;
    exit(0);
}


int 
main(int argc, char *argv[])
{
    if (argc <= 4) usage(argv[0], "insufficient arguments");

    string stat_type(argv[1]);
    string extent_type_prefix(argv[2]);
    
    TypeIndexModule source(extent_type_prefix);
    PrefetchBufferModule *prefetch = new PrefetchBufferModule(source, 64*1024*1024);

    SequenceModule seq(prefetch);

    int argpos;
    for(argpos = 3; argpos < argc; argpos += 2) {
	if (argpos + 2 > argc) usage(argv[0], "missing -- in arguments");
	string expr(argv[argpos]);
	string group_by(argv[argpos+1]);

	if (expr == "--") 
	    break;

	seq.addModule(new DSStatGroupByModule(seq.tail(), expr, group_by, 
					      stat_type));
    }

    ++argpos;
    if (argpos >= argc) usage(argv[0], "missing -- in arguments");
    for(;argpos<argc; ++argpos) {
	source.addSource(argv[argpos]);
    }

    DataSeriesModule::getAndDelete(seq);
    
    RowAnalysisModule::printAllResults(seq, 1);

    printf("\n");
    printf("# extents: %.2f MB -> %.2f MB in %.2f secs decode time\n",
	   (double)(source.total_compressed_bytes)/(1024.0*1024),
	   (double)(source.total_uncompressed_bytes)/(1024.0*1024),
	   source.decode_time);
    printf("#                    common\n");
    printf("# MB compressed:   %8.2f\n",
	   (double)source.total_compressed_bytes/(1024.0*1024));
    printf("# MB uncompressed: %8.2f\n",
	   (double)source.total_uncompressed_bytes/(1024.0*1024));
    printf("# decode seconds:  %8.2f\n",
	   source.decode_time);
    printf("# wait fraction :  %8.2f\n",
	   source.waitFraction());
    
    return 0;
}

