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
    // TODO: should we make the usage ... from <prefix> in <file...>?
    cerr << error << "\n"
	 << "Usage: " << program_name 
	 << " <extent-type-prefix> (<stat-type> <expr> [where <expr>] group by <group-by>)*\n"
	 << "  from file...\n"
	 << "  supported stat-types: basic, quantile\n"
	 << "  expressions include field names, numeric (double) constants, +,-,*,/,()\n"
	 << "  boolean expressions currently support <\n"
	 << "  for fields with non-alpha-numeric or _ in the name, escape with \\\n";
    
    exit(0);
}


int 
main(int argc, char *_argv[])
{
    // converting to string from char * makes argv[i] == "blah" work.
    vector<string> argv;
    argv.reserve(argc);
    for(int i=0; i<argc; ++i) {
	argv.push_back(string(_argv[i]));
    }
    if (argc <= 8) usage(argv[0], "insufficient arguments");

    string extent_type_prefix(argv[1]);
    
    TypeIndexModule source(extent_type_prefix);
    PrefetchBufferModule *prefetch = new PrefetchBufferModule(source, 64*1024*1024);

    SequenceModule seq(prefetch);

    int argpos;
    for(argpos = 2; argpos < argc;) {
	if (argv[argpos] == "from") 
	    break;
	if (argpos + 5 >= argc) usage(argv[0], "missing from in arguments");
	string stat_type(argv[argpos]); 
	++argpos;
	string expr(argv[argpos]); 
	++argpos;
	string where_expr;
	if (argv[argpos] == "where") {
	    ++argpos;
	    where_expr = argv[argpos]; 
	    ++argpos;
	}
	if (argpos + 3 >= argc || argv[argpos] != "group" ||
	    argv[argpos+1] != "by") {
	    usage(argv[0], "missing group by <fieldname>");
	}
	argpos += 2;
	string group_by(argv[argpos]);
	++argpos;

	seq.addModule(new DSStatGroupByModule(seq.tail(), expr, group_by, 
					      stat_type, where_expr));
    }

    ++argpos;
    if (argpos >= argc) usage(argv[0], "missing from in arguments");
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

