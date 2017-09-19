// -*-C++-*-
/*
  (c) Copyright 2007, Hewlett-Packard Development Company, LP

  See the file named COPYING for license details
*/

/*
=pod

=head1 NAME

dsstatgroupby - Calculate some derived value from a dataseries and calculate a statistic over it.

=head1 SYNOPSIS

% dsstatgroupby I<extent-type-match> I<statistic-description>... from file...

=head1 STATISTIC DESCRIPTION

Each statistic is described by a minimum of two arguments -- the statistic type and the expression.
Two types of statistic types are currently implemented basic (mean, stddev, min, max), and quantile
(percentile/100).  The expression implements the standard + - * / () and constants.  Two optional
arguments can be added.  where I<expr> adds in a conditional expression so you could calculate
separate statistics over large and small files.  group by <field> specifies a column that should be
used for grouping the statistics.

=head1 DESCRIPTION

dsstatgroupby processes one or more input files calculating multiple statistics in a single pass
over that input file.

*/

#include <boost/format.hpp>

#include <DataSeries/DSStatGroupByModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/PrefetchBufferModule.hpp>
#include <DataSeries/SequenceModule.hpp>

using namespace std;
using boost::format;

void 
usage(const std::string &program_name, const std::string &error)
{
    // TODO: should we make the usage ... from <prefix> in <file...>?
    cerr << error << "\n"
         //Arg1: Program name   
         << "Usage: " << program_name 
         //Arg2: extent type match: WTF???
         //Arg3: statistic needed
         //4: Expression
         //Where to save
         //What you group by/order by
         << " <extent-type-match> (<stat-type> <expr> [where <expr>] [group by <group-by>])+\n"
         << "  from file...\n"
         << "\n"
         << "  stat-types include:\n\n"
         << "    basic, quantile\n\n"
         << DSExpr::usage();
    exit(4);
}


int 
main(int argc, char *_argv[])
{
    // converting to string from char * makes argv[i] == "blah" work.
    vector<string> argv;1
    argv.reserve(argc);
    for (int i=0; i<argc; ++i) {
        argv.push_back(string(_argv[i]));
    }
    if (argc <= 5) usage(argv[0], "insufficient arguments");

    string extent_type_match(argv[1]);
    
    TypeIndexModule source(extent_type_match);
    //Magic numbers are used, this is bad
    PrefetchBufferModule *prefetch = new PrefetchBufferModule(source, 64*1024*1024);
    
    //SequenceModule available in "Modules folder
    SequenceModule seq(prefetch);
    
    //Entire sequence below if argument checking, but done badly
    //Should fix it
    uint32_t argpos;
    for (argpos = 2; argpos < argv.size();) {
        if (argv[argpos] == "from") 
            break;
        if (argpos + 2 + 2 > argv.size()) { // stat-type, expr + from, file
            usage(argv[0], "missing from in arguments");
        }
        string stat_type(argv[argpos]); 
        ++argpos;
        if (!DSStatGroupByModule::validStatType(stat_type)) {
            usage(argv[0], str(format("'%s' is an invalid stat type") % stat_type));
        }

        string expr(argv[argpos]); 
        ++argpos;

        string where_expr;
        if (argv[argpos] == "where") {
            ++argpos;
            where_expr = argv[argpos]; 
            ++argpos;
        }

        string group_by;

        if (argv[argpos] == "group" && argv[argpos+1] == "by" && argpos + 2 < argv.size()) {
            group_by = argv[argpos + 2];
            argpos += 3;
        }

        seq.addModule(new DSStatGroupByModule(seq.tail(), expr, group_by, 
                                              stat_type, where_expr));
    }

    if (argpos >= argv.size() || argv[argpos] != "from") {
        usage(argv[0], "missing from in arguments");
    }
    ++argpos;
    for (;argpos<argv.size(); ++argpos) {
        source.addSource(argv[argpos]);
    }
    
    //getAndDeleteShared() is badly named
    seq.getAndDeleteShared();
    
    RowAnalysisModule::printAllResults(seq, 1);

    printf("\n");
    printf("# extents: %.2f MB -> %.2f MB\n",
           (double)(source.total_compressed_bytes)/(1024.0*1024),
           (double)(source.total_uncompressed_bytes)/(1024.0*1024));
    printf("#                    common\n");
    printf("# MB compressed:   %8.2f\n",
           (double)source.total_compressed_bytes/(1024.0*1024));
    printf("# MB uncompressed: %8.2f\n",
           (double)source.total_uncompressed_bytes/(1024.0*1024));
    printf("# wait fraction :  %8.2f\n",
           source.waitFraction());
    
    return 0;
}

