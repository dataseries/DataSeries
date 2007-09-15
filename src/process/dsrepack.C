// -*-C++-*-
/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Select subset of fields from a collection of traces, generate a new trace
*/

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/format.hpp>

#include <Lintel/StringUtil.H>
#include <Lintel/AssertBoost.H>

#include <DataSeries/commonargs.H>
#include <DataSeries/DataSeriesFile.H>
#include <DataSeries/GeneralField.H>
#include <DataSeries/DataSeriesModule.H>
#include <DataSeries/TypeIndexModule.H>
#include <DataSeries/PrefetchBufferModule.H>

static const bool debug = false;
static const bool show_progress = true;

using namespace std;

struct PerTypeWork {
    OutputModule *output_module;
    ExtentSeries inputseries, outputseries;
    vector<GeneralField *> infields, outfields;
    double sum_unpacked_size, sum_packed_size;
    PerTypeWork(DataSeriesSink &output, unsigned extent_size, ExtentType *t) 
	: inputseries(t), outputseries(t), 
	  sum_unpacked_size(0), sum_packed_size(0) 
    {
	for(int i = 0; i < t->getNFields(); ++i) {
	    const string &s = t->getFieldName(i);
	    infields.push_back(GeneralField::create(NULL, inputseries, s));
	    outfields.push_back(GeneralField::create(NULL, outputseries, s));
	}
	output_module = new OutputModule(output, outputseries, t, 
					 extent_size);
    }

    void rotateOutput(DataSeriesSink &output) {
	OutputModule *old = output_module;
	old->flushExtent();
	
	INVARIANT(outputseries.extent()->fixeddata.size() == 0, "huh?");
	delete outputseries.extent();
	outputseries.clearExtent();
	output_module = new OutputModule(output, outputseries, 
					 old->outputtype, old->target_extent_size);
	DataSeriesSink::Stats stats = old->getStats();
	sum_unpacked_size += stats.unpacked_size;
	sum_packed_size += stats.packed_size;
	delete old;
    }

    uint64_t estimateCurSize() {
	// Assume 3:1 compression if we have no statistics
	double ratio = sum_unpacked_size > 0 ? sum_packed_size / sum_unpacked_size : 0.3;
	return static_cast<uint64_t>(output_module->curExtentSize() * ratio);
    }
};

uint64_t fileSize(const string &filename)
{
    struct stat buf;
    INVARIANT(stat(filename.c_str(),&buf) == 0, 
	      boost::format("stat(%s) failed: %s")
	      % filename % strerror(errno));

    BOOST_STATIC_ASSERT(sizeof(buf.st_size) >= 8);
    return buf.st_size;
}

/* 
TODO: add the Makefile.am bits to turn this into a manpage during installation.
pod2man dsrepack.C | sed 's/User Contributed Perl/DataSeries/' does the
right thin.

=pod

=head1 NAME

dsrepack - split or merge DataSeries files and change the compression types

=head1 SYNOPSIS

dsrepack [common-options] [--target-file-size=MiB] input-filename... 
  output-filename

=head1 DESCRIPTION

In the simplest case, dsrepack takes the input files, and copies them
to the output filename changing the extent size and compresion level
as specified in the common options.  If max-file-size is set,
output-filename is used as a base name, and the actual output names
will be output-filename.##.ds, starting from 0.

=head1 EXAMPLES

  dsrepack --extent-size=131072 --compress-lzo cello97*ds all-cello97.ds

  dsrepack --extent-size=67108864 --compress-bz2 --target-file-size=100 \
     nettrace.000000-000499.ds -- nettrace.000000-000499.split


=head1 OPTIONS

All of the common options apply, see dataseries(7).

=over 4

=item B<--target-file-size=MiB>

What is the target file size to be generated.  Each of the individual
files will be about the target file size, but the vagaries of
compression mean that the files will vary around the target file size.
The last file may be arbitrarily small.  MiB can be specified as a
double, so one could say 0.1 MiB, but it's not clear if that is
useful.

=head1 SEE ALSO

dataseries(7), dsselect(1)

=cut
*/

void
checkFileMissing(const std::string &filename)
{
    struct stat buf;
    INVARIANT(stat(filename.c_str(), &buf) != 0, 
	      boost::format("Refusing to overwrite existing file %s.\n")
	      % filename);
}



// TODO: Split up main(), it's getting a bit large
const string target_file_size_arg("--target-file-size=");
int
main(int argc, char *argv[])
{
    uint64_t target_file_bytes = 0;
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    if (argc > 1 && prefixequal(argv[1], target_file_size_arg)) {
	double mib = stringToDouble(string(argv[1]).substr(target_file_size_arg.size()));
	INVARIANT(mib > 0, "max file size MiB must be > 0");
	target_file_bytes = static_cast<uint64_t>(mib * 1024.0 * 1024.0);
	for(int i = 2; i < argc; ++i) {
	    argv[i-1] = argv[i];
	}
	--argc;
    }

    INVARIANT(argc > 2,
	      boost::format("Usage: %s <common-options> [--target-file-size=MiB] input-filename... output-filename\n") 
	      % argv[0]);
    
    // Always check on repacking...
    if (getenv("DATASERIES_EXTENT_CHECKS")) {
	cerr << "Warning: DATASERIES_EXTENT_CHECKS is set; generally you want all checks on during a dsrepack.\n";
    }
    Extent::setReadChecksFromEnv(true);

    TypeIndexModule source("");
    ExtentTypeLibrary library;
    map<string, PerTypeWork *> per_type_work;

    string output_base_path(argv[argc-1]);
    unsigned output_file_count = 0;
    string output_path;
    if (target_file_bytes == 0) {
	output_path = output_base_path;
    } else {
	// %02d -- possible but unlikely that we would write over 100
	// split files, but seems sufficiently unlikely that it's not
	// worth having three digits of split numbers always.  Common
	// case likely to be below 10, but splitting ~1G into 100MB
	// chunks could end up with more than 10, so want two digits.
	output_path = (boost::format("%s.%02d.ds") 
		       % output_base_path % output_file_count).str();
    }
    checkFileMissing(output_path);
    DataSeriesSink *output = 
	new DataSeriesSink(output_path, packing_args.compress_modes,
			   packing_args.compress_level);

    uint32_t extent_count = 0;
    for(int i=1;i<(argc-1);++i) {
	source.addSource(argv[i]);

	// Nothing helping the fact that we have to open all of the
	// files to verify type identicalness before we can re-pack
	// things.  Luckily people should only end up doing this
	// infrequently when they've just retrieved bz2 compressed
	// extents and want to make them larger and faster, or to do
	// the reverse for distribution.

	DataSeriesSource f(argv[i]);

	for(map<const string, ExtentType *>::iterator j = f.mylibrary.name_to_type.begin();
	    j != f.mylibrary.name_to_type.end(); ++j) {
	    if (j->first == "DataSeries: ExtentIndex" ||
		j->first == "DataSeries: XmlType") {
		continue;
	    }
	    if (prefixequal(j->first, "DataSeries:")) {
		cerr << boost::format("Warning, found extent type of name '%s'; probably should skip it") % j->first << endl;
	    }
	    ExtentType *tmp = library.getTypeByName(j->first, true);
	    INVARIANT(tmp == NULL || tmp == j->second,
		      boost::format("XML types for type '%s' differ between file %s and an earlier file")
		      % j->first % argv[i]);
	    if (tmp == NULL) {
		if (debug) {
		    cout << "Registering type of name " << j->first << endl;
		}
		ExtentType *t = library.registerType(j->second->xmldesc);
		per_type_work[j->first] = 
		    new PerTypeWork(*output, packing_args.extent_size, t);
	    }
	    DEBUG_INVARIANT(per_type_work[j->first] != NULL, "internal");
	}

	ExtentSeries s(f.indexExtent);
	Variable32Field extenttype(s,"extenttype");

	for(;s.pos.morerecords();++s.pos) {
	    if (extenttype.equal("DataSeries: ExtentIndex") ||
		extenttype.equal("DataSeries: XmlType")) {
		continue;
	    }
	    ++extent_count;
	}
    }

    DataSeriesModule *from = &source;
    if (getenv("DISABLE_PREFETCHING") == NULL) {
	from = new PrefetchBufferModule(source, 64*1024*1024);
    }
    output->writeExtentLibrary(library);

    DataSeriesSink::Stats all_stats;
    uint32_t extent_num = 0;
    unsigned partial_row_count = 0;
    unsigned max_partial_row_count 
	= target_file_bytes > 0 ? 10000 : 2000*1000*1000;
    while(true) {
	Extent *inextent = from->getExtent();
	if (inextent == NULL)
	    break;
	
	if (inextent->type->name == "DataSeries: ExtentIndex" ||
	    inextent->type->name == "DataSeries: XmlType") {
	    continue;
	}
	++extent_num;
	if (show_progress) {
	    cout << boost::format("Processing extent #%d/%d of type %s")
		 % extent_num % extent_count % inextent->type->name
		 << endl;
	}
	PerTypeWork *ptw = per_type_work[inextent->type->name];
	INVARIANT(ptw != NULL, "internal");
	for(ptw->inputseries.setExtent(inextent);
	    ptw->inputseries.pos.morerecords();
	    ++ptw->inputseries.pos) {
	    ptw->output_module->newRecord();
	    for(unsigned int i=0; i<ptw->infields.size(); ++i) {
		ptw->outfields[i]->set(ptw->infields[i]);
	    }
	    ++partial_row_count;
	    if (partial_row_count == max_partial_row_count) {
		partial_row_count = 0;
		uint64_t est_file_size = fileSize(output_path);
		for(map<string, PerTypeWork *>::iterator i = per_type_work.begin();
		    i != per_type_work.end(); ++i) {
		    est_file_size += i->second->estimateCurSize();
		}
		if (est_file_size >= target_file_bytes) {
		    ++output_file_count;
		    output_path = (boost::format("%s.%02d.ds") 
				   % output_base_path 
				   % output_file_count).str();
		    checkFileMissing(output_path);
		    DataSeriesSink *new_output = 
			new DataSeriesSink(output_path, 
					   packing_args.compress_modes,
					   packing_args.compress_level);
		    new_output->writeExtentLibrary(library);
		    for(map<string, PerTypeWork *>::iterator i = per_type_work.begin();
			i != per_type_work.end(); ++i) {
			i->second->rotateOutput(*new_output);
		    }
		    output->close();
		    all_stats += output->getStats();
		    delete output;
		    output = new_output;
		}
	    }
	}
	delete inextent;
    }

    for(map<string, PerTypeWork *>::iterator i = per_type_work.begin();
	i != per_type_work.end(); ++i) {
	i->second->output_module->flushExtent();
    }
    output->close();
    all_stats += output->getStats();

    printf("decode_time:%.6g expanded:%lld\n", source.decode_time, source.total_uncompressed_bytes);

    all_stats.printText(cout);
    Extent::ByteArray::flushRetainedAllocations();
    return 0;
}

