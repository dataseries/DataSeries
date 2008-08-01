// -*-C++-*-
/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Select subset of fields from a collection of traces, generate a new trace
*/

/* 
TODO: add the Makefile.am bits to turn this into a manpage during installation.
pod2man dsrepack.C | sed 's/User Contributed Perl/DataSeries/' does the
right thing.

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

// TODO: use GeneralField/ExtentRecordCopy, we aren't changing the type so
// it should be much faster.

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/format.hpp>

#include <Lintel/StringUtil.hpp>
#include <Lintel/AssertBoost.hpp>

#include <DataSeries/commonargs.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/PrefetchBufferModule.hpp>

static const bool debug = false;
static const bool show_progress = true;

using namespace std;

// TODO: figure out why when compressing from lzo to lzf, we can only
// get up to ~200% cpu utilization rather than going to 400% on a 4way
// machine.  Possibilites are decompression bandwidth and copy
// bandwidth, but neither seem particularly likely.  One oddness from
// watching the strace is that the source file seems to be very bursty
// rather than smoothly reading in data.  Watching the threads in top
// on a 2 way machine, we only got to ~150% cpu utilization yet no
// thread was at 100%.  Fiddling with priorities of the compression
// threads in DataSeriesFile didn't seem to do anything.

// Derived from boost implementation; TODO move this into Lintel.
template <class Target, class Source>
inline Target *safe_downcast(Source *x)
{
    // detect logic error
    Target *ret = dynamic_cast<Target *>(x);
    INVARIANT(ret == x,
              boost::format("dynamic cast failed in %s")
              % __PRETTY_FUNCTION__);  
    return ret;
}

struct PerTypeWork {
    OutputModule *output_module;
    ExtentSeries inputseries, outputseries;
    // Splitting out the types here is ugly.  However, it's a huge
    // performance improvement to not be going through the virtual
    // function call.  In particular, repacking an BlockIO::SRT file
    // from LZO to LZF took 285 CPU seconds originally, 244 CPU s
    // after adding in the bool specific case, and 235 seconds after
    // special casing for int32's.  That file has lots of booleans
    // somewhat fewer int32s, and then some doubles.

    // TODO: remove special case, use ExtentRecordCopy
    vector<GeneralField *> infields, outfields, all_fields;
    vector<GF_Bool *> in_boolfields, out_boolfields;
    vector<GF_Int32 *> in_int32fields, out_int32fields;
    vector<GF_Variable32 *> in_var32fields, out_var32fields;

    double sum_unpacked_size, sum_packed_size;
    PerTypeWork(DataSeriesSink &output, unsigned extent_size, 
		const ExtentType *t) 
	: inputseries(t), outputseries(t), 
	  sum_unpacked_size(0), sum_packed_size(0) 
    {
	for(unsigned i = 0; i < t->getNFields(); ++i) {
	    const string &s = t->getFieldName(i);
	    GeneralField *in = GeneralField::create(NULL, inputseries, s);
	    GeneralField *out = GeneralField::create(NULL, outputseries, s);
	    all_fields.push_back(in);
	    all_fields.push_back(out);
	    switch(t->getFieldType(s)) 
		{
		case ExtentType::ft_bool: 
		    in_boolfields.push_back(safe_downcast<GF_Bool>(in));
		    out_boolfields.push_back(safe_downcast<GF_Bool>(out));
		    break;
		case ExtentType::ft_int32: 
		    in_int32fields.push_back(safe_downcast<GF_Int32>(in));
		    out_int32fields.push_back(safe_downcast<GF_Int32>(out));
		    break;
		case ExtentType::ft_variable32: 
		    in_var32fields.push_back(safe_downcast<GF_Variable32>(in));
		    out_var32fields.push_back(safe_downcast<GF_Variable32>(out));
		    break;
		default:
		    infields.push_back(in);
		    outfields.push_back(out);
		    break;
		}
	}
	output_module = new OutputModule(output, outputseries, t, 
					 extent_size);
    }

    ~PerTypeWork() {
	for(vector<GeneralField *>::iterator i = all_fields.begin();
	    i != all_fields.end(); ++i) {
	    delete *i;
	}
    }

    void rotateOutput(DataSeriesSink &output) {
	OutputModule *old = output_module;
	old->close();
	
	INVARIANT(outputseries.extent() == NULL, "huh?");

	DataSeriesSink::Stats stats = old->getStats();
	sum_unpacked_size += stats.unpacked_size;
	sum_packed_size += stats.packed_size;

	output_module = new OutputModule(output, outputseries, 
					 old->outputtype, 
					 old->getTargetExtentSize());
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

void
checkFileMissing(const std::string &filename)
{
    struct stat buf;
    INVARIANT(stat(filename.c_str(), &buf) != 0, 
	      boost::format("Refusing to overwrite existing file %s.\n")
	      % filename);
}

const string dsrepack_info_type_xml(
  "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Info::DSRepack\" version=\"1.0\" >\n"
  "  <field type=\"bool\" name=\"enable_bz2\" print_true=\"with_bz2\" print_false=\"no_bz2\" />\n"
  "  <field type=\"bool\" name=\"enable_gz\" print_true=\"with_gz\" print_false=\"no_gz\" />\n"
  "  <field type=\"bool\" name=\"enable_lzo\" print_true=\"with_lzo\" print_false=\"no_lzo\" />\n"
  "  <field type=\"bool\" name=\"enable_lzf\" print_true=\"with_lzf\" print_false=\"no_lzf\" />\n"
  "  <field type=\"int32\" name=\"compress_level\" />\n"
  "  <field type=\"int32\" name=\"extent_size\" />\n"
  "  <field type=\"int32\" name=\"part\" opt_nullable=\"yes\" />\n"
  "</ExtentType>\n"
  );

const ExtentType *dsrepack_info_type;

void
writeRepackInfo(DataSeriesSink &sink,
		const commonPackingArgs &cpa, int file_count)
{
    ExtentSeries series(*dsrepack_info_type);
    OutputModule out_module(sink, series, dsrepack_info_type, 
			    cpa.extent_size);
    BoolField enable_bz2(series, "enable_bz2");
    BoolField enable_gz(series, "enable_gz");
    BoolField enable_lzo(series, "enable_lzo");
    BoolField enable_lzf(series, "enable_lzf");
    Int32Field compress_level(series, "compress_level");
    Int32Field extent_size(series, "extent_size");
    Int32Field part(series, "part", Field::flag_nullable);

    out_module.newRecord();
    enable_bz2.set(cpa.compress_modes & Extent::compress_bz2 ? true : false);
    enable_gz.set(cpa.compress_modes & Extent::compress_gz ? true : false);
    enable_lzo.set(cpa.compress_modes & Extent::compress_lzo ? true : false);
    enable_lzf.set(cpa.compress_modes & Extent::compress_lzf ? true : false);
    compress_level.set(cpa.compress_level);
    extent_size.set(cpa.extent_size);
    if (file_count >= 0) {
	part.set(file_count);
    } else {
	part.setNull();
    }
}

bool
skipType(const ExtentType &type)
{
    return type.getName() == "DataSeries: ExtentIndex"
	|| type.getName() == "DataSeries: XmlType"
	|| (type.getName() == "Info::DSRepack"
	    && type.getNamespace() == "ssd.hpl.hp.com");
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
	      boost::format("Usage: %s [common-args] [--target-file-size=MiB] input-filename... output-filename\nCommon args:\n%s") 
	      % argv[0] % packingOptions());
    
    // Always check on repacking...
    if (getenv("DATASERIES_EXTENT_CHECKS")) {
	cerr << "Warning: DATASERIES_EXTENT_CHECKS is set; generally you want all checks on during a dsrepack.\n";
    }
    Extent::setReadChecksFromEnv(true);

    TypeIndexModule source("");
    ExtentTypeLibrary library;
    dsrepack_info_type = library.registerType(dsrepack_info_type_xml);
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
	output_path = (boost::format("%s.part-%02d.ds") 
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

	for(map<const string, const ExtentType *>::iterator j = f.mylibrary.name_to_type.begin();
	    j != f.mylibrary.name_to_type.end(); ++j) {
	    if (skipType(*j->second)) {
		continue;
	    }
	    if (prefixequal(j->first, "DataSeries:")) {
		cerr << boost::format("Warning, found extent type of name '%s'; probably should skip it") % j->first << endl;
	    }
	    const ExtentType *tmp = library.getTypeByName(j->first, true);
	    INVARIANT(tmp == NULL || tmp == j->second,
		      boost::format("XML types for type '%s' differ between file %s and an earlier file")
		      % j->first % argv[i]);
	    if (tmp == NULL) {
		if (debug) {
		    cout << "Registering type of name " << j->first << endl;
		}
		const ExtentType *t = library.registerType(j->second->xmldesc);
		per_type_work[j->first] = 
		    new PerTypeWork(*output, packing_args.extent_size, t);
	    }
	    DEBUG_INVARIANT(per_type_work[j->first] != NULL, "internal");
	}

	ExtentSeries s(f.indexExtent);
	Variable32Field extenttype(s,"extenttype");

	for(;s.pos.morerecords();++s.pos) {
	    if (skipType(*library.getTypeByName(extenttype.stringval()))) {
		continue;
	    }
	    ++extent_count;
	}
    }

    DataSeriesModule *from = &source;
    source.startPrefetching(16*1024*1024, 112*1024*1024); // 128MiB total
    if (getenv("DISABLE_PREFETCHING") == NULL) {
	from = new PrefetchBufferModule(source, 64*1024*1024);
    }
    output->writeExtentLibrary(library);

    DataSeriesSink::Stats all_stats;
    uint32_t extent_num = 0;
    uint64_t cur_file_bytes = 0;

    while(true) {
	Extent *inextent = from->getExtent();
	if (inextent == NULL)
	    break;
	
	if (skipType(inextent->type)) {
	    continue;
	}
	++extent_num;
	if (show_progress) {
	    cout << boost::format("Processing extent #%d/%d of type %s\n")
		% extent_num % extent_count % inextent->type.getName();
	}
	PerTypeWork *ptw = per_type_work[inextent->type.getName()];
	INVARIANT(ptw != NULL, "internal");
	for(ptw->inputseries.setExtent(inextent);
	    ptw->inputseries.pos.morerecords();
	    ++ptw->inputseries.pos) {
	    ptw->output_module->newRecord();
	    cur_file_bytes += ptw->outputseries.type->fixedrecordsize();
	    for(unsigned int i=0; i < ptw->in_boolfields.size(); ++i) {
		ptw->out_boolfields[i]->set(ptw->in_boolfields[i]);
	    }
	    for(unsigned int i=0; i < ptw->in_int32fields.size(); ++i) {
		ptw->out_int32fields[i]->set(ptw->in_int32fields[i]);
	    }
	    for(unsigned int i=0; i < ptw->in_var32fields.size(); ++i) {
		cur_file_bytes += ptw->in_var32fields[i]->myfield.size();
		ptw->out_var32fields[i]->set(ptw->in_var32fields[i]);
	    }
	    for(unsigned int i=0; i<ptw->infields.size(); ++i) {
		ptw->outfields[i]->set(ptw->infields[i]);
	    }
	    if (target_file_bytes > 0 && cur_file_bytes >= target_file_bytes) {
		output->flushPending();
		uint64_t est_file_size = fileSize(output_path);
		for(map<string, PerTypeWork *>::iterator i = per_type_work.begin();
		    i != per_type_work.end(); ++i) {
		    est_file_size += i->second->estimateCurSize();
		}
		if (est_file_size >= target_file_bytes) {
		    ++output_file_count;
		    INVARIANT(output_file_count < 1000, 
			      "split into 1000 parts; assuming you didn't want that and stopping");
		    output_path = (boost::format("%s.part-%02d.ds") 
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
		    writeRepackInfo(*output, packing_args, output_file_count);
		    output->close();
		    all_stats += output->getStats();
		    delete output;
		    output = new_output;
		}
		cur_file_bytes = est_file_size;
	    }
	}
	delete inextent;
    }

    for(map<string, PerTypeWork *>::iterator i = per_type_work.begin();
	i != per_type_work.end(); ++i) {
	i->second->output_module->flushExtent();
    }
    writeRepackInfo(*output, packing_args, target_file_bytes > 0 
		    ? static_cast<int>(output_file_count) : -1);
    output->close();
    all_stats += output->getStats();

    cout << boost::format("decode_time:%.6g expanded:%lld\n")
	% source.decode_time % source.total_uncompressed_bytes;

    all_stats.printText(cout);
    return 0;
}

