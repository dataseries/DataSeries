/*
   (c) Copyright 2007-2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details

=pod

=head1 NAME

nfssubset - calculate a consistent subset of the animation nfs traces

=head1 SYNOPSIS

 % nfssubset nfssubset [common-args] I<min-id> I<max-id> input.ds... output.ds

=head1 DESCRIPTION

The animation nfs trace schema stores data across multiple files.  Therefore calculating a subset
of a file requires selecting out the appropriate records from each of the input files.  This
process could be implemented by a script and dsselect, but is easier to implement using a
specialized program.  It selects the appropriate ids across all of the different extent types.

=head1 SEE ALSO

dataseries-utils(7)

=cut

*/

#include <sys/stat.h>

#include <iostream>

#include <Lintel/StringUtil.hpp>

#include <DataSeries/GeneralField.hpp>
#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/commonargs.hpp>

using namespace std;

class SubsetCopyModule : public RowAnalysisModule {
public:
    SubsetCopyModule(DataSeriesModule &_source_module,
		     OutputModule &_output_module,
		     const std::string &select_1_name,
		     const std::string &select_2_name,
		     int64_t _min_keep, int64_t _max_keep)
	: RowAnalysisModule(_source_module),
	  
	  output_module(_output_module),
	  
	  select_1(series, select_1_name),
	  select_2(series, select_2_name),
	
	  min_keep(_min_keep), max_keep(_max_keep),
	  
	  copier(series, output_module.getSeries()),
	  records_copied(0)
    { 
	
    }
 
    virtual ~SubsetCopyModule() { }

    virtual void prepareForProcessing() {
	copier.prep();
    }

    bool inrange(int64_t v, int64_t min, int64_t max) {
	return min <= v && v <= max;
    }

    virtual void processRow() {
	if (inrange(select_1.val(), min_keep, max_keep) ||
	    inrange(select_2.val(), min_keep, max_keep)) {
	    ++records_copied;
	    output_module.newRecord();
	    copier.copyRecord();
	}
    }

    virtual void printResult() {
	cout << boost::format("copied %d records of type %s\n")
	    % records_copied % series.getTypePtr()->getName();
    }

    OutputModule &output_module;
    Int64Field select_1, select_2;

    const int64_t min_keep, max_keep;
    
    ExtentRecordCopy copier;
    uint64_t records_copied;
};

void
checkFileMissing(const std::string &filename)
{
    struct stat buf;
    INVARIANT(stat(filename.c_str(), &buf) != 0, 
	      boost::format("Refusing to overwrite existing file %s.\n")
	      % filename);
}

void
doCopy(const ExtentType::Ptr extenttype, int64_t min_keep, int64_t max_keep,
       vector<string> source_files, DataSeriesSink &output, 
       uint32_t extent_size) 
{
    TypeIndexModule indata(extenttype->getName());
    for(vector<string>::iterator i = source_files.begin();
	i != source_files.end(); ++i) {
	indata.addSource(*i);
    }
    ExtentSeries output_series(extenttype);
    OutputModule output_module(output, output_series, extenttype, extent_size);

    string sel1, sel2;
    if (extenttype->hasColumn("request-id")) {
	sel1 = "request-id";
	sel2 = "reply-id";
    } else if (extenttype->hasColumn("record-id")) {
	sel1 = sel2 = "record-id";
    } else if (extenttype->hasColumn("request_id")) {
	sel1 = "request_id";
	sel2 = "reply_id";
    } else if (extenttype->hasColumn("record_id")) {
	sel1 = sel2 = "record_id";
    } else {
	FATAL_ERROR("?");
    }

    SubsetCopyModule copy(indata, output_module, sel1, sel2, 
			  min_keep, max_keep);
    copy.getAndDeleteShared();
}

int
main(int argc, char *argv[])
{
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    Extent::setReadChecksFromEnv(true);

    INVARIANT(argc > 4,
	      boost::format("Usage: %s [common-args] min_keep_id max_keep_id_inclusive input-filename... output-filename\nCommon args:\n%s") 
	      % argv[0] % packingOptions());
    
    int64_t min_keep = stringToInteger<int64_t>(argv[1]);
    int64_t max_keep = stringToInteger<int64_t>(argv[2]);
    vector<string> source_files;
    for(int i=3; i<(argc-1); ++i) {
	source_files.push_back(argv[i]);
    }
    
    string output_path(argv[argc-1]);
    checkFileMissing(output_path);
    DataSeriesSink output(output_path, packing_args.compress_modes,
			  packing_args.compress_level);
    
    vector<string> copy_names;

    ExtentTypeLibrary out_library;
    {
	DataSeriesSource tmp(source_files[0]);
	ExtentTypeLibrary &lib(tmp.getLibrary());

	if (lib.getTypeByNamePtr("NFS trace: common", true) != NULL) {
	    copy_names.push_back("NFS trace: attr-ops");
	    copy_names.push_back("NFS trace: common");
	    copy_names.push_back("NFS trace: read-write");
	} else if (lib.getTypeByNamePtr("Trace::NFS::common") != NULL) {
	    copy_names.push_back("Trace::NFS::attr-ops");
	    copy_names.push_back("Trace::NFS::common");
	    copy_names.push_back("Trace::NFS::read-write");
	} else {
	    FATAL_ERROR("do not recognize this type of NFS trace.");
	}
	
	for(vector<string>::iterator i = copy_names.begin();
	    i != copy_names.end(); ++i) {
	    const ExtentType::Ptr t(tmp.getLibrary().getTypeByNamePtr(*i));
	    out_library.registerTypePtr(t->getXmlDescriptionString());
	}

	output.writeExtentLibrary(out_library);
    }

    for(vector<string>::iterator i = copy_names.begin();
	i != copy_names.end(); ++i) {
	doCopy(out_library.getTypeByNamePtr(*i), min_keep, max_keep, 
	       source_files, output, packing_args.extent_size);
    }
    return 0;
}
