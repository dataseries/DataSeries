/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <DataSeries/DStoTextModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/commonargs.hpp>

#include <analysis/nfs/join.hpp>

using namespace std;

const string output_xml(
  "<ExtentType namespace=\"ticoli.hpl.hp.com\" name=\"CooperativeCacheSimulation\" version=\"1.0\" >\n"
  "  <field type=\"int64\" name=\"request_at\" units=\"2^-32 seconds\" epoch=\"unix\" pack_relative=\"request_at\" print_format=\"sec.nsec\" />\n"
  "  <field type=\"int64\" name=\"reply_at\" units=\"2^-32 seconds\" epoch=\"unix\" pack_relative=\"reply_at\" />\n"
  "  <field type=\"byte\" name=\"operation_type\" comment=\"0=read, 1=write\" />\n"
  "  <field type=\"int32\" name=\"client_id\" />\n"
  "  <field type=\"variable32\" name=\"file_id\" comment=\"globally-unique\" />\n"
  "  <field type=\"int64\" name=\"offset\" />\n"
  "  <field type=\"int32\" name=\"bytes\" />\n"
  "</ExtentType>\n"
);

// TODO: write regression test

class AnimationConvert : public RowAnalysisModule {
public:
    AnimationConvert(DataSeriesModule &source, OutputModule &out_module)
        : RowAnalysisModule(source),
          in_request_at(series, "request_at"),
          in_reply_at(series, "reply_at"),
          in_server(series, "server"),
          in_client(series, "client"),
          in_filehandle(series, "filehandle"),
          in_is_read(series, "is_read"),
          in_file_size(series, "file_size", Field::flag_nullable),
          in_modify_time(series, "modify_time", Field::flag_nullable),
          in_offset(series, "offset"),
          in_bytes(series, "bytes"),

	  out_module(out_module),
	  out_series(out_module.getSeries()),
          out_request_at(out_series, "request_at"),
          out_reply_at(out_series, "reply_at"),
          out_operation_type(out_series, "operation_type"),
          out_client_id(out_series, "client_id"),
          out_file_id(out_series, "file_id"),
          out_offset(out_series, "offset"),
          out_bytes(out_series, "bytes")
    { }

    virtual ~AnimationConvert() { }

    virtual void processRow() {
	out_module.newRecord();

	out_request_at.set(in_request_at.valFrac32());
	out_reply_at.set(in_reply_at.valFrac32());
	out_operation_type.set(in_is_read() ? 0 : 1);
	out_client_id.set(in_client());
	out_file_id.set(in_filehandle); // Animation traces had globally unique filehandles
	out_offset.set(in_offset());
	out_bytes.set(in_bytes());
    }

    virtual void printResult() {
        // Here you put your code to print out your result, if so desired.
    }

private:
    Int64TimeField in_request_at;
    Int64TimeField in_reply_at;
    Int32Field in_server;
    Int32Field in_client;
    Variable32Field in_filehandle;
    BoolField in_is_read;
    Int64Field in_file_size;
    Int64Field in_modify_time;
    Int64Field in_offset;
    Int32Field in_bytes;

    OutputModule &out_module;
    ExtentSeries &out_series;
		     
    Int64Field out_request_at;
    Int64Field out_reply_at;
    ByteField out_operation_type;
    Int32Field out_client_id;
    Variable32Field out_file_id;
    Int64Field out_offset;
    Int32Field out_bytes;
};

void doAnimation(const vector<string> &args, const commonPackingArgs &packing_args) {
    NFSDSAnalysisMod::registerUnitsEpoch();

    TypeIndexModule *sourcea = new TypeIndexModule("NFS trace: common");
    sourcea->setSecondMatch("Trace::NFS::common");
    TypeIndexModule *sourceb = new TypeIndexModule("NFS trace: attr-ops");
    sourceb->setSecondMatch("Trace::NFS::attr-ops");
    TypeIndexModule *sourcec = new TypeIndexModule("NFS trace: read-write");
    sourcec->setSecondMatch("Trace::NFS::read-write");

    for (unsigned i = 1; i < args.size() - 1; ++i) {
	sourcea->addSource(args[i]);
    }

    sourceb->sameInputFiles(*sourcea);
    sourcec->sameInputFiles(*sourcea);

    SequenceModule seq_common(sourcea);
    SequenceModule seq_attr(sourceb);

    NFSDSModule *attr_common_join = NFSDSAnalysisMod::newAttrOpsCommonJoin();
    NFSDSAnalysisMod::setAttrOpsSources(attr_common_join, seq_common, seq_attr);
    
    SequenceModule seq_common_attr(attr_common_join);
    SequenceModule seq_rw(sourcec);

    NFSDSModule *attr_common_rw_join = NFSDSAnalysisMod::newCommonAttrRWJoin();
    NFSDSAnalysisMod::setCommonAttrRWSources(attr_common_rw_join, seq_common_attr, seq_rw);

    if (false) {
	DStoTextModule *ds_to_text = new DStoTextModule(*attr_common_rw_join);
	ds_to_text->getAndDelete();

	return;
    }

    DataSeriesSink outds(args.back(), packing_args.compress_modes, packing_args.compress_level);
    ExtentTypeLibrary library;
    const ExtentType *extent_type = library.registerType(output_xml);
    ExtentSeries out_series(extent_type);

    OutputModule out_module(outds, out_series, extent_type, packing_args.extent_size);

    outds.writeExtentLibrary(library);

    AnimationConvert convert(*attr_common_rw_join, out_module);
    convert.getAndDelete();
}

int main(int argc, char *argv[]) {
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    vector<string> args;

    for (int i = 1; i < argc; ++i) {
	args.push_back(string(argv[i]));
    }

    INVARIANT(args.size() >= 3, "Usage: extractCacheSim animation <input...> output");

    if (args[0] == "animation") {
	doAnimation(args, packing_args);
    } else {
	FATAL_ERROR("?");
    }
    return 0;
}
