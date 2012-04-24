/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <DataSeries/DStoTextModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/commonargs.hpp>

#include <analysis/nfs/join.hpp>

using namespace std;
using boost::format;

/*
=pod

=head1 NAME

extract-cache-sim - convert nfs traces into a simpler form for cache simulation

=head1 SYNOPSIS

 % extract-cache-sim [common-args] (animation|ellard) <input.ds...> <output.ds>

=head1 DESCRIPTION

Both the animation and ellard traces are useful inputs for cache simulation. However, 
cache simulators generally need a tiny subset of the total information stored in those
two input formats.  Therefore it can be useful to pre-process the files to convert them
to a simpler form.  This approach makes it easier to write the cache simulator and also
gives it a single input format.  This program provides that conversion.

=head1 SEE ALSO

dataseries-utils(7)

=cut
*/


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
	DStoTextModule ds_to_text(*attr_common_rw_join);
	ds_to_text.getAndDeleteShared();

	return;
    }

    DataSeriesSink outds(args.back(), packing_args.compress_modes, packing_args.compress_level);
    ExtentTypeLibrary library;
    const ExtentType::Ptr extent_type(library.registerTypePtr(output_xml));
    ExtentSeries out_series(extent_type);

    OutputModule out_module(outds, out_series, extent_type, packing_args.extent_size);

    outds.writeExtentLibrary(library);

    AnimationConvert convert(*attr_common_rw_join, out_module);
    convert.getAndDeleteShared();
}

// TODO: pull this out into common code, duplicated from ellardanalysis
struct opinfo {
    const string name;
    unsigned unified_id;
};

const string unified_ops[] = {
    "null",        // 0
    "getattr",     // 1
    "setattr",     // 2
    "root",        // 3
    "lookup",      // 4
    "readlink",	   // 5
    "read",	   // 6
    "writecache",  // 7
    "write",	   // 8
    "create",	   // 9
    "remove",	   // 10
    "rename",	   // 11
    "link",	   // 12
    "symlink",	   // 13
    "mkdir",	   // 14
    "rmdir",	   // 15
    "readdir",	   // 16
    "fsstat",	   // 17 -- use V3 naming, V2 called this statfs
    "access",      // 18
    "mknod",       // 19
    "readdirp",    // 20 -- ellard traces call this readdirp not readdirplus
    "fsinfo",      // 21
    "pathconf",    // 22
    "commit",      // 23
};

unsigned n_unified = sizeof(unified_ops) / sizeof(string);

const opinfo nfsv2ops[] = {
    { "null", 0 },
    { "getattr", 1 },
    { "setattr", 2 },
    { "root", 3 },
    { "lookup", 4 },
    { "readlink", 5 },
    { "read", 6 },
    { "writecache", 7 },
    { "write", 8 },
    { "create", 9 },
    { "remove", 10 },
    { "rename", 11 },
    { "link", 12 },
    { "symlink", 13 },
    { "mkdir", 14 },
    { "rmdir", 15 },
    { "readdir", 16 },
    { "statfs", 17 }
};

unsigned n_nfsv2ops = sizeof(nfsv2ops) / sizeof(opinfo);

const opinfo nfsv3ops[] = {
    { "null", 0 },
    { "getattr", 1 },
    { "setattr", 2 },
    { "lookup", 4 },
    { "access", 18 },
    { "readlink", 5 },
    { "read", 6 },
    { "write", 8 },
    { "create", 9 },
    { "mkdir", 14 },
    { "symlink", 13 },
    { "mknod", 19 },
    { "remove", 10 },
    { "rmdir", 15 },
    { "rename", 11 },
    { "link", 12 },
    { "readdir", 16 },
    { "readdirp", 20 }, // follow ellard traces naming convention
    { "fsstat", 17 },
    { "fsinfo", 21 },
    { "pathconf", 22 },
    { "commit", 23 }
};

unsigned n_nfsv3ops = sizeof(nfsv3ops) / sizeof(opinfo);

class EllardConvert : public RowAnalysisModule {
public:
    EllardConvert(DataSeriesModule &source, OutputModule &out_module)
        : RowAnalysisModule(source),
          time(series, "time"),
          source_ip(series, "source_ip"),
          source_port(series, "source_port"),
          dest_ip(series, "dest_ip"),
          dest_port(series, "dest_port"),
          is_call(series, "is_call"),
          nfs_version(series, "nfs_version"),
          rpc_transaction_id(series, "rpc_transaction_id"),
          rpc_function_id(series, "rpc_function_id"),
          rpc_function(series, "rpc_function"),
	  off(series, "off", Field::flag_nullable),
	  offset(series, "offset", Field::flag_nullable),
          return_value(series, "return_value", Field::flag_nullable),
          fh(series, "fh", Field::flag_nullable),
	  count(series, "count", Field::flag_nullable),
          garbage(series, "garbage", Field::flag_nullable),

	  out_module(out_module),
	  out_series(out_module.getSeries()),
          out_request_at(out_series, "request_at"),
          out_reply_at(out_series, "reply_at"),
          out_operation_type(out_series, "operation_type"),
          out_client_id(out_series, "client_id"),
          out_file_id(out_series, "file_id"),
          out_offset(out_series, "offset"),
          out_bytes(out_series, "bytes"),

	  duplicate_requests(0), duplicate_replies(0), failed_requests(0)

    { 
	SINVARIANT(unified_ops[read_id] == "read" && unified_ops[write_id] == "write");
    }

    static const uint32_t read_id = 6;
    static const uint32_t write_id = 8;

    virtual ~EllardConvert() { }

    virtual void processRow() {
	if (!garbage.isNull()) return;
	
	unsigned unified_id;

	// TODO: unify next bit with ellardanalysis
	if (nfs_version.val() == 2) {
	    INVARIANT(rpc_function_id.val() < n_nfsv2ops, "?");
	    unified_id = nfsv2ops[rpc_function_id.val()].unified_id;
	} else if (nfs_version.val() == 3) {
	    INVARIANT(rpc_function_id.val() < n_nfsv3ops, "?");
	    unified_id = nfsv3ops[rpc_function_id.val()].unified_id;
	} else {
	    FATAL_ERROR("bad nfs version");
	}

	if (unified_id != read_id && unified_id != write_id) {
	    return; // ignore
	}
	bool is_read = unified_id == read_id;
	if (is_call()) {
	    RpcKey key(dest_ip(), dest_port(), source_ip(), source_port(), rpc_transaction_id());
	    
	    if (table.exists(key)) {
		++duplicate_requests;
	    } else {
		SINVARIANT((nfs_version() == 2 && !offset.isNull())
			   || (nfs_version() == 3 && !off.isNull()));
		SINVARIANT(!fh.isNull());
		int64_t tmp = nfs_version() == 2 ? offset() : off();
		table[key] = RpcValue(time.valFrac32(), is_read, fh.stringval(), tmp);
	    }
	} else {
	    INVARIANT(!return_value.isNull(), format("rv null @ %s") % time.valStrSecNano());
	    RpcKey key(source_ip(), source_port(), dest_ip(), dest_port(), rpc_transaction_id());
	    
	    RpcValue *v = table.lookup(key);
	    if (v == NULL) {
		++duplicate_replies;
	    } else if (return_value() != 0) {
		++failed_requests;
		table.remove(key);
	    } else {
		INVARIANT(!count.isNull(), format("count null @ %s") % time.valStrSecNano());
		out_module.newRecord();

		out_request_at.set(v->at);
		out_reply_at.set(time.valFrac32());
		out_operation_type.set(is_read ? 0 : 1);
		out_client_id.set(key.client_ip);
		out_file_id.allocateSpace(4+v->file_handle.size());
		// Make fh's unique.
		out_file_id.partialSet(&key.server_ip, 4, 0);
		out_file_id.partialSet(v->file_handle.data(), v->file_handle.size(), 4);
		out_offset.set(v->offset);
		out_bytes.set(count());
		table.remove(key);
	    }
	}
    }

    virtual void printResult() {
	cout << format("%d duplicate requests, %d duplicate replies\n")
	    % duplicate_requests % duplicate_replies;
	cout << format("%d requests w/o replies, %d failed requests\n") 
	    % table.size() % failed_requests;
    }

    struct RpcKey {
	int32_t server_ip, client_ip;
	int16_t server_port, client_port;
	int32_t xact_id;
	
	RpcKey(int32_t a, int16_t b, int32_t c, int16_t d, int32_t e) 
	    : server_ip(a), client_ip(c), server_port(b), client_port(d), xact_id(e) { }
	RpcKey() : server_port(-1) { }

	bool operator ==(const RpcKey &rhs) const {
	    return xact_id == rhs.xact_id 
		&& server_ip == rhs.server_ip && client_ip == rhs.client_ip
		&& server_port == rhs.server_port && client_port == rhs.client_port;
	}
	
	uint32_t hash() const {
	    uint32_t tmp = lintel::BobJenkinsHashMix3(xact_id, server_ip, client_ip);
	    return lintel::BobJenkinsHashMix3(tmp, server_port, client_port);
	}
	    
    };

    struct RpcValue {
	int64_t at;
	bool is_read;
	string file_handle;
	int64_t offset;

	RpcValue(int64_t a, bool b, const string &c, int64_t d)
	    : at(a), is_read(b), file_handle(c), offset(d) { }

	RpcValue() { }
    };

    HashMap<RpcKey, RpcValue> table;

private:
    Int64TimeField time;
    Int32Field source_ip;
    Int32Field source_port;
    Int32Field dest_ip;
    Int32Field dest_port;
    BoolField is_call;
    ByteField nfs_version;
    Int32Field rpc_transaction_id;
    ByteField rpc_function_id;
    Variable32Field rpc_function;
    Int64Field off;
    Int32Field offset;
    Int32Field return_value;
    Variable32Field fh;
    Int32Field count;
    Variable32Field garbage;

    OutputModule &out_module;
    ExtentSeries &out_series;
		     
    Int64Field out_request_at;
    Int64Field out_reply_at;
    ByteField out_operation_type;
    Int32Field out_client_id;
    Variable32Field out_file_id;
    Int64Field out_offset;
    Int32Field out_bytes;

    uint64_t duplicate_requests, duplicate_replies, failed_requests;
};

void doEllard(const vector<string> &args, const commonPackingArgs &packing_args) {
    NFSDSAnalysisMod::registerUnitsEpoch();

    TypeIndexModule *source = new TypeIndexModule("Trace::NFS::Ellard");

    for (unsigned i = 1; i < args.size() - 1; ++i) {
	source->addSource(args[i]);
    }

    DataSeriesSink outds(args.back(), packing_args.compress_modes, packing_args.compress_level);
    ExtentTypeLibrary library;
    const ExtentType::Ptr extent_type(library.registerTypePtr(output_xml));
    ExtentSeries out_series(extent_type);

    OutputModule out_module(outds, out_series, extent_type, packing_args.extent_size);

    outds.writeExtentLibrary(library);

    EllardConvert convert(*source, out_module);
    convert.getAndDeleteShared();
    convert.printResult();
}

int main(int argc, char *argv[]) {
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    vector<string> args;

    for (int i = 1; i < argc; ++i) {
	args.push_back(string(argv[i]));
    }

    INVARIANT(args.size() >= 3, "Usage: extract-cache-sim animation <input...> output");

    if (args[0] == "animation") {
	doAnimation(args, packing_args);
    } else if (args[0] == "ellard") {
	doEllard(args, packing_args);
    } else {
	FATAL_ERROR("?");
    }
    return 0;
}
