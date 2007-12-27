// -*-C++-*-
/*
   (c) Copyright 2007 Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file 
    Emulate the Ellard nfsscan per-client analysis
*/
#include <Lintel/HashMap.H>

#include <DataSeries/PrefetchBufferModule.H>
#include <DataSeries/RowAnalysisModule.H>
#include <DataSeries/SequenceModule.H>
#include <DataSeries/TypeIndexModule.H>

using namespace std;
using boost::format;

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

unsigned
unifiedOpToId(const string &operation)
{
    for(unsigned i = 0; i < n_unified; ++i) {
	if (operation == unified_ops[i])
	    return i;
    }
    FATAL_ERROR("?");
}

int
v2optoUnifiedId(const string &operation)
{
    for(unsigned i = 0; i < n_nfsv2ops; ++i) {
	if (operation == nfsv2ops[i].name)
	    return nfsv2ops[i].unified_id;
    }
    return -1;
}

int
v3optoUnifiedId(const string &operation)
{
    for(unsigned i = 0; i < n_nfsv3ops; ++i) {
	if (operation == nfsv3ops[i].name)
	    return nfsv3ops[i].unified_id;
    }
    return -1;
}

class EllardAnalysisCountPerInt32 : public RowAnalysisModule {
public:
    EllardAnalysisCountPerInt32(DataSeriesModule &source,
				const string &per_what,
				const vector<string> &_interesting)
        : RowAnalysisModule(source),
	  time(series, "time"),
	  is_call(series, "is_call"),
	  nfs_version(series, "nfs_version"),
	  op_id(series, "rpc_function_id"),
	  operation(series, "rpc_function"),
	  per(series, per_what),
	  interesting_ops(_interesting)
    {
	interesting.resize(n_unified);
	for(vector<string>::const_iterator i = interesting_ops.begin();
	    i != interesting_ops.end(); ++i) {
	    int v2id = v2optoUnifiedId(*i);
	    int v3id = v3optoUnifiedId(*i);
	    INVARIANT(v2id >= 0 || v3id >= 0,
		      format("unrecognized operation %s") % *i);
	    INVARIANT(v2id == v3id || v2id == -1 || v3id == -1, "?");

	    if (v2id >= 0) interesting[v2id] = true;
	    if (v3id >= 0) interesting[v3id] = true;
	}
    }

    virtual ~EllardAnalysisCountPerInt32() { }

    virtual void processRow() {
	if (!is_call.val())
	    return;
	unsigned unified_id;
	if (nfs_version.val() == 2) {
	    INVARIANT(op_id.val() < n_nfsv2ops, "?");
	    DEBUG_INVARIANT(operation.equal(nfsv2ops[op_id.val()].name), "?");
	    unified_id = nfsv2ops[op_id.val()].unified_id;
	} else if (nfs_version.val() == 3) {
	    INVARIANT(op_id.val() < n_nfsv3ops, "?");
	    DEBUG_INVARIANT(operation.equal(nfsv3ops[op_id.val()].name), 
			    format("? %d %s %s")
			    % op_id.val() % operation.stringval() 
			    % nfsv3ops[op_id.val()].name);
	    unified_id = nfsv3ops[op_id.val()].unified_id;
	} else {
	    FATAL_ERROR("bad nfs version");
	    return;
	}
	
	infoT *info = per_to_info[per.val()];
	if (info == NULL) {
	    per_to_info[per.val()] = info = new infoT;
	}
	++info->total;
	if (interesting[unified_id]) {
	    ++info->interesting;
	    ++info->count[unified_id];
	}
    }

    virtual void prepareForProcessing() {
	first_seconds = time.val() / 1000000.0;
    }

    virtual void printResult() {
	cout << format("#C time client euid egid fh TOTAL INTERESTING %s\n")
	    % join(" ", interesting_ops);

	string what_fmt;

	for(HashMap<int32_t, infoT *>::iterator i = per_to_info.begin();
	    i != per_to_info.end(); ++i) {
	    if (per.getName() == "source_ip") {
		cout << format("C %.0f %s u u u %d %d")
		    % first_seconds
		    % ipv4tostring(i->first) % i->second->total
		    % i->second->interesting;
	    } else { 
		// euid, egid not in home traces
		FATAL_ERROR("?");
	    }
	    for(vector<string>::const_iterator j = interesting_ops.begin();
		j != interesting_ops.end(); ++j) {
		unsigned unified_id = unifiedOpToId(*j);
		INVARIANT(*j == unified_ops[unified_id], "?");
		cout << format(" %d") % i->second->count[unified_id];
	    }
	    cout << "\n";
	}
    }

    struct infoT {
	uint64_t total, interesting;
	vector<uint64_t> count;
	infoT() : total(0), interesting(0) {
	    count.resize(n_unified);
	}
    };


private:
    Int64Field time;
    BoolField is_call;
    ByteField nfs_version;
    ByteField op_id;
    Variable32Field operation;
    Int32Field per;
    const vector<string> interesting_ops;
    vector<bool> interesting;
    
    HashMap<int32_t, infoT *> per_to_info;
    double first_seconds;
};

int
main(int argc, char *argv[])
{
    TypeIndexModule source("Trace::NFS::Ellard");
    PrefetchBufferModule *prefetch
	= new PrefetchBufferModule(source, 64*1024*1024);

    INVARIANT(argc >= 2 && strcmp(argv[1], "-h") != 0,
	      boost::format("Usage: %s <file...>\n") % argv[0]);

    for(int i = 1; i < argc; ++i) {
	source.addSource(argv[i]);
    }

    SequenceModule seq(prefetch);

    vector<string> interesting;
    interesting.push_back("read");
    interesting.push_back("write");
    interesting.push_back("lookup");
    interesting.push_back("getattr");
    interesting.push_back("access");
    interesting.push_back("create");
    interesting.push_back("remove");

    seq.addModule(new EllardAnalysisCountPerInt32(seq.tail(),
						  "source_ip",
						  interesting));

    seq.getAndDelete();
    RowAnalysisModule::printAllResults(seq);
    return 0;
}


