/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <Lintel/HashTable.hpp>
#include <Lintel/StringUtil.hpp>

#include "analysis/nfs/mod2.hpp"

using namespace std;
using boost::format;

// Two possible implementation of the large size * analysis:
// 1. Build a hash table over the attr-ops extents, and hash-join with
//    the common extents in a single pass
// 2. Do a merge join over the two tables (both are sorted roughly by
//    record-id) and do the analysis over the merged data
//
// We take the second approach because it will take a lot less memory when the
// attr-ops gets too large

// not intended for writing, leaves out packing options
const string attropscommonjoin_xml_in( 
  "<ExtentType name=\"attr-ops-join\">\n"
  "  <field type=\"int64\" name=\"request-at\" %1% comment=\"time of the request packet\" />\n"
  "  <field type=\"int64\" name=\"reply-at\" %1% comment=\"time of the reply packet\" />\n"
  "  <field type=\"int32\" name=\"server\" comment=\"32 bit packed IPV4 address\" print_format=\"%%08x\" />\n"
  "  <field type=\"int32\" name=\"client\" comment=\"32 bit packed IPV4 address\" print_format=\"%%08x\" />\n"
  "  <field type=\"variable32\" name=\"operation\" />\n"
  "  <field type=\"int64\" name=\"record-id\" />\n"
  "  <field type=\"variable32\" name=\"filename\" opt_nullable=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"filehandle\" print_maybehex=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"type\" />\n"
  "  <field type=\"int64\" name=\"file-size\" />\n"
  "  <field type=\"int64\" name=\"modify-time\" units=\"nanoseconds\" epoch=\"unix\" />\n"
  "  <field type=\"int32\" name=\"payload-length\" units=\"bytes\" />\n"
  "</ExtentType>\n");

// we cheat here and use the reply packet rather than the request
// packet because the request packets aren't quite sorted and we don't
// have a generic resort implemented, nor do I feel like writing a
// sort module here.

class AttrOpsCommonJoin : public NFSDSModule {
public:
    AttrOpsCommonJoin(DataSeriesModule &_nfs_common,
		      DataSeriesModule &_nfs_attrops)
	: nfs_common(_nfs_common), nfs_attrops(_nfs_attrops),
	  es_common(ExtentSeries::typeExact),
	  es_attrops(ExtentSeries::typeExact),
	  in_packetat(es_common,""),
	  out_requestat(es_out,"request-at"),
	  out_replyat(es_out,"reply-at"),
	  in_source(es_common,"source"),
	  out_server(es_out,"server"),
	  in_dest(es_common,"dest"),
	  out_client(es_out,"client"),
	  in_is_request(es_common,""),
	  in_operation(es_common,"operation"),
	  out_operation(es_out,"operation"),
	  in_recordid(es_common,""),
	  out_recordid(es_out,"record-id"),
	  in_requestid(es_attrops,""),
	  in_replyid(es_attrops,""),
	  in_filename(es_attrops,"filename",Field::flag_nullable),
	  out_filename(es_out,"filename",Field::flag_nullable),
	  in_filehandle(es_attrops,"filehandle"),
	  out_filehandle(es_out,"filehandle"),
	  in_type(es_attrops,"type"),
	  out_type(es_out,"type"),
	  in_filesize(es_attrops,""),
	  out_filesize(es_out,"file-size"),
	  in_modifytime(es_attrops,""),
	  out_modifytime(es_out,"modify-time"),
	  in_payloadlen(es_common,""),
	  out_payloadlen(es_out,"payload-length"),
	  all_done(false), prev_replyid(-1),
	  rotate_interval(5LL*60*1000*1000*1000LL), // 5 minutes in ns.
	  output_record_count(0), 
	  force_1us_turnaround_count(0),
	  output_bytes(0),
	  in_initial_skip_mode(true),
	  first_keep_time(0),
	  skipped_common_count(0), skipped_attrops_count(0), 
	  skipped_duplicate_attr_count(0), last_reply_id(-1)
    { 
	curreqht = new reqHashTable();
	prevreqht = new reqHashTable();
	last_rotate_time = 0;
    }

    virtual ~AttrOpsCommonJoin() { }

    void prepFields() {
	const ExtentType *type = es_common.getType();
	SINVARIANT(type != NULL);
	SINVARIANT(es_attrops.getType() != NULL);
	if (type->getName() == "NFS trace: common") {
	    SINVARIANT(type->getNamespace() == "" &&
		       type->majorVersion() == 0 &&
		       type->minorVersion() == 0);
	    SINVARIANT(es_attrops.getType()->getName() == "NFS trace: attr-ops" &&
		       es_attrops.getType()->majorVersion() == 0 &&
		       es_attrops.getType()->minorVersion() == 0);
	    in_packetat.setFieldName("packet-at");
	    in_is_request.setFieldName("is-request");
	    in_recordid.setFieldName("record-id");
	    in_requestid.setFieldName("request-id");
	    in_replyid.setFieldName("reply-id");
	    in_filesize.setFieldName("file-size");
	    in_modifytime.setFieldName("modify-time");
	    in_payloadlen.setFieldName("payload-length");
	} else if (type->getName() == "Trace::NFS::common"
		   && type->versionCompatible(2,0)) {
	    SINVARIANT(es_attrops.getType()->getName() == "Trace::NFS::attr-ops" &&
		       es_attrops.getType()->versionCompatible(2,0));

	    in_packetat.setFieldName("packet_at");
	    in_is_request.setFieldName("is_request");
	    in_recordid.setFieldName("record_id");
	    in_requestid.setFieldName("request_id");
	    in_replyid.setFieldName("reply_id");
	    in_filesize.setFieldName("file_size");
	    in_modifytime.setFieldName("modify_time");
	    in_payloadlen.setFieldName("payload_length");
	} else {
	    FATAL_ERROR("?");
	}
    }

    struct reqData {
	ExtentType::int64 request_id;
	ExtentType::int64 request_at;
    };
    
    struct reqHash {
	unsigned operator()(const reqData &k) {
	    unsigned ret,a,b;
	    a = (unsigned)(k.request_id >> 32);
	    b = (unsigned)(k.request_id & 0xFFFFFFFF);
	    ret = 1972;
	    BobJenkinsHashMix(a,b,ret);
	    return ret;
	}
    };

    struct reqEqual {
	bool operator()(const reqData &a, const reqData &b) {
	    return a.request_id == b.request_id;
	}
    };

    typedef HashTable<reqData,reqHash,reqEqual> reqHashTable;

    void initOutType() {
	SINVARIANT(es_common.getType() != NULL &&
		   es_attrops.getType() != NULL);
	
	string units_epoch;
	if (es_common.getType()->majorVersion() == 0) {
	    SINVARIANT(es_attrops.getType()->majorVersion() == 0);
	    units_epoch = "units=\"nanoseconds\" epoch=\"unix\"";
	} else if (es_common.getType()->majorVersion() == 2) {
	    SINVARIANT(es_attrops.getType()->majorVersion() == 2);
	    units_epoch = "units=\"2^-32 seconds\" epoch=\"unix\"";
	} else {
	    FATAL_ERROR("don't know units and epoch");
	}
	string tmp = (boost::format(attropscommonjoin_xml_in) 
		      % units_epoch).str();
	es_out.setType(ExtentTypeLibrary::sharedExtentType(tmp));
    }

    virtual Extent *getExtent() {
	if (all_done)
	    return NULL;
	if(es_common.curExtent() == NULL) {
	    Extent *tmp = nfs_common.getExtent();
	    if (tmp == NULL) {
		all_done = true;
		delete es_attrops.curExtent();
		es_attrops.clearExtent();
		return NULL;
	    }
	    es_common.setExtent(tmp);
	}

	Extent *attrextent = nfs_attrops.getExtent();
	if (attrextent == NULL) {
	    delete es_common.curExtent();
	    es_common.clearExtent();
	    all_done = true;
	    return NULL;
	}
	
	es_attrops.setExtent(attrextent);

	if (es_out.getType() == NULL) {
	    initOutType();
	    SINVARIANT(es_out.getType() != NULL);
	}

	Extent *outextent = new Extent(*es_out.getType());
	es_out.setExtent(outextent);

	if (in_packetat.getName().empty()) {
	    prepFields();
	    last_reply_id = in_replyid.val() - 1;
	}

	string fh;
	while(es_attrops.pos.morerecords()) {
	    if (es_common.pos.morerecords() == false) {
		delete es_common.curExtent();
		Extent *tmp = nfs_common.getExtent();
		if (tmp == NULL) {
		    es_common.clearExtent();
		    delete es_attrops.curExtent();
		    es_attrops.clearExtent();
		    return outextent;
		}
		es_common.setExtent(tmp);
	    }
	    AssertAlways(in_replyid.val() >= prev_replyid,
			 ("needsort %lld %lld",in_replyid.val(),prev_replyid));
	    prev_replyid = in_replyid.val();
	    if (in_recordid.val() < in_replyid.val()) {
		if (in_is_request.val()) {
		    if (last_rotate_time < (in_packetat.val() - rotate_interval)) {
			// much cheaper than scanning through the hash table looking for
			// old entries.
			delete prevreqht;
			prevreqht = curreqht;
			curreqht = new reqHashTable();
			last_rotate_time = in_packetat.val();
		    }
		    reqData d;
		    d.request_id = in_recordid.val();
		    d.request_at = in_packetat.val();
		    curreqht->add(d);
		}
		++es_common.pos;
	    } else if (in_initial_skip_mode && in_replyid.val() < in_recordid.val()) {
		// because we now prune some of the initial entries in
		// the common record extents so as to match exactly
		// the times that we want to capture, we now have a
		// chance that some of the attr-op reply ids that we
		// see at the beginning of the attr-op extent will not
		// match any of the entries.  However, they should all
		// come in a row right at the beginning, so check that
		// condition and allow the recordid == replyid check
		// fail if somehow this rule is violated.  This may
		// not actually be safe as there were some cases of
		// fairly substantial out of orderness in the data.
		++es_attrops.pos;
		++skipped_attrops_count;
	    } else if (in_replyid.val() == last_reply_id) {
		// This happens from the conversion of readdirs into
		// lots of attrops entries; not clear exactly what we
		// should do here.
		es_attrops.next();
		++skipped_duplicate_attr_count;
	    } else {
		if (in_initial_skip_mode) {
		    in_initial_skip_mode = false;
		    first_keep_time = in_packetat.val();
		}
		// Following can happen now that we generate multiple
		// attr-ops bits as a result of parsing all of the
		// readdir bits.

		INVARIANT(in_recordid.val() == in_replyid.val(),
			  format("mismatch on common(%d) and attr-ops(%d) tables")
			  % in_recordid.val() % in_replyid.val());
		AssertAlways(!in_is_request.val(), ("request not response being joined"));
		last_reply_id = in_replyid.val();

		reqData k;
		k.request_id = in_requestid.val();
		reqData *d = curreqht->lookup(k);
		if (d == NULL) {
		    //			printf("ZZ plook %lld\n",k.request_id);
		    d = prevreqht->lookup(k);
		}
		if (d == NULL) {
		    ++skipped_common_count;
		    ++skipped_attrops_count;
		    // because of the initial common pruning, we can
		    // now get the case where the reply was in the
		    // acceptable set, but the request wasn't.
		    AssertAlways(// assume request took at most 30 seconds to process
				 (in_packetat.val() - first_keep_time) < (ExtentType::int64)30*1000*1000*1000,
				 ("bad missing request %lld - %lld = %lld",
				  in_packetat.val(), first_keep_time, 
				  in_packetat.val() - first_keep_time));
		    ++es_common.pos;
		    ++es_attrops.pos;
		    continue;
		}
		AssertAlways(d != NULL,("unable to find request id %lld\n",k.request_id));
		es_out.newRecord();
		++output_record_count;
		out_requestat.set(d->request_at);
		out_replyat.set(in_packetat.val());
		if (in_packetat.val() <= d->request_at) {
		    if (false)
			fprintf(stderr,"Warning: %lld <= %lld on ids %lld/%lld; forcing 1us turnaround\n",
				d->request_at, in_packetat.val(),
				in_requestid.val(), in_replyid.val());
		    out_replyat.set(d->request_at + 1000);
		    ++force_1us_turnaround_count;
		}
		out_server.set(in_source.val());
		out_client.set(in_dest.val());
		out_operation.set(in_operation);
		out_recordid.set(in_requestid.val());
		if (in_filename.isNull()) {
		    out_filename.setNull(true);
		} else {
		    out_filename.set(in_filename);
		}
		out_filehandle.set(in_filehandle);
		out_type.set(in_type);
		out_filesize.set(in_filesize.val());
		out_modifytime.set(in_modifytime.val());
		out_payloadlen.set(in_payloadlen.val());

		++es_common.pos;
		++es_attrops.pos;
	    }
	}

	delete es_attrops.curExtent();
	es_attrops.clearExtent();
	output_bytes += outextent->extentsize();
	return outextent;
    }
    virtual void printResult() {
	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;
	cout << format("  generated %.3f million records, %.2f MB of extent output\n")
	    % ((double)output_record_count/1000000.0)
	    % ((double)output_bytes/(1024.0*1024.0));
	cout << format("  %lld records, or %.4f%% forced to 1us turnaround from previous 0 or negative turnaround\n")
	    % force_1us_turnaround_count
	    % (100.0*(double)force_1us_turnaround_count/(double)output_record_count);
	cout << format("  %d skipped common, %d skipped attrops, %d semi-duplicate attrops, first keep %.9fs\n")
	    % skipped_common_count % skipped_attrops_count % skipped_duplicate_attr_count
	    % ((double)first_keep_time/1.0e9);
	cout << format("End-%s\n") % __PRETTY_FUNCTION__;
    }

private:
    DataSeriesModule &nfs_common, &nfs_attrops;
    ExtentSeries es_common, es_attrops, es_out;
    Int64Field in_packetat, out_requestat, out_replyat;
    Int32Field in_source, out_server;
    Int32Field in_dest, out_client;
    BoolField in_is_request;
    Variable32Field in_operation, out_operation;
    Int64Field in_recordid, out_recordid, in_requestid, in_replyid;

    Variable32Field in_filename, out_filename, in_filehandle, out_filehandle, in_type, out_type;
    Int64Field in_filesize, out_filesize, in_modifytime, out_modifytime;
    Int32Field in_payloadlen, out_payloadlen;
    bool all_done;
    ExtentType::int64 prev_replyid;
    vector<string> ignore_filehandles;
    reqHashTable *curreqht, *prevreqht;
    ExtentType::int64 last_rotate_time;
    const ExtentType::int64 rotate_interval;

    ExtentType::int64 output_record_count, force_1us_turnaround_count, 
	output_bytes;
    bool in_initial_skip_mode;
    ExtentType::int64 first_keep_time;
    uint64_t skipped_common_count, skipped_attrops_count, skipped_duplicate_attr_count;
    int64_t last_reply_id;
};
	
NFSDSModule *
NFSDSAnalysisMod::newAttrOpsCommonJoin(DataSeriesModule &nfs_common,
				       DataSeriesModule &nfs_attrops)
{
    return new AttrOpsCommonJoin(nfs_common, nfs_attrops);
}

class LargeSizeFileHandle : public NFSDSModule {
public:
    LargeSizeFileHandle(DataSeriesModule &_source, int _nkeep)
	: source(_source),
	  operation(s,"operation"),
	  filehandle(s,"filehandle"),
	  filesize(s,"file-size"),
	  nkeep(_nkeep),min_file_size(0)
    { }
    virtual ~LargeSizeFileHandle() { }

    struct hteData {
	string filehandle,operation;
	ExtentType::int64 maxsize;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) {
	    return HashTable_hashbytes(k.filehandle.data(),k.filehandle.size());
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) {
	    return a.filehandle == b.filehandle;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	AssertAlways(e->type.getName() == "attr-ops-join",("bad\n"));

	hteData k;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (filesize.val() < min_file_size)
		continue;
	    k.filehandle = filehandle.stringval();
	    hteData *v = stats_table.lookup(k);
	    if (v == NULL) {
		k.operation = operation.stringval();
		k.maxsize = filesize.val();
		stats_table.add(k);
	    } else if (v->maxsize < filesize.val()) {
		k.operation = operation.stringval();
		k.maxsize = filesize.val();
	    }
	    // should do a min file size prune from time to time...
	}
	return e;
    }
    
    class sortByMaxSize {
    public:
	bool operator()(hteData *a, hteData *b) {
	    return a->maxsize > b->maxsize;
	}
    };
    
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	vector<hteData *> vals;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	}
	sort(vals.begin(),vals.end(),sortByMaxSize());
	for(unsigned int i=0;i<nkeep;++i) {
	    printf("%9s %s %lld\n",vals[i]->operation.c_str(),
		   hexstring(vals[i]->filehandle).c_str(),
		   vals[i]->maxsize);
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Variable32Field operation,filehandle;
    Int64Field filesize;
    unsigned int nkeep;
    ExtentType::int64 min_file_size;
};

NFSDSModule *
NFSDSAnalysisMod::newLargeSizeFileHandle(DataSeriesModule &prev,
					 int nkeep)
{
    return new LargeSizeFileHandle(prev,nkeep);
}

class LargeSizeFilename : public NFSDSModule {
public:
    LargeSizeFilename(DataSeriesModule &_source, int _nkeep)
	: source(_source),
	  operation(s,"operation"),
	  filehandle(s,"filehandle"),
	  filename(s,"filename",Field::flag_nullable),
	  filesize(s,"file-size"),
	  nkeep(_nkeep),min_file_size(0)
    { }
    virtual ~LargeSizeFilename() { }

    struct hteData {
	string filename,operation;
	ExtentType::int64 maxsize;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) {
	    return HashTable_hashbytes(k.filename.data(),k.filename.size());
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) {
	    return a.filename == b.filename;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	AssertAlways(e->type.getName() == "attr-ops-join",("bad\n"));

	hteData k;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (filesize.val() < min_file_size)
		continue;
	    if (filename.isNull()) {
		string *tmp = fnByFileHandle(filehandle.stringval());
		if (tmp == NULL) {
		    continue;
		} else {
		    k.filename = *tmp;
		}
	    } else {
		k.filename = filename.stringval();
	    }
	    hteData *v = stats_table.lookup(k);
	    if (v == NULL) {
		k.operation = operation.stringval();
		k.maxsize = filesize.val();
		stats_table.add(k);
	    } else if (v->maxsize < filesize.val()) {
		v->operation = operation.stringval();
		v->maxsize = filesize.val();
	    }
	    // should do a min file size prune from time to time...
	}
	return e;
    }
    
    class sortByMaxSize {
    public:
	bool operator()(hteData *a, hteData *b) {
	    return a->maxsize > b->maxsize;
	}
    };
    
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	vector<hteData *> vals;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	}
	sort(vals.begin(),vals.end(),sortByMaxSize());
	if (nkeep > vals.size()) 
	    nkeep = vals.size();
	for(unsigned int i=0;i<nkeep;++i) {
	    printf("%10s %10lld %s\n",vals[i]->operation.c_str(),
		   vals[i]->maxsize, maybehexstring(vals[i]->filename).c_str());
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Variable32Field operation,filehandle,filename;
    Int64Field filesize;
    unsigned int nkeep;
    ExtentType::int64 min_file_size;
};

NFSDSModule *
NFSDSAnalysisMod::newLargeSizeFilename(DataSeriesModule &prev,
				       int nkeep)
{
    return new LargeSizeFilename(prev,nkeep);
}

class LargeSizeFilenameWrite : public NFSDSModule {
public:
    LargeSizeFilenameWrite(DataSeriesModule &_source, int _nkeep)
	: source(_source),
	  operation(s,"operation"),
	  filehandle(s,"filehandle"),
	  filename(s,"filename",Field::flag_nullable),
	  filesize(s,"file-size"),
	  dest(s,"server"),
	  nkeep(_nkeep),min_file_size(0)
    { }
    virtual ~LargeSizeFilenameWrite() { }

    struct hteData {
	string filename;
	ExtentType::int64 maxsize;
	unsigned int dest;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) {
	    return HashTable_hashbytes(k.filename.data(),k.filename.size());
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) {
	    return a.filename == b.filename;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	AssertAlways(e->type.getName() == "attr-ops-join",("bad\n"));

	hteData k;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (filesize.val() < min_file_size)
		continue;
	    if (operation.size() != 5 || operation.stringval() != "write") {
		continue;
	    }
	    if (filename.isNull()) {
		string *tmp = fnByFileHandle(filehandle.stringval());
		if (tmp == NULL) {
		    continue;
		} else {
		    k.filename = *tmp;
		}
	    } else {
		k.filename = filename.stringval();
	    }
	    hteData *v = stats_table.lookup(k);
	    if (v == NULL) {
		k.maxsize = filesize.val();
		k.dest = dest.val();
		stats_table.add(k);
	    } else if (v->maxsize < filesize.val()) {
		v->maxsize = filesize.val();
		v->dest = dest.val();
	    }
	    // should do a min file size prune from time to time...
	}
	return e;
    }
    
    class sortByMaxSize {
    public:
	bool operator()(hteData *a, hteData *b) {
	    return a->maxsize > b->maxsize;
	}
    };
    
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	vector<hteData *> vals;
	unsigned long long sum_write = 0;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	    sum_write += i->maxsize;
	}
	printf("sum(max size seen for each written file with known name) = %.2f MB\n",
	       (double)sum_write/(1024.0*1024.0));
	sort(vals.begin(),vals.end(),sortByMaxSize());
	if (nkeep > vals.size()) 
	    nkeep = vals.size();
	for(unsigned int i=0;i<nkeep;++i) {
	    printf("%10lld %d.%d.%d.%d %s\n",
		   vals[i]->maxsize, 
		   (vals[i]->dest >> 24) & 0xFF,
		   (vals[i]->dest >> 16) & 0xFF,
		   (vals[i]->dest >> 8) & 0xFF,
		   (vals[i]->dest >> 0) & 0xFF,
		   maybehexstring(vals[i]->filename).c_str());
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Variable32Field operation,filehandle,filename;
    Int64Field filesize;
    Int32Field dest;
    unsigned int nkeep;
    ExtentType::int64 min_file_size;
};

NFSDSModule *
NFSDSAnalysisMod::newLargeSizeFilenameWrite(DataSeriesModule &prev,
					    int nkeep)
{
    return new LargeSizeFilenameWrite(prev,nkeep);
}

class LargeSizeFilehandleWrite : public NFSDSModule {
public:
    LargeSizeFilehandleWrite(DataSeriesModule &_source, int _nkeep)
	: source(_source),
	  operation(s,"operation"),
	  filehandle(s,"filehandle"),
	  filesize(s,"file-size"),
	  dest(s,"server"),
	  nkeep(_nkeep),min_file_size(0)
    { }
    virtual ~LargeSizeFilehandleWrite() { }

    struct hteData {
	string filehandle;
	ExtentType::int64 maxsize;
	unsigned int dest;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) {
	    return HashTable_hashbytes(k.filehandle.data(),k.filehandle.size());
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) {
	    return a.filehandle == b.filehandle;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	AssertAlways(e->type.getName() == "attr-ops-join",("bad\n"));

	hteData k;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (filesize.val() < min_file_size)
		continue;
	    if (operation.size() != 5 || operation.stringval() != "write") {
		continue;
	    }
	    k.filehandle = filehandle.stringval();
	    hteData *v = stats_table.lookup(k);
	    if (v == NULL) {
		k.maxsize = filesize.val();
		k.dest = dest.val();
		stats_table.add(k);
	    } else if (v->maxsize < filesize.val()) {
		v->maxsize = filesize.val();
		v->dest = dest.val();
	    }
	    // should do a min file size prune from time to time...
	}
	return e;
    }
    
    class sortByMaxSize {
    public:
	bool operator()(hteData *a, hteData *b) {
	    return a->maxsize > b->maxsize;
	}
    };
    
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	vector<hteData *> vals;
	unsigned long long sum_write = 0;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	    sum_write += i->maxsize;
	}
	printf("sum(max size seen for each written filehandle) = %.2f MB\n",
	       (double)sum_write/(1024.0*1024.0));
	sort(vals.begin(),vals.end(),sortByMaxSize());
	if (nkeep > vals.size()) 
	    nkeep = vals.size();
	for(unsigned int i=0;i<nkeep;++i) {
	    printf("%10lld %d.%d.%d.%d %s\n",
		   vals[i]->maxsize, 
		   (vals[i]->dest >> 24) & 0xFF,
		   (vals[i]->dest >> 16) & 0xFF,
		   (vals[i]->dest >> 8) & 0xFF,
		   (vals[i]->dest >> 0) & 0xFF,
		   hexstring(vals[i]->filehandle).c_str());
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Variable32Field operation,filehandle;
    Int64Field filesize;
    Int32Field dest;
    unsigned int nkeep;
    ExtentType::int64 min_file_size;
};

NFSDSModule *
NFSDSAnalysisMod::newLargeSizeFilehandleWrite(DataSeriesModule &prev,
					      int nkeep)
{
    return new LargeSizeFilehandleWrite(prev,nkeep);
}

class FileageByFilehandle : public NFSDSModule {
public:
    FileageByFilehandle(DataSeriesModule &_source, int _nkeep,
			int _recent_age_seconds)
	: source(_source),
	  server(s,"server"),
	  packet_at(s,"reply-at"),
	  modify_time(s,"modify-time"),
	  filehandle(s,"filehandle"),
	  filename(s,"filename",Field::flag_nullable),
	  type(s,"type"),
	  filesize(s,"file-size"),
	  nkeep(_nkeep),min_file_size(0),
	  recent_age_seconds(_recent_age_seconds)
    { }
    virtual ~FileageByFilehandle() { }

    // TODO: add option to include server in the "filehandle"; current
    // implementation assumes globally unique filehandles, which
    // happens to be true for NetApp filers, but does not have to be
    // true in general.

    struct hteData {
	ExtentType::int32 server;
	string filehandle, filename, type;
	ExtentType::int64 maxsize, file_age;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) {
	    return HashTable_hashbytes(k.filehandle.data(),k.filehandle.size(),k.server);
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) {
	    return a.server == b.server && a.filehandle == b.filehandle;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    ExtentType::int64 llround(ExtentType::int64 v,int to) {
	if (v < 0) return -llround(-v,to);
	int vx = v % to;
	if (vx >= to/2) {
	    return v + to - vx;
	} else {
	    return v - vx;
	}
    }

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	AssertAlways(e->type.getName() == "attr-ops-join",("bad\n"));

	hteData k;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    k.filehandle = filehandle.stringval();
	    k.server = server.val();

	    hteData *v = stats_table.lookup(k);
	    ExtentType::int64 file_age = packet_at.val() - modify_time.val();
	    //	    file_age = llround(file_age,100000);

	    if (v == NULL) {
		k.type = type.stringval();
		k.maxsize = filesize.val();
		k.file_age = file_age;
		v = stats_table.add(k);
	    } else {
		if (filesize.val() > v->maxsize) {
		    v->maxsize = filesize.val();
		}
		if (file_age < v->file_age) {
		    v->file_age = file_age;
		}
	    }
	    if (v->filename.empty() == true && filename.isNull() == false) {
		v->filename = filename.stringval();
	    }
	}
	return e;
    }
    
    class sortByMinFileAge {
    public:
	bool operator()(hteData *a, hteData *b) {
	    return a->file_age == b->file_age ? a->maxsize < b->maxsize : a->file_age < b->file_age;
	}
    };
    
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	vector<hteData *> vals;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	}
	sort(vals.begin(),vals.end(),sortByMinFileAge());
	if (nkeep > vals.size()) 
	    nkeep = vals.size();
	for(unsigned int i=0;i<nkeep;++i) {
	    if (vals[i]->filename.empty()) {
		vals[i]->filename = "*unknown*";
	    }
	    printf("%10.3f secs %s %20s %8s %lld\n",
		   (double)vals[i]->file_age / (1.0e9),
		   hexstring(vals[i]->filehandle).c_str(),
		   maybehexstring(vals[i]->filename).c_str(),
		   vals[i]->type.c_str(),
		   vals[i]->maxsize);
	}
	double recent_mb = 0;
	double old_mb = 0;
	const double recent_age_boundary = recent_age_seconds;
	int nrecent = 0;
	for(unsigned int i=0;i<vals.size();++i) {
	    double age = vals[i]->file_age / 1.0e9;
	    if (age < recent_age_boundary) {
		++nrecent;
		//		printf("%.5f %lld\n",age,vals[i]->maxsize);
		recent_mb += vals[i]->maxsize / (1024.0*1024.0);
	    } else {
		old_mb += vals[i]->maxsize / (1024.0*1024.0);
	    }
	}
	printf("%d unique filehandles, %d recent (%d seconds): %.2f GB total files accessed; %.2f GB recent, or %.2f%%\n",
	       vals.size(), nrecent, recent_age_seconds, (recent_mb + old_mb)/1024.0, recent_mb/1024.0, 100.0 * recent_mb / (recent_mb+old_mb));
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Int32Field server;
    Int64Field packet_at, modify_time;
    Variable32Field filehandle,filename,type;
    Int64Field filesize;
    unsigned int nkeep;
    ExtentType::int64 min_file_size;
    const int recent_age_seconds;
};



NFSDSModule *
NFSDSAnalysisMod::newFileageByFilehandle(DataSeriesModule &prev,
					 int nkeep,
					 int recent_age_seconds)
{
    return new FileageByFilehandle(prev, nkeep, recent_age_seconds);
}

