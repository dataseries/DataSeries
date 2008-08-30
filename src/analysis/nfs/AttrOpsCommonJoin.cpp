#include <string>

#include <Lintel/PointerUtil.hpp>

#include <DataSeries/SequenceModule.hpp>
#include <analysis/nfs/common.hpp>

using namespace std;
using boost::format;

// not intended for writing, leaves out packing options
const string attropscommonjoin_xml_in( 
  "<ExtentType name=\"attr-ops-join\">\n"
  "  <field type=\"int64\" name=\"request-at\" %1% />\n"
  "  <field type=\"int64\" name=\"reply-at\" %1% />\n"
  "  <field type=\"int32\" name=\"server\" />\n"
  "  <field type=\"int32\" name=\"client\" />\n"
  "  <field type=\"byte\" name=\"unified-op-id\" />\n"
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

// TODO: redo-with rotating hash map.
class AttrOpsCommonJoin : public NFSDSModule {
public:
    AttrOpsCommonJoin()
	: nfs_common(NULL), nfs_attrops(NULL),
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
	  in_op_id(es_common, "", Field::flag_nullable),
	  in_nfs_version(es_common, "", Field::flag_nullable),
	  out_unified_op_id(es_out,"unified-op-id"),
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
	  curreqht(new reqHashTable()),
	  prevreqht(new reqHashTable()),
	  last_rotate_time_raw(0),
	  rotate_interval_raw(0), 
	  output_record_count(0), 
	  force_1us_turnaround_count(0),
	  output_bytes(0),
	  in_initial_skip_mode(true),
	  first_keep_time_raw(0),
	  skipped_common_count(0), skipped_attrops_count(0), 
	  skipped_duplicate_attr_count(0), 
	  last_reply_id(numeric_limits<int64_t>::min())
    { }

    virtual ~AttrOpsCommonJoin() { }

    void setInputs(DataSeriesModule &common, DataSeriesModule &attr_ops) {
	nfs_common = &common;
	nfs_attrops = &attr_ops;
    }

    void prepFields(Extent *e) {
	const ExtentType &type = e->getType();
	if (type.getName() == "NFS trace: common") {
	    SINVARIANT(type.getNamespace() == "" &&
		       type.majorVersion() == 0 &&
		       type.minorVersion() == 0);
	    in_packetat.setFieldName("packet-at");
	    in_is_request.setFieldName("is-request");
	    in_op_id.setFieldName("op-id");
	    in_nfs_version.setFieldName("nfs-version");
	    in_recordid.setFieldName("record-id");
	    in_requestid.setFieldName("request-id");
	    in_replyid.setFieldName("reply-id");
	    in_filesize.setFieldName("file-size");
	    in_modifytime.setFieldName("modify-time");
	    in_payloadlen.setFieldName("payload-length");
	} else if (type.getName() == "Trace::NFS::common"
		   && type.versionCompatible(1,0)) {
	    in_packetat.setFieldName("packet-at");
	    in_is_request.setFieldName("is-request");
	    in_op_id.setFieldName("op-id");
	    in_nfs_version.setFieldName("nfs-version");
	    in_recordid.setFieldName("record-id");
	    in_requestid.setFieldName("request-id");
	    in_replyid.setFieldName("reply-id");
	    in_filesize.setFieldName("file-size");
	    in_modifytime.setFieldName("modify-time");
	    in_payloadlen.setFieldName("payload-length");
	} else if (type.getName() == "Trace::NFS::common"
		   && type.versionCompatible(2,0)) {
	    in_packetat.setFieldName("packet_at");
	    in_is_request.setFieldName("is_request");
	    in_op_id.setFieldName("op_id");
	    in_nfs_version.setFieldName("nfs_version");
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
	Int64TimeField::Raw request_at_raw;
	uint8_t unified_op_id;
    };
    
    struct reqHash {
	unsigned operator()(const reqData &k) const {
	    unsigned ret,a,b;
	    a = (unsigned)(k.request_id >> 32);
	    b = (unsigned)(k.request_id & 0xFFFFFFFF);
	    ret = 1972;
	    BobJenkinsHashMix(a,b,ret);
	    return ret;
	}
    };

    struct reqEqual {
	bool operator()(const reqData &a, const reqData &b) const {
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
	} else if (es_common.getType()->majorVersion() == 1) { 
	    SINVARIANT(es_attrops.getType()->majorVersion() == 1);
	    units_epoch = "units=\"2^-32 seconds\" epoch=\"unix\"";
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
	    Extent *tmp = nfs_common->getExtent();
	    if (tmp == NULL) {
		all_done = true;
		delete es_attrops.curExtent();
		es_attrops.clearExtent();
		return NULL;
	    }

	    if (in_packetat.getName().empty()) {
		prepFields(tmp);
	    }

	    es_common.setExtent(tmp);
	    if (rotate_interval_raw == 0) {
		rotate_interval_raw = in_packetat.secNanoToRaw(5*60,0);
	    }
	}

	Extent *attrextent = nfs_attrops->getExtent();
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

	string fh;
	while(es_attrops.pos.morerecords()) {
	    if (es_common.pos.morerecords() == false) {
		delete es_common.curExtent();
		Extent *tmp = nfs_common->getExtent();
		if (tmp == NULL) {
		    es_common.clearExtent();
		    delete es_attrops.curExtent();
		    es_attrops.clearExtent();
		    return outextent;
		}
		es_common.setExtent(tmp);
	    }
	    INVARIANT(in_replyid.val() >= prev_replyid, 
		      format("needsort %lld %lld") 
		      % in_replyid.val() % prev_replyid);
	    prev_replyid = in_replyid.val();
	    if (in_recordid.val() < in_replyid.val()) {
		if (in_is_request.val()) {
		    if (last_rotate_time_raw 
			< (in_packetat.valRaw() - rotate_interval_raw)) {
			// much cheaper than scanning through the hash
			// table looking for old entries.
			delete prevreqht;
			prevreqht = curreqht;
			curreqht = new reqHashTable();
			last_rotate_time_raw = in_packetat.valRaw();
		    }
		    reqData d;
		    d.request_id = in_recordid.val();
		    d.request_at_raw = in_packetat.valRaw();
		    SINVARIANT(!in_nfs_version.isNull() &&
			       !in_op_id.isNull());
		    d.unified_op_id = opIdToUnifiedId(in_nfs_version.val(),
						      in_op_id.val());
		    curreqht->add(d);
		} else {
		    // reply common record entry that occurs before
		    // the first attr-ops entry we have; usually a
		    // result of common operations that have no
		    // attr-ops.
		    ++skipped_common_count;
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
		    first_keep_time_raw = in_packetat.valRaw();
		}
		// Following can happen now that we generate multiple
		// attr-ops bits as a result of parsing all of the
		// readdir bits.

		INVARIANT(in_recordid.val() == in_replyid.val(),
			  format("mismatch on common(%d) and attr-ops(%d) tables")
			  % in_recordid.val() % in_replyid.val());
		INVARIANT(!in_is_request.val(), 
			  "request not response being joined");
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
		    INVARIANT(// assume request took at most 30 seconds to process; rare so we don't try to be efficient
			      (in_packetat.valRaw() - first_keep_time_raw) 
			      < in_packetat.secNanoToRaw(30,0),
			      format("bad missing request %d - %d = %d")
			      % in_packetat.valRaw() % first_keep_time_raw  
			      % (in_packetat.valRaw() - first_keep_time_raw));
		    ++es_common.pos;
		    ++es_attrops.pos;
		    continue;
		}
		es_out.newRecord();
		++output_record_count;
		out_requestat.setRaw(d->request_at_raw);
		out_replyat.setRaw(in_packetat.valRaw());
		out_unified_op_id.set(d->unified_op_id);
		if (in_packetat.valRaw() <= d->request_at_raw) {
		    if (false)
			fprintf(stderr,"Warning: %lld <= %lld on ids %lld/%lld; forcing 1us turnaround\n",
				d->request_at_raw, in_packetat.valRaw(),
				in_requestid.val(), in_replyid.val());
		    out_replyat.setRaw(d->request_at_raw 
				       + in_packetat.secNanoToRaw(0,1000));
		    ++force_1us_turnaround_count;
		}
		out_server.set(in_source.val());
		out_client.set(in_dest.val());
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
	if (output_record_count == 0) {
	    cout << "  No output from Join.\n";
	} else {
	    cout << format("  generated %.3f million records, %.2f MB of extent output\n")
		% ((double)output_record_count/1000000.0)
		% ((double)output_bytes/(1024.0*1024.0));
	    cout << format("  %lld records, or %.4f%% forced to 1us turnaround from previous 0 or negative turnaround\n")
		% force_1us_turnaround_count
		% (100.0*(double)force_1us_turnaround_count/(double)output_record_count);
	    cout << format("  %d skipped common, %d skipped attrops, %d semi-duplicate attrops, first keep %ss\n")
		% skipped_common_count % skipped_attrops_count % skipped_duplicate_attr_count
		% in_packetat.rawToStrSecNano(first_keep_time_raw);
	    cout << format("End-%s\n") % __PRETTY_FUNCTION__;
	}
    }

private:
    DataSeriesModule *nfs_common, *nfs_attrops;
    ExtentSeries es_common, es_attrops, es_out;
    Int64TimeField in_packetat, out_requestat, out_replyat;
    Int32Field in_source, out_server;
    Int32Field in_dest, out_client;
    BoolField in_is_request;
    ByteField in_op_id, in_nfs_version, out_unified_op_id;
    Int64Field in_recordid, out_recordid, in_requestid, in_replyid;

    Variable32Field in_filename, out_filename, in_filehandle, out_filehandle, in_type, out_type;
    Int64Field in_filesize, out_filesize, in_modifytime, out_modifytime;
    Int32Field in_payloadlen, out_payloadlen;
    bool all_done;
    ExtentType::int64 prev_replyid;
    vector<string> ignore_filehandles;
    reqHashTable *curreqht, *prevreqht;
    Int64TimeField::Raw last_rotate_time_raw;
    Int64TimeField::Raw rotate_interval_raw;

    ExtentType::int64 output_record_count, force_1us_turnaround_count, 
	output_bytes;
    bool in_initial_skip_mode;
    ExtentType::int64 first_keep_time_raw;
    uint64_t skipped_common_count, skipped_attrops_count, 
	      skipped_duplicate_attr_count;
    int64_t last_reply_id;
};
	
namespace NFSDSAnalysisMod {
    NFSDSModule *newAttrOpsCommonJoin() {
	return new AttrOpsCommonJoin();
    }

    void setAttrOpsSources(DataSeriesModule *join, 
			   SequenceModule &common_seq, 
			   SequenceModule &attrops_seq) {
	lintel::safeDownCast<AttrOpsCommonJoin>(join)
	    ->setInputs(common_seq.tail(), attrops_seq.tail());
    }
}

