#include <string>

#include <Lintel/PointerUtil.hpp>
#include <Lintel/PriorityQueue.hpp>

#include <DataSeries/SequenceModule.hpp>
#include <analysis/nfs/common.hpp>

using namespace std;
using boost::format;
using dataseries::TFixedField;

// not intended for writing, leaves out packing options
const string commonattrrw_xml_in( 
  "<ExtentType name=\"common-attr-rw-join\">\n"
  "  <field type=\"int64\" %1% name=\"request_at\" />\n"
  "  <field type=\"int64\" %1% name=\"reply_at\" note=\"sorted by this\" />\n"
  "  <field type=\"int32\" name=\"server\" />\n"
  "  <field type=\"int32\" name=\"client\" />\n"
  "  <field type=\"byte\" name=\"unified_op_id\" />\n"
  "  <field type=\"variable32\" name=\"filehandle\" print_hex=\"yes\" />\n"
  "  <field type=\"bool\" name=\"is_read\" />\n"
  "  <field type=\"int64\" name=\"file_size\" />\n"
  "  <field type=\"int64\" name=\"modify_time\" />\n"
  "  <field type=\"int64\" name=\"offset\" />\n"
  "  <field type=\"int32\" name=\"bytes\" />\n"
  "</ExtentType>\n");

class CommonAttrRWJoin: public NFSDSModule {
public:
    CommonAttrRWJoin()
	: commonattr(NULL), rw(NULL),
	  es_commonattr(ExtentSeries::typeExact),
	  es_rw(ExtentSeries::typeExact),
	  in_request_at(es_commonattr,"request-at"),
	  out_request_at(es_out,"request_at"),
	  in_reply_at(es_commonattr,"reply-at"),
	  out_reply_at(es_out,"reply_at"),
	  in_server(es_commonattr,"server"),
	  out_server(es_out,"server"),
	  in_client(es_commonattr,"client"),
	  out_client(es_out,"client"),
	  in_unified_op_id(es_commonattr,"unified-op-id"),
	  out_unified_op_id(es_out,"unified_op_id"),
	  in_ca_reply_id(es_commonattr, "reply-id"),
	  in_rw_reply_id(es_rw, "reply-id"),
	  in_filehandle(es_commonattr,"filehandle"),
	  out_filehandle(es_out,"filehandle"),
	  in_filesize(es_commonattr,"file-size"),
	  out_filesize(es_out,"file_size"),
	  in_modifytime(es_commonattr,"modify-time"),
	  out_modifytime(es_out,"modify_time"),
	  in_offset(es_rw,"offset"),
	  out_offset(es_out,"offset"),
	  in_bytes(es_rw,"bytes"),
	  out_bytes(es_out,"bytes"),
	  in_is_read(es_rw,"is-read"),
	  out_is_read(es_out,"is_read"),
	  rw_done(false), commonattr_done(false),
	  skip_count(0), last_reply_id(-1), 
	  read_unified_id(nameToUnifiedId("read")),
	  write_unified_id(nameToUnifiedId("write")),
	  output_bytes(0)
    { }

    virtual ~CommonAttrRWJoin() { }

    void initOutType() {
	SINVARIANT(es_commonattr.getType() != NULL &&
		   es_out.getType() == NULL);

	xmlNodePtr ftype 
	    = es_commonattr.getType()->xmlNodeFieldDesc("request-at");
	string units = ExtentType::strGetXMLProp(ftype, "units");
	string epoch = ExtentType::strGetXMLProp(ftype, "epoch");
	
	string repl = str(format("units=\"%s\" epoch=\"%s\"") % units % epoch);
	string tmp = str(format(commonattrrw_xml_in) % repl);
	es_out.setType(ExtentTypeLibrary::sharedExtentType(tmp));
    }

    void setInputs(DataSeriesModule &common_attr_mod, 
		   DataSeriesModule &rw_mod) {
	commonattr = &common_attr_mod;
	rw = &rw_mod;
    }

    void nextRWExtent() {
	if (rw_done) {
	    return;
	}
	delete es_rw.extent();
	Extent *tmp = rw->getExtent();
	if (tmp == NULL) {
	    rw_done = true;
	} 
	es_rw.setExtent(tmp);
    }

    void nextCommonAttrExtent() {
	if (commonattr_done) {
	    return;
	}
	delete es_commonattr.extent();
	Extent *tmp = commonattr->getExtent();
	if (tmp == NULL) {
	    commonattr_done = true;
	} 
	es_commonattr.setExtent(tmp);
    }

    void finishCheck() {
	// should be at end of rw extents
	if (!rw_done) {
	    SINVARIANT(!es_rw.morerecords());
	    nextRWExtent();
	    SINVARIANT(rw_done);
	}
	if (!commonattr_done && !es_commonattr.morerecords()) {
	    nextCommonAttrExtent();
	}
	// may have more attr ops, but no read/write ones.
	while(!commonattr_done) {
	    SINVARIANT(es_commonattr.morerecords());
	    SINVARIANT(in_unified_op_id.val() != read_unified_id &&
		       in_unified_op_id.val() != write_unified_id);
	    ++es_commonattr;
	    if (!es_commonattr.morerecords()) {
		nextCommonAttrExtent();
	    }
	}
    }

    virtual Extent *getExtent() { 
	if (commonattr_done && rw_done) {
	    return NULL;
	}
	if (es_out.getType() == NULL) {
	    SINVARIANT(es_commonattr.extent() == NULL
		       && es_rw.extent() == NULL);
	    nextCommonAttrExtent();
	    nextRWExtent();
	    initOutType();
	}
	Extent *outextent = new Extent(*es_out.getType());
	es_out.setExtent(outextent);

	while(outextent->extentsize() < 128*1024) {
	restart:
	    if (es_rw.extent() == NULL || es_rw.morerecords() == false) {
		nextRWExtent();
	    }
	    if (es_commonattr.extent() == NULL 
		|| es_commonattr.morerecords() == false) {
		nextCommonAttrExtent();
	    }
	    if (rw_done || commonattr_done) {
		finishCheck();
		break;
	    }

	    while (in_ca_reply_id.val() < in_rw_reply_id.val()) {
		SINVARIANT(in_unified_op_id.val() != read_unified_id &&
			   in_unified_op_id.val() != write_unified_id);
		++es_commonattr;
		if (!es_commonattr.morerecords()) {
		    goto restart;
		}
	    }
	    SINVARIANT(in_ca_reply_id.val() == in_rw_reply_id.val());
	    
	    es_out.newRecord();
	    out_request_at.set(in_request_at.val());
	    out_reply_at.set(in_reply_at.val());
	    out_server.set(in_server.val());
	    out_client.set(in_client.val());
	    out_unified_op_id.set(in_unified_op_id.val());
	    out_filehandle.set(in_filehandle);
	    out_filesize.set(in_filesize.val());
	    out_modifytime.set(in_modifytime.val());
	    out_offset.set(in_offset.val());
	    out_bytes.set(in_bytes.val());
	    out_is_read.set(in_is_read.val());
	    ++es_commonattr;
	    ++es_rw;
	}

	output_bytes += outextent->extentsize();
	return outextent;
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	printf("  skipped %d records in join -- no match found\n",skip_count);
	printf("  generated %.2f MB of extent output\n",(double)output_bytes/(1024.0*1024.0));
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
private:
    DataSeriesModule *commonattr, *rw;
    ExtentSeries es_commonattr, es_rw, es_out;
    TFixedField<int64_t> in_request_at, out_request_at;
    TFixedField<int64_t> in_reply_at, out_reply_at;
    TFixedField<int32_t> in_server, out_server;
    TFixedField<int32_t> in_client, out_client;
    TFixedField<uint8_t> in_unified_op_id, out_unified_op_id;
    TFixedField<int64_t> in_ca_reply_id, in_rw_reply_id;
    Variable32Field in_filehandle, out_filehandle; 
    TFixedField<int64_t> in_filesize, out_filesize;
    TFixedField<int64_t> in_modifytime, out_modifytime;
    TFixedField<int64_t> in_offset, out_offset;
    TFixedField<int32_t> in_bytes, out_bytes;
    BoolField in_is_read, out_is_read;

    bool rw_done, commonattr_done;
    int skip_count;
    int64_t last_reply_id;

    uint8_t read_unified_id, write_unified_id;
    ExtentType::int64 output_bytes;
};

#if OLD_IMPLEMENTATION

const string commonattrrw_xml( // not intended for writing, leaves out packing options
  "<ExtentType name=\"common-attr-rw-join\">\n"
  "  <field type=\"int64\" name=\"packet-at\" comment=\"currently the reply packet, not the request one\" />\n"
  "  <field type=\"int32\" name=\"server\" comment=\"32 bit packed IPV4 address\" print_format=\"%08x\" />\n"
  "  <field type=\"int32\" name=\"client\" comment=\"32 bit packed IPV4 address\" print_format=\"%08x\" />\n"
  "  <field type=\"variable32\" name=\"operation\" />\n"
  "  <field type=\"variable32\" name=\"filehandle\" print_hex=\"yes\" />\n"
  //  "  <field type=\"variable32\" name=\"type\" />\n"
  "  <field type=\"int64\" name=\"file-size\" />\n"
  "  <field type=\"int64\" name=\"modify-time\" />\n"
  "  <field type=\"int64\" name=\"offset\" />\n"
  "  <field type=\"int32\" name=\"bytes\" />\n"
  "</ExtentType>\n");

static const string str_read("read");
static const string str_write("write");

// This class outputs a 3-way join, by joining the 2-way-join output of
// AttrOpsCommonJoin with the extent type "NFS trace: read-write".

class CommonAttrRWJoin: public NFSDSModule {
public:
    CommonAttrRWJoin()
	: commonattr(NULL), rw(NULL),
	  es_commonattr(ExtentSeries::typeExact),
	  es_rw(ExtentSeries::typeExact),
	  in_packetat(es_commonattr,"reply-at"),
	  out_packetat(es_out,"packet-at"),
	  in_server(es_commonattr,"server"),
	  out_server(es_out,"server"),
	  in_client(es_commonattr,"client"),
	  out_client(es_out,"client"),
	  in_operation(es_commonattr,"operation"),
	  out_operation(es_out,"operation"),
	  in_recordid(es_commonattr,"record-id"),
	  in_filehandle(es_commonattr,"filehandle"),
	  out_filehandle(es_out,"filehandle"),
	  in_filesize(es_commonattr,"file-size"),
	  out_filesize(es_out,"file-size"),
	  in_modifytime(es_commonattr,"modify-time"),
	  out_modifytime(es_out,"modify-time"),
	  in_requestid(es_rw,"request-id"),
	  in_offset(es_rw,"offset"),
	  out_offset(es_out,"offset"),
	  in_bytes(es_rw,"bytes"),
	  out_bytes(es_out,"bytes"),
	  in_is_read(es_rw,"is-read"),
	  rw_done(false), common_done(false),
	  skip_count(0), output_bytes(0)
    { 
    }

    virtual ~CommonAttrRWJoin() { }

    void initOutType() {
	SINVARIANT(es_commonattr.getType() != NULL &&
		   es_out.getType() == NULL);

	xmlNodePtr ftype = es_commonattr.getType()->xmlNodeFieldDesc("request-at");
	string units = ExtentType::strGetXMLProp(ftype, "units");
	string epoch = ExtentType::strGetXMLProp(ftype, "epoch");
	
	string repl = str(format("units=\"%s\" epoch=\"%s\"") % units % epoch);
	string tmp = str(format(commonattrrw_xml_in) % repl);
	es_out.setType(ExtentTypeLibrary::sharedExtentType(tmp));
    }

    void setInputs(DataSeriesModule &common_attr_mod, 
		   DataSeriesModule &rw_mod) {
	commonattr = &common_attr_mod;
	rw = &rw_mod;
    }

    struct rwinfo { // for re-ordering to match by-reply-id order of attr-ops join
	ExtentType::int64 request_id, offset;
	ExtentType::int32 bytes;
    };
    struct rwinfo_geq { 
	bool operator()(const rwinfo *a, const rwinfo *b) const {
	    return a->request_id >= b->request_id;
	}
    };
    PriorityQueue<rwinfo *,rwinfo_geq> rw_reorder;

    void fillRWReorder() {
	if (es_rw.extent() == NULL || es_rw.pos.morerecords() == false) {
	    Extent *tmp = rw->getExtent();
	    if (tmp == NULL) {
		rw_done = true;
		delete es_rw.extent();
		es_rw.clearExtent();
		return;
	    }
	    delete es_rw.extent();
	    es_rw.setExtent(tmp);
	}
	rwinfo *tmp = new rwinfo;
	tmp->request_id = in_requestid.val();
	tmp->offset = in_offset.val();
	tmp->bytes = in_bytes.val();
	++es_rw.pos;
	rw_reorder.push(tmp);
    }

    struct commonattrinfo { // for re-ordering 
	ExtentType::int64 record_id, packet_at, filesize, modifytime;
	ExtentType::int32 server, client;
	string operation, filehandle; 
    };

    struct commonattrinfo_geq {
	bool operator()(const commonattrinfo *a, const commonattrinfo *b) const {
	    return a->record_id >= b->record_id;
	}
    };
    PriorityQueue<commonattrinfo *,commonattrinfo_geq> commonattr_reorder;

    void fillCommonAttrReorder() {
	if (es_commonattr.extent() == NULL || es_commonattr.pos.morerecords() == false) {
	    Extent *tmp = commonattr->getExtent();
	    if (tmp == NULL) {
		common_done = true;
		delete es_commonattr.extent();
		es_commonattr.clearExtent();
		return;
	    }
	    delete es_commonattr.extent();
	    es_commonattr.setExtent(tmp);
	}
	if (in_operation.equal(str_read) == false && // .equal(str_read) == false ||
	    in_operation.equal(str_write) == false) {
	    ++es_commonattr.pos;
	    return; // only "reads" and "writes" can be joined
	}
	commonattrinfo *tmp = new commonattrinfo;
	tmp->record_id = in_recordid.val();
	tmp->packet_at = in_packetat.val();
	tmp->filesize = in_filesize.val();
	tmp->modifytime = in_modifytime.val();
	tmp->server = in_server.val();
	tmp->client = in_client.val();
	tmp->operation = in_operation.stringval();
	tmp->filehandle = in_filehandle.stringval();
	++es_commonattr.pos;
	commonattr_reorder.push(tmp);
    }

    // > 1000 needed by set-2/025
    // > 2000 needed by set-2/045
    // > 3000 needed by set-2/046
    // > 4000 needed by set-2/047
    static const unsigned max_out_of_order = 10000;
    virtual Extent *getExtent() { 
	// have to do the full re-ordering join because we have
	// duplicates in the entries, so that doing the simple
	// remember entries from one side or another doesn't work, but
	// we seem to get entries that are pretty amazingly out of
	// order.
	if (common_done && rw_done)
	    return NULL;

	if (es_out.getType() == NULL) {
	    initOutType();
	}
	Extent *outextent = new Extent(*es_out.getType());
	es_out.setExtent(outextent);

	while(outextent->extentsize() < 8*1024*1024) {
	    while (rw_done == false && rw_reorder.size() < max_out_of_order) {
		fillRWReorder();
	    } 
	    while (common_done == false 
		   && commonattr_reorder.size() < max_out_of_order) {
		fillCommonAttrReorder();
	    }
	    INVARIANT(commonattr_reorder.empty() == false,
		      format("internal %d") % rw_reorder.size());
	    INVARIANT(rw_reorder.empty() == false, "out of rw before out of common?");
	    while (rw_reorder.top()->request_id < commonattr_reorder.top()->record_id) {
		// rw_reorder has a few duplicates in it which have no
		// response, so don't show up in the other join; drop
		// these, we will verify that we find a match for
		// everything else though, so we will have to be
		// handling the reordering correctly.

		// TODO: count the number of missed duplicates and
		// verify it is a small fraction of the total # rows
		if (false) 
		    printf("skip %lld %d %d\n",
			   rw_reorder.top()->request_id,
			   rw_reorder.size(), commonattr_reorder.size());
		++skip_count;
		delete rw_reorder.top();
		rw_reorder.pop();
	    }
	    INVARIANT(rw_reorder.top()->request_id == commonattr_reorder.top()->record_id,
		      format("whoa, mismatch in join; %lld(%d) vs %lld(%d); skip %d; too out of order?")
		      % rw_reorder.top()->request_id % rw_reorder.size()
		      % commonattr_reorder.top()->record_id % commonattr_reorder.size()
		      % skip_count);

	    es_out.newRecord();
	    out_packetat.set(commonattr_reorder.top()->packet_at);
	    out_server.set(commonattr_reorder.top()->server);
	    out_client.set(commonattr_reorder.top()->client);
	    out_operation.set(commonattr_reorder.top()->operation);
	    out_filehandle.set(commonattr_reorder.top()->filehandle);
	    out_filesize.set(commonattr_reorder.top()->filesize);
	    out_modifytime.set(commonattr_reorder.top()->modifytime);
	    out_offset.set(rw_reorder.top()->offset);
	    out_bytes.set(rw_reorder.top()->bytes);

	    ExtentType::int64 rw_request_id = rw_reorder.top()->request_id;
      	    delete commonattr_reorder.top();
	    commonattr_reorder.pop();
	    if (false) 
		printf("join %lld\n",rw_request_id);
	    if (commonattr_reorder.empty() || commonattr_reorder.top()->record_id > rw_request_id) {
		while (rw_reorder.empty() == false && 
		       rw_reorder.top()->request_id == rw_request_id) {
		    delete rw_reorder.top();
		    rw_reorder.pop();
		}
	    }
	    if (commonattr_reorder.empty()) {
		SINVARIANT(common_done == true && rw_done == true);
		// a small number of leftover is ok, as we may have
		// seen the request without the response
		INVARIANT(rw_reorder.size() < 10, 
			  format("too many leftover rw attrs (%d)?") % rw_reorder.size());
		while(rw_reorder.empty() == false) {
		    delete rw_reorder.top();
		    rw_reorder.pop();
		}
		break;
	    }
	}

	output_bytes += outextent->extentsize();
	return outextent;
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	printf("  skipped %d records in join -- no match found\n",skip_count);
	printf("  generated %.2f MB of extent output\n",(double)output_bytes/(1024.0*1024.0));
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
private:
    DataSeriesModule *commonattr, *rw;
    ExtentSeries es_commonattr, es_rw, es_out;
    Int64Field in_packetat, out_packetat;
    Int32Field in_server, out_server;
    Int32Field in_client, out_client;
    Variable32Field in_operation, out_operation;
    Int64Field in_recordid;
    Variable32Field in_filehandle, out_filehandle; 
    Int64Field in_filesize, out_filesize, in_modifytime, out_modifytime;
    Int64Field in_requestid;
    Int64Field in_offset, out_offset;
    Int32Field in_bytes, out_bytes;
    BoolField in_is_read;

    bool rw_done, common_done;
    int skip_count;
    ExtentType::int64 output_bytes;
};
#endif

namespace NFSDSAnalysisMod {
    NFSDSModule *newCommonAttrRWJoin() {
	return new CommonAttrRWJoin();
    }

    void setCommonAttrRWSources(DataSeriesModule *join, 
				SequenceModule &commonattr_seq,
				SequenceModule &attrops_seq) {
	lintel::safeDownCast<CommonAttrRWJoin>(join)
	    ->setInputs(commonattr_seq.tail(), attrops_seq.tail());
    }
}

