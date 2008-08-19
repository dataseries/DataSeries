/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <list>

#include <Lintel/LintelAssert.hpp>
#include <Lintel/PointerUtil.hpp>
#include <Lintel/PriorityQueue.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/SequenceModule.hpp>

#include "analysis/nfs/mod3.hpp"

using namespace std;

class FileSizeByType : public NFSDSModule {
public:
    FileSizeByType(DataSeriesModule &_source)
	: source(_source), s(ExtentSeries::typeExact),
	  type(s,"type"),filesize(s,"file-size"),filehandle(s,"filehandle")
    { 
    }
    virtual ~FileSizeByType() { }
    struct hteData {
	string type;
	hteData(const string &a) : type(a),file_size(NULL) { }
	Stats *file_size;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) const {
	    return HashTable_hashbytes(k.type.data(),k.type.size());
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.type == b.type;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	if (e->type.getName() != "NFS trace: attr-ops")
	    return e;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    string fh = filehandle.stringval();
	    bool skip = false;
	    for(vector<string>::iterator i=ignore_filehandles.begin();
		i != ignore_filehandles.end();++i) {
		if (*i == fh) {
		    skip = true;
		    break;
		}
	    }
	    if (skip)
		continue;

	    hteData *d = stats_table.lookup(hteData(type.stringval()));
	    if (d == NULL) {
		hteData newd(type.stringval());
		newd.file_size = new Stats;
		d = stats_table.add(newd);
	    }
	    d->file_size->add(filesize.val());
	}
	return e;
    }

    class sortByType {
    public:
	bool operator()(hteData *a, hteData *b) const {
	    return a->type < b->type;
	}
    };

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	vector<hteData *> vals;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	}
	sort(vals.begin(),vals.end(),sortByType());
	for(vector<hteData *>::iterator i = vals.begin();
	    i != vals.end();++i) {
	    hteData *j = *i;
	    printf("%10s %ld ents, %.2f MB total size, %.2f kB avg size, %.0f max bytes\n",
		   j->type.c_str(),
		   j->file_size->count(),
		   j->file_size->total() / (1024*1024.0),
		   j->file_size->mean() / (1024.0),
		   j->file_size->max());
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    DataSeriesModule &source;
    ExtentSeries s;
    Variable32Field type;
    Int64Field filesize;
    Variable32Field filehandle;
    vector<string> ignore_filehandles;
};

NFSDSModule *
NFSDSAnalysisMod::newFileSizeByType(DataSeriesModule &prev)
{
    return new FileSizeByType(prev);
}

class UniqueBytesInFilehandles : public NFSDSModule {
public:
    typedef vector<long long> byteCount;
    vector<byteCount> perOctetInfo;

    UniqueBytesInFilehandles(DataSeriesModule &_source)
	: source(_source),
	  s(ExtentSeries::typeExact),
	  filehandle(s,"filehandle"),
	  max_seen_fh_size(0)
    { 
	perOctetInfo.resize(64); // 64 is max nfs fh size
	for(unsigned i=0;i<perOctetInfo.size();++i) {
	    perOctetInfo[i].resize(256);
	}
    }
    virtual ~UniqueBytesInFilehandles() { }

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	SINVARIANT(e->type.getName() == "NFS trace: attr-ops");

	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (filehandle.size() > max_seen_fh_size) {
		max_seen_fh_size = filehandle.size();
	    }
	    const ExtentType::byte *v = filehandle.val();
	    for(int i=0;i<filehandle.size();++i) {
		++(perOctetInfo[i][v[i]]);
	    }
	}
	return e;
    }
    
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	for(int i=0;i<max_seen_fh_size;++i) {
	    byteCount &bc = perOctetInfo[i];
	    long long sum = 0;
	    int usedcount = 0;
	    for(int j=0;j<256;++j) {
		sum += bc[j];
		if (bc[j] != 0) ++usedcount;
	    }
	    double mean = (double)sum/256.0;
	    long long minused = (long long)(mean * 4);
	    printf("octet #%d; %d values used; ones used 4x more than mean(%.2f): ",
		   i,usedcount,mean);
	    for(int j=0;j<256;++j) {
		if (bc[j] > minused) {
		    printf("%d, ",j);
		}
	    }
	    printf("\n");
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Variable32Field filehandle;
    int max_seen_fh_size;
};

NFSDSModule *
NFSDSAnalysisMod::newUniqueBytesInFilehandles(DataSeriesModule &prev)
{
    return new UniqueBytesInFilehandles(prev);
}


class CommonBytesInFilehandles : public NFSDSModule {
public:
    static const unsigned max_used_count = 1024;
    vector<map<uint32_t,bool> > used_ints;
    vector<bool> commonbytes;

    static const int max_fh_size = 64;
    CommonBytesInFilehandles(DataSeriesModule &_source)
	: source(_source),
	  s(ExtentSeries::typeExact),
	  filehandle(s,"filehandle"),
	  dirfilehandle(s,"lookup-dir-filehandle",Field::flag_nullable),
	  max_seen_fh_size(0)
    { 
	commonbytes.resize(max_fh_size,true);
	used_ints.resize(max_fh_size/4);
    }
    virtual ~CommonBytesInFilehandles() { }

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	SINVARIANT(e->type.getName() == "NFS trace: attr-ops");

	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (dirfilehandle.isNull())
		continue;
	    AssertAlways(filehandle.size() == dirfilehandle.size(),("can't handle size mismatch"));
	    fh2mountData d(dirfilehandle.val(),dirfilehandle.size());
	    fh2mountData *v = fh2mount.lookup(d);

      	    if (v != NULL) {
		++v->common_bytes_seen_count;
		// lots of things won't be seen because they are subdirs; 
		// would like to find root things which are not seen, but
		// not easy to do that.
	    }
	    if (dirfilehandle.size() > max_seen_fh_size) {
		max_seen_fh_size = dirfilehandle.size();
	    }
	    const ExtentType::byte *v1 = filehandle.val();
	    const ExtentType::byte *v2 = dirfilehandle.val();
	    for(int i=0;i<filehandle.size();++i) {
		if (v1[i] != v2[i]) {
		    commonbytes[i] = false;
		}
	    }
	    for(int i=0;i<filehandle.size()/4;++i) {
		uint32_t v = ((uint32_t *)v1)[i];
		if (used_ints[i].size() < max_used_count) {
		    used_ints[i][v] = true;
		}
	    }
	}
	return e;
    }
    
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	printf("common bytes:");
	for(int i=0;i<max_seen_fh_size;++i) {
	    if (commonbytes[i]) {
		printf("%d ",i);
	    }
	}
	printf("\n");
	for(int i=0;i<max_seen_fh_size/4;++i) {
	    printf("quad %d: ",i);
	    if (used_ints[i].size() >= max_used_count) {
		printf("> %d used\n",used_ints[i].size());
	    } else {
		printf("%d used: ",used_ints[i].size());
		if (used_ints[i].size() < 50) {
		    for(map<uint32_t,bool>::iterator j = used_ints[i].begin();
			j != used_ints[i].end();++j) {
			printf("%08x, ",j->first);
		    }
		}
		printf("\n");
	    }
	}
	printf("%d mount entries\n",fh2mount.size());
	for(fh2mountT::iterator i = fh2mount.begin();
	    i != fh2mount.end();++i) {
	    if (i->common_bytes_seen_count > 0) {
		AssertAlways(i->fullfh.size() >= 32,("unhandled"));
		const ExtentType::int32 *v = (const ExtentType::int32 *)i->fullfh.data();
		printf("mount %13s:%s seen %4d times; quads(0,1,6,7): %08x %08x  %08x %08x\n",
		       ipv4tostring(i->server).c_str(),
		       maybehexstring(i->pathname).c_str(),
		       i->common_bytes_seen_count,v[0],v[1],v[6],v[7]);
	    }
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Variable32Field filehandle;
    Variable32Field dirfilehandle;
    int max_seen_fh_size;
};

NFSDSModule *
NFSDSAnalysisMod::newCommonBytesInFilehandles(DataSeriesModule &prev)
{
    return new CommonBytesInFilehandles(prev);
}

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
	  output_type(ExtentTypeLibrary::sharedExtentType(commonattrrw_xml)),
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
	es_out.setType(output_type);
    }

    virtual ~CommonAttrRWJoin() { }

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

	Extent *outextent = new Extent(output_type);
	es_out.setExtent(outextent);

	while(outextent->extentsize() < 8*1024*1024) {
	    while (rw_done == false && rw_reorder.size() < max_out_of_order) {
		fillRWReorder();
	    } 
	    while (common_done == false 
		   && commonattr_reorder.size() < max_out_of_order) {
		fillCommonAttrReorder();
	    }
	    AssertAlways(commonattr_reorder.empty() == false,
			 ("internal %d",rw_reorder.size()));
	    AssertAlways(rw_reorder.empty() == false,("out of rw before out of common?"));
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
	    AssertAlways(rw_reorder.top()->request_id == commonattr_reorder.top()->record_id,
			 ("whoa, mismatch in join; %lld(%d) vs %lld(%d); skip %d; too out of order?",
			  rw_reorder.top()->request_id,rw_reorder.size(),
			  commonattr_reorder.top()->record_id,commonattr_reorder.size(),
			  skip_count));

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
		AssertAlways(common_done == true && rw_done == true,
			     ("internal"));
		// a small number of leftover is ok, as we may have
		// seen the request without the response
		AssertAlways(rw_reorder.size() < 10, 
			     ("too many leftover rw attrs (%d)?",rw_reorder.size()));
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
    const ExtentType &output_type;
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


NFSDSModule *
NFSDSAnalysisMod::newCommonAttrRWJoin() {
    return new CommonAttrRWJoin();
}

void NFSDSAnalysisMod::setCommonAttrRWSources(DataSeriesModule *join, 
					      SequenceModule &commonattr_seq,
					      SequenceModule &attrops_seq) {
    lintel::safeDownCast<CommonAttrRWJoin>(join)
	->setInputs(commonattr_seq.tail(), attrops_seq.tail());
}

//
// FilesRead performs read analysis.  It outputs four tables meant to be
// plotted, each table having 3 columns. The first column is the time in
// seconds from the beginning of the trace; the second columns shows #
// of read operations; and the third column shows # of bytes.  For the
// first table, these #s refer to unique-filehandle reads.  For the
// second table, they refer to unique-filehandle reads for recently-changed
// files.  For the third table, they refer to reads of data that has not
// been recently changed, which would hit a cache of not-recently-changed
// files.  For the fourth table, they refer to reads of files that have
// been previously read, which would hit a cache of recently-read files.
//
// For the first and second tables, the # of bytes refers to the the size
// of the files, and for the third and fourth tables, it refers to the
// size of the reads.

double NFSDSAnalysisMod::read_sampling = 1000.0;

#define UNIQUEFILESREAD_PLOTSIZE 100
#define RECENTAGE 86400
#define BYTESUNIQUEFILESREAD_PLOTSIZE 100

class FilesRead : public NFSDSModule {
public:
    FilesRead(DataSeriesModule &_source)
	: source(_source),
	  serverip(s,"server"),
	  packet_at(s,"packet-at"),
	  modify_time(s,"modify-time"),
	  operation(s,"operation"),
	  filehandle(s,"filehandle"),
	  filesize(s,"file-size"),
	  in_bytes(s,"bytes"),
	  last1(-1), last2(-1), last3(-1), last4(-1),
	  cum1(0), cum2(0), cum3(0), cum4(0),
	  n1(0), n2(0), n3(0), n4(0),
	  starttime(-1)
    { }
    virtual ~FilesRead() { }

    struct hteData {
	string filehandle;
	unsigned server;
    };

    struct entry {
	long long time;
	long long bytes;
	int n;
    };
    
    class hteHash {
    public: unsigned int operator()(const hteData &k) const {
       return HashTable_hashbytes(k.filehandle.data(),k.filehandle.size());
    }};
    
    class hteEqual {
    public: bool operator()(const hteData &a, const hteData &b) const {
	return a.filehandle == b.filehandle && a.server == b.server;
    }};
    

    DataSeriesModule &source;
    ExtentSeries s;
    Int32Field serverip;
    Int64Field packet_at, modify_time;
    Variable32Field operation,filehandle;
    Int64Field filesize;
    Int32Field in_bytes;    
    list<entry> list1; // recent-cache misses
    list<entry> list2; // all-cache misses
    list<entry> list3; // old-cache hits
    list<entry> list4; // recent-cache hist

    long long last1, last2, last3, last4; // time of last entry recorded
    long long cum1, cum2, cum3, cum4;     // accumulated bytes
    long long n1, n2, n3, n4;             // number of entries
    entry e1, e2, e3, e4;
    HashTable<hteData, hteHash, hteEqual> seen;
    long long starttime;


    virtual Extent *getExtent() {
	long long reltime;
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	SINVARIANT(e->type.getName() == "common-attr-rw-join");

	hteData k;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (starttime == -1)
		starttime = packet_at.val();
	    // cout << "Found " << operation.stringval() << "\n";
	    if (operation.equal(str_read) == false)
		continue;
	    ExtentType::int64 file_age = packet_at.val() - modify_time.val();
   
	    k.filehandle = filehandle.stringval();
	    k.server = serverip.val(); // this is return packet, so server ip
                                       // is in sourceip
	    // cout << "ip: " << ipstring(sourceip.val()).c_str() << "\n";
	    reltime = packet_at.val() - starttime;
	    hteData *v = seen.lookup(k);
	    if (v == NULL) {
		seen.add(k);

		// recent-cache misses
		cum1 += filesize.val();
		n1++;
		e1.time = reltime;
		e1.bytes = cum1;
		e1.n = n1;
		
		if ((double)(reltime - last1)/1000000 > NFSDSAnalysisMod::read_sampling || last1 == -1)
		{
		    last1 = reltime;
		    list1.push_back(e1);
		}

		fflush(stdout);
		if (file_age / 1.0e9 < RECENTAGE) // newly changed file
		{
		    // all-cache misses
		    fflush(stdout);
		    cum2 += filesize.val();
		    n2++;
		    e2.time = reltime;
		    e2.bytes = cum2;
		    e2.n = n2;
		    if ((double)(reltime - last2)/1000000 > NFSDSAnalysisMod::read_sampling || last2 == -1)
		    {
			last2 = reltime;
			list2.push_back(e2);
		    }
		}
	    }
	    else // hits cache of recent files
	    {
		if (file_age / 1.0e9 < RECENTAGE) // newly changed file
		{
		    cum4 += in_bytes.val();
		    n4++;
		    e4.time = reltime;
		    e4.bytes = cum4;
		    e4.n = n4;
		    if ((double)(reltime - last4)/1000000 > NFSDSAnalysisMod::read_sampling || last4 == -1)
		    {
			last4 = reltime;
			list4.push_back(e4);
		    }
		}
	    }
	    if (file_age / 1.0e9 >= RECENTAGE) // hits cache of old files
	    {
		cum3 += in_bytes.val();
		n3++;
		e3.time = reltime;
		e3.bytes = cum3;
		e3.n = n3;
		if ((double)(reltime - last3)/1000000 > NFSDSAnalysisMod::read_sampling || last3 == -1)
		{
		    last3 = reltime;
		    list3.push_back(e3);
		}
	    }
	}
	return e;
    }
    
    virtual void printResult() {
	list<entry>::iterator p;

	// to be executed after trace is processed: add last entry to lists
	// this is probably not the best place to put this
	if (e1.time != last1){ last1 = e1.time; list1.push_back(e1); }
	if (e2.time != last2){ last2 = e2.time; list2.push_back(e2); }
	if (e3.time != last3){ last3 = e3.time; list3.push_back(e3); }
	if (e4.time != last4){ last4 = e4.time; list4.push_back(e4); }
	
	printf("Begin-%s\n",__PRETTY_FUNCTION__);

	// first print unique files read with byte count
	printf("#RECENT FILE CACHE MISSES\n");
	for (p = list1.begin(); p != list1.end(); p++)
	{
	    printf("%f %d %Ld\n", p->time/1000000000.0, p->n, p->bytes);
	}

	// now print recent unique files read with byte count
	printf("\n\n#BOTH CACHE MISSES\n");
	for (p = list2.begin(); p != list2.end(); p++)
	{
	    printf("%f %d %Ld\n", p->time/1000000000.0, p->n, p->bytes);
	}

	// now print recent-cache hits
	printf("\n\n#OLD-CACHE HITS\n");
	for (p = list3.begin(); p != list3.end(); p++)
	{
	    printf("%f %d %Ld\n", p->time/1000000000.0, p->n, p->bytes);
	}
	
	// now print old-cache hits
	printf("\n\n#RECENT-CACHE HITS\n");
	for (p = list4.begin(); p != list4.end(); p++)
	{
	    printf("%f %d %Ld\n", p->time/1000000000.0, p->n, p->bytes);
	}
	
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
};

NFSDSModule *
NFSDSAnalysisMod::newFilesRead(DataSeriesModule &prev)
{
    return new FilesRead(prev);
}
