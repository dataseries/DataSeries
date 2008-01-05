/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <string>
#include <list>

#include <boost/format.hpp>

#include <Lintel/ConstantString.H>
#include <Lintel/HashMap.H>
#include <Lintel/StatsQuantile.H>
#include <Lintel/StringUtil.H>

#include "analysis/nfs/mod1.hpp"

using namespace std;
using boost::format;

class NFSOpPayload : public NFSDSModule {
public:
    NFSOpPayload(DataSeriesModule &_source) 
	: source(_source), s(ExtentSeries::typeExact),
	  payload_length(s,"payload-length"),
	  is_udp(s,"is-udp"),
	  is_request(s,"is-request"),
	  op_id(s,"op-id",Field::flag_nullable),
	  operation(s,"operation")
    {}
    virtual ~NFSOpPayload() { }
    struct hteData {
	bool is_udp, is_request;
	int op_id;
	hteData(bool a, bool b, int c) : is_udp(a), is_request(b), op_id(c), payload_length(NULL) {}
	Stats *payload_length;
	string operation;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) {
	    return (k.is_udp ? 256 : 0) + (k.is_request ? 512 : 0) + k.op_id;
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) {
	    return a.is_udp == b.is_udp && a.is_request == b.is_request &&
		a.op_id == b.op_id;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	if (e->type.getName() != "NFS trace: common")
	    return e;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (op_id.isNull())
		continue;
	    hteData *d = stats_table.lookup(hteData(is_udp.val(),is_request.val(),op_id.val()));
	    if (d == NULL) {
		hteData newd(is_udp.val(),is_request.val(),op_id.val());
		newd.payload_length = new Stats;
		newd.operation = operation.stringval();
		stats_table.add(newd);
		d = stats_table.lookup(newd);
	    }
	    d->payload_length->add(payload_length.val());
	}
	return e;
    }

    class sortByTotal {
    public:
	bool operator()(hteData *a, hteData *b) {
	    return a->payload_length->total() > b->payload_length->total();
	}
    };

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	vector<hteData *> vals;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	}
	sort(vals.begin(),vals.end(),sortByTotal());
	for(vector<hteData *>::iterator i = vals.begin();
	    i != vals.end();++i) {
	    hteData *j = *i;
	    printf("%s %12s %7s: %7.3f MB, %6.2f bytes/op, %4.0f max bytes/op, %6ld ops\n",
		   j->is_udp ? "UDP" : "TCP", j->operation.c_str(),
		   j->is_request ? "Request" : "Reply",
		   j->payload_length->total()/(1024*1024.0),
		   j->payload_length->mean(),
		   j->payload_length->max(),
		   j->payload_length->count());
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    DataSeriesModule &source;
    ExtentSeries s;
    Int32Field payload_length;
    BoolField is_udp;
    BoolField is_request;
    ByteField op_id;
    Variable32Field operation;
};

NFSDSModule *
NFSDSAnalysisMod::newNFSOpPayload(DataSeriesModule &prev)
{
    return new NFSOpPayload(prev);
}

class ClientServerPairInfo : public NFSDSModule {
public:
    ClientServerPairInfo(DataSeriesModule &_source) 
	: source(_source), s(ExtentSeries::typeExact),
	  payload_length(s,"payload-length"),
	  is_udp(s,"is-udp"),is_request(s,"is-request"),
	  op_id(s,"op-id",Field::flag_nullable),
	  sourceip(s,"source"),destip(s,"dest")
    {}
    virtual ~ClientServerPairInfo() { }
    struct hteData {
	bool is_udp;
	ExtentType::int32 client, server;
	hteData(bool a, ExtentType::int32 b, ExtentType::int32 c) 
	    : is_udp(a), client(b), server(c),payload_length(NULL) {}
	Stats *payload_length;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) {
	    return BobJenkinsHashMix3(k.client,k.server,k.is_udp ? 0x55555555 : 0x0);
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) {
	    return a.is_udp == b.is_udp && a.client == b.client &&
		a.server == b.server;
	}
    };

    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	if (e->type.getName() != "NFS trace: common")
	    return e;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (op_id.isNull())
		continue;
	    ExtentType::int32 client,server;
	    if (is_request.val()) {
		client = sourceip.val();
		server = destip.val();
	    } else {
		client = destip.val();
		server = sourceip.val();
	    }
	    hteData *d = stats_table.lookup(hteData(is_udp.val(),client,server));
	    if (d == NULL) {
		hteData newd(is_udp.val(),client,server);
		newd.payload_length = new Stats;
		stats_table.add(newd);
		d = stats_table.lookup(newd);
	    }
	    d->payload_length->add(payload_length.val());
	}
	return e;
    }

    class sortByTotal {
    public:
	bool operator()(hteData *a, hteData *b) {
	    return a->payload_length->total() > b->payload_length->total();
	}
    };

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	printf("protocol client server: total data ...\n");
	vector<hteData *> vals;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	}
	sort(vals.begin(),vals.end(),sortByTotal());
	int count = 0;
	for(vector<hteData *>::iterator i = vals.begin();
	    i != vals.end();++i) {
	    hteData *j = *i;
	    printf("%s %12s %12s: %.3f MB, %.2f bytes/op, %.0f max bytes/op, %ld ops\n",
		   j->is_udp ? "UDP" : "TCP", 
		   ipv4tostring(j->client).c_str(),
		   ipv4tostring(j->server).c_str(),
		   j->payload_length->total()/(1024.0*1024.0),
		   j->payload_length->mean(),
		   j->payload_length->max(),
		   j->payload_length->count());
	    if (++count > 100) break;
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    DataSeriesModule &source;
    ExtentSeries s;
    Int32Field payload_length;
    BoolField is_udp,is_request;
    ByteField op_id;
    Int32Field sourceip, destip;
};

NFSDSModule *
NFSDSAnalysisMod::newClientServerPairInfo(DataSeriesModule &prev)
{
    return new ClientServerPairInfo(prev);
}

class HostInfo : public RowAnalysisModule {
public:
    HostInfo(DataSeriesModule &_source, const std::string &arg) 
	: RowAnalysisModule(source),
	  packet_at(series, "packet-at"),
	  payload_length(series, "payload-length"),
	  op_id(series, "op-id", Field::flag_nullable),
	  nfs_version(series, "nfs-version"),
	  source_ip(series,"source"), 
	  dest_ip(series, "dest")
    {
	group_seconds = stringToUInt32(arg);
    }
    virtual ~HostInfo() { }

    struct PerDirectionData {
	vector<Stats *> data;
	void add(unsigned char raw_op_id, unsigned char nfs_version, 
		 uint32_t bytes) {
	    unsigned op_id = opIdToUnifiedId(raw_op_id, nfs_version);
	    if (data.size() < op_id) {
		data.resize(op_id+1);
	    }
	    if (data[op_id] == NULL) {
		data[op_id] = new Stats;
	    }
	    data[op_id]->add(bytes);
	}
	void print(uint32_t host_id, uint32_t seconds, 
		   const std::string &direction) {
	    for(unsigned i = 0; i < data.size(); ++i) {
		if (data[i] == NULL) {
		    continue;
		}
		cout << format("%08x %10d %s %12s %lld %8.2f\n")
		    % host_id % seconds % direction
		    % unifiedIdToName(i) % data[i]->countll()
		    % data[i]->mean();
	    }
	}
    };

    struct PerTimeData {
	PerDirectionData send, recv;
    };

    struct PerHostData {
	PerHostData() 
	    : first_time_seconds(0) {}
	vector<PerTimeData *> time_entries;
	uint32_t first_time_seconds; 
	PerTimeData &getPerTimeData(uint32_t seconds, uint32_t group_seconds) {
	    seconds -= seconds % group_seconds;
	    if (time_entries.empty()) {
		first_time_seconds = seconds;
	    }
	    SINVARIANT(seconds >= first_time_seconds);
	    uint32_t entry = (seconds - first_time_seconds) / group_seconds;
	    INVARIANT(entry < 100000, format("time range of %d..%d seconds grouped every %d seconds leads to more than 100,000 entries; this is probably not what you want") % first_time_seconds % seconds % group_seconds);
	    if (entry >= time_entries.size()) {
		time_entries.resize(entry+1);
	    }
	    DEBUG_SINVARIANT(entry < time_entries.size());
	    if (time_entries[entry] == NULL) {
		time_entries[entry] = new PerTimeData();
	    }
	    return *time_entries[entry];
	}

	void print(uint32_t host_id, uint32_t group_seconds) {
	    uint32_t start_seconds = first_time_seconds;
	    for(vector<PerTimeData *>::iterator i = time_entries.begin();
		i != time_entries.end(); ++i, start_seconds += group_seconds) {
		(**i).send.print(host_id, start_seconds, "send");
		(**i).recv.print(host_id, start_seconds, "recv");
	    }

	}
    };

    HashMap<uint32_t, PerHostData> host_to_data;

    virtual void processRow() {
	uint32_t seconds = packet_at.val() / 1000000000;
	host_to_data[source_ip.val()]
	    .getPerTimeData(seconds, group_seconds)
	    .send.add(op_id.val(), nfs_version.val(), payload_length.val());
	host_to_data[dest_ip.val()]
	    .getPerTimeData(seconds, group_seconds)
	    .recv.add(op_id.val(), nfs_version.val(), payload_length.val());

    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	for(HashMap<uint32_t, PerHostData>::iterator i = host_to_data.begin();
	    i != host_to_data.end(); ++i) {
	    i->second.print(i->first, group_seconds);
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    Int64Field packet_at;
    Int32Field payload_length;
    ByteField op_id, nfs_version;
    Int32Field source_ip, dest_ip;
    uint32_t group_seconds;
};

RowAnalysisModule *
NFSDSAnalysisMod::newHostInfo(DataSeriesModule &prev, char *arg)
{
    return new HostInfo(prev, arg);
}

class PayloadInfo : public NFSDSModule {
public:
    PayloadInfo(DataSeriesModule &_source)
	: source(_source), s(ExtentSeries::typeExact),
	  packet_at(s,"packet-at"),
	  payload_length(s,"payload-length"),
	  is_udp(s,"is-udp"),
	  op_id(s,"op-id",Field::flag_nullable),
	  min_time(ExtentType::int64_max), max_time(ExtentType::int64_min)
    { }
    virtual ~PayloadInfo() {}

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) return NULL;
	if (e->type.getName() != "NFS trace: common") return e;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (op_id.isNull()) continue;
	    stat_payload_length.add(payload_length.val());
	    if (packet_at.val() < min_time) {
		min_time = packet_at.val();
	    }
	    if (packet_at.val() > max_time) {
		max_time = packet_at.val();
	    }
	    if (is_udp.val()) {
		stat_payload_length_udp.add(payload_length.val());
	    } else {
		stat_payload_length_tcp.add(payload_length.val());
	    }
	}
	return e;
    }
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	const double oneb = (1000.0*1000.0*1000.0);
	double time_range = (double)(max_time - min_time)/oneb;
	printf("time range: %.2fs .. %.2fs = %.2fs, %.2fhours\n",
	       (double)min_time/oneb, (double)max_time/oneb, time_range, 
	       time_range / (3600.0));
	printf("payload_length: avg = %.2f bytes, stddev = %.4f bytes, sum = %.4f GB, %.4fMB/s, count=%.2f million %.2f k/s\n",
	       stat_payload_length.mean(),
	       stat_payload_length.stddev(),
	       stat_payload_length.total()/(1024.0*1024.0*1024.0),
	       stat_payload_length.total()/(time_range * 1024.0*1024.0),
	       (double)stat_payload_length.count()/(1000000.0),
	       (double)stat_payload_length.count()/(time_range * 1000.0));
	
	printf("payload_length(udp): avg = %.2f bytes, stddev = %.4f bytes, sum = %.4f GB, %.4fMB/s, count=%.2f million %.2f k/s\n",
	       stat_payload_length_udp.mean(),
	       stat_payload_length_udp.stddev(),
	       stat_payload_length_udp.total()/(1024.0*1024.0*1024.0),
	       stat_payload_length_udp.total()/(time_range * 1024.0*1024.0),
	       (double)stat_payload_length_udp.count()/(1000000.0),
	       (double)stat_payload_length_udp.count()/(time_range * 1000.0));
	
	printf("payload_length(tcp): avg = %.2f bytes, stddev = %.4f bytes, sum = %.4f GB, %.4fMB/s, count=%.2f million %.2f k/s\n",
	       stat_payload_length_tcp.mean(),
	       stat_payload_length_tcp.stddev(),
	       stat_payload_length_tcp.total()/(1024.0*1024.0*1024.0),
	       stat_payload_length_tcp.total()/(time_range * 1024.0*1024.0),
	       (double)stat_payload_length_tcp.count()/(1000000.0),
	       (double)stat_payload_length_tcp.count()/(time_range * 1000.0));
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Int64Field packet_at;
    Int32Field payload_length;
    BoolField is_udp;
    ByteField op_id;
    Stats stat_payload_length, stat_payload_length_udp, stat_payload_length_tcp;
    Extent::int64 min_time, max_time;
};

NFSDSModule *
NFSDSAnalysisMod::newPayloadInfo(DataSeriesModule &prev)
{
    return new PayloadInfo(prev);
}

// TODO: add code to every so often throw away any old requests to
// limit excessive memory fillup; one of the other analysis does this
// by keeping two hash tables and rotating them every x "seconds",
// which is faster than scanning through one table to find old entries
// and removing them.  Partway through implementing this, when it is,
// make sure to do the missing reply handling bits.  This is important
// to do because the conversion code doesn't handle replies where the
// request was in a different file

class ServerLatency : public NFSDSModule {
public:
    ServerLatency(DataSeriesModule &_source) 
	: source(_source), 
	  s(ExtentSeries::typeExact),
	  reqtime(s,"packet-at"),
	  sourceip(s,"source"), 
	  destip(s,"dest"),	  
	  is_request(s,"is-request"),
	  transaction_id(s, "transaction-id"),
	  op_id(s,"op-id",Field::flag_nullable),
	  operation(s,"operation"),
	  pending1(NULL), pending2(NULL),
	  duplicate_request_delay(0.001),
	  missing_request_count(0),
	  duplicate_reply_count(0),
	  min_packet_time(1ULL<<63), // TODO: replace with appropriate max thing
	  max_packet_time(0)
    {
	pending1 = new pendingT;
	pending2 = new pendingT;
    }

    // TODO: maek this options and configurable.
    static const bool enable_first_latency_stat = true; // Estimate the latency seen by client
    static const bool enable_last_latency_stat = false; // Estimate the latency for the reply by the server

    static const bool enable_server_rollup = true;
    static const bool enable_operation_rollup = true;
    static const bool enable_overall_rollup = true;
    
    virtual ~ServerLatency() { }

    DataSeriesModule &source;
    ExtentSeries s;
    Int64Field reqtime;
    Int32Field sourceip, destip;
    BoolField is_request;
    Int32Field transaction_id;
    ByteField op_id;
    Variable32Field operation;

    // data structure for keeping statistics per request type and server
    struct StatsData {
        uint32_t serverip;
	ConstantString operation;
	StatsData() : serverip(0) { initCommon(); }
	StatsData(uint32_t a, const string &b) 
	    : serverip(a), operation(b)
	{ initCommon(); }
	void initCommon() {
	    first_latency_ms = NULL; 
	    last_latency_ms = NULL;
	    duplicates = NULL; 
	    missing_reply_count = 0; 
	    missing_request_count = 0; 
	    duplicate_reply_count = 0; 
	    missing_reply_firstlat = 0;
	    missing_reply_lastlat = 0;
	}
        StatsQuantile *first_latency_ms, *last_latency_ms;
	Stats *duplicates;
	uint64_t missing_reply_count, missing_request_count, 
	    duplicate_reply_count;
	double missing_reply_firstlat, missing_reply_lastlat;
	void initStats(bool enable_first, bool enable_last) {
	    INVARIANT(first_latency_ms == NULL && last_latency_ms == NULL && duplicates == NULL, "bad");
	    double error = 0.005;
	    uint64_t nvals = (uint64_t)10*1000*1000*1000;
	    if (enable_first) {
		first_latency_ms = new StatsQuantile(error, nvals);
	    }
	    if (enable_last) {
		last_latency_ms = new StatsQuantile(error, nvals);
	    }
	    duplicates = new Stats;
	};
	void add(StatsData &d, bool enable_first, bool enable_last) {
	    if (first_latency_ms == NULL) {
		initStats(enable_first, enable_last);
	    }
	    if (first_latency_ms) {
		first_latency_ms->add(*d.first_latency_ms);
	    }
	    if (last_latency_ms) {
		last_latency_ms->add(*d.last_latency_ms);
	    }
	    duplicates->add(*d.duplicates);
	}
	void add(double delay_first_ms, double delay_last_ms) {
	    if (first_latency_ms) {
		first_latency_ms->add(delay_first_ms);
	    }
	    if (last_latency_ms) {
		last_latency_ms->add(delay_last_ms);
	    }
	}	    
	int64_t nops() {
	    if (first_latency_ms) {
		return first_latency_ms->countll();
	    } else if (last_latency_ms) {
		return last_latency_ms->countll();
	    } else {
		return -1; // didn't calculate ;)
	    }
	}
    };

    class StatsHash {
    public: uint32_t operator()(const StatsData &k) {
	    return k.serverip ^ k.operation.hash();
    }};

    class StatsEqual {
    public: bool operator()(const StatsData &a, const StatsData &b) {
	return a.serverip == b.serverip && a.operation == b.operation;
    }};

    // data structure to keep transactions that do not yet have a
    // matching response
    struct TidData {
	uint32_t tid, server, client, duplicate_count;
	int64_t first_reqtime, last_reqtime;
	bool seen_reply;
	TidData(uint32_t tid_in, uint32_t server_in, uint32_t client_in)
	    : tid(tid_in), server(server_in), client(client_in), 
	      duplicate_count(0), first_reqtime(0), last_reqtime(0), 
	      seen_reply(false) {}
    };

    class TidHash {
    public: uint32_t operator()(const TidData &t) {
	return t.tid ^ t.client;
    }};

    class TidEqual {
    public: bool operator()(const TidData &t1, const TidData &t2) {
	return t1.tid == t2.tid && t1.client == t2.client;
    }};

    typedef HashTable<StatsData, StatsHash, StatsEqual> statsT;
    statsT stats_table;

    typedef HashTable<TidData, TidHash, TidEqual> pendingT;
    pendingT *pending1,*pending2;

    void updateDuplicateRequest(TidData *t) {
	// this check is here in case we are somehow getting duplicate
	// packets delivered by the monitoring process, and we want to
	// catch this and not think that we have realy lots of
	// duplicate requests.  Tried 50ms, but found a case about 5ms
	// apart, so dropped to 2ms
	const uint32_t min_retry_ns = 2*1000*1000;
	// inter arrival time
	int64_t request_iat = reqtime.val() - t->last_reqtime; 
	if (request_iat < min_retry_ns) {
	    cerr << format("warning: duplicate requests unexpectedly close together %lld - %lld = %lld >= %d\n")
		% reqtime.val() % request_iat % min_retry_ns;
	}
	++t->duplicate_count;
	duplicate_request_delay.add(request_iat/(1000.0*1000.0));
	t->last_reqtime = reqtime.val();
    }

    void handleRequest() {
	// address of server = destip
	TidData dummy(transaction_id.val(), destip.val(), 
		      sourceip.val()); 
	TidData *t = pending1->lookup(dummy);
	if (t == NULL) {
	    t = pending2->lookup(dummy);
	    if (t == NULL) {
		// add request to list of pending requests (requests without a response yet)
		dummy.first_reqtime = dummy.last_reqtime = reqtime.val();
		t = pending1->add(dummy);
	    } else {
		// wow, long enough delay that we've rotated the entry; add it to pending1, 
		// delete from pending2
		pending1->add(*t);
		pending2->remove(*t);
		updateDuplicateRequest(t);
	    }
	} else {
	    updateDuplicateRequest(t);
	}
    }
    
    void handleResponse() {
	// row is a response, so address of server = sourceip
	TidData dummy(transaction_id.val(), sourceip.val(), destip.val());
	TidData *t = pending1->lookup(dummy);
	bool in_pending2 = false;
	if (t == NULL) {
	    t = pending2->lookup(dummy);
	    if (t != NULL) {
		in_pending2 = true;
	    }
	}

	if (t == NULL) {
	    ++missing_request_count;
	} else {
	    // we now have both request and response, so we can add latency to statistics
	    double delay_first_ms = (double)(reqtime.val() - t->first_reqtime) / 1000000.0;
	    double delay_last_ms = (double)(reqtime.val() - t->last_reqtime) / 1000000.0;
	    StatsData hdummy(sourceip.val(), operation.stringval());
	    StatsData *d = stats_table.lookup(hdummy);

	    // add to statistics per request type and server
	    if (d == NULL) { // create new entry
		hdummy.initStats(enable_first_latency_stat, enable_last_latency_stat);
		d = stats_table.add(hdummy);
	    }
	    d->add(delay_first_ms, delay_last_ms);
	    // remove request from pending hashtable only if
	    // we haven't seen a duplicate request.  If we
	    // have seen a duplicate request, then there might
	    // be a duplicate reply coming
	    if (t->duplicate_count > 0) {
		d->duplicates->add(t->duplicate_count);
		if (t->seen_reply) {
		    ++duplicate_reply_count;
		} else {
		    t->seen_reply = true;
		}
	    } else {
		if (in_pending2) {
		    pending2->remove(*t);
		} else {
		    pending1->remove(*t);
		}
	    }
	}
    }

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	INVARIANT(e->type.getName() == "NFS trace: common", "??");
	s.setExtent(e);

	// TODO: check to see if enough time has passed that we should
	// switch pending1 and pending2, e.g. wait for 5 minutes or
	// something like that, or wait until 5 minutes, the latter is
	// probably safer, i.e. more likely to catch really late
	// replies, it might be worth counting them in this case.  if
	// we do this, some of the code in printResult that tracks
	// missing replies needs to get moved here so it runs on rotation.

	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (op_id.isNull())
		continue;
	    if (reqtime.val() < min_packet_time) {
		min_packet_time = reqtime.val();
	    } 
	    if (reqtime.val() > max_packet_time) {
		max_packet_time = reqtime.val();
	    }
	    if (is_request.val()) {
		handleRequest();
	    } else { 
		handleResponse();
	    }
	}
	return e;
    }

    class sortByServerOp {
    public: bool operator()(StatsData *a, StatsData *b) {
	if (a->serverip != b->serverip)
	    return a->serverip < b->serverip;
	return a->operation < b->operation;
    }};

    class sortByNOpsReverse {
    public: bool operator()(StatsData *a, StatsData *b) {
	uint64_t nops_a = a->nops();
	uint64_t nops_b = b->nops();
	return nops_a > nops_b;
    }};

    void printOneQuant(StatsQuantile *v) {
	if (v) {
	    cout << format("; %7.3f %6.2f %6.2f ") 
		% v->mean() % v->getQuantile(0.5)
		% v->getQuantile(0.9);
	}
    }

    void printVector(vector<StatsData *> &vals, 
		     HashMap<uint32_t, uint32_t> serverip_to_shortid,
		     const string &header) {
	cout << format("\n%s:\n") % header;
	cout << "server operation   #ops  #dups mean ";
	if (enable_first_latency_stat) {
	    cout << "; firstlat mean 50% 90% ";
	}
	if (enable_last_latency_stat) {
	    cout << "; lastlat mean 50% 90%";
	}
	cout << "\n";

	sort(vals.begin(),vals.end(),sortByServerOp());
	for(vector<StatsData *>::iterator i = vals.begin();
	    i != vals.end();++i) {
	    StatsData *j = *i;
	    int64_t noperations = j->nops();
	    if (j->serverip == 0) {
		cout << " *  ";
	    } else {
		cout << format("%03d ") % serverip_to_shortid[j->serverip];
	    }
	    cout << format("%9s %9lld %6lld %4.2f ")
		% j->operation % noperations
		% j->duplicates->countll()
		% j->duplicates->mean();
	    printOneQuant(j->first_latency_ms);
	    printOneQuant(j->last_latency_ms);
	    cout << "\n";
	}
    }

    virtual void printResult() {
	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;

	uint64_t missing_reply_count = 0;
	double missing_reply_firstlat = 0;
	double missing_reply_lastlat = 0;

	// 1s -- measured peak retransmit in one dataset was
	// 420ms with 95% <= 80ms

	int64_t max_noretransmit_ns = 1000*1000*1000; 
	for(pendingT::iterator i = pending1->begin(); 
	    i != pending1->end(); ++i) {
	    // the check against noretransmit handles the fact that we
	    // could just accidentally miss the reply and/or we could
	    // miss the reply due to the processing issue of not
	    // handling replies that cross request boundaries.
	    if (!i->seen_reply && 
		(max_packet_time - i->last_reqtime) < max_noretransmit_ns) {
		++missing_reply_count;
		missing_reply_firstlat += 
		    (max_packet_time - i->first_reqtime)/(1000.0*1000.0);
		missing_reply_lastlat += 
		    (max_packet_time - i->last_reqtime)/(1000.0*1000.0);
	    }
	}

	HashMap<uint32_t, uint32_t> serverip_to_shortid;

	// TODO: consider replacing this with the unsafeGetRawDataVector
	// trick of using the underlying hash table structure to perform
	// the sort.
	vector<StatsData *> all_vals;
	vector<uint32_t> server_ips;
	for(statsT::iterator i = stats_table.begin(); 
	    i != stats_table.end(); ++i) {
	    if (!serverip_to_shortid.exists(i->serverip)) {
		server_ips.push_back(i->serverip);
		serverip_to_shortid[i->serverip] = serverip_to_shortid.size() + 1;
	    }
	    all_vals.push_back(&(*i));
	}
	
	// Speeds up quantile merging since we special case the add
	// into empty operation, so this makes the longest add go fast.
	sort(all_vals.begin(), all_vals.end(), sortByNOpsReverse());
	
	HashMap<uint32_t, StatsData> server_rollup;
	HashMap<ConstantString, StatsData> operation_rollup;
	StatsData overall(0, "*");
	for(vector<StatsData *>::iterator i = all_vals.begin();
	    i != all_vals.end(); ++i) {
	    StatsData &d(**i);
	    
	    if (enable_server_rollup) {
		server_rollup[d.serverip].add(d, enable_first_latency_stat, 
					      enable_last_latency_stat);
	    }
	    if (enable_operation_rollup) {
		operation_rollup[d.operation].add(d, enable_first_latency_stat,
						  enable_last_latency_stat);
	    }
	    if (enable_overall_rollup) {
		overall.add(d, enable_first_latency_stat, 
			    enable_last_latency_stat);
	    }
	}

	vector<StatsData *> server_vals;
	for(HashMap<uint32_t, StatsData>::iterator i = server_rollup.begin();
	    i != server_rollup.end(); ++i) {
	    StatsData *tmp = &i->second;
	    tmp->serverip = i->first;
	    tmp->operation = "*";
	    server_vals.push_back(tmp);
	}

	vector<StatsData *> operation_vals;
	for(HashMap<ConstantString, StatsData>::iterator i = operation_rollup.begin();
	    i != operation_rollup.end(); ++i) {
	    StatsData *tmp = &i->second;
	    tmp->serverip = 0;
	    tmp->operation = i->first;
	    operation_vals.push_back(tmp);
	}

	vector<StatsData *> overall_vals;
	overall_vals.push_back(&overall);

	sort(server_ips.begin(), server_ips.end());
	cout << " id: server ip\n";
	for(vector<uint32_t>::iterator i = server_ips.begin();
	    i != server_ips.end(); ++i) {
	    serverip_to_shortid[*i] = (i - server_ips.begin()) + 1;
	    cout << boost::format("%03d: %s\n")
		% serverip_to_shortid[*i]
		% ipv4tostring(*i);
	}
	double missing_reply_mean_est_firstlat = 0;
	double missing_reply_mean_est_lastlat = 0;
	if (missing_reply_count > 0) {
	    missing_reply_mean_est_firstlat = (double)missing_reply_firstlat/(double)missing_reply_count;
	    missing_reply_mean_est_lastlat = (double)missing_reply_lastlat/(double)missing_reply_count;
	}
	cout << format("%d missing requests, %d missing replies; %.2fms mean est firstlat, %.2fms mean est lastlat\n")
	    % missing_request_count % missing_reply_count 
	    % missing_reply_mean_est_firstlat % missing_reply_mean_est_lastlat;
	       
        cout << format("duplicates: %d replies, %d requests; delays min=%.4fms\n")
	    % duplicate_reply_count % duplicate_request_delay.count() 
	    % duplicate_request_delay.min();
	duplicate_request_delay.printFile(stdout,20);

	printVector(all_vals, serverip_to_shortid, 
		    "Grouped by (Server,Operation) pair");
	printVector(server_vals, serverip_to_shortid,
		    "Grouped by Server");
	printVector(operation_vals, serverip_to_shortid,
		    "Grouped by Operation");
	printVector(overall_vals, serverip_to_shortid,
		    "Complete rollup");

	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    StatsQuantile duplicate_request_delay;
    uint64_t missing_request_count;
    uint64_t duplicate_reply_count;
    int64_t min_packet_time, max_packet_time;
};

NFSDSModule *
NFSDSAnalysisMod::newServerLatency(DataSeriesModule &prev)
{
    return new ServerLatency(prev);
}


class UnbalancedOps : public NFSDSModule {
public:
    UnbalancedOps(DataSeriesModule &_source) 
	: source(_source), s(ExtentSeries::typeExact),
	  reqtime(s,"packet-at"),
	  sourceip(s,"source"),destip(s,"dest"),	  
	  is_udp(s,"is-udp"),
	  is_request(s,"is-request"),
          transaction_id(s, "transaction-id"),
	  op_id(s,"op-id",Field::flag_nullable),
	  operation(s,"operation"),
	  request_count(0),
	  lastrotate(0),
	  rotate_interval_ns((ExtentType::int64)3600*1000*1000*1000), // 1 hour
	  missing_response_rotated_count(0)
    {
	pending_prev = new HashTable<tidData, tidHash, tidEqual>();
	pending_cur = new HashTable<tidData, tidHash, tidEqual>();
    }

    virtual ~UnbalancedOps() { }

    static const bool print_detail = false;

    DataSeriesModule &source;
    ExtentSeries s;
    Int64Field reqtime;
    Int32Field sourceip, destip;
    BoolField is_udp;
    BoolField is_request;
    Int32Field transaction_id;
    ByteField op_id;
    Variable32Field operation;

    long long request_count;
    
    // data structure to keep requests that do not yet have a matching response
    struct tidData {
	unsigned clientip, serverip;
	int is_udp;
	unsigned transaction_id;
	int op_id;
	string operation;
	long long reqtime;

	tidData(unsigned tid_in, unsigned c_in, unsigned s_in)
	    : clientip(c_in), serverip(s_in), is_udp(0),
	       transaction_id(tid_in), op_id(0),
	       reqtime(0) {}
    };
    class tidHash {
    public: unsigned int operator()(const tidData &t) {
	return BobJenkinsHashMix3(t.transaction_id,t.clientip,2004); // transaction id assigned by client
    }};
    class tidEqual {
    public: bool operator()(const tidData &t1, const tidData &t2) {
	return t1.transaction_id == t2.transaction_id && t1.clientip == t2.clientip;
    }};

    // rotated hash tables to prevent requests with missing replies 
    // from accumulating forever.
    HashTable<tidData, tidHash, tidEqual> *pending_prev, *pending_cur;
    ExtentType::int64 lastrotate;
    const ExtentType::int64 rotate_interval_ns;
    ExtentType::int64 missing_response_rotated_count;
    vector<tidData *> retransmit;
    vector<tidData *> duplicateresponse;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	if (e->type.getName() != "NFS trace: common")
	    return e;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (op_id.isNull())
		continue;
	    if ((reqtime.val() - lastrotate) > rotate_interval_ns) {
		missing_response_rotated_count += pending_prev->size();
		delete pending_prev;
		pending_prev = pending_cur;
		pending_cur = new HashTable<tidData, tidHash, tidEqual>();
		lastrotate = reqtime.val();
	    }
	    if (is_request.val()) {
		++request_count;
		tidData dummy(transaction_id.val(), sourceip.val(), // request source is client
			      destip.val());
		
		tidData *t = pending_cur->lookup(dummy); // expect to find (if at all) in most recent
		if (t == NULL) {
		    t = pending_prev->lookup(dummy); // have to check both for correctness
		}
		if (t == NULL) {
		    // add request to list of pending requests
		    dummy.reqtime = reqtime.val();
		    dummy.is_udp = is_udp.val();
		    dummy.op_id = op_id.val();
		    dummy.operation = operation.stringval();
		    t = pending_cur->add(dummy);
		} else {
		    // add request to list of retransmitted requests
		    tidData *tcopy = new tidData(*t);
		    retransmit.push_back(tcopy);
		}
	    } else { // request is a response
		tidData dummy(transaction_id.val(), destip.val(), sourceip.val()); // response source is server
		tidData *t = pending_cur->lookup(dummy);
		if (t != NULL) {
		    pending_cur->remove(*t);
		} else {
		    t = pending_prev->lookup(dummy);
		    if (t != NULL) {
			pending_prev->remove(*t);
		    } else {
			t = new tidData(dummy);
			t->reqtime = reqtime.val();
			t->is_udp = is_udp.val();
			t->op_id = op_id.val();
			t->operation = operation.stringval();
			duplicateresponse.push_back(t);
		    }
		}
	    }
	}
	return e;
    }

    class sortByTime {
    public: bool operator()(tidData *a, tidData *b) {
	return a->reqtime < b->reqtime;
    }};

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);

	vector<tidData *> vals;
	for(HashTable<tidData, tidHash, tidEqual>::iterator i =
                                               pending_cur->begin();
	    i != pending_cur->end(); ++i) {
	    vals.push_back(&(*i));
	}
	for(HashTable<tidData, tidHash, tidEqual>::iterator i =
                                               pending_prev->begin();
	    i != pending_prev->end(); ++i) {
	    vals.push_back(&(*i));
	}
	sort(vals.begin(),vals.end(),sortByTime());
	cout << format("Summary: %lld/%lld (%.2f%%) requests without responses, %d shown\n")
	    % (missing_response_rotated_count + vals.size())
	    % request_count
	    % ((double)(missing_response_rotated_count + vals.size())/(double)request_count)
	    % vals.size();
	if (print_detail) {
	    printf("%-13s %-15s %-15s %-4s %-8s %-2s %-12s\n",
		   "time",
		   "client",
		   "server",
		   "prot",
		   "xid",
		   "op",
		   "operation");
	    
	    printf("%-13s %-15s %-15s %-4s %-8s %-2s %-12s\n",
		   "-------------", "---------------", "---------------",
		   "----", "--------", "--", "------------");
	    for(vector<tidData *>::iterator i = vals.begin();
		i != vals.end();++i) {
		tidData *j = *i;
		printf("%13Ld %-15s %-15s %-4s %08x %2d %-12s\n",
		       j->reqtime / (1000000),
		       ipv4tostring(j->clientip).c_str(),
		       ipv4tostring(j->serverip).c_str(),
		       j->is_udp ? "UDP" : "TCP",
		       j->transaction_id,
		       j->op_id,
		       j->operation.c_str());
	    }
	    printf("\n");
	}

	cout << format("Summary: %d/%lld duplicate requests\n")
	    % retransmit.size() % request_count;
	if (print_detail) {
	    printf("%-13s %-15s %-15s %-4s %-8s %-2s %-12s\n",
		   "time", "client", "server", "prot", "xid", "op", "operation");
	    printf("%-13s %-15s %-15s %-4s %-8s %-2s %-12s\n",
		   "-------------", "---------------", "---------------",
		   "----", "--------", "--", "------------");
	    for(vector<tidData *>::iterator p = retransmit.begin();
		p != retransmit.end();++p) {
		tidData *j = *p;
		printf("%13Ld %-15s %-15s %-4s %08x %2d %-12s\n",
		       j->reqtime / (1000000),
		       ipv4tostring(j->clientip).c_str(),
		       ipv4tostring(j->serverip).c_str(),
		       j->is_udp ? "UDP" : "TCP",
		       j->transaction_id,
		       j->op_id,
		       j->operation.c_str());
	    }
	    printf("\n");
	}

	printf("Summary: %d duplicate responses\n", duplicateresponse.size());
	if (print_detail) {
	    printf("%-13s %-15s %-15s %-4s %-8s %-2s %-12s\n",
		   "time", "client", "server", "prot", "xid", "op", "operation");
	    printf("%-13s %-15s %-15s %-4s %-8s %-2s %-12s\n",
		   "-------------", "---------------", "---------------",
		   "----", "--------", "--", "------------");
	    for(vector<tidData *>::iterator p = duplicateresponse.begin();
		p != duplicateresponse.end();++p) {
		tidData *j = *p;
		printf("%13Ld %-15s %-15s %-4s %08x %2d %-12s\n",
		       j->reqtime / (1000000),
		       ipv4tostring(j->clientip).c_str(),
		       ipv4tostring(j->serverip).c_str(),
		       j->is_udp ? "UDP" : "TCP",
		       j->transaction_id,
		       j->op_id,
		       j->operation.c_str());
	    }
	}
	
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
};

NFSDSModule *
NFSDSAnalysisMod::newUnbalancedOps(DataSeriesModule &prev)
{
    return new UnbalancedOps(prev);
}

double NFSDSAnalysisMod::gap_parm = 5000.0; // in ms 

class NFSTimeGaps : public NFSDSModule {
public:
    NFSTimeGaps(DataSeriesModule &_source) 
	: source(_source), s(ExtentSeries::typeExact),
	  reqtime(s,"packet-at"), currstart(0), last(0)
    {}

    virtual ~NFSTimeGaps() { }

    DataSeriesModule &source;
    ExtentSeries s;
    Int64Field reqtime;

    // data structure to keep requests that do not yet have a matching response
    struct RangeData {
	long long start, end;
	RangeData(unsigned long long s_in, unsigned long long e_in) :
	    start(s_in), end(e_in) {}
    };
    list<RangeData> ranges;
    long long currstart, last, curr;
    long long starttime;

    virtual Extent *getExtent() {
	
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	if (e->type.getName() != "NFS trace: common")
	    return e;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    curr = reqtime.val();
	    if (currstart == 0) {
		starttime = curr;
		currstart = curr;
	    }
	    else
	    {
		if ((double) (curr - last) / 1000000 > NFSDSAnalysisMod::gap_parm)
		{
		    // new range found
		    ranges.push_back(RangeData(currstart, last));
		    currstart = curr;
		}
	    }
	    last = curr;
	}
	return e;
    }

    virtual void printResult() {
	long long next;
	
	// This code should be executed after all extents are processed.
	// This is probably not the best place to put it.
	if (currstart != 0)
	{
	    ranges.push_back(RangeData(currstart, last));
	    currstart = 0;
	}
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	printf("%-17s %-17s %-15s %-15s %-8s\n", "start", "end", "start from beg", "end from beg", "gap");
	printf("%-17s %-17s %-15s %-15s %-8s\n", "-----------------", "-----------------",
	       "---------------", "---------------", "--------");
	

	for (list<RangeData>::iterator p = ranges.begin();
	     p != ranges.end();
	     )
	{
	    RangeData j = *p++;
	    if (p != ranges.end())
		next = p->start;
	    else
		next = j.end;
	    
	    printf("%17.3f %17.3f %15.3f %15.3f %8.3f\n",
		   (double) j.start / 1000000,
		   (double) j.end / 1000000,
		   (double) (j.start - starttime) / 1000000,
		   (double) (j.end - starttime) / 1000000,
		   (double) (next - j.end) / 1000000);
	}
	
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
};

NFSDSModule *
NFSDSAnalysisMod::newNFSTimeGaps(DataSeriesModule &prev)
{
    return new NFSTimeGaps(prev);
}
