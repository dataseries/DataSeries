#include <vector>

#include <Lintel/HashMap.hpp>
#include <Lintel/StatsQuantile.hpp>

#include <DataSeries/GeneralField.hpp>

#include <analysis/nfs/common.hpp>

using namespace std;
using boost::format;

// TODO: re-do this with the rotating hash-map, re-do with cube for rollup.

// TODO: add code to every so often throw away any old requests to
// limit excessive memory fillup; one of the other analysis does this
// by keeping two hash tables and rotating them every x "seconds",
// which is faster than scanning through one table to find old entries
// and removing them.  Partway through implementing this, when it is,
// make sure to do the missing reply handling bits.  This is important
// to do because the conversion code doesn't handle replies where the
// request was in a different file

// TODO: check to see if enough time has passed that we should
// switch pending1 and pending2, e.g. wait for 5 minutes or
// something like that, or wait until 5 minutes, the latter is
// probably safer, i.e. more likely to catch really late
// replies, it might be worth counting them in this case.  if
// we do this, some of the code in printResult that tracks
// missing replies needs to get moved here so it runs on rotation.

namespace {
    string str_star("*");
    string str_null("null");
}

class ServerLatency : public RowAnalysisModule {
public:
    ServerLatency(DataSeriesModule &source, const string &arg)
	: RowAnalysisModule(source),
	  reqtime(series,""),
	  sourceip(series,"source"), 
	  destip(series,"dest"),	  
	  is_request(series,""),
	  transaction_id(series, ""),
	  op_id(series,"",Field::flag_nullable),
	  operation(series,"operation"),
	  pending1(NULL), 
	  duplicate_request_delay(0.001),
	  missing_request_count(0),
	  duplicate_reply_count(0),
	  min_packet_time_raw(numeric_limits<int64_t>::max()),
	  max_packet_time_raw(numeric_limits<int64_t>::min()),
	  duplicate_request_min_retry_raw(numeric_limits<int64_t>::max()),
	  output_text(true)
    {
	pending1 = new pendingT;
	if (!arg.empty()) {
	    SINVARIANT(arg == "output_sql");
	    output_text = false;
	}
    }

    // TODO: make these configurable options.
    static const bool enable_first_latency_stat = true; // Estimate the latency seen by client
    static const bool enable_last_latency_stat = false; // Estimate the latency for the reply by the server

    static const bool enable_server_rollup = true;
    static const bool enable_operation_rollup = true;
    static const bool enable_overall_rollup = true;
    
    virtual ~ServerLatency() { }

    Int64TimeField reqtime;
    Int32Field sourceip, destip;
    BoolField is_request;
    Int32Field transaction_id;
    ByteField op_id;
    Variable32Field operation;

    void firstExtent(const Extent &e) {
	const ExtentType &type = e.getType();
	if (type.versionCompatible(0,0) || type.versionCompatible(1,0)) {
	    reqtime.setFieldName("packet-at");
	    is_request.setFieldName("is-request");
	    transaction_id.setFieldName("transaction-id");
	    op_id.setFieldName("op-id");
	} else if (type.versionCompatible(2,0)) {
	    reqtime.setFieldName("packet_at");
	    is_request.setFieldName("is_request");
	    transaction_id.setFieldName("transaction_id");
	    op_id.setFieldName("op_id");
	} else {
	    FATAL_ERROR(format("can only handle v[0,1,2].*; not %d.%d")
			% type.majorVersion() % type.minorVersion());
	}
    }

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
    public: uint32_t operator()(const StatsData &k) const {
	    return k.serverip ^ k.operation.hash();
    }};

    class StatsEqual {
    public: bool operator()(const StatsData &a, const StatsData &b) const {
	return a.serverip == b.serverip && a.operation == b.operation;
    }};

    // data structure to keep transactions that do not yet have a
    // matching response
    struct TidData {
	uint32_t tid, server, client, duplicate_count;
	int64_t first_reqtime_raw, last_reqtime_raw;
	bool seen_reply;
	TidData(uint32_t tid_in, uint32_t server_in, uint32_t client_in)
	    : tid(tid_in), server(server_in), client(client_in), 
	      duplicate_count(0), first_reqtime_raw(0), 
	      last_reqtime_raw(0), seen_reply(false) {}
    };

    class TidHash {
    public: uint32_t operator()(const TidData &t) const {
	return t.tid ^ t.client;
    }};

    class TidEqual {
    public: bool operator()(const TidData &t1, const TidData &t2) const {
	return t1.tid == t2.tid && t1.client == t2.client;
    }};

    typedef HashTable<StatsData, StatsHash, StatsEqual> statsT;
    statsT stats_table;

    typedef HashTable<TidData, TidHash, TidEqual> pendingT;
    pendingT *pending1;

    void updateDuplicateRequest(TidData *t) {
	// this check is here in case we are somehow getting duplicate
	// packets delivered by the monitoring process, and we want to
	// catch this and not think that we have realy lots of
	// duplicate requests.  Tried 50ms, but found a case about 5ms
	// apart, so dropped to 2ms
	// inter arrival time
	int64_t request_iat = reqtime.valRaw() - t->last_reqtime_raw; 
	if (request_iat < duplicate_request_min_retry_raw) {
	    cerr << format("warning: duplicate requests unexpectedly close together %s - %s = %s >= %s\n")
		% reqtime.valStrSecNano() 
		% reqtime.rawToStrSecNano(t->last_reqtime_raw)
		% reqtime.rawToStrSecNano(request_iat)
		% reqtime.rawToStrSecNano(duplicate_request_min_retry_raw);
	}
	++t->duplicate_count;
	duplicate_request_delay.add(request_iat/(1000.0*1000.0));
	t->last_reqtime_raw = reqtime.valRaw();
    }

    void handleRequest() {
	// address of server = destip
	TidData dummy(transaction_id.val(), destip.val(), 
		      sourceip.val()); 
	TidData *t = pending1->lookup(dummy);
	if (t == NULL) {
	    // add request to list of pending requests (requests without a response yet)
	    dummy.first_reqtime_raw = dummy.last_reqtime_raw 
		= reqtime.valRaw();
	    t = pending1->add(dummy);
	} else {
	    updateDuplicateRequest(t);
	}
    }
    
    // TODO: use rotating hash map
    void handleResponse() {
	// row is a response, so address of server = sourceip
	TidData dummy(transaction_id.val(), sourceip.val(), destip.val());
	TidData *t = pending1->lookup(dummy);

	if (t == NULL) {
	    ++missing_request_count;
	} else {
	    // we now have both request and response, so we can add latency to statistics
	    // TODO: do the calculation in raw units and convert at the end.
	    int64_t delay_first_raw = reqtime.valRaw() - t->first_reqtime_raw;
	    int64_t delay_last_raw = reqtime.valRaw() - t->last_reqtime_raw;

	    double delay_first_ms 
		= reqtime.rawToDoubleSeconds(delay_first_raw) * 1.0e3;
	    double delay_last_ms 
		= reqtime.rawToDoubleSeconds(delay_last_raw) * 1.0e3;
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
		pending1->remove(*t);
	    }
	}
    }

    virtual void prepareForProcessing() {
	// See updateDuplicateRequest for definition of this.
	duplicate_request_min_retry_raw 
	    = reqtime.secNanoToRaw(0, 2*1000*1000);
    }

    virtual void processRow() {
	if (op_id.isNull()) {
	    return;
	}
	min_packet_time_raw = min(reqtime.valRaw(), min_packet_time_raw);
	max_packet_time_raw = max(reqtime.valRaw(), max_packet_time_raw);

	if (is_request.val()) {
	    handleRequest();
	} else { 
	    handleResponse();
	}
    }

    class sortByServerOp {
    public: bool operator()(StatsData *a, StatsData *b) const {
	if (a->serverip != b->serverip)
	    return a->serverip < b->serverip;
	return a->operation < b->operation;
    }};

    class sortByNOpsReverse {
    public: bool operator()(StatsData *a, StatsData *b) const {
	uint64_t nops_a = a->nops();
	uint64_t nops_b = b->nops();
	return nops_a > nops_b;
    }};

    void printOneQuant(StatsQuantile *v) {
	if (v) {
	    cout << format("; %7.3f %6.3f %6.3f ") 
		% v->mean() % v->getQuantile(0.5)
		% v->getQuantile(0.9);
	}
    }

    void printSQLVector(vector<StatsData *> &vals) {
	SINVARIANT(enable_first_latency_stat);
	for(vector<StatsData *>::iterator i = vals.begin();
	    i != vals.end();++i) {
	    StatsData &j = **i;
	    string serverip;
	    if (j.serverip == 0) {
		serverip = str_null;
	    } else {
		serverip = str(format("%d") % j.serverip);
	    }
	    string operation;
	    if (j.operation == str_star) {
		operation = str_null;
	    } else {
		operation = str(format("'%s'") % j.operation);
	    }

	    cout << format("insert into server_latency_basic (server, operation, op_count, dup_count, mean_lat, stddev_lat, min_lat, max_lat) values (%s, %s, %d, %d, %.8g, %.8g, %.8g, %.8g);\n")
		% serverip % operation % j.first_latency_ms->countll() % j.duplicates->countll()
		% j.first_latency_ms->mean() % j.first_latency_ms->stddev() 
		% j.first_latency_ms->min() % j.first_latency_ms->max();

	    for(double quantile = 0.01; quantile < 0.995; quantile += 0.01) {
		cout << format("insert into server_latency_quantile (server, operation, quantile, latency) values (%s, %s, %.2f, %.8g);\n")
		% serverip % operation % quantile % j.first_latency_ms->getQuantile(quantile);
	    }
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
	double missing_reply_firstlat_ms = 0;
	double missing_reply_lastlat_ms = 0;

	// 1s -- measured peak retransmit in one dataset was
	// 420ms with 95% <= 80ms

	int64_t max_noretransmit_raw = reqtime.secNanoToRaw(1,0);
	for(pendingT::iterator i = pending1->begin(); 
	    i != pending1->end(); ++i) {
	    // the check against noretransmit handles the fact that we
	    // could just accidentally miss the reply and/or we could
	    // miss the reply due to the processing issue of not
	    // handling replies that cross request boundaries.
	    if (!i->seen_reply && 
		(max_packet_time_raw - i->last_reqtime_raw) 
		< max_noretransmit_raw) {
		++missing_reply_count;

		int64_t missing_reply_firstlat_raw =
		    max_packet_time_raw - i->first_reqtime_raw;
		int64_t missing_reply_lastlat_raw =
		    max_packet_time_raw - i->last_reqtime_raw;

		missing_reply_firstlat_ms += 1.0e3 * 
		    reqtime.rawToDoubleSeconds(missing_reply_firstlat_raw);
		missing_reply_lastlat_ms += 1.0e3 *
		    reqtime.rawToDoubleSeconds(missing_reply_lastlat_raw);
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
	StatsData overall(0, str_star);
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
	    tmp->operation = str_star;
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
	    missing_reply_mean_est_firstlat 
		= missing_reply_firstlat_ms/(double)missing_reply_count;
	    missing_reply_mean_est_lastlat 
		= missing_reply_lastlat_ms/(double)missing_reply_count;
	}
	cout << format("%d missing requests, %d missing replies; %.2fms mean est firstlat, %.2fms mean est lastlat\n")
	    % missing_request_count % missing_reply_count 
	    % missing_reply_mean_est_firstlat % missing_reply_mean_est_lastlat;
	       
        cout << format("duplicates: %d replies, %d requests; delays min=%.4fms\n")
	    % duplicate_reply_count % duplicate_request_delay.count() 
	    % duplicate_request_delay.min();
	duplicate_request_delay.printFile(stdout,20);

	if (output_text) {
	    printVector(all_vals, serverip_to_shortid, 
			"Grouped by (Server,Operation) pair");
	    printVector(server_vals, serverip_to_shortid,
			"Grouped by Server");
	    printVector(operation_vals, serverip_to_shortid,
			"Grouped by Operation");
	    printVector(overall_vals, serverip_to_shortid,
			"Complete rollup");
	} else {
	    printSQLVector(all_vals);
	    printSQLVector(server_vals);
	    printSQLVector(operation_vals);
	    printSQLVector(overall_vals);
	}

	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    StatsQuantile duplicate_request_delay;
    uint64_t missing_request_count;
    uint64_t duplicate_reply_count;
    int64_t min_packet_time_raw, max_packet_time_raw;

    int64_t duplicate_request_min_retry_raw;
    bool output_text;
};

namespace NFSDSAnalysisMod {
    RowAnalysisModule *newServerLatency(DataSeriesModule &prev, const string &arg) {
	return new ServerLatency(prev, arg);
    }
}


