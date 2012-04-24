/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <string>
#include <list>

#include <boost/format.hpp>

#include <Lintel/ConstantString.hpp>
#include <Lintel/HashMap.hpp>
#include <Lintel/StatsQuantile.hpp>
#include <Lintel/StringUtil.hpp>

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
	unsigned int operator()(const hteData &k) const {
	    return (k.is_udp ? 256 : 0) + (k.is_request ? 512 : 0) + k.op_id;
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.is_udp == b.is_udp && a.is_request == b.is_request &&
		a.op_id == b.op_id;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent::Ptr getSharedExtent() {
        Extent::Ptr e = source.getSharedExtent();
	if (e == NULL) 
	    return e;
	if (e->getType().getName() != "NFS trace: common")
	    return e;
	for(s.setExtent(e);s.morerecords();++s) {
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
	bool operator()(hteData *a, hteData *b) const {
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
	unsigned int operator()(const hteData &k) const {
	    return lintel::BobJenkinsHashMix3(k.client,k.server,k.is_udp ? 0x55555555 : 0x0);
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.is_udp == b.is_udp && a.client == b.client &&
		a.server == b.server;
	}
    };

    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent::Ptr getSharedExtent() {
        Extent::Ptr e = source.getSharedExtent();
	if (e == NULL) 
	    return e;
	if (e->getType().getName() != "NFS trace: common")
	    return e;
	for(s.setExtent(e);s.morerecords();++s) {
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
	bool operator()(hteData *a, hteData *b) const {
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

class PayloadInfo : public NFSDSModule {
public:
    PayloadInfo(DataSeriesModule &_source)
	: source(_source), s(ExtentSeries::typeExact),
	  packet_at(s,"packet-at"),
	  payload_length(s,"payload-length"),
	  is_udp(s,"is-udp"),
	  op_id(s,"op-id",Field::flag_nullable),
	  min_time(numeric_limits<int64_t>::max()), 
	  max_time(numeric_limits<int64_t>::min()) { 
	FATAL_ERROR("need to fix time code");
    }
    virtual ~PayloadInfo() {}

    virtual Extent::Ptr getSharedExtent() {
        Extent::Ptr e = source.getSharedExtent();
	if (e == NULL) return e;
	if (e->getType().getName() != "NFS trace: common") return e;
	for(s.setExtent(e);s.morerecords();++s) {
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
    int64_t min_time, max_time;
};

NFSDSModule *
NFSDSAnalysisMod::newPayloadInfo(DataSeriesModule &prev)
{
    return new PayloadInfo(prev);
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

	tidData() { }
	tidData(unsigned tid_in, unsigned c_in, unsigned s_in)
	    : clientip(c_in), serverip(s_in), is_udp(0),
	       transaction_id(tid_in), op_id(0),
	       reqtime(0) {}
    };
    class tidHash {
    public: unsigned int operator()(const tidData &t) const {
	return lintel::BobJenkinsHashMix3(t.transaction_id,t.clientip,2004); // transaction id assigned by client
    }};
    class tidEqual {
    public: bool operator()(const tidData &t1, const tidData &t2) const {
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

    virtual Extent::Ptr getSharedExtent() {
        Extent::Ptr e = source.getSharedExtent();
	if (e == NULL) 
	    return e;
	if (e->getType().getName() != "NFS trace: common")
	    return e;
	for(s.setExtent(e);s.morerecords();++s) {
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
    public: bool operator()(tidData *a, tidData *b) const {
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

	cout << format("Summary: %d duplicate responses\n") % duplicateresponse.size();
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

    virtual Extent::Ptr getSharedExtent() {
	
        Extent::Ptr e = source.getSharedExtent();
	if (e == NULL) 
	    return e;
	if (e->getType().getName() != "NFS trace: common")
	    return e;
	for(s.setExtent(e);s.morerecords();++s) {
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
