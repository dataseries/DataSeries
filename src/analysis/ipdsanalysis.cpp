/* -*-C++-*-
   (c) Copyright 2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    IP trace data analysis
*/

// TODO: update this to handle the newer style trace files.

// Examine set-0/051000-051499.ds-log; should have impossible values
// with this program calculating the rates at 2x the mbits of the
// reported numbers.

// set-2/011000-011499.ds-log: ~864 seconds; 500 files @128MiB each
// 67108864000 bytes, 536870912000 bits, 621,378,370 bits/s

#include <iostream>

#include <Lintel/Deque.hpp>
#include <Lintel/HashTable.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/PriorityQueue.hpp>
#include <Lintel/StatsQuantile.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/PrefetchBufferModule.hpp>
#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/DStoTextModule.hpp>

#include "process/sourcebyrange.hpp"

using namespace std;
using boost::format;

// needed to make g++-3.3 not suck.
extern int printf (__const char *__restrict __format, ...) 
   __attribute__ ((format (printf, 1, 2)));

enum optionsEnum {
    optIPUsage = 0, // packets/bytes by pairs of source/dest; and by individual source or dest
    optIPRollingPacketStatistics,
    optIPTimeSeriesBandwidthPacketsPerSecond,
    LastOption
};

static bool options[LastOption];

class IPUsage : public RowAnalysisModule {
public:
    IPUsage(DataSeriesModule &_source, double interval) 
	: RowAnalysisModule(_source),
	  packets(0), bytes(0),
	  interval_ns((long long)(interval * 1e9)),
	  interval_end(0),
	  packet_at(series,"packet-at"),
	  ipsrc(series,"source"),
	  ipdest(series,"destination"),
	  wire_len(series,"wire-length")
    {
	printf("# ipusage output (ipinterval|ipnode|ippair): <interval-start> <interval-len> [node1 [node2]] packets bytes\n");
    }

    virtual ~IPUsage() { }

    virtual void processRow() {
	if (packet_at.val() > interval_end) {
	    printInterval();
	    interval_end += interval_ns;
	    if (interval_end < packet_at.val()) {
		double tmp = packet_at.val()/(double)interval_ns;
		interval_end = interval_ns * (long long)ceil(tmp);
		if (packet_at.val() == interval_end) {
		    interval_end += interval_ns;
		}
		SINVARIANT(interval_end > packet_at.val());
	    }
	}
	++packets;
	bytes += wire_len.val();

	// update pair information
	hteData k;
	k.source = ipsrc.val();
	k.dest = ipdest.val();
	hteData *v = ipinfo.lookup(k);
	if (v == NULL) {
	    k.bytes = k.packets = 0;
	    v = ipinfo.add(k);
	}
	++v->packets;
	v->bytes += wire_len.val();
	
	// update individual node information
	nodeData n;
	n.node = k.source;
	nodeData *nv = nodeinfo.lookup(n);
	if (nv == NULL) {
	    n.bytes = n.packets = 0;
	    nv = nodeinfo.add(n);
	}
	++nv->packets;
	nv->bytes += wire_len.val();
	
	n.node = k.dest;
	nv = nodeinfo.lookup(n);
	if (nv == NULL) {
	    n.bytes = n.packets = 0;
	    nv = nodeinfo.add(n);
	}
	++nv->packets;
	nv->bytes += wire_len.val();
    }

    void printInterval() {
	if (false) cout << format("interval ending %d\n") % interval_end;
	double iend = (double)interval_end / 1.0e9;
	double ilen = (double)interval_ns / 1.0e9;
	if (iend > 0) {
	    printf("ipinterval: %.3f %.3f %d %lld\n",
		   iend-ilen,ilen,packets,bytes);
	    bytes = packets = 0;
	}
	for(nodeinfoT::iterator i = nodeinfo.begin();i != nodeinfo.end();++i) {
	    if (i->packets > 0) {
		printf("ipnode: %.3f %.3f %s %d %lld\n",
		       iend-ilen,ilen,ipv4tostring(i->node).c_str(),
		       i->packets,i->bytes);
		i->packets = 0;
		i->bytes = 0;
	    }
	}
	for(ipinfoT::iterator i = ipinfo.begin();i != ipinfo.end();++i) {
	    if (i->packets > 0) {
		printf("ippair: %.3f %.3f %s %s %d %lld\n",
		       iend-ilen,ilen,ipv4tostring(i->source).c_str(),
		       ipv4tostring(i->dest).c_str(),i->packets,i->bytes);
		i->packets = 0;
		i->bytes = 0;
	    }
	}
    }

    virtual void completeProcessing() {
	printInterval();
    }

    struct hteData {
	int packets;
	long long bytes;
	int source, dest;
    };

    struct nodeData {
	int packets;
	int node;
	long long bytes;
    };

    struct hteHash {
	unsigned operator()(const hteData &k) const {
	    return BobJenkinsHashMix3(k.source,k.dest,1972);
	}
    };

    struct nodeHash {
	unsigned operator()(const nodeData &k) const {
	    return BobJenkinsHashMix3(k.node,1776,1972);
	}
    };

    struct hteEqual {
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.source == b.source && a.dest == b.dest;
	}
    };
    
    struct nodeEqual {
	bool operator()(const nodeData &a, const nodeData &b) const {
	    return a.node == b.node;
	}
    };

    typedef HashTable<hteData, hteHash, hteEqual> ipinfoT;
    ipinfoT ipinfo;

    typedef HashTable<nodeData, nodeHash, nodeEqual> nodeinfoT;
    nodeinfoT nodeinfo;

    int packets;
    long long bytes;

    ExtentType::int64 interval_ns, interval_end;
    Int64Field packet_at;
    Int32Field ipsrc, ipdest, wire_len;
};

class IPRollingPacketStatistics : public RowAnalysisModule {
public:
    IPRollingPacketStatistics(DataSeriesModule &_source,
			      const string &args)
	: RowAnalysisModule(_source),
	  max_packettime_raw(0),
	  measurement_reorder_seconds(0),
	  measurement_reorder_raw(0),
	  sum_wire_len(0),
	  packet_at(series, ""),
	  wire_len(series, "")
    { 
	// interval_seconds(,interval_seconds)*[:reorder_seconds]
	vector<string> subargs;
	split(args,":",subargs);

	INVARIANT(subargs.size() <= 3, "too many arguments to iprollingpacketstatistics");
		  
	interval_seconds = split(subargs[0], ","); 

	measurement_reorder_seconds = subargs.size() >= 2 ? stringToDouble(subargs[1]) : 0;
	INVARIANT(measurement_reorder_seconds >= 0, "invalid reorder-seconds, < 0");
    }

    virtual ~IPRollingPacketStatistics() { };
    
    virtual void firstExtent(const Extent &e) {
	const ExtentType &type = e.getType();
	if (type.getName() == "Network trace: IP packets") {
	    packet_at.setFieldName("packet-at");
	    wire_len.setFieldName("wire-len");
	} else if (type.getName() == "Trace::Network::IP" &&
		   type.versionCompatible(1,0)) {
	    packet_at.setFieldName("packet-at");
	    wire_len.setFieldName("wire-length");
	} else if (type.getName() == "Trace::Network::IP" &&
		   type.versionCompatible(2,0)) {
	    packet_at.setFieldName("packet_at");
	    wire_len.setFieldName("wire_length");
	} else {
	    FATAL_ERROR("?");
	}
    }
    
    void initBWRolling(int64_t first_timestamp) {
	bw_info.reserve(interval_seconds.size());
	for(unsigned i = 0; i < interval_seconds.size(); ++i) {
	    double seconds = stringToDouble(interval_seconds[i]);
	    bw_info.push_back(new BandwidthRolling(packet_at.doubleSecondsToRaw(seconds),
						   seconds, first_timestamp,
						   2 * 7 * 86400));
	}
    }

    virtual void prepareForProcessing() {
	measurement_reorder_raw = packet_at.doubleSecondsToRaw(measurement_reorder_seconds);
	if (measurement_reorder_raw == 0) {
	    initBWRolling(packet_at.valRaw());
	}
    }

    // arrival time of the packet assuming an arrival entirely
    // within the smallest measurement interval
    virtual void processRow() {
	sum_wire_len += wire_len.val();
	if (measurement_reorder_raw == 0) {
	    processOnePacket(packet_at.valRaw(), wire_len.val());
	} else {
	    reorderProcessRow();
	}
    }

    void reorderProcessRow() {
	pending_packets.push(packetTimeSize(packet_at.valRaw(), wire_len.val()));
	if (packet_at.valRaw() > max_packettime_raw) {
	    max_packettime_raw = packet_at.valRaw();
	}
	if ((max_packettime_raw - pending_packets.top().timestamp_raw) 
	    >= 2*measurement_reorder_raw) {
	    if (bw_info.empty()) {
		initBWRolling(pending_packets.top().timestamp_raw);
	    }
	    processPendingPackets(max_packettime_raw - measurement_reorder_raw);
	    INVARIANT(pending_packets.empty() == false,
		      format("internal %lld %lld")
		      % max_packettime_raw % measurement_reorder_raw);
	}
    }

    virtual void completeProcessing() {
	if (measurement_reorder_raw > 0) {
	    processPendingPackets(max_packettime_raw);
	}
	SINVARIANT(pending_packets.empty());
    }

    virtual void printResult() {
	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;

	cout << format("sum-wire-len=%d\n") % sum_wire_len;
	static const double MIBtoMbps = 1024*1024 * 8 / 1.0e6;
	static const unsigned nranges = 10;
	for(unsigned i = 0;i<bw_info.size();++i) {
	    if (bw_info[i]->MiB_per_second.count() > 0) {
		cout << format("MiB/s for interval len of %.4gs with samples every %.4gs\n")
		    % packet_at.rawToDoubleSeconds(bw_info[i]->interval_width_raw)
		    % packet_at.rawToDoubleSeconds(bw_info[i]->update_step_raw);
		bw_info[i]->MiB_per_second.printTextRanges(cout, nranges);
		bw_info[i]->MiB_per_second.printTextTail(cout);
		cout << format("Mbps for interval len of %.4gs with samples every %.4gs\n")
		    % packet_at.rawToDoubleSeconds(bw_info[i]->interval_width_raw)
		    % packet_at.rawToDoubleSeconds(bw_info[i]->update_step_raw);
		bw_info[i]->MiB_per_second.printTextRanges(cout, nranges, MIBtoMbps);
		bw_info[i]->MiB_per_second.printTextTail(cout, MIBtoMbps);

		cout << format("kpps for interval len of %.4gs with samples every %.4gs\n")
		    % packet_at.rawToDoubleSeconds(bw_info[i]->interval_width_raw)
		    % packet_at.rawToDoubleSeconds(bw_info[i]->update_step_raw);
		bw_info[i]->kpackets_per_second.printTextRanges(cout, nranges);
		bw_info[i]->kpackets_per_second.printTextTail(cout);
		cout << "\n";
	    }
	}
	
	cout << format("End-%s\n") % __PRETTY_FUNCTION__;
    }
private:
    void processOnePacket(int64_t timestamp_raw, int32_t packet_size) {
	SINVARIANT(!bw_info.empty());
	for(unsigned i=0; i < bw_info.size(); ++i) {
	    bw_info[i]->update(timestamp_raw, packet_size);
	}
    }	
    void processPendingPackets(int64_t max_process_raw) {
	while (!pending_packets.empty() 
	       && pending_packets.top().timestamp_raw <= max_process_raw) {
	    processOnePacket(pending_packets.top().timestamp_raw, 
			     pending_packets.top().packetsize);
	    pending_packets.pop();
	}
    }

    struct packetTimeSize {
	int64_t timestamp_raw;
	int packetsize;
	packetTimeSize(int64_t a, int b)
	    : timestamp_raw(a), packetsize(b) { }
	packetTimeSize()
	    : timestamp_raw(0), packetsize(0) { }
    };
    
    struct packetTimeSizeGeq {
	bool operator()(const packetTimeSize &a, const packetTimeSize &b) const {
	    return a.timestamp_raw >= b.timestamp_raw;
	}
    };

    struct BandwidthRolling {
	int64_t interval_width_raw, update_step_raw, cur_time_raw;
	double MiB_per_second_convert, kpackets_per_second_convert, cur_bytes_in_queue;
	int64_t quantile_nbound;
	Deque<packetTimeSize> packets_in_flight;
	StatsQuantile MiB_per_second, kpackets_per_second;

      	void update(int64_t packet_raw, int packet_size) {
	    LintelLogDebug("IPRolling::packet", format("UPD %d %d") % packet_raw % packet_size);
	    INVARIANT(packets_in_flight.empty() || 
		      packet_raw >= packets_in_flight.back().timestamp_raw,
		      format("out of order %d < %d") % packet_raw 
		      % packets_in_flight.back().timestamp_raw);
	    while ((packet_raw - cur_time_raw) > interval_width_raw) {
		// update statistics for the interval from cur_time to cur_time + interval_width
		// all packets in p_i_f must have been recieved in that interval
		double bw = cur_bytes_in_queue * MiB_per_second_convert;
		MiB_per_second.add(bw);
		double pps = packets_in_flight.size() * kpackets_per_second_convert;
		kpackets_per_second.add(pps);
		LintelLogDebug("IPRolling::detail", format("[%d..%d[: %.0f, %d -> %.6g %.6g")
			       % cur_time_raw % (cur_time_raw + update_step_raw)
			       % cur_bytes_in_queue % packets_in_flight.size() % bw % pps);
		cur_time_raw += update_step_raw;
		while(! packets_in_flight.empty() &&
		      packets_in_flight.front().timestamp_raw < cur_time_raw) {
		    cur_bytes_in_queue -= packets_in_flight.front().packetsize;
		    packets_in_flight.pop_front();
		}
	    }
	    packets_in_flight.push_back(packetTimeSize(packet_raw, packet_size));
	    cur_bytes_in_queue += packet_size;
	}

	BandwidthRolling(int64_t interval_raw, double interval_seconds,
			 int64_t start_time_raw, double max_total_seconds,
			 int substep_count = 20) 
	    : interval_width_raw(interval_raw), update_step_raw(interval_raw/substep_count), 
	      cur_time_raw(start_time_raw), 
	      MiB_per_second_convert((1/(1024.0*1024.0)) * (1.0/interval_seconds)),
	      //	      MiB_per_second_convert((8/1.0e6) * (1.0/interval_seconds)),
	      kpackets_per_second_convert((1/1000.0) * (1.0/interval_seconds)),
	      cur_bytes_in_queue(0), 
	      quantile_nbound(static_cast<int64_t>(round(substep_count * max_total_seconds 
							 / interval_seconds))),
	      MiB_per_second(0.001, quantile_nbound), kpackets_per_second(0.001, quantile_nbound)
	{ 
	    SINVARIANT(substep_count > 0);
	}
    };

    PriorityQueue<packetTimeSize, packetTimeSizeGeq> pending_packets;
    int64_t max_packettime_raw;

    vector<BandwidthRolling *> bw_info;
    vector<string> interval_seconds;

    double measurement_reorder_seconds;
    int64_t measurement_reorder_raw;

    uint64_t sum_wire_len;
    Int64TimeField packet_at;
    Int32Field wire_len;
};

class IPTimeSeriesBandwidthPacketsPerSecond : public RowAnalysisModule {
public:
    IPTimeSeriesBandwidthPacketsPerSecond(DataSeriesModule &_source,
					  const string &args)
	: RowAnalysisModule(_source),
	  intervals_base(0),
	  last_interval_start(0), last_interval_end(0),
	  packet_at(series,"packet-at"),
	  wire_len(series,"wire-length")
    { 
	interval_nsecs = (long long)(atof(args.c_str()) * 1.0e9);
	SINVARIANT(interval_nsecs > 0);
    }

    virtual ~IPTimeSeriesBandwidthPacketsPerSecond() { };
    
    // arrival time of the packet assuming an arrival entirely
    // within the smallest measurement interval
    virtual void processRow() {
	if (intervals_base == 0) {
	    intervals_base = packet_at.val() - 500000000;
	    intervals_base -= intervals_base % interval_nsecs;
	    last_interval_start = intervals_base - interval_nsecs;
	    last_interval_end = intervals_base;
	}
	INVARIANT(packet_at.val() > intervals_base,
		  boost::format("bad %lld %lld")
		  % packet_at.val() % intervals_base);
	if (packet_at.val() >= last_interval_end) {
	    uint64_t total_intervals = (packet_at.val() - intervals_base)/interval_nsecs+1;
	    SINVARIANT(total_intervals > sum_bytes.size());
	    sum_bytes.resize(total_intervals);
	    sum_packets.resize(total_intervals);
	    last_interval_end = intervals_base + interval_nsecs * total_intervals;
	    last_interval_start = last_interval_end - interval_nsecs;
	    SINVARIANT(packet_at.val() >= last_interval_start 
		       && packet_at.val() < last_interval_end);
	}

	if (packet_at.val() >= last_interval_start) {
	    SINVARIANT(packet_at.val() < last_interval_end);
	    sum_packets[sum_packets.size()-1] += 1;
	    sum_bytes[sum_bytes.size()-1] += wire_len.val();
	} else {
	    uint64_t interval = (packet_at.val() - intervals_base)/interval_nsecs;
	    SINVARIANT(interval >= 0 && interval < sum_bytes.size() - 1);
	    sum_packets[interval] += 1;
	    sum_bytes[interval] += wire_len.val();
	}
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);

	for(unsigned i=0;i<sum_bytes.size(); ++i) {
	    long long interval_start = intervals_base + interval_nsecs * i;
	    printf(" insert into ip_timeseries_data (interval_start, interval_nsecs, packets, bytes) values (%lld, %lld, %.0f, %.0f); \n",
		   interval_start, interval_nsecs, sum_packets[i], sum_bytes[i]);
	}
	
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
private:
    std::vector<double> sum_packets;
    std::vector<double> sum_bytes;
    long long interval_nsecs, intervals_base, last_interval_start, last_interval_end;
    
    Int64Field packet_at;
    Int32Field wire_len;
};

void
usage(char *argv0)
{
    cerr << "Usage: " << argv0 << " flags... (file...)|(index.ds start-time end-time)\n";
    cerr << " flags\n";
    cerr << "    -a <interval>; packet and byte counts by (src,dest) and src | dest\n";
    cerr << "    -b <interval_seconds,...>[:reorder_seconds[:update_check_interval]]\n";
    cerr << "    -c <interval-width> # time-series pps, bps\n";
    exit(1);
}

double ippair_interval = 60.0;
char *ip_rolling_packet_statistics_arg;
char *ip_time_series_bandwidth_packets_per_second_arg;

int
parseopts(int argc, char *argv[])
{
    bool any_selected;

    any_selected = false;
    while (1) {
	int opt = getopt(argc, argv, "a:b:c:h");
	if (opt == -1) break;
	any_selected = true;
	switch(opt){
	case 'a': options[optIPUsage] = 1;
	    ippair_interval = atof(optarg);
	    INVARIANT(ippair_interval >= 0.001,
		      "bad option to -a, expect interval seconds");
	    break;
	case 'b': options[optIPRollingPacketStatistics] = 1;
	    ip_rolling_packet_statistics_arg = optarg;
	    break;
	case 'c': options[optIPTimeSeriesBandwidthPacketsPerSecond] = 1;
	    ip_time_series_bandwidth_packets_per_second_arg = optarg;
	    break;
	case 'h': usage(argv[0]);
	    break;
	case '?': FATAL_ERROR("invalid option");
	default:
	    FATAL_ERROR(format("getopt returned '%c'\n") % opt);
	}
    }
    INVARIANT(any_selected, "must select at least one option for analysis");

    return optind;
}

void
printResult(DataSeriesModule *mod)
{
    if (mod == NULL) {
	return;
    }
    RowAnalysisModule *rowmod = dynamic_cast<RowAnalysisModule *>(mod);
    if (rowmod == NULL && 
	(dynamic_cast<PrefetchBufferModule *>(mod) != NULL ||
	 dynamic_cast<DStoTextModule *>(mod) != NULL)) {
	return; // this is ok
    }
	
    INVARIANT(rowmod != NULL, "dynamic cast failed?!");
    rowmod->printResult();
    printf("\n");
}

void registerUnitsEpoch() {
    // Register time types for some of the old traces so we don't have to
    // do it in each of the modules.
    Int64TimeField::registerUnitsEpoch("packet-at", "Network trace: IP packets", "", 0,
				       "nanoseconds", "unix");
    Int64TimeField::registerUnitsEpoch("packet-at", "Trace::Network::IP", 
				       "ssd.hpl.hp.com", 1, "2^-32 seconds", 
				       "unix");
    Int64TimeField::registerUnitsEpoch("packet_at", "Trace::Network::IP", 
				       "ssd.hpl.hp.com", 2, "2^-32 seconds", 
				       "unix");
}

int main(int argc, char *argv[]) {
    LintelLog::parseEnv();
    registerUnitsEpoch();
    int first = parseopts(argc, argv);

    if (argc - first < 1) {
	usage(argv[0]);
    }
    TypeIndexModule *ipsource = 
	new TypeIndexModule("Network trace: IP packets");
    ipsource->setSecondMatch("Trace::Network::IP");
    
    if ((argc - first) == 3 && isnumber(argv[first+1]) && isnumber(argv[first+2])) {
	sourceByIndex(ipsource,argv[first],atoi(argv[first+1]),atoi(argv[first+2]));
    } else {
	for(int i=first; i<argc; ++i) {
	    ipsource->addSource(argv[i]); 
	}
    }

    ipsource->startPrefetching();
    PrefetchBufferModule *ipprefetch = 
	new PrefetchBufferModule(*ipsource,32*1024*1024);
    
    SequenceModule ipSequence(ipprefetch);
    if (options[optIPUsage]) {
	ipSequence.addModule(new IPUsage(ipSequence.tail(),
					 ippair_interval));
    }
    if (options[optIPRollingPacketStatistics]) {
	ipSequence.addModule(new IPRollingPacketStatistics(ipSequence.tail(),
							   ip_rolling_packet_statistics_arg));
    }

    if (options[optIPTimeSeriesBandwidthPacketsPerSecond]) {
	ipSequence.addModule(new IPTimeSeriesBandwidthPacketsPerSecond(ipSequence.tail(),
								       ip_time_series_bandwidth_packets_per_second_arg));
    }

    DataSeriesModule::getAndDelete(ipSequence.tail());

    for(SequenceModule::iterator i = ipSequence.begin() + 1;
	i != ipSequence.end(); ++i) {
	printResult(*i);
    }
    return 0;
}


