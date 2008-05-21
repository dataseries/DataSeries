/* -*-C++-*-
   (c) Copyright 2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    IP trace data analysis
*/

// TODO: update this to handle the newer style trace files.

#include <iostream>

#include <Lintel/HashTable.hpp>
#include <Lintel/StatsQuantile.hpp>
#include <Lintel/PriorityQueue.hpp>
#include <Lintel/Deque.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/PrefetchBufferModule.hpp>
#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/DStoTextModule.hpp>

#include "sourcebyrange.hpp"

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
	if (false) printf("interval ending %lld\n",interval_end);
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
	unsigned operator()(const hteData &k) {
	    return BobJenkinsHashMix3(k.source,k.dest,1972);
	}
    };

    struct nodeHash {
	unsigned operator()(const nodeData &k) {
	    return BobJenkinsHashMix3(k.node,1776,1972);
	}
    };

    struct hteEqual {
	bool operator()(const hteData &a, const hteData &b) {
	    return a.source == b.source && a.dest == b.dest;
	}
    };
    
    struct nodeEqual {
	bool operator()(const nodeData &a, const nodeData &b) {
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
	  max_packettime(0),
	  packet_at(series,"packet-at"),
	  wire_len(series,"wire-length")
    { 
	vector<string> subargs;
	split(args,":",subargs);

	INVARIANT(subargs.size() <= 3,
		  "too many arguments to iprollingpacketstatistics");
	vector<string> interval_secs;
	split(subargs[0],",",interval_secs);
	double reorder_seconds = subargs.size() >= 2 ? atof(subargs[1].c_str()) : 1;
	INVARIANT(reorder_seconds >= 1, format("reorder_seconds too small"
					       " %.2f < 1") % reorder_seconds);
	measurement_reorder_ns = (long long)(reorder_seconds * 1.0e9);
	base_update_check_interval = subargs.size() >= 3 ? atoi(subargs[2].c_str()) : 100000;
	update_check_interval = base_update_check_interval;
	INVARIANT(update_check_interval >= 10000,
		  format("update_check_interval too small %d < 10000")
		  % update_check_interval);

	measurement_intervals_ns.reserve(interval_secs.size());
	for(unsigned i = 0; i < interval_secs.size(); ++i) {
	    double dbl_secs = atof(interval_secs[i].c_str());
	    INVARIANT(dbl_secs >= 1e-6, format("invalid interval seconds"
					       " %.4g < 1e-6") % dbl_secs);
	    long long time_ns = (long long)(dbl_secs * 1.0e9); 
	    measurement_intervals_ns.push_back(time_ns);
	}
    }

    virtual ~IPRollingPacketStatistics() { };
    
    // arrival time of the packet assuming an arrival entirely
    // within the smallest measurement interval
    virtual void processRow() {
	pending_packets.push(packetTimeSize(packet_at.val(), wire_len.val()));
	if (packet_at.val() > max_packettime) {
	    max_packettime = packet_at.val();
	}
	if (pending_packets.size() > update_check_interval) {
	    if ((max_packettime - pending_packets.top().timestamp_ns) < measurement_reorder_ns) {
		// not enough time has passed to try updating packet time stuff
		update_check_interval += base_update_check_interval;
	    } else {
		int start_size = pending_packets.size();
		processPendingPackets(max_packettime - measurement_reorder_ns);
		INVARIANT(pending_packets.empty() == false,
			  format("internal %lld %lld")
			  % max_packettime % measurement_reorder_ns);
		int end_size = pending_packets.size();
		if ((start_size - end_size) < start_size / 2) {
		    // didn't process enough packets, increase update_check_interval a bit.
		    update_check_interval += base_update_check_interval/10;
		}
	    }
	}
    }

    virtual void completeProcessing() {
	processPendingPackets(max_packettime);
	SINVARIANT(pending_packets.empty() == true);
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);

	for(unsigned i = 0;i<bw_info.size();++i) {
	    if (bw_info[i]->MiB_per_second.count() > 0) {
		printf("MiB/s for interval len of %.4gs with samples every %.4gs\n",
		       (double)bw_info[i]->interval_width/1.0e9, 
		       (double)bw_info[i]->update_step/1.0e9);
		bw_info[i]->MiB_per_second.printFile(stdout);
		bw_info[i]->MiB_per_second.printTail(stdout);
		printf("kpps for interval len of %.4gs with samples every %.4gs\n",
		       (double)bw_info[i]->interval_width/1.0e9, 
		       (double)bw_info[i]->update_step/1.0e9);
		bw_info[i]->kpackets_per_second.printFile(stdout);
		bw_info[i]->kpackets_per_second.printTail(stdout);
		printf("\n");
	    }
	}
	
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
private:
    void processPendingPackets(long long max_to_process) {
	if (bw_info.size() == 0) {
	    bw_info.reserve(measurement_intervals_ns.size());
	    for(unsigned i = 0; i < measurement_intervals_ns.size(); ++i) {
		bw_info.push_back(new bandwidth_rolling(measurement_intervals_ns[i],
							pending_packets.top().timestamp_ns));
	    }
	}

	while (!pending_packets.empty() && pending_packets.top().timestamp_ns <= max_to_process) {
	    for(unsigned i=0; i < bw_info.size(); ++i) {
		bw_info[i]->update(pending_packets.top().timestamp_ns,
				   pending_packets.top().packetsize);
	    }
	    pending_packets.pop();
	}
    }

    struct packetTimeSize {
	long long timestamp_ns;
	int packetsize;
	packetTimeSize(ExtentType::int64 a, int b)
	    : timestamp_ns(a), packetsize(b) { }
	packetTimeSize()
	    : timestamp_ns(0), packetsize(0) { }
    };
    
    struct packetTimeSizeGeq {
	bool operator()(const packetTimeSize &a, const packetTimeSize &b) {
	    return a.timestamp_ns >= b.timestamp_ns;
	}
    };

    struct bandwidth_rolling {
	long long interval_width, update_step, cur_time;
	double MiB_per_second_convert, kpackets_per_second_convert, cur_bytes_in_queue;
	Deque<packetTimeSize> packets_in_flight;
	StatsQuantile MiB_per_second, kpackets_per_second;

      	void update(long long packet_ns, int packet_size) {
	    SINVARIANT(packets_in_flight.empty() || 
		       packet_ns >= packets_in_flight.back().timestamp_ns);
	    while ((packet_ns - cur_time) > interval_width) {
		// update statistics for the interval from cur_time to cur_time + interval_width
		// all packets in p_i_f must have been recieved in that interval
		MiB_per_second.add(cur_bytes_in_queue*MiB_per_second_convert);
		kpackets_per_second.add(packets_in_flight.size()*kpackets_per_second_convert);
		cur_time += update_step;
		while(packets_in_flight.empty() == false &&
		      packets_in_flight.front().timestamp_ns < cur_time) {
		    cur_bytes_in_queue -= packets_in_flight.front().packetsize;
		    packets_in_flight.pop_front();
		}
	    }
	    packets_in_flight.push_back(packetTimeSize(packet_ns, packet_size));
	    cur_bytes_in_queue += packet_size;
	}
	bandwidth_rolling(ExtentType::int64 interval_ns, 
			  ExtentType::int64 start_time, 
			  int substep_count = 20) 
	    : interval_width(interval_ns), update_step(interval_ns/substep_count), 
	    cur_time(start_time), 
	    MiB_per_second_convert((1/(1024.0*1024.0)) * (1.0e9/(double)interval_ns)),
	    kpackets_per_second_convert((1/1000.0) * (1.0e9/(double)interval_ns)),
	    cur_bytes_in_queue(0), MiB_per_second(0.001), 
	    kpackets_per_second(0.001) { 
	    SINVARIANT(substep_count > 0);
	}
    };

    unsigned base_update_check_interval, update_check_interval;

    PriorityQueue<packetTimeSize, packetTimeSizeGeq> pending_packets;
    long long max_packettime;

    std::vector<bandwidth_rolling *> bw_info;
    std::vector<long long> measurement_intervals_ns;
    long long measurement_reorder_ns;

    Int64Field packet_at;
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

int
main(int argc, char *argv[]) 
{
    int first = parseopts(argc, argv);

    if (argc - first < 1) {
	usage(argv[0]);
    }
    TypeIndexModule *ipsource = 
	new TypeIndexModule("Network trace: IP packets");

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


