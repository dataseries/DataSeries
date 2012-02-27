/* -*-C++-*-
   (c) Copyright 2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** 
=pod

=head1 NAME

ipdsanalysis - IP trace data analysis

=head1 SYNOPSIS

 % ipdsanalysis [module-selection-options] (file...)|(index.ds start-time end-time)

=head1 DESCRIPTION

ipdsanalysis is a program for running analysis on the IP extents in the nfs traces.
It has four different types of modules that are described below.

=head1 OPTIONS

=over 4

=item -a I<interval in seconds>

Calculate package and byte counts for intervals of length I<interval> grouped by (src,dest) pairs,
src only, andllow llow llow  dest only.

=item -b I<interval-seconds,...>[:I<reorder-seconds>[:I<update-check-interval>]]

Calculate rolling packet statistics simultaneously for the different specified interval-seconds.
Calculates quantile distributions on the bandwidth and packets per second that are achieved.  Allow
packets to be reorder by a specified number of seconds to deal with glitches in the traces, and
TODO: something about update-check-interval.

=item -c I<interval-width>

Calculate a time series of packets per second and bytes per second at the specified interval width.

=item -d <min-bytes|fraction>

Calculate a datacube over source, source port, dest, dest port, and packet size.  Specify a minimum
number of bytes, and print out all of the cube entries above that minimum number, or specify a
fraction of the entries to print, and print out the # bytes required to select that fraction of
entries, and then print out the entires.

=back

=cut
*/

// TODO: update this to handle the newer style trace files.

#include <iostream>

#include <Lintel/Deque.hpp>
#include <Lintel/HashTable.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/PriorityQueue.hpp>
#include <Lintel/StatsCube.hpp>
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
using dataseries::TFixedField;

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
	FATAL_ERROR("bad time conversion");
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
	    return lintel::BobJenkinsHashMix3(k.source,k.dest,1972);
	}
    };

    struct nodeHash {
	unsigned operator()(const nodeData &k) const {
	    return lintel::BobJenkinsHashMix3(k.node,1776,1972);
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
	  last_process_time_raw(numeric_limits<int64_t>::min()),
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
	    wire_len.setFieldName("wire-length");
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
	    processOnePacket(packet_at.valRaw(), wire_len.val(),
                             series.getExtentRef().extent_source);
	} else {
	    reorderProcessRow();
	}
    }

    void reorderProcessRow() {
	INVARIANT(packet_at.valRaw() > last_process_time_raw,
		  format("bad ordering %d <= %d in %s") % packet_at.valRaw() 
		  % last_process_time_raw % series.getExtentRef().extent_source);
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
	    last_process_time_raw = max_packettime_raw - measurement_reorder_raw;
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

    static const unsigned nranges = 100;
    virtual void printResult() {
	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;

	cout << format("sum-wire-len=%d\n") % sum_wire_len;
	static const double MIBtoMbps = 1024*1024 * 8 / 1.0e6;
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
    void processOnePacket(int64_t timestamp_raw, int32_t packet_size, const string &file) {
	SINVARIANT(!bw_info.empty());
	for(unsigned i=0; i < bw_info.size(); ++i) {
	    bw_info[i]->update(timestamp_raw, packet_size, packet_at, file);
	}
    }	
    void processPendingPackets(int64_t max_process_raw) {
	const string &file = series.hasExtent() ? series.getExtentRef().extent_source : "END";
	while (!pending_packets.empty() 
	       && pending_packets.top().timestamp_raw <= max_process_raw) {
	    processOnePacket(pending_packets.top().timestamp_raw, 
			     pending_packets.top().packetsize, file);
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

      	void update(int64_t packet_raw, int packet_size, const Int64TimeField &packet_at,
		    const string &filename) {
	    LintelLogDebug("IPRolling::packet", format("UPD %d %d") % packet_raw % packet_size);
	    if (!packets_in_flight.empty()) {
		int64_t cur_back_ts_raw = packets_in_flight.back().timestamp_raw;
		INVARIANT(packet_raw >= cur_back_ts_raw,
			  format("out of order by %.4fs in %s; %d < %d") 
			  % packet_at.rawToDoubleSeconds(cur_back_ts_raw - packet_raw) % filename
			  % packet_raw % cur_back_ts_raw);
	    }
	    while ((packet_raw - cur_time_raw) > interval_width_raw) {
		// update statistics for the interval from cur_time to cur_time + interval_width
		// all packets in p_i_f must have been received in that interval
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
	      kpackets_per_second_convert((1/1000.0) * (1.0/interval_seconds)),
	      cur_bytes_in_queue(0), 
	      quantile_nbound(static_cast<int64_t>(round(substep_count * max_total_seconds 
							 / interval_seconds))),
	      MiB_per_second(1.0/(2*nranges), quantile_nbound), 
	      kpackets_per_second(1.0/(2*nranges), quantile_nbound)
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
    int64_t last_process_time_raw;

    uint64_t sum_wire_len;
    Int64TimeField packet_at;
    Int32Field wire_len;
};

// TODO: write a general reorder module and use it instead of
// buffering all the statistics here.
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
	interval_seconds = stringToDouble(args);
	SINVARIANT(interval_seconds > 0);
    }

    virtual ~IPTimeSeriesBandwidthPacketsPerSecond() { };
    
    virtual void firstExtent(const Extent &e) {
	const ExtentType &type = e.getType();
	if (type.getName() == "Network trace: IP packets") {
	    packet_at.setFieldName("packet-at");
	    wire_len.setFieldName("wire-length");
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

    virtual void prepareForProcessing() {
	interval_raw = packet_at.doubleSecondsToRaw(interval_seconds);
    }

    // arrival time of the packet assuming an arrival entirely
    // within the smallest measurement interval
    virtual void processRow() {
	if (intervals_base == 0) {
	    intervals_base = packet_at.valRaw();
	    intervals_base -= intervals_base % interval_raw; // align to start
	    last_interval_start = intervals_base - interval_raw;
	    last_interval_end = interval_raw;
	}
	INVARIANT(packet_at.valRaw() > intervals_base, boost::format("bad %lld %lld")
		  % packet_at.valRaw() % intervals_base);
	if (packet_at.valRaw() >= last_interval_end) {
	    uint64_t total_intervals = (packet_at.valRaw() - intervals_base)/interval_raw+1;
	    SINVARIANT(total_intervals > sum_bytes.size());
	    sum_bytes.resize(total_intervals);
	    sum_packets.resize(total_intervals);
	    last_interval_end = intervals_base + interval_raw * total_intervals;
	    last_interval_start = last_interval_end - interval_raw;
	    SINVARIANT(packet_at.valRaw() >= last_interval_start 
		       && packet_at.valRaw() < last_interval_end);
	}

	if (packet_at.valRaw() >= last_interval_start) {
	    SINVARIANT(packet_at.valRaw() < last_interval_end);
	    sum_packets[sum_packets.size()-1] += 1;
	    sum_bytes[sum_bytes.size()-1] += wire_len.val();
	} else {
	    // Allow packets to be out of order
	    uint64_t interval = (packet_at.valRaw() - intervals_base)/interval_raw;
	    SINVARIANT(interval < sum_bytes.size() - 1);
	    sum_packets[interval] += 1;
	    sum_bytes[interval] += wire_len.val();
	}
    }

    virtual void printResult() {
	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;

	StatsQuantile packets, bytes;
	for(size_t i=0;i<sum_bytes.size(); ++i) {
	    int64_t interval_start_raw = intervals_base + interval_raw * i;
	    
	    double interval_start_sec = packet_at.rawToDoubleSeconds(interval_start_raw, false);

	    cout << format(" insert into ip_timeseries_data (interval_start, interval_secs, packets, bytes) values (%.3f, %.3f, %.0f, %.0f); \n")
		% interval_start_sec % interval_seconds % sum_packets[i] % sum_bytes[i];
	    packets.add(sum_packets[i] / interval_seconds);
	    bytes.add(sum_bytes[i] / interval_seconds);
	}
	
	cout << format("packets/s:\n");
	packets.printText(cout);
	cout << format("bytes/s:\n");
	bytes.printText(cout);

	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
private:
    std::vector<double> sum_packets;
    std::vector<double> sum_bytes;
    double interval_seconds;
    int64_t interval_raw, intervals_base, last_interval_start, last_interval_end;
    
    Int64TimeField packet_at;
    Int32Field wire_len;
};

namespace {
    static string str_star("*");
    static string str_udp("udp");
    static string str_tcp("tcp");
    static string str_other("other");
}

enum TcpUdpOther { Tcp, Udp, Other };

namespace lintel {
    static inline uint32_t hashType(TcpUdpOther tuo) {
	return static_cast<uint32_t>(tuo);
    }
}


class IPTransmitCube : public RowAnalysisModule {
public:
    IPTransmitCube(DataSeriesModule &_source, char *arg) 
	: RowAnalysisModule(_source),
	  min_packet_at(numeric_limits<int64_t>::max()), 
	  max_packet_at(numeric_limits<int64_t>::min()),
	  packet_at(series, ""), wire_len(series, ""),
	  is_udp(series, "", Field::flag_nullable),
	  source(series, "source"), dest(series, "destination"), 
	  source_port(series, "", Field::flag_nullable), 
	  dest_port(series, "", Field::flag_nullable),
	  top_fraction(1.0), min_bytes(0), bytes_stat(NULL) 
    { 
	top_fraction = stringToDouble(arg);
	SINVARIANT(top_fraction > 0);
    }

    virtual ~IPTransmitCube() { };
    
    virtual void firstExtent(const Extent &e) {
	const ExtentType &type = e.getType();
	if (type.getName() == "Network trace: IP packets"
	    || (type.getName() == "Trace::Network::IP" && type.versionCompatible(1,0))) {
	    packet_at.setFieldName("packet-at");
	    wire_len.setFieldName("wire-length");
	    is_udp.setFieldName("udp-tcp");
	    source_port.setFieldName("source-port");
	    dest_port.setFieldName("destination-port");
	} else if (type.getName() == "Trace::Network::IP" &&
		   type.versionCompatible(2,0)) {
	    packet_at.setFieldName("packet_at");
	    wire_len.setFieldName("wire_length");
	    is_udp.setFieldName("udp_tcp");
	    source_port.setFieldName("source_port");
	    dest_port.setFieldName("destination_port");
	} else {
	    FATAL_ERROR("?");
	}
    }

    static const int source_idx = 0;
    static const int source_port_idx = 1;
    static const int dest_idx = 2;
    static const int dest_port_idx = 3;
    static const int packet_type_idx = 4;

    typedef boost::tuple<int32_t, int32_t, int32_t, int32_t, TcpUdpOther> Key;
    
    virtual void processRow() {
	min_packet_at = min(min_packet_at, packet_at.val());
	max_packet_at = max(max_packet_at, packet_at.val());

	TcpUdpOther tcp_udp_other;
	if (is_udp.isNull()) {
	    tcp_udp_other = Other;
	} else if (is_udp.val()) {
	    tcp_udp_other = Udp;
	} else {
	    tcp_udp_other = Tcp;
	}

	int32_t v_source_port = source_port.isNull() ? -1 : source_port.val();
	int32_t v_dest_port = dest_port.isNull() ? -1 : dest_port.val();
	Key key(source.val(), v_source_port, dest.val(), v_dest_port, tcp_udp_other);
	base_data.add(key, wire_len.val());
    }

    virtual void completeProcessing() {
    }

    static const string host32Str(int32_t v, bool any) {
	if (any) {
	    return str_star;
	} else {
	    return str(format("%08x") % v);
	}
    }

    static const string int32Str(int32_t v, bool any) {
	if (any) {
	    return str_star;
	} else {
	    return str(format("%d") % v);
	}
    }

    static const string &tuoStr(TcpUdpOther v, bool any) {
	if (any) {
	    return str_star;
	} else {
	    switch(v) 
		{
		case Tcp: return str_tcp;
		case Udp: return str_udp;
		case Other: return str_other;
		default: FATAL_ERROR("?");
		}
	}
    }

    void addCubeStat(const Stats &val) {
	bytes_stat->add(val.mean() * val.countll());
    }

    void printCubeEntry(const lintel::StatsCube<Key>::MyAny &atuple, const Stats &val) {
	double bytes = val.mean() * val.countll();
	if (bytes < min_bytes) {
	    return;
	}
	cout << format("%8s:%-5s %8s:%-5s %5s %d packets %.2f MiB\n") 
	    % host32Str(atuple.data.get<0>(), atuple.any[0])
	    % int32Str(atuple.data.get<1>(), atuple.any[1]) 
	    % host32Str(atuple.data.get<2>(), atuple.any[2])
	    % int32Str(atuple.data.get<3>(), atuple.any[3]) 
	    % tuoStr(atuple.data.get<4>(), atuple.any[4])
	    % val.countll() % (val.mean() * val.countll() / (1024.0*1024));
    }
	
    virtual void printResult() {
	cube_data.setOptionalCubeFn(boost::bind(&lintel::StatsCubeFns::cubeAll));
	cube_data.cube(base_data);
	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;
	if (top_fraction < 1.0) {
	    bytes_stat = new StatsQuantile(0.001, cube_data.size());
	    cube_data.walk(boost::bind(&IPTransmitCube::addCubeStat, this, _2));
	    min_bytes = bytes_stat->getQuantile(1-top_fraction);
	    cout << format("# Set min bytes to %.0f to only print top %.2f%% of %d entries\n") 
		% min_bytes % (100.0 * top_fraction) % bytes_stat->countll();
	} else if (top_fraction > 1) {
	    min_bytes = top_fraction;
	    cout << format("# User chose min bytes of %.0f to print subset of %d entries\n") 
		% min_bytes % cube_data.size();
	}
	cube_data.walkOrdered(boost::bind(&IPTransmitCube::printCubeEntry, this, _1, _2));

	cout << format("End-%s\n") % __PRETTY_FUNCTION__;
    }

private:
    int64_t min_packet_at, max_packet_at;
    
    TFixedField<int64_t> packet_at;
    TFixedField<int32_t> wire_len;
    BoolField is_udp;
    TFixedField<int32_t> source, dest;
    Int32Field source_port, dest_port;

    // TODO: just use the base_data in cube_data.
    lintel::HashTupleStats<Key> base_data;
    lintel::StatsCube<Key> cube_data;
    double top_fraction, min_bytes;
    StatsQuantile *bytes_stat;
};

void
usage(char *argv0)
{
    cerr << "Usage: " << argv0 << " flags... (file...)|(index.ds start-time end-time)\n"
	 << " flags\n"
	 << "    -a <interval>; packet and byte counts by (src,dest) and src | dest\n"
	 << "    -b <interval_seconds,...>[:reorder_seconds[:update_check_interval]]\n"
	 << "    -c <interval-width> # time-series pps, bps\n"
	 << "    -d <fraction|min-bytes> # cube of src/dest host-port; either print an\n"
	 << "       # approximate fraction of the total entries (0 <= arg <= 1), or specify\n" 
	 << "       # the min number of bytes needed to print out the entries (arg > 1)\n";
    
    exit(1);
}

double ippair_interval = 60.0;
char *ip_time_series_bandwidth_packets_per_second_arg;

int parseopts(int argc, char *argv[], SequenceModule &ipSequence) {
    bool any_selected;

    any_selected = false;
    while (1) {
	int opt = getopt(argc, argv, "ha:b:c:d:");
	if (opt == -1) break;
	any_selected = true;
	switch(opt){
	case 'a': options[optIPUsage] = 1;
	    ippair_interval = atof(optarg);
	    INVARIANT(ippair_interval >= 0.001,
		      "bad option to -a, expect interval seconds");
	    break;
	case 'b': 
	    ipSequence.addModule(new PrefetchBufferModule(ipSequence.tail(), 1024*1024));
	    ipSequence.addModule(new IPRollingPacketStatistics(ipSequence.tail(), optarg));
	    break;
	case 'c': options[optIPTimeSeriesBandwidthPacketsPerSecond] = 1;
	    ip_time_series_bandwidth_packets_per_second_arg = optarg;
	    break;
	case 'd':
	    ipSequence.addModule(new IPTransmitCube(ipSequence.tail(), optarg));
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

void printResult(SequenceModule::DsmPtr mod) {
    if (mod == NULL) {
	return;
    }
    RowAnalysisModule *rowmod = dynamic_cast<RowAnalysisModule *>(mod.get());
    if (rowmod == NULL && 
	(dynamic_cast<PrefetchBufferModule *>(mod.get()) != NULL ||
	 dynamic_cast<DStoTextModule *>(mod.get()) != NULL)) {
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

    TypeIndexModule *ipsource = 
	new TypeIndexModule("Network trace: IP packets");
    ipsource->setSecondMatch("Trace::Network::IP");

    SequenceModule ipSequence(ipsource);
    
    int first = parseopts(argc, argv, ipSequence);
    if (argc - first < 1) {
	usage(argv[0]);
    }

    if ((argc - first) == 3 && isNumber(argv[first+1]) && isNumber(argv[first+2])) {
	sourceByIndex(ipsource,argv[first],atoi(argv[first+1]),atoi(argv[first+2]));
    } else {
	for(int i=first; i<argc; ++i) {
	    ipsource->addSource(argv[i]); 
	}
    }

    ipsource->startPrefetching();

    if (options[optIPUsage]) {
	ipSequence.addModule(new IPUsage(ipSequence.tail(),
					 ippair_interval));
    }

    if (options[optIPTimeSeriesBandwidthPacketsPerSecond]) {
	ipSequence.addModule(new IPTimeSeriesBandwidthPacketsPerSecond(ipSequence.tail(),
								       ip_time_series_bandwidth_packets_per_second_arg));
    }

    ipSequence.tail().getAndDeleteShared();

    for(SequenceModule::iterator i = ipSequence.begin() + 1;
	i != ipSequence.end(); ++i) {
	printResult(*i);
    }
    return 0;
}


