#include <Lintel/Deque.hpp>
#include <Lintel/HashMap.hpp>
#include <Lintel/StatsQuantile.hpp>

#include <DataSeries/TypeIndexModule.hpp>

using namespace std;
using boost::format;

struct IPSrcDest {
    uint32_t source_ip, dest_ip;
    uint16_t source_port, dest_port;

    uint32_t hash() const {
	return lintel::BobJenkinsHashMix3(source_ip, dest_ip, 
					  (static_cast<uint32_t>(source_port) << 16) | dest_port);
    };
    bool operator == (const IPSrcDest &rhs) const {
	return source_ip == rhs.source_ip && dest_ip == rhs.dest_ip 
	    && source_port == rhs.source_port && dest_port == rhs.dest_port;
    }

    bool operator < (const IPSrcDest &rhs) const {
	if (source_ip != rhs.source_ip) {
	    return source_ip < rhs.source_ip;
	}
	if (source_port != rhs.source_port) {
	    return source_port < rhs.source_port;
	}
	if (dest_ip != rhs.dest_ip) {
	    return dest_ip < rhs.dest_ip;
	}
	return dest_port < rhs.dest_port;
    }

    IPSrcDest() { }
    IPSrcDest(uint32_t a, uint32_t b, uint32_t c, uint32_t d) 
	: source_ip(a), dest_ip(c), source_port(b), dest_port(d) 
    { 
	SINVARIANT(b < 65536 && d < 65536);
    }
};

ostream & operator <<(ostream &to, const IPSrcDest &ipsd) {
    to << format("%d.%d -> %d.%d") % ipsd.source_ip % ipsd.source_port
	% ipsd.dest_ip % ipsd.dest_port;
    return to;
}

struct Packet {
    int64_t at;
    uint16_t size;
    uint32_t tcp_seqnum;
    Packet(int64_t a, uint32_t b, uint32_t c) : at(a), size(b), tcp_seqnum(c) {
	SINVARIANT(b < 65536); 
    }
    Packet() : at(0), size(0), tcp_seqnum(0) { }

    bool operator <(const Packet &rhs) const {
	return at < rhs.at;
    }
};

struct PairInfo {
    void init() {
	if (!ip_packets) {
	    ip_packets = DequePtr(new Deque<Packet>());
	    nfs_packets = DequePtr(new Deque<Packet>());
	    extra_packets_size.reset(new StatsQuantile(0.05, 100000000));
	}
    }
    void addIp(const IPSrcDest &ipsd, int64_t at, uint32_t size, bool is_udp, 
	       uint32_t tcp_seqnum) {
	init();
	if (!is_udp) {
	    ip_packets->push_back(Packet(at, size, tcp_seqnum));
	}
	doPartial(ipsd);
    }

    void addNfs(const IPSrcDest &ipsd, int64_t at, uint32_t size, bool is_udp) {
	init();
	if (!is_udp) {
	    ++nfs_count;
	    nfs_packets->push_back(Packet(at, size, 0));
	}
	doPartial(ipsd);
    }

    // TODO: once deque is copyable, remove use of pointers, but check
    // to see what performance looks like, could make it much slower.
    typedef boost::shared_ptr<Deque<Packet> > DequePtr;
    DequePtr ip_packets, nfs_packets;
    boost::shared_ptr<StatsQuantile> extra_packets_size;
    uint32_t tcp_overhead;

    int64_t ip_extra_bytes, ip_extra_packets, correlated_bytes, correlated_requests,
	multiple_nfs_in_single_ip, missed_nfs_count, ip_count, nfs_count;

    static const uint32_t check_size = 2000;
    PairInfo() : tcp_overhead(0), ip_extra_bytes(0), ip_extra_packets(0), 
		 correlated_bytes(0), correlated_requests(0), multiple_nfs_in_single_ip(0), 
		 missed_nfs_count(0), nfs_count(0)
    { }
    ~PairInfo() { } 

    void calculateTcpOverhead(const IPSrcDest &ipsd) {
	vector<uint32_t> overhead;

	overhead.reserve(ip_packets->size());
	
	uint32_t prev_seqnum = ip_packets->front().tcp_seqnum;
	uint32_t prev_size = ip_packets->front().size;
	Deque<Packet>::iterator i = ip_packets->begin();
	for(++i; i != ip_packets->end(); ++i) {
	    uint32_t seqnum = i->tcp_seqnum;
	    uint32_t overhead_est = prev_size - (seqnum - prev_seqnum);
	    if (false) {
		cout << format("est %s @ %d - (%d - %d) = %d\n") % ipsd % prev_size % seqnum 
		    % prev_seqnum % overhead_est;
	    }
	    prev_seqnum = seqnum;
	    prev_size = i->size;
	    overhead.push_back(overhead_est);
	}
	sort(overhead.begin(), overhead.end());
	uint32_t pos_10 = static_cast<uint32_t>(0.1 * overhead.size());
	uint32_t pos_90 = static_cast<uint32_t>(0.9 * overhead.size());
	SINVARIANT(overhead[pos_10] == overhead[pos_90]);
	tcp_overhead = overhead[pos_10];
    }

    // Somewhat more complicated would be to calculate the TCP overhead
    void doPartial(const IPSrcDest &ipsd) {
	if (tcp_overhead == 0 && ip_packets->size() > 100) {
	    calculateTcpOverhead(ipsd);
	}
	while (ip_packets->size() > 100 && nfs_packets->size() > 100) {
	    if (ip_packets->front().at < nfs_packets->front().at) {
		if (ip_packets->front().size == tcp_overhead) {
		    // ack only 
		    correlated_bytes += ip_packets->front().size;
		    ip_packets->pop_front();
		} else {
		    if (false) {
			cout << format("%s: extra packet size %d @%d\n") % ipsd 
			    % ip_packets->front().size % ip_packets->front().at;
		    }
		    ip_extra_packets += 1;
		    ip_extra_bytes += ip_packets->front().size;
		    extra_packets_size->add(ip_packets->front().size);
		    ip_packets->pop_front();
		}
	    } else if (ip_packets->front().size > nfs_packets->front().size) {
		// one or more nfs packets in one ip packet.
		correlated_requests += 1;
		uint32_t remain = ip_packets->front().size - nfs_packets->front().size;
		nfs_packets->pop_front();
		while (nfs_packets->front().at == ip_packets->front().at) {
		    multiple_nfs_in_single_ip += 1;
		    SINVARIANT(remain > nfs_packets->front().size);
		    SINVARIANT(!nfs_packets->empty());
		    nfs_packets->pop_front();
		}
		correlated_bytes += ip_packets->front().size;
		ip_packets->pop_front();
	    } else {
		// NFS op covers multiple packets
		uint32_t full_packet_size = ip_packets->front().size;
		// 14 + 20 + 20 - ethernet + ip + tcp
		int32_t remain = nfs_packets->front().size 
		    - (ip_packets->front().size - (14 + 20 + 20));
		correlated_requests += 1;
		correlated_bytes += ip_packets->front().size;
		nfs_packets->pop_front();
		ip_packets->pop_front();
		while (ip_packets->front().at < nfs_packets->front().at && remain > 0) {
		    remain -= ip_packets->front().size - (14 + 20 + 20);
		    if (remain > 0) {
			correlated_bytes += ip_packets->front().size;
		    } else {
			// definite start of another nfs op, and unaligned
			missed_nfs_count += 1;
			int32_t ip_extra = -remain;
			SINVARIANT(ip_extra >= 0);
			ip_extra_bytes += ip_extra;
		    }
		    if (ip_packets->front().size < full_packet_size) {
			remain = 0; // could be < 0 right now if there was a short request
			// tacked on the end, but we don't care.
		    }
		    ip_packets->pop_front();
		}
	    }		
	}
    }

    void summarizeStatus(const IPSrcDest &ipsd, StatsQuantile &correlated_percent,
			 double print_below) {
	// ignore the extra ones we never try to correlate.
	SINVARIANT(ip_extra_bytes >= 0);
	double ip_bytes = ip_extra_bytes + correlated_bytes;;
	if (nfs_packets == 0 || ip_bytes < 1000000) {
	    return;
	}
	correlated_percent.add(100.0 * correlated_bytes / ip_bytes);

	if (100.0 * correlated_bytes / ip_bytes > print_below) {
	    return;
	}

	cout << format("%s: %.0f total bytes, %.2f%% correlated, %.2f%% ip-extra")
	    % ipsd % ip_bytes % (100.0 * correlated_bytes / ip_bytes)
	    % (100.0 * ip_extra_bytes / ip_bytes);
	cout << format("    %d multiple nfs/ip, %d known missed nfs\n")
	    % multiple_nfs_in_single_ip % missed_nfs_count;
	//	correlated_percent.add(100.0 * correlated_bytes / ip_bytes);
	if (extra_packets_size->count() > 0) {
	    extra_packets_size->printTextRanges(cout, 10);
	}
    }

#if 0
    // vector<Packet> ip_packets, nfs_packets;

    // first attempt, doesn't really do what we want since the
    // overhead for operations is slightly different depending on the
    // operation, and the udp cross-correlation isn't quite right
    // either.
    void doCrossCorrelate(const IPSrcDest &ipsd) {
	cout << format("dcc %s\n") % ipsd;
	vector<Packet>::iterator ip_iter = ip_packets.begin(), nfs_iter = nfs_packets.begin();

	while(ip_iter != ip_packets.end() && nfs_iter != nfs_packets.end()) {
	    if (ip_iter->at < nfs_iter->at) {
		cout << format("%s uncorrelated %d @ %d\n") % ipsd % ip_iter->size % ip_iter->at;
		ip_extra_bytes += ip_iter->size;
		ip_extra_packets += 1;
		++ip_iter;
	    } else {
		SINVARIANT(ip_iter->at == nfs_iter->at);
		if (nfs_iter->size < ip_iter->size 
		    && ip_iter->size <= nfs_iter->size + max_nfs_overhead) {
		    correlated_bytes += ip_iter->size;
		    correlated_requests += 1;
		    ++nfs_iter;
		    ++ip_iter;
		} else if (nfs_iter->size < ip_iter->size) {
		    // likely one packet, but longer than expected
		    correlated_bytes += nfs_iter->size + min_nfs_overhead;
		    correlated_requests += 1;
		    uint32_t remain = ip_iter->size - (nfs_iter->size + min_nfs_overhead);
		    ++nfs_iter;
		    while (nfs_iter < nfs_packets.end() && nfs_iter->at == ip_iter->at) {
			++multiple_nfs_in_single_ip;
			if (remain < nfs_iter->size + min_nfs_overhead && 
			    (ip_iter + 1) < ip_packets.end() && (ip_iter + 1)->at == ip_iter->at) {
			    // nothing remains in this packet, next packet is at the same time
			    break;
			}
			INVARIANT(remain >= nfs_iter->size + min_nfs_overhead, 
				  format("%s@%d: multiple %d vs %d + %d") % ipsd % nfs_iter->at
				  % remain % nfs_iter->size % min_nfs_overhead);
			correlated_bytes += nfs_iter->size + min_nfs_overhead;
			correlated_requests += 1;
			remain -= nfs_iter->size + min_nfs_overhead;
		    }
		    SINVARIANT(remain < 64);
		    correlated_bytes += remain;

		    ++ip_iter;
		} else if (nfs_iter->size > ip_iter->size + 100) { // fair bit over a packet
		    ++ip_iter;
		    ++nfs_iter;
		} else {
		    FATAL_ERROR(format("%s unimpl @ %d %d + %d > %d") % ipsd % nfs_iter->at 
				% nfs_iter->size % max_nfs_overhead % ip_iter->size);
		}
	    }
	}
	ip_packets.clear(); 
	nfs_packets.clear();
    }

    void overheadEstimate(const IPSrcDest &ipsd) {
	if (false) {
	    sort(ip_packets.begin(), ip_packets.end());
	    sort(nfs_packets.begin(), nfs_packets.end());
	}

	if (!packet_overhead) {
	    packet_overhead.reset(new StatsQuantile(0.05, check_size + 100));
	}

	vector<Packet>::iterator ip_iter = ip_packets.begin(), nfs_iter = nfs_packets.begin();
	while(ip_iter != ip_packets.end() && nfs_iter != nfs_packets.end()) {
	    if (ip_iter->at < nfs_iter->at) {
		++ip_iter;
	    } else {
		SINVARIANT(ip_iter->at == nfs_iter->at);
		if (false) {
		    cout << format("@%d %d vs %d\n") % ip_iter->at % ip_iter->size 
			% nfs_iter->size;
		}
		
		if (false && ipsd.source_ip == 0x0a0c0d31) {
		    cout << format("x %d - %d = %d\n") % ip_iter->size % nfs_iter->size 
			% (ip_iter->size - nfs_iter->size);
		}
		
		// Prune out nfs sizes > packet size, and ip sizes at min packet size; 
		// can get weirdnesses at either end.
		if (nfs_iter->size < ip_iter->size && ip_iter->size > 64) {
		    packet_overhead->add(ip_iter->size - nfs_iter->size);
		}
		// multiple nfs ops in one ip possible, don't increment ip iter.
		++nfs_iter;
	    }
	}
	if (packet_overhead->count() > 0) {
	    double q_25 = packet_overhead->getQuantile(0.25);
	    double q_75 = packet_overhead->getQuantile(0.75);
	    if (q_25 == q_75) {
		min_nfs_overhead = max_nfs_overhead = static_cast<uint32_t>(q_25);
		cout << format("%d: overhead is %d\n") % ipsd % min_nfs_overhead;
		packet_overhead.reset();
	    } else {
		packet_overhead->printTextRanges(cout, 10);
		cout << format("Warning: %d mismatch on overhead estimate %.0f != %.0f\n")
		    % ipsd % q_25 % q_75;
		// Min Ethernet: 14, min UDP: 8 ; min RPC 20
		if ((q_75 - q_25) < 42) { // < min nfs overhead
		    min_nfs_overhead = static_cast<uint32_t>(q_25);
		    max_nfs_overhead = static_cast<uint32_t>(q_75);
		    packet_overhead.reset();
		} else if ((q_75 - q_25) <= 44) {
		    // Found case in nfs-1/set-0; 0a0c0d31; initial
		    // overhead is all 114, then transitions to 158.
		    cout << format("WARNING: on %d, overhead range exceeds min overhead size") 
			% ipsd;
		    min_nfs_overhead = static_cast<uint32_t>(q_25);
		    max_nfs_overhead = static_cast<uint32_t>(q_75);
		    packet_overhead.reset();
		} else {
		    FATAL_ERROR("?");
		}
	    }
	}
	if (min_nfs_overhead > 0) {
	    doCrossCorrelate(ipsd);
	}
    }

    void doPartial(const IPSrcDest &ipsd) {
	if (min_nfs_overhead > 0) {
	    doCrossCorrelate(ipsd);
	} else if (ip_packets.size() > check_size && nfs_packets.size() > check_size) {
	    overheadEstimate(ipsd);
	} else if (ip_packets.size() > 2*check_size && nfs_packets.size() < 10) {
	    ++clear_count;
	    ip_packets.clear();
	    nfs_packets.clear();
	    cout << "clear\n";
	} else if (ip_packets.size() > 50*check_size || nfs_packets.size() > 2*check_size) {
	    FATAL_ERROR(format("for %s: %d ip, %d nfs") 
			% ipsd % ip_packets.size() % nfs_packets.size());
	}
    }
#endif
};

struct SortByFn {
public:
    typedef pair<IPSrcDest, PairInfo> Pair;

    bool operator () (const Pair &a, const Pair &b) const {
	return a.first < b.first;
    }
};

int main(int argc, char *argv[]) {
    TypeIndexModule *nfs_input = new TypeIndexModule("NFS trace: common");
    nfs_input->setSecondMatch("Trace::NFS::common");

    for(int i = 1; i < argc; ++i) {
	nfs_input->addSource(argv[i]);
    }

    TypeIndexModule *ip_input = new TypeIndexModule("Network trace: IP packets");
    ip_input->setSecondMatch("Trace::Network::IP");

    ip_input->sameInputFiles(*nfs_input);

    ExtentSeries nfs_series;
    ExtentSeries ip_series;

    // fields for Trace::NFS::common (ns = ssd.hpl.hp.com, version = 1.0)
    dataseries::TFixedField<int64_t> nfs_packet_at(nfs_series, "packet-at");
    dataseries::TFixedField<int32_t> nfs_source(nfs_series, "source");
    dataseries::TFixedField<int32_t> nfs_source_port(nfs_series, "source-port");
    dataseries::TFixedField<int32_t> nfs_dest(nfs_series, "dest");
    dataseries::TFixedField<int32_t> nfs_dest_port(nfs_series, "dest-port");
    BoolField nfs_is_request(nfs_series, "is-request");
    dataseries::TFixedField<int32_t> nfs_payload_length(nfs_series, "payload-length");
    BoolField nfs_is_udp(nfs_series, "is-udp");

    // fields for Trace::Network::IP (ns = ssd.hpl.hp.com, version = 1.0)
    dataseries::TFixedField<int64_t> ip_packet_at(ip_series, "packet-at");
    dataseries::TFixedField<int32_t> ip_source(ip_series, "source");
    dataseries::TFixedField<int32_t> ip_dest(ip_series, "destination");
    dataseries::TFixedField<int32_t> ip_wire_length(ip_series, "wire-length");
    Int32Field ip_source_port(ip_series, "source-port", Field::flag_nullable);
    Int32Field ip_dest_port(ip_series, "destination-port", Field::flag_nullable);
    Int32Field ip_tcp_seqnum(ip_series, "tcp-seqnum", Field::flag_nullable);
    BoolField ip_is_udp(ip_series, "udp-tcp", Field::flag_nullable);

    HashMap<IPSrcDest, PairInfo> pair_info;
    
    uint64_t row_count = 0;
    static const int32_t test_ips = 0; // 171977751; 
    static const int32_t test_ipd = 174981408;
    while(true) {
	++row_count;
	if ((row_count & 0x1FFFF) == 0) {
	    cout << format("%d rows processed, %d pairs\n") % row_count % pair_info.size();
	}

	if (nfs_series.getExtent() == NULL) {
	    nfs_series.setExtent(nfs_input->getExtent());
	} 
	if (ip_series.getExtent() == NULL) {
	    ip_series.setExtent(ip_input->getExtent());
	}

	if (nfs_series.getExtent() == NULL || ip_series.getExtent() == NULL) {
	    break; // slight potential error at end of analysis
	}
	if (false) cout << format("ip @%d, nfs@%d\n") % ip_packet_at() % nfs_packet_at();
	if (ip_packet_at() < nfs_packet_at()) {
	    if (!ip_is_udp.isNull()) {
		IPSrcDest ipsd(ip_source(), ip_source_port(), ip_dest(), ip_dest_port());
	    
		if (test_ips == 0 || (ip_source() == test_ips && ip_dest() == test_ipd)) {
		    pair_info[ipsd].addIp(ipsd, ip_packet_at(), ip_wire_length(),
					  ip_is_udp(), ip_tcp_seqnum());
		}
	    }
	    ip_series.next();
	    if (!ip_series.morerecords()) {
		delete ip_series.getExtent();
		ip_series.clearExtent();
	    }
	} else {
	    IPSrcDest ipsd(nfs_source(), nfs_source_port(), nfs_dest(), nfs_dest_port());
	    
	    if (test_ips == 0 || (nfs_source() == test_ips && nfs_dest() == test_ipd)) {
		pair_info[ipsd].addNfs(ipsd, nfs_packet_at(), nfs_payload_length(),
				       nfs_is_udp());
	    }
	    nfs_series.next();
	    if (!nfs_series.morerecords()) {
		delete nfs_series.getExtent();
		nfs_series.clearExtent();
	    }
	}
    }

    StatsQuantile correlated_percent(0.01, pair_info.size());
    vector<pair<IPSrcDest, PairInfo> > sorted_data;
    for(HashMap<IPSrcDest, PairInfo>::iterator i = pair_info.begin(); i != pair_info.end(); ++i) {
	i->second.summarizeStatus(i->first, correlated_percent, -1);
	sorted_data.push_back(make_pair(i->first, i->second));
    }
    correlated_percent.printTextRanges(cout, 20);
    double print_below = correlated_percent.getQuantile(0.1);
    correlated_percent.reset();
    sort(sorted_data.begin(), sorted_data.end(), SortByFn());
    for(vector<pair<IPSrcDest, PairInfo> >::iterator i = sorted_data.begin(); 
	i != sorted_data.end(); ++i) {
	i->second.summarizeStatus(i->first, correlated_percent, print_below);
    }
    correlated_percent.printTextRanges(cout, 20);
    return 0;
}
