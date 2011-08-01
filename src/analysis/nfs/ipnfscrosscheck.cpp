#include <Lintel/Deque.hpp>
#include <Lintel/RotatingHashMap.hpp>
#include <Lintel/StatsQuantile.hpp>

#include <DataSeries/TypeIndexModule.hpp>

/*
=pod

=head1 NAME

ipnfscrosscheck - attempt to correlate packets between the IP and NFS groups

=head1 SYNOPSIS

 % ipnfscrosscheck I<input.ds>...

=head1 DESCRIPTION

This program compares the IP and NFS trace records to look for a) NFS records that are missing in
the IP traces (e.g. dropped due to being fragments), or b) IP records that are missing from the NFS
traces (e.g. bad NFS parsing).

=head1 BUGS

There are tons of #if 0 in this code, and it had no documentation, so the above is at best an
approximate guess.

=cut
 */

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

struct SummaryInfo {
    static const double q_error = 0.005;
    static const uint32_t n_bound = 1000 * 1000 * 1000;
    const char *first_file, *last_file;
    StatsQuantile correlated_percent, correlated_bytes, uncorrelated_bytes,
	correlated_percent_dport2049, correlated_percent_sport2049;
    SummaryInfo(const char *a, const char *b) 
	: first_file(a), last_file(b), correlated_percent(q_error, n_bound), 
	  correlated_bytes(q_error, n_bound), uncorrelated_bytes(q_error, n_bound),
	  correlated_percent_dport2049(q_error, n_bound), 
	  correlated_percent_sport2049(q_error, n_bound)
    { }
};

struct PairInfo {
    void addIp(const IPSrcDest &ipsd, int64_t at, uint32_t size, bool is_udp, 
	       uint32_t tcp_seqnum) {
	if (!is_udp) {
	    if (ip_only_resets >= ip_only_reset_threshold) {
		return;
	    }
	    if (!ip_packets) {
		ip_packets = DequePtr(new Deque<Packet>());
	    }
		
	    ip_packets->push_back(Packet(at, size, tcp_seqnum));
	}
	doPartial(ipsd);
    }

    void addNfs(const IPSrcDest &ipsd, int64_t at, uint32_t size, bool is_udp) {
	if (!is_udp) {
	    if (!nfs_packets) {
		if (ip_only_resets >= ip_only_reset_threshold) {
		    cout << "WOW, had connection that did only IP for a while and then did something that looked NFS-like\n";
		    ip_only_resets = 0;
		}
		nfs_packets = DequePtr(new Deque<Packet>());
	    }

	    ++nfs_count;
	    nfs_packets->push_back(Packet(at, size, 0));
	}
	doPartial(ipsd);
    }

    static const uint32_t ip_only_reset_threshold = 50;
    // TODO: once deque is copyable, remove use of pointers, but check
    // to see what performance looks like, could make it much slower.
    typedef boost::shared_ptr<Deque<Packet> > DequePtr;
    DequePtr ip_packets, nfs_packets;
    boost::shared_ptr<StatsQuantile> extra_packets_size;
    uint32_t tcp_overhead, tcp_overhead_q90, ip_only_resets;

    int64_t ip_extra_bytes, ip_extra_packets, correlated_bytes, correlated_requests,
	ip_long_ack_bytes, ip_long_ack_packets, multiple_nfs_in_single_ip, missed_nfs_count, 
	ip_count, nfs_count;

    static const uint32_t check_size = 2000;
    PairInfo() : tcp_overhead(0), ip_only_resets(0), ip_extra_bytes(0), ip_extra_packets(0), 
		 correlated_bytes(0), correlated_requests(0), ip_long_ack_bytes(0),
		 ip_long_ack_packets(0), multiple_nfs_in_single_ip(0), missed_nfs_count(0), 
		 nfs_count(0)
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
	    if (overhead_est >= (14 + 20 + 20) &&  overhead_est < 150) {
		// only allow sane overhead estimates, assume ethernet + ip + tcp
		overhead.push_back(overhead_est);
	    }
	}
	SINVARIANT(overhead.size() > 20);
	sort(overhead.begin(), overhead.end());
	uint32_t pos_10 = static_cast<uint32_t>(0.1 * overhead.size());
	uint32_t pos_90 = static_cast<uint32_t>(0.9 * overhead.size());
	if (overhead[pos_10] != overhead[pos_90]) {
	    uint32_t pos_50 = static_cast<uint32_t>(0.50 * overhead.size());
	    
	    if (overhead[pos_10] == overhead[pos_50]) {
		static unsigned warning_count;
		
		if (++warning_count < 100) {
		    cout << format("Warning, tolerating variable tcp overhead on %s: %d,%d,%d\n")
			% ipsd % overhead[pos_10] % overhead[pos_50] % overhead[pos_90];
		    if (warning_count == 99) {
			cout << "suppressing further warnings.\n";
		    }
		}
		// Probably initial setup bits.
	    } else {
		for(vector<uint32_t>::iterator i = overhead.begin(); i != overhead.end(); ++i) {
		    cout << format("%d, ") % *i;
		}
		cout << "\n";
		cout << format("Warning, very bad tcp overhead on %s: %d != %d / %d") 
		    % ipsd % overhead[pos_10] % overhead[pos_50] % overhead[pos_90];
	    }
	}
	tcp_overhead = overhead[pos_10];
	tcp_overhead_q90 = overhead[pos_90];
	if (tcp_overhead_q90 > tcp_overhead + 16) {
	    tcp_overhead_q90 = tcp_overhead + 16;
	}
    }

    // Somewhat more complicated would be to calculate the TCP overhead
    void doPartial(const IPSrcDest &ipsd) {
	static const unsigned batch_size = 100;
	if (!!ip_packets && !nfs_packets && ip_packets->size() > batch_size * 10) {
	    ++ip_only_resets;
	    // not seeing nfs things, don't retain.
	    ip_packets->clear();
	    if (ip_only_resets >= ip_only_reset_threshold) {
		ip_packets.reset();
	    }
	}
	if (!ip_packets || !nfs_packets) {
	    return;
	}
	if (ip_packets->size() < 2*batch_size || nfs_packets->size() < 2*batch_size) {
	    // accumulate multiple before processing
	    return;
	}
	if (tcp_overhead == 0 && ip_packets->size() > batch_size) {
	    calculateTcpOverhead(ipsd);
	}
	if (!extra_packets_size) {
	    extra_packets_size.reset(new StatsQuantile(0.05, 100000000));
	}	    
	while (ip_packets->size() > batch_size && nfs_packets->size() > batch_size) {
	    if (ip_packets->front().at < nfs_packets->front().at) {
		if (ip_packets->front().size == tcp_overhead) {
		    // ack only 
		    correlated_bytes += ip_packets->front().size;
		    ip_packets->pop_front();
		} else if (ip_packets->at(1).tcp_seqnum == ip_packets->front().tcp_seqnum) {
		    ++ip_long_ack_packets;
		    ip_long_ack_bytes += ip_packets->front().size;
		    // ??? current packet carried no data, but was bigger than "standard" ack.
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
		    // following can not work if we get a write as the second bit, the
		    // remaining can fall over the end of the packet; ought to jump to
		    // the multiple packets part of the code in that case.
//		    INVARIANT(remain > nfs_packets->front().size, format("?? %d %d")
//			      % remain % nfs_packets->front().size);
		    remain -= nfs_packets->front().size;
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

    double epsq(double quantile) { // extra packets size quantile
	if (extra_packets_size->count() == 0) {
	    return 0;
	} else {
	    return extra_packets_size->getQuantile(quantile);
	}
    }

    void summarizeStatus(const IPSrcDest &ipsd, SummaryInfo &summary_info, double print_below) {
	// ignore the extra ones we never try to correlate.
	SINVARIANT(ip_extra_bytes >= 0);
	double ip_bytes = ip_extra_bytes + correlated_bytes;;
	if (nfs_packets == 0 || ip_bytes < 1000*1000) {
	    // if we don't have very many bytes at all, ignore, it's too small to make 
	    // much of a difference, and the percents could be badly thrown off.
	    return;
	}
	summary_info.correlated_bytes.add(correlated_bytes);
	double percent = 100.0 * correlated_bytes / ip_bytes;
	if (ipsd.dest_port == 2049) {
	    summary_info.correlated_percent_dport2049.add(percent);
	} else if (ipsd.source_port == 2049) {
	    summary_info.correlated_percent_sport2049.add(percent);
	}
	summary_info.uncorrelated_bytes.add(ip_extra_bytes);
	summary_info.correlated_percent.add(percent);

	cout << format("insert into ipnfscrosscheck_raw (first_file, last_file, src_ip, src_port, dest_ip, dest_port, correlated_bytes, uncorrelated_bytes, uncorrelated_size_q10, uncorrelated_size_q25, uncorrelated_size_q50) values ('%s', '%s', '%s', %d, '%s', %d, %d, %d, %.0f, %.0f, %.0f);\n") % summary_info.first_file % summary_info.last_file % ipv4tostring(ipsd.source_ip) % ipsd.source_port % ipv4tostring(ipsd.dest_ip) % ipsd.dest_port % correlated_bytes % ip_extra_bytes % epsq(0.1) % epsq(0.25) % epsq(0.5);
	if (percent > print_below) {
	    return;
	}

	cout << format("%s: %.0f total bytes, %.2f%% correlated, %.2f%% ip-extra")
	    % ipsd % ip_bytes % percent % (100.0 * ip_extra_bytes / ip_bytes);
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

void rotateFn(const IPSrcDest &ipsd, PairInfo &info, SummaryInfo *summary_info) {
    info.summarizeStatus(ipsd, *summary_info, 95.0);
}

void registerUnitsEpoch() {
    // Register time types for some of the old traces so we don't have to
    // do it in each of the modules.
    Int64TimeField::registerUnitsEpoch("packet-at", "Trace::Network::IP", 
				       "ssd.hpl.hp.com", 1, "2^-32 seconds", 
				       "unix");
    Int64TimeField::registerUnitsEpoch("packet_at", "Trace::Network::IP", 
				       "ssd.hpl.hp.com", 2, "2^-32 seconds", 
				       "unix");
}

int main(int argc, char *argv[]) {
    TypeIndexModule *nfs_input = new TypeIndexModule("NFS trace: common");
    nfs_input->setSecondMatch("Trace::NFS::common");

    SINVARIANT(argc > 1);
    for(int i = 1; i < argc; ++i) {
	nfs_input->addSource(argv[i]);
    }

    TypeIndexModule *ip_input = new TypeIndexModule("Network trace: IP packets");
    ip_input->setSecondMatch("Trace::Network::IP");

    ip_input->sameInputFiles(*nfs_input);

    ExtentSeries nfs_series;
    ExtentSeries ip_series;

    Extent *tmp_nfs_extent = nfs_input->getExtent();
    SINVARIANT(tmp_nfs_extent != NULL);
    nfs_series.setExtent(tmp_nfs_extent);

    registerUnitsEpoch();

    // fields for Trace::NFS::common (ns = ssd.hpl.hp.com, version = 1.0)
    dataseries::TFixedField<int64_t> nfs_packet_at(nfs_series, "");
    dataseries::TFixedField<int32_t> nfs_source(nfs_series, "source");
    dataseries::TFixedField<int32_t> nfs_source_port(nfs_series, "");
    dataseries::TFixedField<int32_t> nfs_dest(nfs_series, "dest");
    dataseries::TFixedField<int32_t> nfs_dest_port(nfs_series, "");
    BoolField nfs_is_request(nfs_series, "");
    dataseries::TFixedField<int32_t> nfs_payload_length(nfs_series, "");
    BoolField nfs_is_udp(nfs_series, "");

    // fields for Trace::Network::IP (ns = ssd.hpl.hp.com, version = 1.0)
    dataseries::TFixedField<int64_t> ip_packet_at(ip_series, "");
    dataseries::TFixedField<int32_t> ip_source(ip_series, "source");
    dataseries::TFixedField<int32_t> ip_dest(ip_series, "destination");
    dataseries::TFixedField<int32_t> ip_wire_length(ip_series, "");
    Int32Field ip_source_port(ip_series, "", Field::flag_nullable);
    Int32Field ip_dest_port(ip_series, "", Field::flag_nullable);
    Int32Field ip_tcp_seqnum(ip_series, "", Field::flag_nullable);
    BoolField ip_is_udp(ip_series, "", Field::flag_nullable);

    if (tmp_nfs_extent->getType().versionCompatible(1,0)) {
	nfs_packet_at.setFieldName("packet-at");
	nfs_source_port.setFieldName("source-port");
	nfs_dest.setFieldName("dest");
	nfs_dest_port.setFieldName("dest-port");
	nfs_is_request.setFieldName("is-request");
	nfs_payload_length.setFieldName("payload-length");
	nfs_is_udp.setFieldName("is-udp");

	ip_packet_at.setFieldName("packet-at");
	ip_wire_length.setFieldName("wire-length");
	ip_source_port.setFieldName("source-port");
	ip_dest_port.setFieldName("destination-port");
	ip_tcp_seqnum.setFieldName("tcp-seqnum");
	ip_is_udp.setFieldName("udp-tcp");
    } else if (tmp_nfs_extent->getType().versionCompatible(2,0)) {
	nfs_packet_at.setFieldName("packet_at");
	nfs_source_port.setFieldName("source_port");
	nfs_dest.setFieldName("dest");
	nfs_dest_port.setFieldName("dest_port");
	nfs_is_request.setFieldName("is_request");
	nfs_payload_length.setFieldName("payload_length");
	nfs_is_udp.setFieldName("is_udp");

	ip_packet_at.setFieldName("packet_at");
	ip_wire_length.setFieldName("wire_length");
	ip_source_port.setFieldName("source_port");
	ip_dest_port.setFieldName("destination_port");
	ip_tcp_seqnum.setFieldName("tcp_seqnum");
	ip_is_udp.setFieldName("udp_tcp");
    } else {
	FATAL_ERROR("...");
    }
    RotatingHashMap<IPSrcDest, PairInfo> pair_info;
    
    uint64_t row_count = 0;
    static const int32_t test_ips = 0; // 171977751; 
    static const int32_t test_ipd = 174981408;
    int64_t last_rotate_at_raw = 0, rotate_interval_raw = 0;
    
    SummaryInfo summary_info(argv[1], argv[argc-1]);

    while(true) {
	if (nfs_series.getExtent() == NULL) {
	    nfs_series.setExtent(nfs_input->getExtent());
	} 
	if (ip_series.getExtent() == NULL) {
	    ip_series.setExtent(ip_input->getExtent());
	}

	if (nfs_series.getExtent() == NULL || ip_series.getExtent() == NULL) {
	    break; // slight potential error at end of analysis
	}

	++row_count;
	if ((row_count & 0x7FFFFF) == 0) {
	    cout << format("%d rows processed, %d pairs\n") % row_count % pair_info.size();
	    cout.flush();
	    if (rotate_interval_raw == 0) {
		last_rotate_at_raw = ip_packet_at();
		Int64TimeField tmp(ip_series, ip_packet_at.getName());
		rotate_interval_raw = tmp.secNanoToRaw(3600,0);
	    }
	    if ((ip_packet_at() - last_rotate_at_raw) > rotate_interval_raw) {
		cout << "rotate...\n";
		last_rotate_at_raw = ip_packet_at();
		pair_info.rotate(boost::bind(&rotateFn, _1, _2, &summary_info));
		cout << "interim statistics (percent of data correlated between IP/NFS)\n";
		summary_info.correlated_percent.printTextRanges(cout, 20);
		cout.flush();
	    }
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

    pair_info.flushRotate(boost::bind(&rotateFn, _1, _2, &summary_info));
    for(double q = 0.01; q < 1; q += 0.01) {
	cout << format("insert into ipnfscrosscheck_quantiles (first_file, last_file, quantile, percent_correlated, bytes_correlated, bytes_uncorrelated) values ('%s', '%s', %.8f, %.8f, %.8f, %.8f);\n") % argv[1] % argv[argc-1] % q % summary_info.correlated_percent.getQuantile(q) % summary_info.correlated_bytes.getQuantile(q) % summary_info.uncorrelated_bytes.getQuantile(q);
    }
    cout << "\n\nFinal Statistics: bytes of data correlated between IP/NFS\n";
    summary_info.correlated_bytes.printTextRanges(cout, 100);

    cout << "\n\nFinal Statistics: bytes of data uncorrelated between IP/NFS\n";
    summary_info.uncorrelated_bytes.printTextRanges(cout, 100);

    cout << "\n\nFinal Statistics: percent of data correlated between IP/NFS (source port 2049)\n";
    cout << "(note expect more entries here because we prune out low # bytes\n";
    cout << " and workloads are read-heavy so most bytes are from port 2049)\n";
    summary_info.correlated_percent_sport2049.printTextRanges(cout, 100);

    cout << "\n\nFinal Statistics: percent of data correlated between IP/NFS (dest port 2049)\n";
    summary_info.correlated_percent_dport2049.printTextRanges(cout, 100);

    cout << "\n\nFinal Statistics: percent of data correlated between IP/NFS\n";
    summary_info.correlated_percent.printTextRanges(cout, 100);
#if 0
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
#endif
    return 0;
}

/*

create table ipnfscrosscheck_raw (
   first_file varchar(255) not null,
   last_file varchar(255) not null,
   src_ip varchar(32) not null,
   src_port int not null,
   dest_ip varchar(32) not null,
   dest_port int not null,
   correlated_bytes bigint not null,
   uncorrelated_bytes bigint not null,
   uncorrelated_size_q10 int not null,
   uncorrelated_size_q25 int not null,
   uncorrelated_size_q50 int not null,
   key key1 (first_file, last_file, src_ip, src_port, dest_ip, dest_port)
);

*/


