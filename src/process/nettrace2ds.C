/* -*-C++-*-
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details

   Description:  NFS tcpdump to data series convert; derived from nfsdump
*/

// Note: info processing should do all of the parsing and validation
// that's going to happen so that we know at the end of that exercise
// that conversion will run cleanly.  Hence tests for mode == Convert
// should only protect record creation/setting operations, not
// assertions.

// TODO: convert all AssertAlways to INVARIANT

// TODO: Modify the short data assert code to make sure that we are at
// wire size for the packet so that if there is something missing in
// an RPC request/reply we don't incorrectly claim it is short and
// incorrectly pass the "at end of packet" check.

// For TCP stream reassembly: 
// http://www.circlemud.org/~jelson/software/tcpflow/

#define enable_encrypt_filenames 1

// Do this first to get byteswap things...
#include <DataSeries/Extent.H>

#include <stdio.h>
#include <fcntl.h>
#include <pcap.h>
#include <time.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <assert.h>
#include <zlib.h>
#include <bzlib.h>

#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/statvfs.h>

#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/udp.h>
#include <netinet/tcp.h>

#include <string>

#include <Lintel/HashTable.H>
#include <Lintel/AssertBoost.H>
#include <Lintel/AssertException.H>
#include <Lintel/StringUtil.H>
#include <Lintel/PriorityQueue.H>
#include <Lintel/StatsQuantile.H>
#include <Lintel/Deque.H>
#include <Lintel/Clock.H>
#include <Lintel/PThread.H>

#include <DataSeries/commonargs.H>
#include <DataSeries/DataSeriesModule.H>

#include <process/nfs_prot.h>
#include <DataSeries/cryptutil.H>
extern "C" {
#include <liblzf-1.6/lzf.h>
}

using namespace std;
using boost::format;

enum ModeT { Info, Convert };

ModeT mode = Info;

enum FileT { UNKNOWN, ERF, PCAP };
FileT file_type = UNKNOWN;

const bool warn_duplicate_reqs = false;
const bool warn_parse_failures = false;
const bool warn_unmatched_rpc_reply = false; // not watching output

enum CountTypes {
    ignored_nonip = 0,
    arp_type,
    weird_ethernet_type,
    ip_packet,
    ip_fragment,
    packet_count,
    rpc_request,
    rpc_reply,
    nfs_request,
    nfs_reply,
    tcp_rpc_message,
    udp_rpc_message,
    tcp_short_data_in_rpc,
    possible_missing_request,
    tcp_multiple_rpcs,
    nfsv2_request,
    nfsv3_request,
    nfsv2_reply,
    nfsv3_reply,
    unhandled_nfsv2_requests,
    unhandled_nfsv3_requests,
    outstanding_rpcs,
    link_layer_error,
    duplicate_request,
    bad_retransmit,
    packet_loss,
    tiny_packet,
    tcp_packet, 
    not_an_rpc,
    rpc_parse_error,
    rpc_tcp_request_len,
    rpc_tcp_reply_len,
    wire_len,
    readdir_continuations_ignored,
    long_packets,
    long_packets_port_2049,
    last_count // marker: maximum count of types
};

string count_names[] = {
    "ignored_nonip",
    "arp_type",
    "weird_ethernet_type",
    "ip_packet",
    "ip_fragment",
    "packet",
    "rpc_request",
    "rpc_reply",
    "nfs_request",
    "nfs_reply",
    "tcp_rpc_message",
    "udp_rpc_message",
    "tcp_short_data_in_rpc",
    "possible_missing_request",
    "tcp_multiple_rpcs",
    "nfsv2_request",
    "nfsv3_request",
    "nfsv2_reply",
    "nfsv3_reply",
    "unhandled_nfsv2_requests",
    "unhandled_nfsv3_requests",
    "outstanding_rpcs",
    "link_layer_error",
    "duplicate_request",
    "bad_retransmit",
    "packet_loss",
    "tiny_packet",
    "tcp_packet",
    "not_an_rpc",
    "rpc_parse_error",
    "rpc_tcp_request_len",
    "rpc_tcp_reply_len",
    "wire_len",
    "readdir_continuations_ignored",
    "long_packets_ignored",
    "long_packets_port_2049_ignored",
};

vector<int64_t> counts;

static int exitvalue = 0;
static string tracename;
int cur_file_packet_num = 0;

ExtentType::int64 cur_record_id = -1000000000;
ExtentType::int64 first_record_id = -1000000000;
int cur_mismatch_duplicate_requests = 0;

struct packetTimeSize {
    ExtentType::int64 timestamp_us;
    int packetsize;
    packetTimeSize(ExtentType::int64 a, int b)
	: timestamp_us(a), packetsize(b) { }
    packetTimeSize()
	: timestamp_us(0), packetsize(0) { }
};

struct packetTimeSizeGeq {
    bool operator()(const packetTimeSize &a, const packetTimeSize &b) {
	return a.timestamp_us >= b.timestamp_us;
    }
};

PriorityQueue<packetTimeSize, packetTimeSizeGeq> packet_bw_rolling_info;

struct bandwidth_rolling {
    ExtentType::int64 interval_microseconds, update_step, cur_time;
    double cur_bytes_to_mbits_multiplier, cur_bytes_in_queue;
    Deque<packetTimeSize> packets_in_flight;
    StatsQuantile mbps;
    void update(ExtentType::int64 packet_us, int packet_size) {
	INVARIANT(cur_time > 0, "bad, didn't call setStartTime()");
	AssertAlways(packets_in_flight.empty() || 
		     packet_us >= packets_in_flight.back().timestamp_us,("internal"));
	while ((packet_us - cur_time) > interval_microseconds) {
	    // update statistics for the interval from cur_time to cur_time + interval_width
	    // all packets in p_i_f must have been recieved in that interval
	    mbps.add(cur_bytes_in_queue * cur_bytes_to_mbits_multiplier);
	    cur_time += update_step;
	    while(packets_in_flight.empty() == false &&
		  packets_in_flight.front().timestamp_us < cur_time) {
		cur_bytes_in_queue -= packets_in_flight.front().packetsize;
		packets_in_flight.pop_front();
	    }
	}
	packets_in_flight.push_back(packetTimeSize(packet_us, packet_size));
	cur_bytes_in_queue += packet_size;
    }

    void setStartTime(ExtentType::int64 start_time) {
	INVARIANT(cur_time == 0 && start_time > 0, format("bad %d %d") % cur_time % start_time);
	cur_time = start_time;
    }

    bandwidth_rolling(ExtentType::int64 _interval_microseconds, 
		      ExtentType::int64 start_time, 
		      int substep_count = 20) 
	       : interval_microseconds(_interval_microseconds), 
		 update_step(interval_microseconds/substep_count), 
		 cur_time(start_time), 
		 cur_bytes_to_mbits_multiplier(8.0/static_cast<double>(interval_microseconds)),
		 cur_bytes_in_queue(0) { 
	AssertAlways(substep_count > 0,("internal"));
    }
};

vector<bandwidth_rolling *> bw_info;
Clock::Tll max_incremental_processed = 0;
int incremental_process_at_packet_count = 500000; // 6-8MB of buffering

#if defined(bswap_64)
inline uint64_t ntohll(uint64_t in)
{
    return bswap_64(in);
}
#else
#error "don't know how to do ntohll"
#endif

// Linux as of 2.6.18.2 if you call madvise(fd, WILLNEED) will block
// until the WILLNEED is done Grrrrr;

class Prefetcher: public PThread {
public:
    Prefetcher(string _filename)
	: fd(-1), data(NULL), datasize(0), filename(_filename) { 
    }

    virtual void *run() {
	struct stat statbuf;

	int ret = stat(filename.c_str(), &statbuf);
    
	INVARIANT(ret == 0, format("could not stat source file %s: %s")
		  % filename % strerror(errno));
	datasize = statbuf.st_size;

	fd = open(filename.c_str(), O_RDONLY);
	INVARIANT(fd > 0, format("could not open source file %s: %s")
		  % filename % strerror(errno));
    
	data = mmap(NULL, statbuf.st_size, PROT_READ, MAP_SHARED, fd, 0);
	INVARIANT(data != MAP_FAILED, format("could not mmap source file %s: %s")
		  % filename % strerror(errno));
	
	// I don't know what madvise willneed does, but the net effect
	// of calling this is to not have the desired effect of
	// running at high cpu utilization in the main thread.

//	printf("madv (%s)...\n", filename.c_str());
//	madvise(data, datasize, MADV_WILLNEED);
	if (false) printf("touch (%s)...\n", filename.c_str());

	unsigned int defeat_opt = 0;
	int readbufsize = 262144;
	char readbuf[readbufsize];
	while(read(fd, readbuf, readbufsize) > 0) {
	    // just force it into memory
	}
	// Now make sure it's there.
	for(unsigned i = 0; i < datasize; i += 2048) {
	    defeat_opt += *(reinterpret_cast<unsigned char *>(data)+i);
	}
	if (false) printf("done (%s)\n", filename.c_str());
	return reinterpret_cast<void *>(defeat_opt);
    }

    void finish() {
	madvise(data, datasize, MADV_DONTNEED);
	INVARIANT(munmap(data, datasize) == 0, "bad");
	data = reinterpret_cast<unsigned char *>(-1);
	INVARIANT(close(fd) == 0, "bad");
	fd = -1;
    }

    static Prefetcher *doit(const string &filename) {
	Prefetcher *fetcher = new Prefetcher(filename);
	fetcher->start();
	return fetcher;
    }

    int fd;
    void *data;
    unsigned datasize;
    string filename;
};

class NettraceReader {
public:
    NettraceReader(const string &_filename) : filename(_filename) { }

    virtual ~NettraceReader() { }
    /// Return false on EOF; all pointers must be valid
    virtual void prefetch() = 0;
    virtual bool nextPacket(unsigned char **packet_ptr, uint32_t *capture_size,
			    uint32_t *wire_length, Clock::Tfrac *time) = 0;
    string filename;
};

class ERFReader : public NettraceReader {
public:
    ERFReader(const string &filename)
	: NettraceReader(filename), myprefetcher(NULL), fd(-1), buffer(NULL), 
	  buffer_cur(NULL), buffer_end(NULL), bufsize(0), eof(false)
    {
    }

    virtual ~ERFReader() { 
	delete myprefetcher;
    }

    virtual void prefetch() {
	if (myprefetcher != NULL) {
	    return; // already started...
	}
	myprefetcher = Prefetcher::doit(filename);
    }

    void openUncompress() {
	if (myprefetcher == NULL) {
	    prefetch();
	}
	myprefetcher->join();
	
	if (suffixequal(filename,".128MiB.lzf")) {
	    buffer = new unsigned char[128*1024*1024];
	    bufsize = lzf_decompress(myprefetcher->data, myprefetcher->datasize, buffer, 128*1024*1024);
	    INVARIANT(bufsize > 0 && bufsize <= 128*1024*1024, "bad");
	} else if (suffixequal(filename,".128MiB.zlib1") 
		   || suffixequal(filename, ".128MiB.zlib6") 
		   || suffixequal(filename, ".128MiB.zlib9")) {
	    buffer = new unsigned char[128*1024*1024];
	    uLongf destlen = 128*1024*1024;
	    int ret = uncompress(static_cast<Bytef *>(buffer),
				 &destlen,static_cast<const Bytef *>(myprefetcher->data),
				 myprefetcher->datasize);
	    INVARIANT(ret == Z_OK && destlen > 0 && destlen <= 128*1024*1024, "bad");
	    bufsize = destlen;
	} else if (suffixequal(filename, ".128MiB.bz2") ||
		   suffixequal(filename, ".128MiB.bz2-new")) { // latter occurs during conversion
	    buffer = new unsigned char[128*1024*1024];
	    unsigned int destlen = 128*1024*1024;
	    int ret = BZ2_bzBuffToBuffDecompress(reinterpret_cast<char *>(buffer),
						 &destlen,
						 reinterpret_cast<char *>(myprefetcher->data),
						 myprefetcher->datasize, 0, 0);
	    INVARIANT(ret == BZ_OK && destlen > 0 && destlen <= 128*1024*1024, "bad");
	    bufsize = destlen;
	} else {
	    FATAL_ERROR(format("Don't know how to unpack %s") % filename);
	}
	myprefetcher->finish();
    }

    virtual bool nextPacket(unsigned char **packet_ptr, uint32_t *capture_size,
			    uint32_t *wire_length, Clock::Tfrac *time) 
    {
	if (eof) 
	    return false;
	if (NULL == buffer) {
	    cur_file_packet_num = 0;
	    openUncompress();
	    buffer_cur = buffer;
	    buffer_end = buffer + bufsize;
	    cout << format("%s size is %d") % filename % bufsize << endl;
	}
	
    retry:
	if (buffer_cur == buffer_end) {
	    eof = true;
	    delete buffer;
	    buffer = buffer_cur = buffer_end = NULL;
	    return false;
	}

	++cur_file_packet_num;
	INVARIANT(buffer_cur + 16 < buffer_end, "bad");
	
	unsigned record_size = ntohs(*reinterpret_cast<uint16_t *>(buffer_cur + 10));

	if ((buffer_cur[9] & 0x10) == 0x10) { // link layer error, skip this packet.
	    ++counts[link_layer_error];
	    buffer_cur += record_size;
	    goto retry;
	}
	
	*time = *reinterpret_cast<uint64_t *>(buffer_cur); // This line is so little-endian specific
	INVARIANT(buffer_cur[8] == 2, "unimplemented"); // type
	INVARIANT(buffer_cur[9] == 4, // varying record lengths
		  format("unimplemented flags 0x%x") % static_cast<int>(buffer_cur[9])); // flags
	if (buffer_cur[12] != 0 || buffer_cur[13] != 0) {
	    counts[packet_loss] += ntohs(*reinterpret_cast<uint16_t *>(buffer_cur + 12));
	    cerr << format("packet loss, %d packets in %s") 
		% ntohs(*reinterpret_cast<uint16_t *>(buffer_cur + 12))
		% tracename
		 << endl;
	}

	*capture_size = record_size - 18;
	// subtracting 4 strips off the CRC that the endace card
	// appears to be capturing.  failing to do this causes some of
	// the checks that we have reached the end of the packet to
	// incorrectly fail.
	*wire_length = ntohs(*reinterpret_cast<uint16_t *>(buffer_cur + 14)) - 4;
	if (*capture_size > *wire_length) { 
	    // can happen because card rounds up in size
	    *capture_size = *wire_length;
	}

	*packet_ptr = buffer_cur + 18;
	buffer_cur += record_size;
	INVARIANT(buffer_cur <= buffer_end, "bad");

	return true;
    }

    Prefetcher *myprefetcher;
    int fd;
    unsigned char *buffer, *buffer_cur, *buffer_end;
    uint32_t bufsize;
    bool eof;
};

// struct pcap_pkthdr uses struct timeval, which can be 16 bytes in size
// as the sub parts can be longs.

struct correct_pcap_pkthdr {
    uint32_t tv_sec, tv_usec, caplen, len;
};

class PCAPReader: public NettraceReader {
public:
    static const bool debug = false;

    PCAPReader(const string &filename) : NettraceReader(filename), 
                                         fp(NULL), 
                                         packet_buf(NULL), 
                                         eof(false), popened(false) 
    { }
    virtual void prefetch() { } // unimplemented yet

    ssize_t readBytes(void *into, size_t bytes) {
	ssize_t ret = fread(into, 1, bytes, fp);

	if (ferror(fp)) {
	    ret = -1;
	}
	if (ret == 0) {
	    INVARIANT(feof(fp), "nothing read but not eof??");
	}
	if (ret < 0 || static_cast<size_t>(ret) != bytes) {
	    if (popened) {
		pclose(fp);
		fp = NULL;
	    } else {
		fclose(fp);
		fp = NULL;
	    }
	}
	if (debug) {
	    if (ret >= 0) {
		cout << format("read(%d) -> %d: %s\n")
		    % bytes % ret % hexstring(string((char *)into, ret));
	    } else {
		cout << format("read(%d) -> error")
		    % bytes;
	    }		

	}
	return ret;
    }

    virtual bool nextPacket(unsigned char **packet_ptr, 
                            uint32_t *capture_size,
			    uint32_t *wire_length, 
                            Clock::Tfrac *time) {
        if (eof) { 
	    return false; 
	}
	// PCAP file either unopened or being read

	if (packet_buf == NULL) { // open the PCAP file
	    if (suffixequal(filename, ".bz2")) {
		popened = true;
		string cmd = (format("bunzip2 -c < %s") % filename).str();
		cout << format("read via cmd %s\n") % cmd;
		fp = popen(cmd.c_str(), "r");
	    } else {
		cout << format("read file %s\n") % filename;
		fp = fopen(filename.c_str(), "r");
	    }
	    INVARIANT(fp != NULL, format("cannot open PCAP file %s: %s")
		      % filename % strerror(errno));
	    cur_file_packet_num = 0;
	    // read in the PCAP file header first

	    ssize_t ret = readBytes(&file_header, sizeof(pcap_file_header));
	    INVARIANT(ret >= 0, format("error when reading PCAP file header from %s: %s")
		      % filename % strerror(errno));	
	    if (ret == 0) { // this file has no PCAP file header (empty file)
		eof = true;
		return false;
	    } else { // this file has a PCAP file header
		INVARIANT(ret == sizeof(pcap_file_header), 
			  format("short read when reading PCAP file header in %s; only got %d bytes not %d")
			  % filename % ret % sizeof(pcap_file_header));
		INVARIANT(file_header.magic == 0xa1b2c3d4, "??");
		INVARIANT(file_header.version_major == 2 && 
			  file_header.version_minor == 4, "??");
		if (debug) {
		    cout << format("zone %d sigfigs %u snaplen %d linktype %d\n")
			% file_header.thiszone % file_header.sigfigs
			% file_header.snaplen % file_header.linktype;
		}
		packet_buf = new unsigned char[file_header.snaplen];
	    }
	}   
	// read the next packet header
	correct_pcap_pkthdr ph; 
	ssize_t ret = readBytes(&ph, sizeof(correct_pcap_pkthdr));
	INVARIANT(ret >= 0, format("error reading packet header (%s, errno=%d)")
		  % filename % strerror(errno));
	if (ret == 0) { // no more packets
	    eof = true;
	    delete [] packet_buf;
	    packet_buf = NULL;
	    return false;
	} else { // read the next packet
	    INVARIANT(ph.caplen <= file_header.snaplen, 
		      format("captured more than specified snapshot length %d > %d")
		      % ph.caplen % file_header.snaplen);
	    ret = readBytes(packet_buf, ph.caplen);
	    INVARIANT(ret >= 0 && static_cast<uint32_t>(ret) == ph.caplen, 
		      format("error reading packet from %s, got %d/%d bytes: %s")
		      % filename % ret % ph.caplen % strerror(errno));
	    // book keeping and return values
	    ++cur_file_packet_num;
	    
	    if (false) {
		cout << "packet: " << cur_file_packet_num << endl;
		cout << "capture length: " << ph.caplen << endl;
		cout << "length: " << ph.len << endl;
		time_t tmp = ph.tv_sec;
		cout << "time: " << ctime(&tmp) << endl; 
	    }
	    
	    *packet_ptr = packet_buf;
	    *capture_size = ph.caplen;
	    *wire_length = ph.len;
	    *time = Clock::secMicroToTfrac(ph.tv_sec, ph.tv_usec);
	    return true;
	}
    }

private:
    pcap_file_header file_header;
    FILE *fp;
    unsigned char *packet_buf; 
    bool eof, popened;
};

class MultiFileReader : public NettraceReader {
public:
    MultiFileReader() : NettraceReader(""), started(false) { }
    virtual ~MultiFileReader() {
	for(vector<NettraceReader *>::iterator i = readers.begin();
	    i != readers.end(); ++i) {
	    delete *i;
	}
    }

    virtual void prefetch() {
	FATAL_ERROR("no");
    }

    // TODO: convert this to prefetching ahead by amount of memory of
    // something like that.
    static const unsigned prefetch_ahead_amount = 3;

    virtual bool nextPacket(unsigned char **packet_ptr, uint32_t *capture_size,
			    uint32_t *wire_length, Clock::Tfrac *time) {
	if (!started) {
	    started = true;
	    cur_reader = readers.begin();
	    INVARIANT(!readers.empty(), "bad");
	    tracename = (**cur_reader).filename.c_str();
	    for(unsigned i = 1;i<=prefetch_ahead_amount && i < readers.size(); ++i) {
		printf("prefetching %d\n", i);
		readers[i]->prefetch();
	    }
	}

	while(cur_reader != readers.end()) {
	    bool ret = (**cur_reader).nextPacket(packet_ptr, capture_size, wire_length, time);
	    if (ret == true) {
		return true;
	    }
	    ++cur_reader;
	    if (cur_reader != readers.end()) {
		tracename = (**cur_reader).filename.c_str();
		if (cur_reader + prefetch_ahead_amount < readers.end()) {
		    (**(cur_reader + prefetch_ahead_amount)).prefetch();
		}
	    }
	}
	return false;
    }

    void addReader(NettraceReader *reader) {
	INVARIANT(!started, "bad");
	readers.push_back(reader);
	if (readers.size() == 1) {
	    reader->prefetch();
	}
    }
private:
    vector<NettraceReader *> readers;
    vector<NettraceReader *>::iterator cur_reader;
    bool started;
};
    
struct network_error_listT {
  ExtentType::int64 h_rid, v_rid;
  unsigned int h_hash, v_hash;
  unsigned int server, client;
};

// For weird reasons, traces will get re-transmissions with the same
// source, dest, xid but with different content.

vector<network_error_listT> known_bad_list; 

#define CONSTANTHOSTNETSWAP(v) ((((uint32_t)(v) >> 24) & 0xFF) | \
                               (((uint32_t)(v)>>8) & 0xFF00) | \
        		       (((uint32_t)(v) & 0xFF00) << 8) | \
        		       (((uint32_t)(v) & 0xFF) << 24))

#define RPCParseAssert(condition) \
  ( (condition) ? (void)0 : throw RPC::parse_exception(#condition, "", __FILE__, __LINE__) )
#define RPCParseAssertMsg(condition,message) \
  ( (condition) ? (void)0 : throw RPC::parse_exception(#condition, AssertExceptionT::stringPrintF message, __FILE__, __LINE__) )
#define RPCParseAssertBoost(condition,message) \
  ( (condition) ? (void)0 : throw RPC::parse_exception(#condition, (message).str(), __FILE__, __LINE__) )

class RPC {
public:
    class parse_exception : public AssertExceptionT {
    public:
	parse_exception(const std::string &_condition, 
			const std::string &_message, const char *filename, const int lineno)
	    : AssertExceptionT(_condition,_message, filename,lineno) {
	}
	virtual ~parse_exception() throw() { }; 
    };

    static const uint32_t net_reply = CONSTANTHOSTNETSWAP(1);

    static inline uint_fast32_t roundup4(uint_fast32_t v) {
	return v + ((4 - (v % 4))%4);
    }

    static void selfCheck() {
	AssertAlways(CONSTANTHOSTNETSWAP(0x12345678) == htonl(0x12345678) &&
		     CONSTANTHOSTNETSWAP(0x9ABCDEF0) == ntohl(0x9ABCDEF0),
		     ("bad\n"));
    }

    // make sure bytes points to a buffer of at least 24 bytes
    // before calling is_request or net_procnum; then is safe, and
    // errors will get caught when the constructor runs.
    static bool is_request(char *bytes) {
	return ((int *)bytes)[1] == 0;
    }
    
    static uint32_t net_procnum(char *bytes) { // network order
	return ((uint32_t *)bytes)[5];
    }
    static uint32_t host_procnum(char *bytes) { // host order
	return ntohl(((uint32_t *)bytes)[5]);
    }

    // bytes are not copied, nor freed your problem to manage consistently
    RPC(const void *bytes, unsigned _len)
	: rpchdr(static_cast<const uint32_t *>(bytes)), len(_len)
    {
	RPCParseAssert(_len >= 8);
	RPCParseAssert(rpchdr[1] == 0 || rpchdr[1] == net_reply);
    }
    virtual ~RPC() { }
    uint32_t xid() { return rpchdr[0]; }
    bool is_request() { return rpchdr[1] == 0; }
    void *duppacket() { 
	u_char *ret = new u_char[len];
	memcpy(ret,rpchdr,len);
	return ret;
    }
    unsigned getlen() { 
	return len;
    }
    const uint32_t *getxdr() {
	return rpchdr;
    }
protected:
    const uint32_t *rpchdr;
    unsigned len;

    virtual void enable_dynamic_cast() { };
};

class ShortDataInRPCException : public RPC::parse_exception {
public:
    ShortDataInRPCException(const std::string &_condition, const std::string &_rpc_type,
			    const std::string &_message, const char *filename, const int lineno)
	: RPC::parse_exception(_condition, _message, filename, lineno),
	       rpc_type(_rpc_type) { // rpc_type example "NFSv2 Write Reply"
    }
    virtual ~ShortDataInRPCException() throw() { }; 
    string rpc_type;
};
  
#define ShortDataAssertMsg(condition,rpc_type,message) \
  ( (condition) ? (void)0 : throw ShortDataInRPCException(#condition, (rpc_type), AssertExceptionT::stringPrintF message, __FILE__, __LINE__) )


class RPCRequest : public RPC {
public:
    static const uint32_t host_prog_portmap = 100000;
    static const uint32_t host_prog_rstat = 100001;
    static const uint32_t host_prog_rusers = 100002;
    static const uint32_t host_prog_nfs = 100003;
    static const uint32_t net_prog_nfs = CONSTANTHOSTNETSWAP(host_prog_nfs);
    static const uint32_t host_prog_yp = 100004;
    static const uint32_t net_prog_yp = CONSTANTHOSTNETSWAP(host_prog_yp);
    static const uint32_t host_prog_mount = 100005;
    static const uint32_t net_prog_mount = CONSTANTHOSTNETSWAP(host_prog_mount);
    static const uint32_t host_rpc_version = 2;
    static const uint32_t net_rpc_version = CONSTANTHOSTNETSWAP(host_rpc_version);
    static const uint32_t host_auth_sys = 1;
    static const uint32_t net_auth_sys = CONSTANTHOSTNETSWAP(host_auth_sys);
    RPCRequest(const void *bytes, int _len) : RPC(bytes,_len), orig_xid(xid()) {
	RPCParseAssert(len >= 10*4); // may mis-interpret tcp length, so may not be at end of segment
	RPCParseAssert(rpchdr[1] == 0);
	RPCParseAssert(rpchdr[2] == net_rpc_version);
	if (rpchdr[6] == 0) {
	    // auth_none
	    RPCParseAssert(rpchdr[7] == 0 && rpchdr[8] == 0 && rpchdr[9] == 0);
	    rpcparam = rpchdr + 10;
	    auth_sys_uid = NULL;
	    auth_sys_len = 0;
	} else if (rpchdr[6] == net_auth_sys) {
	    const uint32_t *cur_pos = rpchdr + 7;
	    unsigned auth_credlen = ntohl(*cur_pos);
	    ++cur_pos; 
	    RPCParseAssert(auth_credlen >= 5*4 && len >= 10*4 + auth_credlen);
	    ++cur_pos; // stamp
	    unsigned hostname_len = ntohl(*cur_pos);
	    RPCParseAssert(hostname_len < 1024);
	    ++cur_pos;
	    hostname_len = roundup4(hostname_len);
	    ShortDataAssertMsg(auth_credlen >= 5*4 + hostname_len,"unknown",
			       ("auth credlen %d < 5*4 + %d\n",
				auth_credlen, hostname_len));
	    cur_pos += hostname_len / 4;
	    auth_sys_uid = cur_pos;
	    cur_pos += 2; // uid, gid
	    int gid_count = ntohl(*cur_pos);
	    ++cur_pos;
	    RPCParseAssert(gid_count >= 0 && gid_count <= 16);
	    RPCParseAssert(auth_credlen == 5*4 + hostname_len + 4*gid_count);
	    cur_pos += gid_count;
	    auth_sys_len = 4*(2 + 1 + gid_count);
	    RPCParseAssert(*cur_pos == 0); // verifier type
	    ++cur_pos;
	    RPCParseAssert(*cur_pos == 0); // verifier len
	    ++cur_pos;
	    rpcparam = cur_pos;
	} else {
	    RPCParseAssert("unsupported authentication" == "");
	}
	rpcparamlen = len - ((char *)rpcparam - (char *)rpchdr);
	RPCParseAssert(rpcparamlen >= 0);
	if (false) {
	    unsigned char *f = (unsigned char *)rpcparam;
	    cout << format("bytes starting @%d: ") % (f-(unsigned char *)rpchdr);
	    for(unsigned i=0;i<rpcparamlen;++i) {
		printf("%02x ",f[i]);
	    }
	    printf("\n");
	}
    }
	
    virtual ~RPCRequest() { }
    uint32_t net_prognum() { return rpchdr[3]; }
    uint32_t host_prognum() { return ntohl(rpchdr[3]); }
    uint32_t net_version() { return rpchdr[4]; }
    uint32_t host_version() { return ntohl(rpchdr[4]); }
    uint32_t net_procnum() { return rpchdr[5]; }
    uint32_t host_procnum() { return ntohl(net_procnum()); }
    bool isauth_none() { return auth_sys_uid == NULL; }
    const uint32_t *getrpcparam() { return rpcparam; }
    unsigned getrpcparamlen() { return rpcparamlen; }
protected:
    const uint32_t *auth_sys_uid, *rpcparam;
    const uint32_t orig_xid;
    unsigned auth_sys_len, rpcparamlen; // both count in bytes!
};

class RPCReply : public RPC {
public:
    static const uint32_t net_msg_denied = CONSTANTHOSTNETSWAP(1);
    static const uint32_t net_auth_error = CONSTANTHOSTNETSWAP(1);
    
    RPCReply(const void *bytes, int _len) : RPC(bytes, _len) {
	RPCParseAssert(len >= 5*4);
	RPCParseAssert(rpchdr[1] == RPC::net_reply);
	if (rpchdr[2] == net_msg_denied) {
	    rpc_results = NULL;
	    // could do further checks; don't care now
	    return;
	}
	RPCParseAssert(rpchdr[2] == 0);
	RPCParseAssert(rpchdr[3] == 0 && rpchdr[4] == 0); // verifier type/len
	rpc_results = rpchdr + 6;
	results_len = len - 6*4;
	//    printf("rpcreply len = %d, results_len = %d\n",len,results_len);
	RPCParseAssert(results_len >= 0);
    }

    virtual ~RPCReply() { }

    bool isaccepted() {
	return rpc_results != NULL && rpc_results[-1] == 0;
    }
    uint32_t status() { // not the status of your operation!, that's usually the first part of results
	if (rpc_results == NULL) {
	    return (1<<30) | rpchdr[2];
	} else {
	    return rpc_results[-1];
	}
    }
    const uint32_t *getrpcresults() { return rpc_results; }
    // TODO: make this a uint32_t, fix everywhere else to eliminate signed/unsigned warnings
    int getrpcresultslen() { return results_len; }
protected:
    const uint32_t *rpc_results;
    int results_len; // in bytes
};

class MountProc {
public:
    static const uint32_t proc_null =      0;
    static const uint32_t proc_mnt =       1;
    static const uint32_t proc_dump =      2;
    static const uint32_t proc_umnt =      3;
    static const uint32_t proc_umntall =   4;
    static const uint32_t proc_export =    5;
    static const uint32_t proc_exportall = 6;
    static const uint32_t proc_pathconf =  7;
};

class MountRequest_MNT : public RPCRequest {
public:
    MountRequest_MNT(RPCRequest &req) : RPCRequest(req)
    {
	parseCheck();
    }

    MountRequest_MNT(void *bytes, int _len) : RPCRequest(bytes,_len)
    {
	parseCheck();
    }

    virtual ~MountRequest_MNT() { }

    char *rawpathname() { // not null terminated
	return (char *)(rpcparam + 1);
    }
    int rawpathnamelen() {
	return ntohl(rpcparam[0]);
    }
    std::string pathname() {
	return std::string(rawpathname(),rawpathnamelen());
    }
private:
    void parseCheck() {
	RPCParseAssert(rpcparamlen >= 8);
	RPCParseAssert(host_version() >= 1 && host_version() <= 3);
	RPCParseAssert(rpcparamlen == 4 + RPC::roundup4(rawpathnamelen()));
    }
};

class MountReply_MNT : public RPCReply {
public:
    MountReply_MNT(int _version, RPCReply &_reply) : RPCReply(_reply), version(_version) {
	RPCParseAssert(version >= 1 && version <= 3);
	RPCParseAssert(results_len >= 4);
	if (mountok()) {
	    if (version == 3) {
		RPCParseAssert(results_len >= 12 &&
			       results_len >= (int)(12+RPC::roundup4(rawfilehandlelen())));
	    } else {
		RPCParseAssert(results_len == 36);
	    }
	}
    }

    virtual ~MountReply_MNT() { }

    bool mountok() { 
	return rpc_results[0] == 0;
    }
    uint32_t mounterror() {
	return ntohl(rpc_results[0]);
    }
    char *rawfilehandle() { // not null terminated
	if (version == 3) {
	    return (char *)(rpc_results + 2);
	} else {
	    return (char *)(rpc_results + 1);
	}
    }
    int rawfilehandlelen() {
	if (version == 3) {
	    return ntohl(rpc_results[1]);
	} else {
	    return 32;
	}
    }
    std::string filehandle() {
	return std::string(rawfilehandle(),rawfilehandlelen());
    }
private:
    int version;
};

const string nfs_convert_stats_xml(
  "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Summary::NFS::convert-stats\" version=\"2.0\" >\n"
  "  <field type=\"variable32\" name=\"stat_name\" pack_unique=\"yes\" />\n"
  "  <field type=\"int64\" name=\"count\" />\n"
  "</ExtentType>\n"
  );

ExtentSeries nfs_convert_stats_series;
OutputModule *nfs_convert_stats_outmodule;
Variable32Field nfs_convert_stats_name(nfs_convert_stats_series, "stat_name");
Int64Field nfs_convert_stats_count(nfs_convert_stats_series, "count");

const string ip_bwrolling_xml(
  "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Summary::Network::IP::bandwidth-rolling\" version=\"2.0\" >\n"
  "  <field type=\"int32\" name=\"interval_us\" />\n"
  "  <field type=\"int32\" name=\"sample_us\" />\n"
  "  <field type=\"int64\" name=\"count\" />\n"
  "  <field type=\"double\" name=\"quantile\" />\n"
  "  <field type=\"double\" name=\"mbps\" />\n"
  "</ExtentType>\n"
  );

ExtentSeries ip_bwrolling_series;
OutputModule *ip_bwrolling_outmodule;
Int32Field ip_bwrolling_interval_us(ip_bwrolling_series, "interval_us");
Int32Field ip_bwrolling_sample_us(ip_bwrolling_series, "sample_us");
Int64Field ip_bwrolling_count(ip_bwrolling_series, "count");
DoubleField ip_bwrolling_quantile(ip_bwrolling_series, "quantile");
DoubleField ip_bwrolling_mbps(ip_bwrolling_series, "mbps");

const string nfs_common_xml(
  "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Trace::NFS::common\" version=\"2.1\"\n"
  "  changes2_1=\"op_id is not null; it was always set in the converter\" >\n"
  "  <field type=\"int64\" name=\"packet_at\" comment=\"time in units of 2^-32 seconds since UNIX epoch, printed in close to microseconds\" pack_relative=\"packet_at\" print_divisor=\"4295\" />\n"
  "  <field type=\"int32\" name=\"source\" comment=\"32 bit packed IPV4 address\" print_format=\"%08x\" />\n"
  "  <field type=\"int32\" name=\"source_port\" />\n"
  "  <field type=\"int32\" name=\"dest\" comment=\"32 bit packed IPV4 address\" print_format=\"%08x\" />\n"
  "  <field type=\"int32\" name=\"dest_port\" />\n"
  "  <field type=\"bool\" name=\"is_udp\" print_true=\"UDP\" print_false=\"TCP\" />\n"
  "  <field type=\"bool\" name=\"is_request\" print_true=\"request\" print_false=\"response\" />\n"
  "  <field type=\"byte\" name=\"nfs_version\" print_format=\"V%d\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"transaction_id\" print_format=\"%08x\" />\n"
  "  <field type=\"byte\" name=\"op_id\" note=\"op_id is nfs-version dependent\" />\n"
  "  <field type=\"variable32\" name=\"operation\" pack_unique=\"yes\" />\n"
  "  <field type=\"int32\" name=\"rpc_status\" opt_nullable=\"yes\" comment=\"null on requests\" />\n"
  "  <field type=\"int32\" name=\"payload_length\" />\n"
  "  <field type=\"int64\" name=\"record_id\" comment=\"for correlating with the records in other extent types\" pack_relative=\"record_id\" />\n"
  "</ExtentType>\n"
);

ExtentSeries nfs_common_series;
OutputModule *nfs_common_outmodule;
Int64Field packet_at(nfs_common_series,"packet_at");
Int32Field source(nfs_common_series,"source");
Int32Field source_port(nfs_common_series,"source_port");
Int32Field dest(nfs_common_series,"dest");
Int32Field dest_port(nfs_common_series,"dest_port");
BoolField is_udp(nfs_common_series,"is_udp");
BoolField is_request(nfs_common_series,"is_request");
ByteField nfs_version(nfs_common_series,"nfs_version",Field::flag_nullable);
Int32Field xid(nfs_common_series,"transaction_id");
// Int32Field euid(nfs_common_series,"euid",Field::flag_nullable);
// Int32Field egid(nfs_common_series,"egid",Field::flag_nullable);
ByteField opid(nfs_common_series,"op_id",Field::flag_nullable);
Variable32Field operation(nfs_common_series,"operation");
Int32Field rpc_status(nfs_common_series,"rpc_status",Field::flag_nullable);
Int32Field payload_length(nfs_common_series,"payload_length");
Int64Field common_record_id(nfs_common_series,"record_id");

const string nfsv2ops[] = {
    "null",
    "getattr",
    "setattr",
    "root",
    "lookup",
    "readlink",
    "read",
    "writecache",
    "write",
    "create",
    "remove",
    "rename",
    "link",
    "symlink",
    "mkdir",
    "rmdir",
    "readdir",
    "statfs"
};

int n_nfsv2ops = sizeof(nfsv2ops) / sizeof(const string);

const string nfsv3ops[] = {
    "null",
    "getattr",
    "setattr",
    "lookup",
    "access",
    "readlink",
    "read",
    "write",
    "create",
    "mkdir",
    "symlink",
    "mknod",
    "remove",
    "rmdir",
    "rename",
    "link",
    "readdir",
    "readdirplus",
    "fsstat",
    "fsinfo",
    "pathconf",
    "commit"
};

int n_nfsv3ops = sizeof(nfsv3ops) / sizeof(const string);

const string nfs_attrops_xml(
  "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Trace::NFS::attr-ops\" version=\"2.1\" >\n"
  "  <field type=\"int64\" name=\"request_id\" comment=\"for correlating with the records in other extent types\" pack_relative=\"request_id\" />\n"
  "  <field type=\"int64\" name=\"reply_id\" comment=\"for correlating with the records in other extent types\" pack_relative=\"request_id\" />\n"
  "  <field type=\"variable32\" name=\"filename\" opt_nullable=\"yes\" pack_unique=\"yes\" print_style=\"maybehex\" />\n"
  "  <field type=\"variable32\" name=\"filehandle\" print_style=\"hex\" pack_unique=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"lookup_dir_filehandle\" print_style=\"hex\" pack_unique=\"yes\" opt_nullable=\"yes\" />\n"
  "  <field type=\"byte\" name=\"typeid\" />\n"
  "  <field type=\"variable32\" name=\"type\" pack_unique=\"yes\" />\n"
  "  <field type=\"int32\" name=\"mode\" comment=\"only includes bits from 0xFFF down, so type is not reproduced here\" print_format=\"%x\" />\n"
  "  <field type=\"int32\" name=\"hard_link_count\" comment=\"number of hard links to this file system object\"/>\n"
  "  <field type=\"int32\" name=\"uid\" />\n"
  "  <field type=\"int32\" name=\"gid\" />\n"
  "  <field type=\"int64\" name=\"file_size\" />\n"
  "  <field type=\"int64\" name=\"used_bytes\" />\n"
  "  <field type=\"int64\" name=\"access_time\" pack_relative=\"access_time\" comment=\"time in ns since Unix epoch; doubles don't have enough precision to represent a year in ns and NFSv3 gives us ns precision access times\" />\n"
  "  <field type=\"int64\" name=\"modify_time\" pack_relative=\"modify_time\" comment=\"time in ns since Unix epoch; doubles don't have enough precision to represent a year in ns and NFSv3 gives us ns precision modify times\" />\n"
  "  <field type=\"int64\" name=\"inode_change_time\" pack_relative=\"inode_change_time\" comment=\"time in ns since Unix epoch; doubles don't have enough precision to represent a year in ns and NFSv3 gives us ns precision inode change times\" />\n"
  "</ExtentType>\n");

ExtentSeries nfs_attrops_series;
OutputModule *nfs_attrops_outmodule;
Int64Field attrops_request_id(nfs_attrops_series,"request_id");
Int64Field attrops_reply_id(nfs_attrops_series,"reply_id");
Variable32Field attrops_filename(nfs_attrops_series,"filename", Field::flag_nullable);
Variable32Field attrops_filehandle(nfs_attrops_series,"filehandle");
Variable32Field attrops_lookupdirfilehandle(nfs_attrops_series,"lookup_dir_filehandle", Field::flag_nullable);
ByteField attrops_typeid(nfs_attrops_series,"typeid");
Variable32Field attrops_type(nfs_attrops_series,"type");
Int32Field attrops_mode(nfs_attrops_series,"mode");
Int32Field attrops_nlink(nfs_attrops_series,"hard_link_count");
Int32Field attrops_uid(nfs_attrops_series,"uid");
Int32Field attrops_gid(nfs_attrops_series,"gid");
Int64Field attrops_filesize(nfs_attrops_series,"file_size");
Int64Field attrops_used_bytes(nfs_attrops_series,"used_bytes");
Int64Field attrops_access_time(nfs_attrops_series,"access_time");
Int64Field attrops_modify_time(nfs_attrops_series,"modify_time");
Int64Field attrops_inochange_time(nfs_attrops_series,"inode_change_time");

const string nfs_readwrite_xml(
  "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Trace::NFS::read-write\" version=\"2.0\" >\n"
  "  <field type=\"int64\" name=\"request_id\" comment=\"for correlating with the records in other extent types\" pack_relative=\"request_id\" />\n"
  "  <field type=\"int64\" name=\"reply_id\" comment=\"for correlating with the records in other extent types\" pack_relative=\"request_id\" />\n"
  "  <field type=\"variable32\" name=\"filehandle\" print_style=\"hex\" pack_unique=\"yes\" />\n"
  "  <field type=\"bool\" name=\"is_read\" />\n"
  "  <field type=\"int64\" name=\"offset\" />\n"
  "  <field type=\"int32\" name=\"bytes\" />\n"
  "</ExtentType>\n");

ExtentSeries nfs_readwrite_series;
OutputModule *nfs_readwrite_outmodule;
Int64Field readwrite_request_id(nfs_readwrite_series,"request_id");
Int64Field readwrite_reply_id(nfs_readwrite_series,"reply_id");
Variable32Field readwrite_filehandle(nfs_readwrite_series,"filehandle");
BoolField readwrite_is_read(nfs_readwrite_series,"is_read");
Int64Field readwrite_offset(nfs_readwrite_series,"offset");
Int32Field readwrite_bytes(nfs_readwrite_series,"bytes");

const string ippacket_xml(
  "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Trace::Network::IP\" version=\"2.0\" >\n"
  "  <field type=\"int64\" name=\"packet_at\" pack_relative=\"packet_at\" comment=\"time in units of 2^-32 seconds since UNIX epoch, printed in close to microseconds\" print_divisor=\"4295\" />\n"
  "  <field type=\"int32\" name=\"source\" print_format=\"%08x\" />\n"
  "  <field type=\"int32\" name=\"destination\" print_format=\"%08x\" />\n"
  "  <field type=\"int32\" name=\"wire_length\" />\n"
  "  <field type=\"bool\" name=\"udp_tcp\" opt_nullable=\"yes\" comment=\"true on udp, false on tcp, null on neither\" />\n"
  "  <field type=\"int32\" name=\"source_port\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"destination_port\" opt_nullable=\"yes\" />\n"
  "  <field type=\"bool\" name=\"is_fragment\" />\n"
  "  <field type=\"int32\" name=\"tcp_seqnum\" opt_nullable=\"yes\" />\n"
  "</ExtentType>\n");

ExtentSeries ippacket_series;
OutputModule *ippacket_outmodule;
Int64Field ippacket_packet_at(ippacket_series,"packet_at");
Int32Field ippacket_source(ippacket_series,"source");
Int32Field ippacket_destination(ippacket_series,"destination");
Int32Field ippacket_wire_length(ippacket_series,"wire_length");
BoolField ippacket_udp_tcp(ippacket_series,"udp_tcp",Field::flag_nullable);
Int32Field ippacket_source_port(ippacket_series,"source_port",Field::flag_nullable);
Int32Field ippacket_destination_port(ippacket_series,"destination_port",Field::flag_nullable);
BoolField ippacket_is_fragment(ippacket_series,"is_fragment");
Int32Field ippacket_tcp_seqnum(ippacket_series,"tcp_seqnum",Field::flag_nullable);

const string nfs_mount_xml(
  "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Trace::NFS::mount\" version=\"2.0\">\n"
  "  <field type=\"int64\" name=\"request_at\" pack_relative=\"request_at\" comment=\"time in units of 2^-32 seconds since UNIX epoch, printed in close to microseconds\" print_divisor=\"4295\" />\n"
  "  <field type=\"int64\" name=\"reply_at\" pack_relative=\"request_at\" comment=\"time in units of 2^-32 seconds since UNIX epoch, printed in close to microseconds\" print_divisor=\"4295\" />\n"
  "  <field type=\"int32\" name=\"server\" print_format=\"%08x\" />\n"
  "  <field type=\"int32\" name=\"client\" print_format=\"%08x\" />\n"
  "  <field type=\"variable32\" name=\"pathname\" pack_unique=\"yes\" print_style=\"maybehex\" />\n"
  "  <field type=\"variable32\" name=\"filehandle\" print_style=\"hex\" pack_unique=\"yes\" />\n"
  "</ExtentType>\n");

ExtentSeries nfs_mount_series;
OutputModule *nfs_mount_outmodule;
Int64Field nfs_mount_request_at(nfs_mount_series,"request_at");
Int64Field nfs_mount_reply_at(nfs_mount_series,"reply_at");
Int32Field nfs_mount_server(nfs_mount_series,"server");
Int32Field nfs_mount_client(nfs_mount_series,"client");
Variable32Field nfs_mount_pathname(nfs_mount_series,"pathname");
Variable32Field nfs_mount_filehandle(nfs_mount_series,"filehandle");

const string NFSV2_typelist[] = 
{
    "*non-file*", "file", "directory", "block-dev", "char-dev", "symlink", "socket", "unused", "named-pipe"
};

const string NFSV3_typelist[] = 
{
    "*non-file*", "file", "directory", "block-dev", "char-dev", "symlink", "socket", "named-pipe"
};

const string empty_string("");

void
prepareBandwidthInformation()
{
    bw_info.push_back(new bandwidth_rolling(1000,0));
    bw_info.push_back(new bandwidth_rolling(10000,0));
    bw_info.push_back(new bandwidth_rolling(100000,0));
    bw_info.push_back(new bandwidth_rolling(1000000,0));
    bw_info.push_back(new bandwidth_rolling(5000000,0));
    bw_info.push_back(new bandwidth_rolling(15000000,0));
    bw_info.push_back(new bandwidth_rolling(60000000,0));
}

void
doBandwidthProcessPacket()
{
    for(unsigned i = 0;i<bw_info.size();++i) {
	bw_info[i]->update(packet_bw_rolling_info.top().timestamp_us,
			   packet_bw_rolling_info.top().packetsize);
    }
    packet_bw_rolling_info.pop();
}

void
incrementalBandwidthInformation()
{
    INVARIANT(!bw_info.empty(), "didn't call prepareBandwidthInformation()");
    if (0 == bw_info[0]->cur_time && packet_bw_rolling_info.size() > incremental_process_at_packet_count) {
	for(unsigned i = 0;i<bw_info.size();++i) {
	    bw_info[i]->setStartTime(packet_bw_rolling_info.top().timestamp_us);
	}
    }
	
    while(packet_bw_rolling_info.size() > incremental_process_at_packet_count) {
	INVARIANT(max_incremental_processed <= packet_bw_rolling_info.top().timestamp_us, 
		  "too much out of orderness");
	max_incremental_processed = packet_bw_rolling_info.top().timestamp_us;
	doBandwidthProcessPacket();
	packet_bw_rolling_info.pop();
    }
}

void ip_bwrolling_row(bandwidth_rolling *bw_info, double quantile) 
{
    ip_bwrolling_outmodule->newRecord();
    ip_bwrolling_interval_us.set(bw_info->interval_microseconds);
    ip_bwrolling_sample_us.set(bw_info->update_step);
    ip_bwrolling_count.set(bw_info->mbps.countll());
    ip_bwrolling_quantile.set(quantile);
    ip_bwrolling_mbps.set(bw_info->mbps.getQuantile(quantile));
}

void
summarizeBandwidthInformation()
{
    cout << "packet_bw_rolling_info.size() = " << packet_bw_rolling_info.size() << endl;
    INVARIANT(bw_info.size() > 0, "didn't call prepareBandwidthInformation()");
    if (0 == bw_info[0]->cur_time) {
	for(unsigned i = 0;i<bw_info.size();++i) {
	    bw_info[i]->setStartTime(packet_bw_rolling_info.top().timestamp_us);
	}
    }

    while(packet_bw_rolling_info.empty() == false) {
	doBandwidthProcessPacket();
    }

    for(unsigned i = 0;i<bw_info.size();++i) {
	if (bw_info[i]->mbps.count() > 0) {
	    printf("mbits for interval len of %lldus with samples every %lldus\n",
		   bw_info[i]->interval_microseconds, bw_info[i]->update_step);
	    bw_info[i]->mbps.printFile(stdout);
	    bw_info[i]->mbps.printTail(stdout);
	    printf("\n");
	    if (mode == Convert) {
		for(double quant=0.05; quant < 1.0; quant+=0.05) {
		    ip_bwrolling_row(bw_info[i], quant);
		}
		double nentries = bw_info[i]->mbps.countll();
		for(double tail_frac = 0.1; (tail_frac * nentries) >= 10.0; ) {
		    ip_bwrolling_row(bw_info[i], 1-tail_frac);
		    tail_frac /= 2.0;
		    ip_bwrolling_row(bw_info[i], 1-tail_frac);
		    tail_frac /= 5.0;
		}
	    }
	}
    }
}

inline ExtentType::int64 xdr_ll(const uint32_t *xdr,int offset)
{
    return ntohll(*reinterpret_cast<const uint64_t *>(xdr + offset));
}

string 
getLookupFilename(const uint32_t *xdr, int remain_len)
{
    AssertAlways(remain_len >= 4,("bad1 %d\n",remain_len));
    uint32_t strlen = ntohl(xdr[0]);
    // Changed to an assertion to handle set-6/cqracks.19352
    RPCParseAssertMsg(remain_len == (int)(strlen + (4 - (strlen % 4))%4 + 4),
		      ("bad2 %d != roundup4(%d) @ record %lld\n",
		       remain_len,strlen,cur_record_id));
    string ret((char *)(xdr + 1),strlen);
    if (enable_encrypt_filenames) {
	string enc_ret = encryptString(ret);
	string dec_ret = decryptString(enc_ret);
	AssertAlways(dec_ret == ret,("bad"));
	return enc_ret;
    } else {
	return ret;
    }
}

ExtentType::int64
getNFS2Time(uint32_t *xdr_time)
{
    ExtentType::int64 seconds = ntohl(xdr_time[0]);
    ExtentType::int64 useconds = ntohl(xdr_time[1]);
    SINVARIANT(seconds >= 0 && useconds < 1000*1000);
    return seconds * 1000 * 1000 * 1000 + useconds * 1000;
}

ExtentType::int64
getNFS3Time(const uint32_t *xdr_time)
{
    ExtentType::int64 seconds = ntohl(xdr_time[0]);
    ExtentType::int64 nseconds = ntohl(xdr_time[1]);
    SINVARIANT(seconds >= 0 && nseconds < 1000*1000*1000);
    return seconds * 1000 * 1000 * 1000 + nseconds;
}

class RPCReplyHandler;

struct RPCRequestData {
    RPCRequest *reqdata;
    RPCReplyHandler *replyhandler;
    ExtentType::int64 request_id; // Unique DataSeries assigned ID to all NFS ops
    ExtentType::int64 request_at;
    uint32_t client, server, xid;
    uint16_t server_port;
    uint32_t program, version, procnum;
    unsigned int rpcreqhashval; // for sanity checking of duplicate requests
    unsigned int ipchecksum, l4checksum;
    RPCRequestData(uint32_t a, uint32_t b, uint32_t c, uint16_t d) 
      : client(a), server(b), xid(c), server_port(d) {}
};

class RPCRequestDataHash {
public:
    unsigned int operator()(const RPCRequestData &k) {
      unsigned ret,a,b;
      ret = k.xid;
      a = k.server;
      b = k.client;
      BobJenkinsHashMix(a,b,ret);
      a = k.server_port;
      BobJenkinsHashMix(a,b,ret);
      return ret;
    }
};

class RPCRequestDataEqual {
public:
    // valid to reuse same xid between same client and server if program is different
    bool operator()(const RPCRequestData &a, const RPCRequestData &b) {
	return a.client == b.client && a.server == b.server && a.xid == b.xid && a.server_port == b.server_port;
    }
};

typedef HashTable<RPCRequestData, RPCRequestDataHash, RPCRequestDataEqual> rpcHashTableT;
rpcHashTableT rpcHashTable;

class RPCReplyHandler {
public:
    RPCReplyHandler() { }

    virtual ~RPCReplyHandler() {
    }
    virtual void handleReply(RPCRequestData *reqdata, 
			     Clock::Tfrac time, const struct iphdr *ip_hdr,
			     int source_port, int dest_port, int l4checksum, 
			     int payload_len, RPCReply &reply) = 0;
};

class NFSV2AttrOpReplyHandler : public RPCReplyHandler {
public:
    // zero length lookup_directory_filehandle and/or filename sets values to null
    NFSV2AttrOpReplyHandler(const string &_filehandle, 
			    const string &_lookup_directory_filehandle,
			    const string &_filename)
	: filehandle(_filehandle), 
	lookup_directory_filehandle(_lookup_directory_filehandle),
	filename(_filename)
    { }
    virtual ~NFSV2AttrOpReplyHandler() { }
    // return < 0 if no file attributes in reply, otherwise should be attributes for
    // the filehandle set in the request; offset is in 4 byte chunks, not in bytes.
    virtual int getfattroffset(RPCRequestData *reqdata, 
			       RPCReply &reply) = 0;

    static const int fattr3_len = 5*4 + 8*8;
    virtual void handleReply(RPCRequestData *reqdata, 
			     Clock::Tfrac time, const struct iphdr *ip_hdr,
			     int source_port, int dest_port, int l4checksum, int payload_len,
			     RPCReply &reply) 
    {
	FATAL_ERROR("unimplemented");
	AssertAlways(reply.status() == 0,("request not accepted?!\n"));
	int v3fattroffset = getfattroffset(reqdata,reply);
	if (v3fattroffset >= 0) {
	    ShortDataAssertMsg(v3fattroffset * 4 + fattr3_len <= reply.getrpcresultslen(),
			       "NFSv3 attribute op",
			       ("%d * 4 + %d <= %d",v3fattroffset,fattr3_len,reply.getrpcresultslen()));
	    const uint32_t *xdr = reply.getrpcresults();
	    xdr += v3fattroffset;
	    int type = ntohl(xdr[0]);
	    AssertAlways(type >= 1 && type < 8,("bad"));

	    if (mode == Convert) {
		nfs_attrops_outmodule->newRecord();
		attrops_request_id.set(reqdata->request_id);
		attrops_reply_id.set(cur_record_id);
		attrops_filehandle.set(filehandle);
		if (lookup_directory_filehandle.empty()) {
		    attrops_lookupdirfilehandle.setNull();
		} else {
		    attrops_lookupdirfilehandle.set(lookup_directory_filehandle);
		}
		if (filename.empty()) {
		    attrops_filename.setNull();
		} else {
		    attrops_filename.set(filename);
		}
		attrops_typeid.set(type);
		attrops_type.set(NFSV2_typelist[type]);
		attrops_mode.set(ntohl(xdr[1] & 0xFFF));
		attrops_uid.set(ntohl(xdr[3]));
		attrops_gid.set(ntohl(xdr[4]));
		attrops_filesize.set(xdr_ll(xdr,5));
		attrops_used_bytes.set(xdr_ll(xdr,7));
		attrops_modify_time.set(getNFS3Time(xdr+17));
	    }
	}
    }

    string filehandle;
    const string lookup_directory_filehandle, filename;
};

class NFSV2GetAttrReplyHandler : public NFSV2AttrOpReplyHandler {
public:
    NFSV2GetAttrReplyHandler(const string &_filehandle)
	: NFSV2AttrOpReplyHandler(_filehandle,empty_string,empty_string)
    { }
    virtual ~NFSV2GetAttrReplyHandler() { }
    virtual int getfattroffset(RPCRequestData *reqdata,
			       RPCReply &reply) 
    {
	FATAL_ERROR("unimplemented");
//	const uint32_t *xdr = reply.getrpcresults();
//	int actual_len = reply.getrpcresultslen();
//	ShortDataAssertMsg(actual_len >= 4,"NFSv3 getattr reply",
//			   ("bad %d", actual_len));
//	uint32_t op_status = ntohl(*xdr);
//	if (op_status != 0) {
//	    AssertAlways(op_status == 70,("bad %d\n",op_status));
//	    return -1;
//	}
//	return 1;
    }
};

class NFSV2ReadWriteReplyHandler : public RPCReplyHandler {
public:
  NFSV2ReadWriteReplyHandler(ExtentType::int64 _reqid, const string &_filehandle, bool _is_read, 
			   ExtentType::int64 _offset, ExtentType::int32 _reqbytes) 
    : RPCReplyHandler(), reqid(_reqid), filehandle(_filehandle),
      is_read(_is_read), offset(_offset), reqbytes(_reqbytes) 
  { } 
  virtual ~NFSV2ReadWriteReplyHandler() {
  }
  
  virtual void handleReply(RPCRequestData *reqdata, 
			   Clock::Tfrac time, const struct iphdr *ip_hdr,
			   int source_port, int dest_port, int l4checksum, int payload_len,
			   RPCReply &reply)
  {
    const uint32_t *xdr = reply.getrpcresults();
    int actual_len = reply.getrpcresultslen();
    AssertAlways(reply.status() == 0,("request not accepted?!\n"));
    uint32_t op_status = ntohl(*xdr);
    xdr += 1;
    actual_len -= 4;
    if (op_status != 0) {
      if (is_read) {
	  AssertAlways(op_status == 70 || // stale file handle
		       op_status == 13, // permission denied
		       ("bad12 %d", op_status)); 
      } else {
	  AssertAlways(op_status == 13 || // permission denied
		       op_status == 28 || // out of space
		       op_status == 69 || // disk quota exceeded
		       op_status == 70, // stale file handle
		       ("bad12 op_status = %d",op_status)); 
      }
    } else {
      nfs_readwrite_outmodule->newRecord();
      readwrite_request_id.set(reqid);
      readwrite_reply_id.set(cur_record_id);
      readwrite_filehandle.set(filehandle);
      readwrite_is_read.set(is_read);
      readwrite_offset.set(offset);
      readwrite_bytes.set(reqbytes);
      
      if (is_read) {
	AssertAlways(actual_len >= 17*4 + 4,("bad %d",actual_len));
	uint32_t actual_bytes = ntohl(xdr[17]);
	AssertAlways(reqbytes >= (ExtentType::int32)actual_bytes,("wrong %d %d\n",reqbytes,actual_bytes));
	readwrite_bytes.set(actual_bytes);
      } else {
	AssertAlways(actual_len == 17*4,("bad %d",actual_len));
      }
    } 
  }

  ExtentType::int64 reqid;
  const string filehandle;
  bool is_read;
  ExtentType::int64 offset;
  ExtentType::int32 reqbytes;
};

class NFSV3AttrOpReplyHandler : public RPCReplyHandler {
public:
    // zero length lookup_directory_filehandle and/or filename sets values to null
    NFSV3AttrOpReplyHandler(const string &_filehandle, 
			    const string &_lookup_directory_filehandle,
			    const string &_filename)
	: filehandle(_filehandle), 
	lookup_directory_filehandle(_lookup_directory_filehandle),
	filename(_filename)
    { }
    virtual ~NFSV3AttrOpReplyHandler() { }
    // return < 0 if no file attributes in reply, otherwise should be attributes for
    // the filehandle set in the request; offset is in 4 byte chunks, not in bytes.
    virtual int getfattroffset(RPCRequestData *reqdata, 
			       RPCReply &reply) = 0;

    static const int fattr3_len = 5*4 + 8*8;
    virtual void handleReply(RPCRequestData *reqdata, 
			     Clock::Tfrac time, const struct iphdr *ip_hdr,
			     int source_port, int dest_port, int l4checksum, int payload_len,
			     RPCReply &reply) 
    {
	AssertAlways(reply.status() == 0,("request not accepted?!\n"));
	int v3fattroffset = getfattroffset(reqdata,reply);
	if (v3fattroffset >= 0) {
	    ShortDataAssertMsg(v3fattroffset * 4 + fattr3_len <= reply.getrpcresultslen(),
			       "NFSv3 attribute op",
			       ("%d * 4 + %d <= %d",v3fattroffset,fattr3_len,reply.getrpcresultslen()));
	    const uint32_t *xdr = reply.getrpcresults();
	    xdr += v3fattroffset;
	    int type = ntohl(xdr[0]);
	    AssertAlways(type >= 1 && type < 8,("bad"));

	    if (mode == Convert) {
		nfs_attrops_outmodule->newRecord();
		attrops_request_id.set(reqdata->request_id);
		attrops_reply_id.set(cur_record_id);
		attrops_filehandle.set(filehandle);
		if (lookup_directory_filehandle.empty()) {
		    attrops_lookupdirfilehandle.setNull();
		} else {
		    attrops_lookupdirfilehandle.set(lookup_directory_filehandle);
		}
		if (filename.empty()) {
		    attrops_filename.setNull();
		} else {
		    attrops_filename.set(filename);
		}
		attrops_typeid.set(type);
		attrops_type.set(NFSV3_typelist[type]);
		attrops_mode.set(ntohl(xdr[1] & 0xFFF));
		attrops_nlink.set(ntohl(xdr[2]));
		attrops_uid.set(ntohl(xdr[3]));
		attrops_gid.set(ntohl(xdr[4]));
		attrops_filesize.set(xdr_ll(xdr,5));
		attrops_used_bytes.set(xdr_ll(xdr,7));
		attrops_access_time.set(getNFS3Time(xdr+15));
		attrops_modify_time.set(getNFS3Time(xdr+17));
		attrops_inochange_time.set(getNFS3Time(xdr+19));
	    }
	}
    }

    string filehandle;
    const string lookup_directory_filehandle, filename;
};

class NFSV3GetAttrReplyHandler : public NFSV3AttrOpReplyHandler {
public:
    NFSV3GetAttrReplyHandler(const string &_filehandle)
	: NFSV3AttrOpReplyHandler(_filehandle,empty_string,empty_string)
    { }
    virtual ~NFSV3GetAttrReplyHandler() { }
    virtual int getfattroffset(RPCRequestData *reqdata,
			       RPCReply &reply) 
    {
	const uint32_t *xdr = reply.getrpcresults();
	int actual_len = reply.getrpcresultslen();
	ShortDataAssertMsg(actual_len >= 4,"NFSv3 getattr reply",
			   ("bad %d", actual_len));
	uint32_t op_status = ntohl(*xdr);
	if (op_status != 0) {
	    AssertAlways(op_status == 70,("bad %d\n",op_status));
	    return -1;
	}
	return 1;
    }
};

class NFSV3LookupReplyHandler : public NFSV3AttrOpReplyHandler {
public:
    NFSV3LookupReplyHandler(const string &dir_filehandle,
			    const string &filename)
	: NFSV3AttrOpReplyHandler("",dir_filehandle,filename)
    { }
    virtual ~NFSV3LookupReplyHandler() { }

    void checkOpStatus(uint32_t op_status) {
	AssertAlways(op_status == 2 || // enoent
		     op_status == 13 || // eaccess
		     op_status == 70, // estale
		     ("bad11 %d",op_status)); 
	// might at some point want to record failed lookups, as per the NFSV2 
	// decode also
    }

    virtual void handleReply(RPCRequestData *reqdata, 
			     Clock::Tfrac time, const struct iphdr *ip_hdr,
			     int source_port, int dest_port, int l4checksum, int payload_len,
			     RPCReply &reply) 
    {
	const uint32_t *xdr = reply.getrpcresults();
	int actual_len = reply.getrpcresultslen();
	ShortDataAssertMsg(actual_len >= 4,"NFSV3LookupReply",("really got nothing"));
	uint32_t op_status = ntohl(*xdr);
	if (op_status != 0) {
	    checkOpStatus(op_status);
	    return;
	}
	int fhlen = ntohl(xdr[1]);
	RPCParseAssertMsg(fhlen >= 4 && (fhlen % 4) == 0,
			  ("bad fhlen = %d", fhlen));
	ShortDataAssertMsg(actual_len >= 4 + 4 + fhlen,"NFSv3 lookup reply",("bad"));
	filehandle.assign((char *)xdr + 8,fhlen);
	NFSV3AttrOpReplyHandler::handleReply(reqdata,time,ip_hdr,source_port,dest_port,
					     l4checksum,payload_len,reply);
    }

    virtual int getfattroffset(RPCRequestData *reqdata,
			       RPCReply &reply) 
    {
	const uint32_t *xdr = reply.getrpcresults();
	int actual_len = reply.getrpcresultslen();
	AssertAlways(actual_len >= 4,("bad"));
	uint32_t op_status = ntohl(*xdr);
	AssertAlways(op_status == 0,("internal"));

	int fhlen = ntohl(xdr[1]);
	AssertAlways(fhlen >= 4 && (fhlen % 4) == 0,("bad"));
	int objattroffset = 1 + 1 + fhlen / 4;
	ShortDataAssertMsg(actual_len >= (objattroffset + 1 + 1) * 4 + fattr3_len,
			   "NFSv3 lookup reply",("bad %d",actual_len));
	AssertAlways(ntohl(xdr[objattroffset]) == 1,("bad"));
	return objattroffset + 1;
    }
};

class NFSV3ReadDirPlusReplyHandler : public NFSV3AttrOpReplyHandler {
public:
    NFSV3ReadDirPlusReplyHandler(const string &filehandle)
	: NFSV3AttrOpReplyHandler(filehandle,empty_string, empty_string)
    { }
    virtual ~NFSV3ReadDirPlusReplyHandler() { }

    void checkOpStatus(uint32_t op_status) {
	if (false) printf("got a nonzero readdir status %d\n", op_status);
	INVARIANT(op_status == NFS3ERR_NOTDIR ||
		  op_status == NFS3ERR_ACCES ||
		  op_status == NFS3ERR_IO ||
		  op_status == NFS3ERR_SERVERFAULT ||
		  op_status == NFS3ERR_NOTSUPP ||
		  op_status == NFS3ERR_BADHANDLE ||
		  op_status == NFS3ERR_TOOSMALL ||
		  op_status == NFS3ERR_BAD_COOKIE ||
		  op_status == NFS3ERR_STALE,
		  format("bad op_status %d") % op_status);
	// might at some point want to record failed lookups, as per the NFSV2 
	// decode also
    }

    // return new curPos
    int32_t parseNameEntry(int32_t curPos, RPCReply &reply, const uint32_t *xdr, int64_t request_id) {
	curPos += 2; // ignore fileid
	ShortDataAssertMsg((curPos+2) * 4 <= reply.getrpcresultslen(),
			   "NFSv3 dirEntry file Info missing",
			   ("(%d + 2) * 4 <= %d",curPos,reply.getrpcresultslen()));
	int32_t nameSize = ntohl(xdr[curPos]); 
	INVARIANT(nameSize > 0, 
		  format("invalid namesize %d in request %lld")
		  % nameSize % request_id);
	++curPos;
	ShortDataAssertMsg(curPos * 4 + nameSize <= reply.getrpcresultslen(),
			   "NFSv3 dirEntry fileName missing",
			   ("%d * 4 + %d <= %d",curPos,nameSize,reply.getrpcresultslen()));
	string name(reinterpret_cast<const char *>(xdr+curPos), nameSize);
	curPos += (nameSize + 3) / 4; // round up
	if (enable_encrypt_filenames) {
	    string enc_ret = encryptString(name);
	    string dec_ret = decryptString(enc_ret);
	    SINVARIANT(dec_ret == name);
	    INVARIANT(enc_ret != name, "Enc bytes == unencrypted bytes!");
	    name = enc_ret;
	}
	curPos += 2; // skip entryplus3::cookie
	if (mode == Convert) {
	    //We know we have at least a filename so make a record for it
	    nfs_attrops_outmodule->newRecord();
	    attrops_request_id.set(request_id);
	    attrops_reply_id.set(cur_record_id);
	    attrops_lookupdirfilehandle.set(filehandle);
	    attrops_filename.set(name);
	}
	return curPos;
    }	

    // return new curPos
    int32_t parseNameAttributes(int32_t curPos, RPCReply &reply, const uint32_t *xdr) {
	ShortDataAssertMsg((1 + curPos) * 4 + fattr3_len  <= reply.getrpcresultslen(),
			   "NFSv3 dirEntry attributes or value follows (afterwards) missing",
			   ("(1 + %d) * 4 + %d  <= %d",curPos, fattr3_len,reply.getrpcresultslen()));

	uint32_t type = ntohl(xdr[curPos]);
	INVARIANT(1 <= type && type < 8,
		  format("NFSv3 dirEntry type not correct, should have 1 <= %d < 8")
		  % type);
	if (mode == Convert) {
	    attrops_typeid.set(ntohl(xdr[curPos]));
	    attrops_type.set(NFSV3_typelist[type]);
	    curPos++;
	    attrops_mode.set(ntohl(xdr[curPos]) & 0xFFF);
	    curPos++;
	    attrops_nlink.set(ntohl(xdr[curPos]));
	    curPos++;
	    attrops_uid.set(ntohl(xdr[curPos]));
	    curPos++;
	    attrops_gid.set(ntohl(xdr[curPos]));
	    curPos++;
	    attrops_filesize.set(xdr_ll(xdr,curPos));
	    curPos+=2; //size is 64 bit
	    attrops_used_bytes.set(xdr_ll(xdr,curPos));
	    curPos+=8;//used is 64 bit,rdev,fsid,fileid don't care
	    attrops_access_time.set(getNFS3Time(xdr+curPos));
	    curPos+=2;
	    attrops_modify_time.set(getNFS3Time(xdr+curPos));
	    curPos+=2;
	    attrops_inochange_time.set(getNFS3Time(xdr+curPos));
	    curPos+=2;
	} else {
	    curPos += 1+2+1+1+2+8+2+2+2;
	}
	return curPos;
    }

    // return new curPos
    int32_t parseNameHandle(int32_t curPos, RPCReply &reply, const uint32_t *xdr,
			    bool got_name_entry) {
	ShortDataAssertMsg((curPos+1) * 4  <= reply.getrpcresultslen(),
			   "NFSv3 dirEntry namehandlesize missing",
			   ("(%d + 1) * 4 <= %d",curPos,reply.getrpcresultslen()));
	int32_t fhSize = ntohl(xdr[curPos]);
	INVARIANT(fhSize >= 0 && fhSize <= 64, "bad");
	curPos++;
	ShortDataAssertMsg((1 + curPos) * 4 + fhSize <= reply.getrpcresultslen(),
			   "NFSv3 dirEntry fileNameHandle or valueAfterwards missing",
			   ("(1 + %d) * 4 + %d <= %d",curPos,fhSize,reply.getrpcresultslen()));
	const char *fileHandle = reinterpret_cast<const char *>(xdr+curPos);
	curPos += (fhSize+3) / 4; // round up
	if (mode == Convert && got_name_entry) {
	    attrops_filehandle.set(fileHandle,fhSize);
	}
	return curPos;
    }

    virtual void handleReply(RPCRequestData *reqdata, 
			     Clock::Tfrac time, const struct iphdr *ip_hdr,
			     int source_port, int dest_port, int l4checksum, int payload_len,
			     RPCReply &reply) 
    {
	INVARIANT(reply.status() == 0, "request not accepted?!\n");
	int v3fattroffset = getfattroffset(reqdata,reply);
	if (v3fattroffset >= 0) {
	    const uint32_t *xdr = reply.getrpcresults();

	    int32_t curEntry = 4 + fattr3_len/4;
	    int type = ntohl(xdr[v3fattroffset]);
	    INVARIANT(type == 2, "Not a ReadDirPlus3 in ReadDirPlus3!");
	    
	    //Handle the attributes of the directory itself as well.
	    NFSV3AttrOpReplyHandler::handleReply(reqdata,time,ip_hdr,source_port,dest_port,
						 l4checksum,payload_len,reply);
	    while (1) {
		int32_t curPos = curEntry; // curOffset relative to curEntry; start after FileID and nameSize
		bool got_name_entry = false;
		if (ntohl(xdr[curEntry]) == 1) {
		    got_name_entry = true;
		    curPos = parseNameEntry(curPos + 1, reply, xdr, reqdata->request_id);
		}
		ShortDataAssertMsg((curPos+1) * 4 <= reply.getrpcresultslen(),
				   "NFSv3 dirEntry padding+attribcookie+valueFollows missing",
				   ("(1 + %d) * 4  <= %d",curPos,reply.getrpcresultslen()));
		if (ntohl(xdr[curPos]) == 1) { // have name_attributes
		    INVARIANT(got_name_entry, "whoa, have attributes w/o a name??");
		    curPos = parseNameAttributes(curPos + 1, reply, xdr);
		} else {
		    ++curPos;
		} 
		ShortDataAssertMsg((curPos+1) * 4 <= reply.getrpcresultslen(),
				   "NFSv3 dirEntry ... + name_HandleFollows",
				   ("(1 + %d) * 4  <= %d",curPos,reply.getrpcresultslen()));
		if (ntohl(xdr[curPos]) == 1) {//name_handle follows
		    curPos = parseNameHandle(curPos + 1, reply, xdr, got_name_entry);
		} else {
		    ++curPos;
		} 
		ShortDataAssertMsg((curPos+1) * 4 <= reply.getrpcresultslen(),
				   "NFSv3 dirEntry ... + next entry",
				   ("(1 + %d) * 4  <= %d",curPos,reply.getrpcresultslen()));

		if (ntohl(xdr[curPos]) == 1) { //There is a valid dir entry coming next
		    curEntry = curPos;
		    if (false) printf("done parsing %d of %d\n",curEntry,reply.getrpcresultslen());
		} else {
		    ++curPos;
		    ShortDataAssertMsg(curPos * 4 <= reply.getrpcresultslen(),
				       "NFSv3 dirEntry ... + next entry",
				       ("(1 + %d) * 4  <= %d",curPos,reply.getrpcresultslen()));

		    if (ntohl(xdr[curPos]) != 1) {
			++counts[readdir_continuations_ignored];
			if (false) {
			    printf("not the end of the directory entry.\n");
			    printf("We're going to lose data if we don't do readdirplus3 continuation parsing.\n");
			}
		    }
		    if (false) printf("done parsing %d of %d\n",curEntry,reply.getrpcresultslen());
		    break;
		}
	    }
	}
    }

    virtual int getfattroffset(RPCRequestData *reqdata,
			       RPCReply &reply) 
    {
	const uint32_t *xdr = reply.getrpcresults();
	int actual_len = reply.getrpcresultslen();
	AssertAlways(actual_len >= 4,("readdirplus3 packet size < 4"));
	uint32_t op_status = ntohl(*xdr);
	if (op_status != 0) {
	    cout << "Warning, readdirplus3 failed, op_status " << op_status << endl;
	    return -1; 
	}

	int dirAttrFollows = ntohl(xdr[1]);
	if (dirAttrFollows) {
	    ShortDataAssertMsg(actual_len >= (2) * 4 + fattr3_len,
		    "NFSv3 ReadDirPlus reply",("bad %d",actual_len));
	    return 2;
	} else {
	    // no File information in this reply
	    return -1;
	}
    }
};


class NFSV3AccessReplyHandler : public NFSV3AttrOpReplyHandler {
public:
    NFSV3AccessReplyHandler(const string &filehandle)
	: NFSV3AttrOpReplyHandler(filehandle, empty_string, empty_string)
    { }
    virtual ~NFSV3AccessReplyHandler() { }

    void checkOpStatus(uint32_t op_status) {
	AssertAlways(op_status == 2 || // enoent
		     op_status == 13 || // eaccess
		     op_status == 70, // estale
		     ("bad11 %d",op_status)); 
	// might at some point want to record failed accesses, as per the NFSV2 
	// decode also
    }

    virtual void handleReply(RPCRequestData *reqdata, 
			     Clock::Tfrac time, const struct iphdr *ip_hdr,
			     int source_port, int dest_port, int l4checksum, int payload_len,
			     RPCReply &reply) 
    {
	const uint32_t *xdr = reply.getrpcresults();
	int actual_len = reply.getrpcresultslen();
	ShortDataAssertMsg(actual_len >= 4,"NFSV3LookupReply",("really got nothing"));
	uint32_t op_status = ntohl(*xdr);
	if (op_status != 0) {
	    checkOpStatus(op_status);
	}
	// may always get post_op_attr's, only get access status if result was ok.
	NFSV3AttrOpReplyHandler::handleReply(reqdata,time,ip_hdr,source_port,dest_port,
					     l4checksum,payload_len,reply);
    }

    virtual int getfattroffset(RPCRequestData *reqdata,
			       RPCReply &reply) 
    {
	const uint32_t *xdr = reply.getrpcresults();
	int actual_len = reply.getrpcresultslen();
	ShortDataAssertMsg(actual_len >= 8, "NFSV3 access reply, firstbits",
			   ("bad %d", actual_len));

	int has_post_op_attr = ntohl(xdr[1]);
	if (0 == has_post_op_attr) {
	    return -1;
	}

	ShortDataAssertMsg(actual_len >= 8 + fattr3_len,
			   "NFSv3 access reply",("bad %d",actual_len));
	INVARIANT(has_post_op_attr == 1, "bad");
	return 2;
    }
};

class NFSV3ReadWriteReplyHandler : public NFSV3AttrOpReplyHandler {
public:
    NFSV3ReadWriteReplyHandler(const string &filehandle, ExtentType::int64 _offset,
			       ExtentType::int32 _reqbytes, bool _is_read)
	: NFSV3AttrOpReplyHandler(filehandle,empty_string, empty_string),
	  offset(_offset), reqbytes(_reqbytes), is_read(_is_read)
    { }
    virtual ~NFSV3ReadWriteReplyHandler() { }
    virtual int getfattroffset(RPCRequestData *reqdata,
			       RPCReply &reply) 
    {
	const uint32_t *xdr = reply.getrpcresults();
	int actual_len = reply.getrpcresultslen();

	ShortDataAssertMsg(actual_len >= 4,"NFSv3 R/W reply",
			   ("actual_len %d",actual_len));
	uint32_t op_status = ntohl(*xdr);
	xdr += 1; 
	actual_len -= 4;

	if (op_status != 0) {
	    if (is_read) {
		if (op_status == 137) {
		    // Seeing these in some cache traces?!
		    cout << "Warning, weird op status in write reply " << op_status << endl;
		} else {
		    AssertAlways(op_status == 70 || // stale file handle
				 op_status == 13, // permission denied
				 ("bad12 %d", op_status)); 
		}
	    } else {
		if ((op_status >= 12000 && op_status <= 16000) ||
		    op_status == 137) {
		    // Seeing these in some cache traces?!
		    cout << "Warning, weird op status in write reply " << op_status << endl;
		} else {
		    AssertAlways(op_status == 70 || // stale file handle
				 op_status == 28 || // out of space
				 op_status == 13 || // permission denied
				 op_status == 69, // disk quota exceeded
				 ("bad12 op_status = %d",op_status)); 
		}
	    }
	    return -1;
	}
	if (is_read) {
	    ShortDataAssertMsg(actual_len >= 4,is_read ? "NFSv3 read reply" : "NFSv3 write reply",
			       ("actual len %d",actual_len));
	    if (ntohl(*xdr) == 0) {
		return -1;
	    }
	    AssertAlways(ntohl(*xdr) == 1,("bad; should be either 0 or 1 to mark whether attrs are there??"));
	    ShortDataAssertMsg(actual_len >= 4 + fattr3_len,is_read ? "NFSv3 read reply" : "NFSv3 write reply",
			       ("actual len %d",actual_len));
	    return 2;
	} else {
	    if (xdr[0]) { // have pre-op attr
		if (xdr[1+3*2]) { // post-op attr
	             //                       flag pre_op flag post_op  count committed writeverf
		    ShortDataAssertMsg(actual_len == 4 + 3*8 + 4 + fattr3_len + 4 + 4 + 8, "NFSV3WriteReply",
				       ("bad on %s %d != %d", tracename.c_str(), actual_len, 4+ 3*8 + 4 + fattr3_len + 4 + 4 + 8));
		    return 1 + 1 + 3*2 + 1;
		} else {
		    // missing post-op fattr3; for write this means we have no useful attributes.
		    ShortDataAssertMsg(actual_len == 4 + 4 + 4*3*2 + 4, "NFSV3WriteReply", 
				       ("bad in %s size is %d", tracename.c_str(), actual_len));
		    return -1;
		}
	    } else if (xdr[1]) { // missing pre-op attr, have post-op
		ShortDataAssertMsg(actual_len == 4 + 4 + fattr3_len + 4 + 4 + 8, "NFSV3WriteReply",
				   ("bad on %s %d != %d", tracename.c_str(), actual_len, 4+ 3*8 + 4 + fattr3_len + 4 + 4 + 8));
		return 1 + 1 + 1;
	    } else {
		INVARIANT(actual_len == 4 + 4 + 4 + 4 + 8, 
			  format("bad in %s size is %d") % tracename % actual_len);
		return -1;
	    }
	}
    }

    virtual void handleReply(RPCRequestData *reqdata, 
			     Clock::Tfrac time, const struct iphdr *ip_hdr,
			     int source_port, int dest_port, int l4checksum, int payload_len,
			     RPCReply &reply) 
    {
	NFSV3AttrOpReplyHandler::handleReply(reqdata, time, ip_hdr,
					     source_port, dest_port, l4checksum,
					     payload_len, reply);
	const uint32_t *xdr = reply.getrpcresults();
	int actual_len = reply.getrpcresultslen();
	AssertAlways(reply.status() == 0,("request not accepted?!\n"));
	uint32_t op_status = ntohl(*xdr);
	xdr += 1;
	actual_len -= 4;
	if (op_status != 0) {
	    if (is_read) {
		if (op_status == 137) {
		    // Weird things being seen in NFS cache traces
		    cout << "Warning, weird op status in write reply " << op_status << endl;
		} else {
		    AssertAlways(op_status == 70 || // stale file handle
				 op_status == 13, // permission denied
				 ("bad12 %d", op_status)); 
		}
	    } else {
		if ((op_status >= 12000 && op_status <= 16000) ||
		    op_status == 137) {
		    // Weird things being seen in NFS cache traces
		    cout << "Warning, weird op status in write reply " << op_status << endl;
		} else {
		    AssertAlways(op_status == 70 || // stale file handle
				 op_status == 28 || // out of space
				 op_status == 13 || // permission denied
				 op_status == 69, // disk quota exceeded
				 ("bad12 op_status = %d",op_status)); 
		}
	    }
	} else {
	    if (mode == Convert) {
		nfs_readwrite_outmodule->newRecord();
		readwrite_request_id.set(reqdata->request_id);
		readwrite_reply_id.set(cur_record_id);
		readwrite_filehandle.set(filehandle);
		readwrite_is_read.set(is_read);
		readwrite_offset.set(offset);
		readwrite_bytes.set(reqbytes);
	    }
	 
	    if (is_read) {
		ShortDataAssertMsg(actual_len >= 4 + fattr3_len + 4 + 4 + 4,
				   "NFSv3 Read Reply",("bad %d",actual_len));
		uint32_t actual_bytes = reqbytes+1;
		if (ntohl(*xdr) == 1) {
		    actual_bytes = ntohl(xdr[1+fattr3_len/4]);
		} else if (ntohl(*xdr) == 0) {
		    actual_bytes = ntohl(xdr[1]);
		} else {
		    FATAL_ERROR("should have been caught in getfattroffset!");
		}
		AssertAlways(reqbytes >= (ExtentType::int32)actual_bytes,
			     ("wrong %d %d\n",reqbytes,actual_bytes));
		if (mode == Convert) {
		    readwrite_bytes.set(actual_bytes);
		}
	    } else {
		uint32_t actual_bytes;
		if (xdr[0]) { // have pre-op attr
		    INVARIANT(xdr[1+3*2],
			      format("Unimplemented, Missing post-op fattr3 for write in %s") % tracename);
		    //                       flag pre_op flag post_op  count committed writeverf
		    AssertAlways(actual_len == 4 + 3*8 + 4 + fattr3_len + 4 + 4 + 8,
				 ("bad"));
		    actual_bytes = ntohl(xdr[1+3*2+1+fattr3_len/4]);
		} else if (xdr[1]) { // missing pre-op attr, have post-op
		    INVARIANT(xdr[1], 
			      format("Unimplemented, Missing post-op fattr3 for write in %s size is %d") % tracename % actual_len);
		    AssertAlways(actual_len == 4 + 4 + fattr3_len + 4 + 4 + 8, 
				 ("bad on %s %d != %d", tracename.c_str(), actual_len, 4+ 3*8 + 4 + fattr3_len + 4 + 4 + 8));
		    actual_bytes = ntohl(xdr[1+1+fattr3_len/4]);
		} else {
		    INVARIANT(actual_len == 4 + 4 + 4 + 4 + 8, 
			      format("bad in %s size is %d") % tracename % actual_len);
		    actual_bytes = ntohl(xdr[1+1]);
		}

		AssertAlways((int)actual_bytes == reqbytes,("bad\n"));

		if (mode == Convert) {
		    readwrite_bytes.set(reqbytes);
		}
	    }
	} 
    }
    const ExtentType::int64 offset;
    const ExtentType::int32 reqbytes;
    const bool is_read;
};

// set these to -1 to disable
const int hex_dump_id_1 = -1;
const int hex_dump_id_2 = -1; 

void
maybeDumpRecord(const uint32_t *xdr, int actual_len)
{
    if (cur_record_id == hex_dump_id_1 ||
	cur_record_id == hex_dump_id_2) {
      printf("hex dump request op = %d; %lld %d:\n",
	     opid.val(),cur_record_id,actual_len);
      unsigned char *f = (unsigned char *)xdr;
      for(int i=0;i<actual_len;++i) {
	printf("%02x ",f[i]);
	if ((i % 26) == 25) {
	  printf("\n");
	}
      }
      printf("\n");
    }
}

void
handleNFSV2Request(Clock::Tfrac time, const struct iphdr *ip_hdr,
		   int ipchecksum, int l4checksum, RPCRequest &req, RPCRequestData &d)
{
    const uint32_t *xdr = req.getrpcparam();
    int actual_len = req.getrpcparamlen();

    maybeDumpRecord(xdr,actual_len);
    switch(d.procnum)
	{
//	case NFSPROC_GETATTR: 
//	    {
//		FATAL_ERROR("untested -- didn't get exercised, so don't trust until checked");
//		if (false) printf("v2GetAttr %lld %8x -> %8x; %d\n",
//				  time,d.client,d.server,actual_len);
//		RPCParseAssertMsg(actual_len == 32,
//				  ("bad getattr request @%d.%09d rec#%lld: %d",
//				   Clock::TfracToSec(time), Clock::TfracToNanoSec(time),
//				   cur_record_id,actual_len));
//		string filehandle((char *)(xdr), 32);
//		d.replyhandler =
//		    new NFSV2GetAttrReplyHandler(filehandle);
//	    }
//	    break;
	default:
	    ++counts[unhandled_nfsv2_requests];
	    return;
	}
}

void
handleNFSV3Request(Clock::Tfrac time, const struct iphdr *ip_hdr,
		   int ipchecksum, int l4checksum, RPCRequest &req, RPCRequestData &d)
{
    const uint32_t *xdr = req.getrpcparam();
    int actual_len = req.getrpcparamlen();

    maybeDumpRecord(xdr,actual_len);
    switch(d.procnum)
	{
	case NFSPROC3_GETATTR: 
	    {
		if (false) cout << format("v3GetAttr %lld %8x -> %8x; %d\n")
			       % time % d.client % d.server % actual_len;
		ShortDataAssertMsg(actual_len >= 8,"NFSv3 getattr request",
				   ("bad getattr in %s request @%lld: %d",
				    tracename.c_str(), cur_record_id,
				    actual_len));
		int fhlen = ntohl(xdr[0]);
		AssertAlways(fhlen % 4 == 0 && fhlen > 0 && fhlen <= 64,
			     ("bad"));
		ShortDataAssertMsg(actual_len == 4+fhlen,"NFSv3 getattr request",("bad"));
		string filehandle((char *)(xdr+1), fhlen);
		d.replyhandler =
		    new NFSV3GetAttrReplyHandler(filehandle);
	    }
	    break;
	case NFSPROC3_LOOKUP: 
	    {
		ShortDataAssertMsg(actual_len >= 12,
				   "NFSv3 lookup request",
				   ("bad lookup len %d @%d.%09d; %d %d",actual_len,
				    Clock::TfracToSec(time),Clock::TfracToNanoSec(time),
				    ip_hdr->protocol,IPPROTO_UDP));
		int fhlen = ntohl(xdr[0]);
		INVARIANT(fhlen % 4 == 0 && fhlen > 0 && fhlen <= 64,"bad");
		ShortDataAssertMsg(actual_len >= 4+fhlen+4,"NFSv3 lookup request",("bad"));
		string filename = getLookupFilename(xdr+1+fhlen/4,actual_len - (4+fhlen));
		if (false) cout << format("v3Lookup %lld %8x -> %8x; %d; %s\n")
			       % time % d.client % d.server
			       % actual_len % filename;
		string lookup_directory_filehandle((char *)(xdr+1),fhlen);
		d.replyhandler =
		    new NFSV3LookupReplyHandler(lookup_directory_filehandle,filename);
						
	    }
	    break;
	case NFSPROC3_ACCESS:
	    {
		ShortDataAssertMsg(actual_len >= 8,
				   "NFSv3 access request",
				   ("bad access len %d @%d.%09d; %d %d",actual_len,
				    Clock::TfracToSec(time),Clock::TfracToNanoSec(time),
				    ip_hdr->protocol,IPPROTO_UDP));
		int fhlen = ntohl(xdr[0]);
		INVARIANT(fhlen % 4 == 0 && fhlen > 0 && fhlen <= 64,"bad");
		ShortDataAssertMsg(actual_len == 4+fhlen+4, "NFSv3 access request",("bad"));
		string access_filehandle(reinterpret_cast<const char *>(xdr+1), fhlen);
		// ignore access mode right after filehandle
		d.replyhandler = 
		    new NFSV3AccessReplyHandler(access_filehandle);
	    }
	    break;
	case NFSPROC3_READDIRPLUS:
	    {
		if (false) printf("got a READDIRPLUS\n");
		int fhlen = ntohl(xdr[0]);
		RPCParseAssertBoost(fhlen % 4 == 0 && fhlen > 0 && fhlen <= 64,
				    format("bad fhlen %d") % fhlen);
		//INVARIANT(actual_len == sizeof(struct READDIRPLUS3args), "ReadDirPlus Error. struct not the correct size\n");
		string access_filehandle(reinterpret_cast<const char *>(xdr+1), fhlen);
		d.replyhandler = 
		    new NFSV3ReadDirPlusReplyHandler(access_filehandle);
	    }
	    break;
	case NFSPROC3_READ:
	    { 
		ShortDataAssertMsg(actual_len >= (8+8+4),
				   "NFSv3 read request",
				   ("bad read len %d @%d.%09d; %d %d",actual_len,
				    Clock::TfracToSec(time),Clock::TfracToNanoSec(time),
				    ip_hdr->protocol,IPPROTO_UDP));
		int fhlen = ntohl(xdr[0]);
		AssertAlways(fhlen % 4 == 0 && fhlen > 0 && fhlen <= 64,
			     ("bad fhlen1"));
		RPCParseAssertMsg(actual_len == 4 + fhlen + 8 + 4,("bad actual_len %d fhlen %d +=16",actual_len,fhlen));
		string filehandle((char *)(xdr+1), fhlen);
		unsigned len = ntohl(xdr[1+ fhlen/4 + 2]);
		AssertAlways(len < 65536,("bad"));
		d.replyhandler = 
		    new NFSV3ReadWriteReplyHandler(filehandle, 
						   xdr_ll(xdr,1+fhlen/4),
						   len,true);
	    }
	    break;
	case NFSPROC3_WRITE:
	    {
		ShortDataAssertMsg(actual_len >= (8+8+4),
				   "NFSv3 Write Request",
				   ("bad read len %d @%d.%09d; %d %d",actual_len,
				    Clock::TfracToSec(time),Clock::TfracToNanoSec(time),
				    ip_hdr->protocol,IPPROTO_UDP));
		int fhlen = ntohl(xdr[0]);
		AssertAlways(fhlen % 4 == 0 && fhlen > 0 && fhlen <= 64,
			     ("bad"));
		ShortDataAssertMsg(actual_len >= (4+ fhlen + 8 + 4),
				   "NFSv3 Write Request",
				   ("bad read len %d @%d.%09d; %d %d",actual_len,
				    Clock::TfracToSec(time),Clock::TfracToNanoSec(time),
				    ip_hdr->protocol,IPPROTO_UDP));   

		string filehandle((char *)(xdr+1), fhlen);
		if (false) cout << format("v2Write %lld %8x -> %8x; %d\n")
			       % time % d.client % d.server
			       % actual_len;
		unsigned len = ntohl(xdr[1+ fhlen/4 + 2]);
		AssertAlways(len < 65536,("bad"));
		d.replyhandler = 
		    new NFSV3ReadWriteReplyHandler(filehandle, 
						   xdr_ll(xdr,1+fhlen/4),
						   len,false);
	    }
	    break;
	default:
	    if (false) cout << "Warn unprocessed NFSV3 #" << d.procnum << endl;
	    ++counts[unhandled_nfsv3_requests];
	    return;
	}
}

void
updateIPPacketSeries(Clock::Tfrac time, uint32_t srcHost, uint32_t destHost, 
		     uint32_t wire_len, struct udphdr *udp_hdr, struct tcphdr *tcp_hdr,
		     bool is_fragment)
{
    ++counts[ip_packet];

    if (mode == Info) {
	// no actual work to do...
	return;
    }
    ippacket_outmodule->newRecord();
    ippacket_packet_at.set(time);
    ippacket_source.set(srcHost);
    ippacket_destination.set(destHost);
    ippacket_wire_length.set(wire_len);
    if (udp_hdr != NULL) {
	ippacket_udp_tcp.set(true);
	ippacket_source_port.set(ntohs(udp_hdr->source));
	ippacket_destination_port.set(ntohs(udp_hdr->dest));
	ippacket_tcp_seqnum.setNull();
    } else if (tcp_hdr != NULL) {
	ippacket_udp_tcp.set(false);
	ippacket_source_port.set(ntohs(tcp_hdr->source));
	ippacket_destination_port.set(ntohs(tcp_hdr->dest));
	ippacket_tcp_seqnum.set(ntohl(tcp_hdr->seq));
    } else {
	ippacket_udp_tcp.setNull();
	ippacket_source_port.setNull();
	ippacket_destination_port.setNull();
	ippacket_tcp_seqnum.setNull();
    }
    ippacket_is_fragment.set(is_fragment);
}


ExtentType::int64 
timeval2ns(const struct timeval &v)
{
    return (ExtentType::int64)v.tv_sec * (ExtentType::int64)1000000000 + v.tv_usec * 1000;
}

// sets all but is_request, rpc_status
void
fillcommonNFSRecord(Clock::Tfrac time, const struct iphdr *ip_hdr,
		    int v_source_port, int v_dest_port, int payload_len,
		    int procnum, int v_nfsversion, uint32_t v_xid)
{
    ++cur_record_id;
    INVARIANT(cur_record_id >= 0 && cur_record_id >= first_record_id, "bad");

    if (v_nfsversion == 2) {
	AssertAlways(procnum >= 0 && procnum < n_nfsv2ops,("bad"));
    } else if (v_nfsversion == 3) {
	AssertAlways(procnum >= 0 && procnum < n_nfsv3ops,("bad"));
    } else if (v_nfsversion == 1) {
	// caches seem to use NFS version 1 null op occasionally
	AssertAlways(procnum == 0,("bad"));
    } else {
	AssertFatal(("bad; nfs version %d",v_nfsversion));
    }

    if (mode == Convert) {
	nfs_common_outmodule->newRecord();
	packet_at.set(time);
	source.set(ntohl(ip_hdr->saddr));
	source_port.set(v_source_port);
	dest.set(ntohl(ip_hdr->daddr));
	dest_port.set(v_dest_port);
	is_udp.set(ip_hdr->protocol == IPPROTO_UDP);
	nfs_version.set(v_nfsversion);
	xid.set(htonl(v_xid));
	opid.set(procnum);
	if (v_nfsversion == 2) {
	    operation.set(nfsv2ops[procnum]);
	} else if (v_nfsversion == 3) {
	    operation.set(nfsv3ops[procnum]);
	} else if (v_nfsversion == 1) {
	    AssertAlways(procnum == 0,("bad"));
	}
	payload_length.set(payload_len);
	common_record_id.set(cur_record_id);
    }
}

void
handleNFSRequest(Clock::Tfrac time, const struct iphdr *ip_hdr,
		 int source_port, int dest_port, int l4checksum, int payload_len,
		 RPCRequest &req, RPCRequestData &d)
{
    ++counts[nfs_request];
    fillcommonNFSRecord(time,ip_hdr,source_port,dest_port,payload_len,
			d.procnum,d.version,req.xid());
    INVARIANT(cur_record_id >= 0, "bad");
    d.request_id = cur_record_id;
    if (mode == Convert) {
	is_request.set(true);
	rpc_status.setNull();
    }
    if (d.version == 2) {
	++counts[nfsv2_request];
	handleNFSV2Request(time,ip_hdr,ntohs(ip_hdr->check),l4checksum,req,d);
    } else if (d.version == 3) {
	++counts[nfsv3_request];
	handleNFSV3Request(time,ip_hdr,ntohs(ip_hdr->check),l4checksum,req,d);
    } else {
	FATAL_ERROR(format("Huh? NFSV%d") % d.version);
    }
}

void
handleNFSReply(Clock::Tfrac time, const struct iphdr *ip_hdr,
	       int source_port, int dest_port, int l4checksum, int payload_len,
	       RPCRequestData *req, RPCReply &reply)
{
    ++counts[nfs_reply];
    fillcommonNFSRecord(time,ip_hdr,source_port,dest_port,payload_len,
			req->procnum,req->version,reply.xid());
    if (req->version == 2) {
	++counts[nfsv2_reply];
    } else if (req->version == 3) {
	++counts[nfsv3_reply];
    }	
    if (mode == Convert) {
	is_request.set(false);
	rpc_status.setNull(); // don't know yet
    }
}

void
handleMountRequest(Clock::Tfrac time, const struct iphdr *ip_hdr,
		   int source_port, int dest_port, int payload_len,
		   RPCRequest &req, RPCRequestData &d)
{
    switch(d.procnum) 
	{
	case MountProc::proc_mnt: {
	    MountRequest_MNT *m = new MountRequest_MNT(req.duppacket(),req.getlen());
	    if (false) printf("mount request for %s\n",m->pathname().c_str());
	    d.reqdata = m;
	    break;
	}
	case MountProc::proc_umnt: {
	    break; // nothing interesting here...
	}
	case 0: {
	    break; // ignore quietly
	}
	default:
	    printf("skipping mount request type %d\n",d.procnum);
	}
}

void
handleMountReply(Clock::Tfrac time, const struct iphdr *ip_hdr,
		 int source_port, int dest_port, int payload_len,
		 RPCRequestData *req, RPCReply &reply)
{
    switch(req->procnum)
	{
	case MountProc::proc_mnt: {
	    MountRequest_MNT *m_req = dynamic_cast<MountRequest_MNT *>(req->reqdata);
	    AssertAlways(m_req != NULL,("bad"));
	    MountReply_MNT m_rep(req->version,reply);
	    if (m_rep.mountok()) {
		string pathname = m_req->pathname();
		if (enable_encrypt_filenames) {
		    string enc_path = encryptString(pathname);
		    string dec_path = decryptString(enc_path);
		    AssertAlways(dec_path == pathname,("bad"));
		    pathname = enc_path;
		}
		if (mode == Convert) {
		    nfs_mount_outmodule->newRecord();
		    nfs_mount_request_at.set(req->request_at);
		    nfs_mount_reply_at.set(time);
		    nfs_mount_server.set(ntohl(ip_hdr->saddr));
		    nfs_mount_client.set(ntohl(ip_hdr->daddr));
		    nfs_mount_pathname.set(pathname);
		    nfs_mount_filehandle.set(m_rep.rawfilehandle(),m_rep.rawfilehandlelen());
		}
		//		printf("mount reply for %s -> %s\n",m_req->pathname().c_str(),hexstring(m_rep.filehandle()).c_str());
	    } else {
		if (false) printf("mount failed for %s: %d\n",m_req->pathname().c_str(),m_rep.mounterror());
	    }
	    break;
	}
	case MountProc::proc_umnt: {
	    break; // nothing interesting here...
	}
	default:
	    if (false) printf("skipping mount reply type %d\n",req->procnum);
	}
}

void
duplicateRequestCheck(RPCRequestData &d, RPCRequestData *hval)
{
    ++counts[duplicate_request];
    AssertAlways(hval->server == d.server &&
		 hval->client == d.client &&
		 hval->xid == d.xid,("internal error\n"));
    if (warn_duplicate_reqs) { // disabled because of tons of them showing up in set-8
	printf("Probable duplicate request detected s=%08x c=%08x xid=%08x; #%lld duped by #%lld\n",
	       hval->server, hval->client, hval->xid, 
	       hval->request_id,d.request_id);
	printf("  Checksums are %d/%d vs %d/%d\n",
	       hval->ipchecksum,hval->l4checksum,d.ipchecksum,d.l4checksum);
	fflush(stdout);
    }
//    INVARIANT((hval->request_id < 0 && d.request_id < 0) ||
//	      (hval->request_id >= first_record_id &&
//	       hval->request_id <= cur_record_id &&
//	       d.request_id >= first_record_id),
//	      format("whoa %d not in [%d .. %d] or %d < %d")
//	      % hval->request_id % first_record_id % cur_record_id % d.request_id % first_record_id);
    if (hval->rpcreqhashval != d.rpcreqhashval) {
	for(vector<network_error_listT>::iterator i = known_bad_list.begin();
	    i != known_bad_list.end(); ++i) {
	    if ((hval->request_id - first_record_id) == i->h_rid &&
		(d.request_id - first_record_id) == i->v_rid &&
		hval->rpcreqhashval == i->h_hash &&
		d.rpcreqhashval == i->v_hash &&
		(unsigned)hval->server == i->server &&
		(unsigned)hval->client == i->client) {
		fprintf(stderr,"speculate bad network transmission/lack of locking in linux write??\n");
		hval->rpcreqhashval = d.rpcreqhashval;
		fflush(stderr);
	    }
	}
    }
    if (hval->rpcreqhashval != d.rpcreqhashval) {
	++counts[bad_retransmit];
	fprintf(stderr, "bad-retransmit { %lld, %lld, 0x%08x, 0x%08x, 0x%08x, 0x%08x }, // %s\n",
		hval->request_id - first_record_id,
		d.request_id - first_record_id,
		hval->rpcreqhashval, d.rpcreqhashval,
		hval->server, hval->client,
		tracename.c_str());
	fprintf(stderr, " // bad retransmit  prog=%d/%d, ver=%d/%d, proc=%d/%d, xid=%x\n",d.program,hval->program,
		d.version,hval->version,d.procnum,hval->procnum, hval->xid);
    }
    // can't remove the original request in case we get a pair of
    // retransmissions; matching is a litle bit odd in this case,
    // since a response will match to the nearest request.
}

void
handleRPCRequest(Clock::Tfrac time, const struct iphdr *ip_hdr,
		 int source_port, int dest_port, int l4checksum, int payload_len,
		 const unsigned char *p, const unsigned char *pend)
{
    RPCRequest req(p,pend-p);
    ++counts[rpc_request];
	
    payload_len -= 4*(req.getrpcparam() - req.getxdr());
    if (false) printf("RPCRequest for prog %d, version %d, proc %d\n",
		      req.host_prognum(),req.host_version(),
		      req.host_procnum());
    RPCRequestData d(ip_hdr->saddr, ip_hdr->daddr, req.xid(), dest_port);
    d.request_id = -99;
    d.version = req.host_version();
    d.program = req.host_prognum();
    d.procnum = req.host_procnum();
    d.request_at = time;
    d.rpcreqhashval = BobJenkinsHash(1972,p,pend-p);
    d.ipchecksum = ntohs(ip_hdr->check);
    d.l4checksum = l4checksum;
    d.reqdata = NULL;
    d.replyhandler = NULL;
    if (d.program == RPCRequest::host_prog_nfs) {
	handleNFSRequest(time,ip_hdr,source_port,dest_port,l4checksum,payload_len,req,d);
	RPCRequestData *hval = rpcHashTable.lookup(d);
	if (hval != NULL) {
	    duplicateRequestCheck(d, hval);
	}
    } else if (d.program == RPCRequest::host_prog_mount) {
	d.request_id = -1; // fake
	handleMountRequest(time,ip_hdr,source_port,dest_port,payload_len,req,d);
    } else if (d.program == RPCRequest::host_prog_yp) {
	d.request_id = -2; // fake
    } else if (d.program == RPCRequest::host_prog_portmap) {
	d.request_id = -3; // fake
    } else {
	// unknown program, just retain request...
	printf("unrecognized rpc request program %d, version %d, procedure %d\n",
	       d.program,d.version,d.procnum);
	d.request_id = -4; // fake
    }
    INVARIANT(d.request_id >= first_record_id || (d.request_id >= -4 && d.request_id <= -1), 
	      format("bad %d / %d %d on %s") % d.program % d.request_id % first_record_id % tracename);
    rpcHashTable.add(d); // only add if successful parse...
}

void
handleRPCReply(Clock::Tfrac time, const struct iphdr *ip_hdr,
	       int source_port, int dest_port, int l4checksum, int payload_len,
	       const unsigned char *p, const unsigned char *pend)
{
    RPCReply reply(p,pend-p);
    ++counts[rpc_reply];
    // RPC reply parsing should handle all underflows    
    payload_len -= 4*(reply.getrpcresults() - reply.getxdr());
    RPCRequestData *req = rpcHashTable.lookup(RPCRequestData(ip_hdr->daddr,ip_hdr->saddr,reply.xid(),source_port));
	
    if (req != NULL) {
	if (req->program == RPCRequest::host_prog_nfs) {
	    handleNFSReply(time,ip_hdr,source_port,dest_port,l4checksum,payload_len,req,reply);
	} else if (req->program == RPCRequest::host_prog_mount) {
	    handleMountReply(time,ip_hdr,source_port,dest_port,payload_len,req,reply);
	} else {
	    // unknown program; ignore
	}
	if (req->replyhandler != NULL) {
	    req->replyhandler->handleReply(req,time,ip_hdr,source_port, dest_port, l4checksum, payload_len,reply);
	}
	if (false) 
	    printf("rpc reply for prog %d, version %d, proc %d?\n",
		   req->program,req->version,req->procnum);
	delete req->replyhandler;
	rpcHashTable.remove(*req);
    } else {
	// False positives do occur here as we can think something is
	// a reply based solely on a few bytes in the packet.

	// Bumped fraction up to 5% as getting lots of apparent false
	// positives on trace-0/189501

	++counts[possible_missing_request];
	INVARIANT(counts[possible_missing_request] < 2000 || counts[possible_missing_request] < counts[ip_packet] * 0.05, 
		  format("whoa, %d possible reply packets without the request %ld packets so far; you need to tcpdump -s 256+; on %s")
		  % counts[possible_missing_request] % counts[ip_packet] % tracename.c_str());
	if (warn_unmatched_rpc_reply) {
	    // many of these appear to be spurious, e.g. not really an rpc reply
	    cout << format("%d.%09d: unmatched rpc reply xid %08x client %08x\n")
		% Clock::TfracToSec(time) % Clock::TfracToNanoSec(time) 
		% ntohl(reply.xid()) % ntohl(ip_hdr->daddr)
		 << endl;
	}
    }
}


void
handleUDPPacket(Clock::Tfrac time, const struct iphdr *ip_hdr,
	        const unsigned char *p, const unsigned char *pend)
{
    struct udphdr *udp_hdr = (struct udphdr *)p;
    p += 8;

    AssertAlways(p < pend,("short capture?"));
    uint32_t *rpcmsg = (uint32_t *)p;
    if ((p+2*4+2*4) > pend) {
	printf("short packet?!\n");
	return; // can't be RPC, short (error) reply is at least this long
    }
    try { 
	if (rpcmsg[1] == 0) {
	    handleRPCRequest(time,ip_hdr,ntohs(udp_hdr->source),
			     ntohs(udp_hdr->dest),ntohs(udp_hdr->check),
			     ntohs(udp_hdr->len) - 8,
			     p,pend);
	    ++counts[udp_rpc_message];
	} else if (rpcmsg[1] == RPC::net_reply) {
	    handleRPCReply(time,ip_hdr,ntohs(udp_hdr->source),
			   ntohs(udp_hdr->dest),ntohs(udp_hdr->check),
			   ntohs(udp_hdr->len) - 8,
			   p,pend);
	    ++counts[udp_rpc_message];
	} else {
	    if (false) printf("unknown\n");
	    return; // can't be RPC
	}
    } catch (ShortDataInRPCException &err) {
	INVARIANT(ntohs(udp_hdr->len) > pend - p, 
		  "unexpected short message, had everything in one udp packet");
	return;
    }
	
}

void 
handleTCPPacket(Clock::Tfrac time, const struct iphdr *ip_hdr,
		const unsigned char *p, const unsigned char *pend,
		uint32_t capture_size, uint32_t wire_length)
{
    ++counts[tcp_packet];
    
    struct tcphdr *tcp_hdr = (struct tcphdr *)p;

    INVARIANT((int)tcp_hdr->doff * 4 >= (int)sizeof(struct tcphdr),
	      format("bad doff %d %d")
	      % (tcp_hdr->doff * 4) % sizeof(struct tcphdr));
    p += tcp_hdr->doff * 4;
    AssertAlways(p <= pend,("short capture? %p %p",p,pend));
    bool multiple_rpcs = false;
    while((pend-p) >= 4) { // handle multiple RPCs in single TCP message; hope they are aligned to start
	uint32_t rpclen = ntohl(*(uint32_t *)p);
	if (false) printf("  rpclen %x\n",rpclen);
	if ((rpclen & 0x80000000) == 0) {
            // note: the highest bit of the length of an RPC (on TCP)
            // packet is supposed to be set; so if this bit is not
            // set, then it cannot be the beginning of an RPC packet;
            // however, if this packet is part of an RPC but just not
            // the beginning, then we could miss it; we may also get a
            // false positive as it is possible that this packet is
            // not RPC but happens to have the highest bit set;
	    return; 
	}
	rpclen &= 0x7FFFFFFF;
	if (false) printf("  rpclen %d\n",rpclen);
	p += 4;
	uint32_t *rpcmsg = (uint32_t *)p;
	const unsigned char *thismsgend;
	if ((p + rpclen) > pend) { 
	    thismsgend = pend;
	} else {
	    thismsgend = p + rpclen;
	}
	try { // note: rpcmsg[1] (the type is uint32_t) is the
              // call/reply (0/1) field of the RPC headers; however,
              // RPC requests/replies may be broken into multiple
              // packets and the "RPC continuation" packets do not
              // have a header; so this test is not accurate for those
              // packets; the statistics of those packets are
              // reflected by other counters (e.g.,
              // reply-missing-request);
            if (rpcmsg[1] == 0) {
		if (false) printf("tcprpcreq\n");
		counts[rpc_tcp_request_len] += rpclen;
		handleRPCRequest(time,ip_hdr,ntohs(tcp_hdr->source),
				 ntohs(tcp_hdr->dest),ntohs(tcp_hdr->check),
				 rpclen,p,thismsgend);
	    } else if (rpcmsg[1] == RPC::net_reply) {
		if (false) printf("tcprpcrep\n");
		counts[rpc_tcp_reply_len] += rpclen;
		handleRPCReply(time,ip_hdr,ntohs(tcp_hdr->source),
			       ntohs(tcp_hdr->dest),ntohs(tcp_hdr->check),
			       rpclen,p,thismsgend);
	    } else {
		return; // not an rpc
	    }
	} catch (ShortDataInRPCException &err) {
	    // TODO: count all the occurences of this based on the
	    // file,line,message in err and print out a summary at the
	    // end of processing
	    INVARIANT(thismsgend == pend,
		      format("Error, got short data error, but not at end of TCP segment (%p != %p; wire=%d cap=%d)\n message was %s at %s:%d")
		      % reinterpret_cast<const void *>(thismsgend) % reinterpret_cast<const void *>(pend) 
		      % wire_length % capture_size 
		      % err.message % err.filename % err.lineno);
	    ++counts[tcp_short_data_in_rpc];
	} catch (RPC::parse_exception &err) {
            // TODO: give a warning for now, incomplete;
            ++counts[rpc_parse_error];
            std::cout << format("RPC parse error: %s %s %s %d\n") % err.condition % err.message % err.filename % err.lineno;
        }
	++counts[tcp_rpc_message];
	if (multiple_rpcs) {
	    ++counts[tcp_multiple_rpcs];
	}
	multiple_rpcs = true;
	p = thismsgend;
    }    
}
    
const int min_ethernet_header_length = 14;
const int min_ip_header_length = 20;

void 
packetHandler(const unsigned char *packetdata, uint32_t capture_size, uint32_t wire_length, 
	      Clock::Tfrac time)
{
    ++counts[packet_count];
    counts[wire_len] += wire_length;

    if (!bw_info.empty()) {
	packet_bw_rolling_info.push(packetTimeSize(Clock::TfracToTll(time), wire_length));
	if ((counts[packet_count] & 0xFFFF) == 0) {
	    incrementalBandwidthInformation();
	}
    }
    const uint32_t capture_remain = capture_size;
    INVARIANT(capture_remain >= min_ethernet_header_length + min_ip_header_length,
	      format("whoa tiny packet %d") % capture_remain);
    const unsigned char *p = packetdata; 
    const unsigned char *pend = p + capture_size;

    if (false) {
	cout << format("%d.%d: %d/%d bytes: %s") 
	    % Clock::TfracToSec(time) % Clock::TfracToNanoSec(time) 
	    % capture_size % wire_length
	    % hexstring(string((const char *)p,32))
	     << endl;
	return;
    }

    int ethtype = (p[12] << 8) | p[13];
    //1522 is the size that the endace card captures at, 1514+4(vlan tag)+4(crc32?)
    //TODO Jumbo Frame Support
    //TODO Capture file (i.e. TCP) Checksum verification
    //TODO also generate TCP offload warning with high percentage of
    //bad checksums or a packet larger than maximum jumbo frame size.

    if (ethtype < 1500) {
        int protonum = (p[20] << 8) | p[21];

	++counts[weird_ethernet_type];
	cout << format("Weird ethernet type in packet @%ld.%06ld len=%d, wire length %d; proto %d jumbo?")
	    % Clock::TfracToSec(time) % Clock::TfracToNanoSec(time) 
	    % ethtype % wire_length % protonum
	     << endl;

	return;
    }

    int ethernet_header_len = 14;
    p += ethernet_header_len;
    if (ethtype == 0x8100) { // vlan
	ethtype = p[2] << 8 | p[3];
	p += 4;
    }

    if (ethtype == 0x0806) {
	++counts[arp_type];
	return;
    }

    if (ethtype != 0x800) { // IP type, the only one we care about
	cout << format("Ignoring packet %d.%d, ethtype %d")
	    % Clock::TfracToSec(time) % Clock::TfracToNanoSec(time) % ethtype
	     << endl;
	++counts[ignored_nonip];
	return;
    }

    const struct iphdr *ip_hdr = reinterpret_cast<const struct iphdr *>(p);
    INVARIANT(ip_hdr->version == 4,
	      format("Non IPV4 (was V%d) unimplemented\n") % ip_hdr->version);
    int ip_hdrlen = ip_hdr->ihl * 4;
    p += ip_hdrlen;
    INVARIANT(p < pend, "short capture?!\n");
    bool is_fragment = (ntohs(ip_hdr->frag_off) & 0x1FFF) != 0;

    struct udphdr *udp_hdr = NULL;
    struct tcphdr *tcp_hdr = NULL;
    if (ip_hdr->protocol == IPPROTO_UDP && ((p+8) <= pend)) {
	udp_hdr = (struct udphdr *)p;
    } else if (ip_hdr->protocol == IPPROTO_TCP && ((p+sizeof(struct tcphdr)) <= pend)) {
	tcp_hdr = (struct tcphdr *)p;
    } 
    updateIPPacketSeries(time, ntohl(ip_hdr->saddr), ntohl(ip_hdr->daddr), 
			 wire_length, udp_hdr, tcp_hdr, is_fragment);
    
    if (is_fragment) {
	++counts[ip_fragment];
	if (false) printf("fragment %d?\n",ntohs(ip_hdr->frag_off));
	return; // fragment; no reassembly for now
    }

    if (wire_length > 1522) {
	static bool warned;
	if (!warned) {
	    cout << format("Found a long packet of length %d.  Support for jumbo frames is unsupported.\nTCP offload may be enabled.\ndisable under Linux with:\tethtool -K eth# tso off\n") % wire_length;
	    warned = true;
	}

	++counts[long_packets];
	if (udp_hdr && (ntohs(udp_hdr->source) == 2049 
			|| ntohs(udp_hdr->dest) == 2049)) {
	    ++counts[long_packets_port_2049];
	} else if (tcp_hdr && (ntohs(tcp_hdr->source) == 2049
			       || ntohs(tcp_hdr->dest) == 2049)) {
	    ++counts[long_packets_port_2049];
	}
	// Might as well try to process the packet.
    }
	    
    try {
	if (tcp_hdr != NULL) {
	    handleTCPPacket(time, ip_hdr, p, pend, capture_size, wire_length); 
	} else if (ip_hdr->protocol == IPPROTO_UDP) {
	    handleUDPPacket(time, ip_hdr, p, pend);
	} 
    } catch (ShortDataInRPCException &err) {
	printf("parse failed on request at %s:%d (%s) was false: %s\n",
	       err.filename,err.lineno,err.condition.c_str(),
	       err.message.c_str()); // ignore
	FATAL_ERROR(format("got Short Data Error unexpectedly in %s packet")
		    % (tcp_hdr != NULL ? "tcp" : "udp") );
    } catch (RPC::parse_exception &err) {
        bool print_failure = warn_parse_failures;
	if (print_failure && err.condition.find(" == net_rpc_version") < err.condition.size()) {
	    print_failure = false;
	}
	if (print_failure) {
	    printf("parse failed on request at %s:%d (%s) was false: %s\n",
		   err.filename,err.lineno,err.condition.c_str(),
		   err.message.c_str()); // ignore
	}
    }	
}
 
int
get_max_missing_request_count(const char *tracename)
{
    return 1000;
}

unsigned long long
freeDiskBytes(const char *outputname)
{
    struct statvfs buf;

    INVARIANT(statvfs(outputname, &buf) == 0, "bad");
    
    return static_cast<unsigned long long>(buf.f_bavail) * buf.f_bsize;
}

void
doProcess(NettraceReader *from, const char *outputname)
{
    unsigned char *packet;
    uint32_t capture_size, wire_length;
    Clock::Tfrac time;

    prepareBandwidthInformation();
    while(from->nextPacket(&packet, &capture_size, &wire_length, &time)) {
        if (file_type == ERF) {
            // for ERF packets, full packets are typically captured, 
            // hence wire_length should = capture_size; however, capture_size 
            // is rounded to 8 bytes, hence wire_length could be < capture_size
            INVARIANT(wire_length <= capture_size, "bad");
	    if (wire_length < 64) {
		cout << format("weird tiny packet length %d") % wire_length
		     << endl;
		++counts[tiny_packet];
		continue;
	    }
        } else if (file_type == PCAP) {
            INVARIANT(wire_length >= capture_size, "bad packet, wire_length shouldn't < capture_size");
        } else {
	    FATAL_ERROR("nuh uh");
	}
	packetHandler(packet, capture_size, wire_length, time);
	if ((outputname != NULL) && (counts[packet_count] & 0x1FFFFF) == 0) { 
	    // every 2 million packets
	    while(freeDiskBytes(outputname) < 1024*1024*1024) {
		cerr << "Pausing in conversion, free disk space < 1GiB" 
		     << endl;
		sleep(300);
	    }
	    cout << format("Free disk bytes: %d") 
		% freeDiskBytes(outputname)
		 << endl;
	}
    }
    delete from;
    for(rpcHashTableT::iterator i = rpcHashTable.begin();
	i != rpcHashTable.end(); ++i) {
	if (false) {
	    cout << format("outstanding RPC %x to %x, xid %x") 
		% i->client % i->server % i->xid
		 << endl;
	}
	++counts[outstanding_rpcs];
    }
    summarizeBandwidthInformation();
    for(unsigned i=0; i < last_count; ++i) { 
	cout << count_names[i] << " count: " << counts[i] << endl;
	if (mode == Convert) {
	    nfs_convert_stats_outmodule->newRecord();
	    nfs_convert_stats_name.set(count_names[i]);
	    nfs_convert_stats_count.set(counts[i]);
	}
    }
    cout << "first_record_id: " << first_record_id << endl;
    cout << "last_record_id (inclusive): " << cur_record_id << endl;
    if (mode == Convert) {
	nfs_convert_stats_outmodule->newRecord();
	nfs_convert_stats_name.set("first_record_id");
	nfs_convert_stats_count.set(first_record_id);
	nfs_convert_stats_outmodule->newRecord();
	nfs_convert_stats_name.set("last_record_id (inclusive)");
	nfs_convert_stats_count.set(cur_record_id);
    }
}

void
doInfo(NettraceReader *from)
{
    mode = Info;
    doProcess(from, NULL);

    exit(exitvalue);
}

void
doConvert(NettraceReader *from, const char *ds_output_name, 
	  commonPackingArgs &packing_args, int expected_records)
{
    mode = Convert;

    DataSeriesSink *nfsdsout = new DataSeriesSink(ds_output_name, 
						  packing_args.compress_modes, 
						  packing_args.compress_level);
    ExtentTypeLibrary library;

    const ExtentType *nfs_convert_stats_type = 
	library.registerType(nfs_convert_stats_xml);
    nfs_convert_stats_series.setType(*nfs_convert_stats_type);
    nfs_convert_stats_outmodule = 
	new OutputModule(*nfsdsout, nfs_convert_stats_series,
			 nfs_convert_stats_type, packing_args.extent_size);

    const ExtentType *ip_bwrolling_type = library.registerType(ip_bwrolling_xml);
    ip_bwrolling_series.setType(*ip_bwrolling_type);
    ip_bwrolling_outmodule 
	= new OutputModule(*nfsdsout, ip_bwrolling_series,
			   ip_bwrolling_type, packing_args.extent_size);

    const ExtentType *nfs_common_type = library.registerType(nfs_common_xml);
    nfs_common_series.setType(*nfs_common_type);
    nfs_common_outmodule 
	= new OutputModule(*nfsdsout,nfs_common_series,
			   nfs_common_type,packing_args.extent_size);

    const ExtentType *nfs_attrops_type = library.registerType(nfs_attrops_xml);
    nfs_attrops_series.setType(*nfs_attrops_type);
    nfs_attrops_outmodule 
	= new OutputModule(*nfsdsout, nfs_attrops_series,
			   nfs_attrops_type,packing_args.extent_size);

    const ExtentType *nfs_readwrite_type = library.registerType(nfs_readwrite_xml);
    nfs_readwrite_series.setType(*nfs_readwrite_type);
    nfs_readwrite_outmodule 
	= new OutputModule(*nfsdsout, nfs_readwrite_series,
			   nfs_readwrite_type,packing_args.extent_size);

    const ExtentType *ippacket_type = library.registerType(ippacket_xml);
    ippacket_series.setType(*ippacket_type);
    ippacket_outmodule 
	= new OutputModule(*nfsdsout, ippacket_series,
			   ippacket_type, packing_args.extent_size);

    const ExtentType *nfs_mount_type = library.registerType(nfs_mount_xml);
    nfs_mount_series.setType(*nfs_mount_type);
    nfs_mount_outmodule 
	= new OutputModule(*nfsdsout, nfs_mount_series,
			   nfs_mount_type, packing_args.extent_size);

    nfsdsout->writeExtentLibrary(library);

    doProcess(from, ds_output_name);
    
    // Want complete statistics, so flush first
    cout << "flushing extents...\n";
    nfs_convert_stats_outmodule->flushExtent();
    ip_bwrolling_outmodule->flushExtent();
    nfs_common_outmodule->flushExtent();
    nfs_attrops_outmodule->flushExtent();
    nfs_readwrite_outmodule->flushExtent();
    ippacket_outmodule->flushExtent();
    nfs_mount_outmodule->flushExtent();

    cout << "Extent statistics:\n";
    nfs_convert_stats_outmodule->printStats(cout); cout << endl;
    ip_bwrolling_outmodule->printStats(cout); cout << endl;
    nfs_common_outmodule->printStats(cout); cout << endl;
    nfs_attrops_outmodule->printStats(cout); cout << endl;
    nfs_readwrite_outmodule->printStats(cout); cout << endl;
    ippacket_outmodule->printStats(cout); cout << endl;
    nfs_mount_outmodule->printStats(cout); cout << endl;

    delete nfs_convert_stats_outmodule;
    delete ip_bwrolling_outmodule;
    delete nfs_common_outmodule;
    delete nfs_attrops_outmodule;
    delete nfs_readwrite_outmodule;
    delete ippacket_outmodule;
    delete nfs_mount_outmodule;
    delete nfsdsout;

    INVARIANT((cur_record_id + 1 - first_record_id) == expected_records,
	      format("mismatch on expected # records: %d - %d != %d")
	      % cur_record_id % first_record_id % expected_records);
    exit(exitvalue);
}

void
check_file_missing(const string &filename)
{
    struct stat statbuf;
    int ret = stat(filename.c_str(), &statbuf);
    INVARIANT(ret == -1 && errno == ENOENT,
	      format("refusing to run with existing file %s: %s")
	      % filename % strerror(errno));
}

void
uncompressFile(const string &src, const string &dest)
{
    FATAL_ERROR("broke it by doing prefetching");
//    check_file_missing(dest);
//
//    unsigned char *outbuf = NULL;
//    unsigned int outsize = 0;
//
//    ERFReader::openUncompress(src, &outbuf, &outsize);
//
//    int outfd = open(dest.c_str(), O_WRONLY | O_CREAT, 0664);
//    INVARIANT(outfd > 0, format("can not open %s for write: %s") 
//	      % dest % strerror(errno));
//    INVARIANT(outbuf != NULL && outsize > 0, "internal");
//    ssize_t write_amt = write(outfd, outbuf, outsize);
//    INVARIANT(write_amt == outsize, "bad");
//    int ret = close(outfd);
//    INVARIANT(ret == 0, "bad close");
    exit(0);
}

void recompressFileBZ2(const string &src, const string &dest)
{
    check_file_missing(dest);

    ERFReader myerfreader(src);
    
    myerfreader.openUncompress();
    
    char *outbuf = new char[myerfreader.bufsize];
    unsigned int outsize = myerfreader.bufsize;

    int ret = BZ2_bzBuffToBuffCompress(outbuf, &outsize, 
				       reinterpret_cast<char *>(myerfreader.buffer), 
				       myerfreader.bufsize,
				       9,0,0);
    INVARIANT(ret == BZ_OK, "bad");

    INVARIANT(outsize <= myerfreader.bufsize, "bad");
	
    int outfd = open(dest.c_str(), O_WRONLY | O_CREAT, 0664);
    INVARIANT(outfd > 0, format("can not open %s for write: %s") 
	      % dest % strerror(errno));
    INVARIANT(outbuf != NULL && outsize > 0, "internal");
    ssize_t write_amt = write(outfd, outbuf, outsize);
    INVARIANT(write_amt == static_cast<ssize_t>(outsize), "bad");
    ret = close(outfd);
    INVARIANT(ret == 0, "bad close");
    exit(0);
}

void checkERFEqual(const string &src1, const string &src2)
{
    ERFReader erf1(src1);
    ERFReader erf2(src2);
    
    erf1.prefetch();
    erf2.prefetch();
    erf1.openUncompress();
    erf2.openUncompress();
    
    INVARIANT(erf1.bufsize == erf2.bufsize, "different bufsize");
    INVARIANT(memcmp(erf1.buffer, erf2.buffer, erf1.bufsize) == 0, "memcmp failed");
    exit(0);
}


void testBWRolling()
{
    prepareBandwidthInformation();
    if (true) {
	// 1000 bytes/333us = 24.024 Mbits
	for(unsigned i = 0; i<10000000; i+= 333) {
	    packet_bw_rolling_info.push(packetTimeSize(i,1000));
	}
    }

    if (true) {
	// 2000 bytes/100us = 160 (+24 = 184) Mbits for 10% of the time
	for(unsigned i = 5000000; i<6000000; i+= 100) {
	    packet_bw_rolling_info.push(packetTimeSize(i, 2000));
	}
    }
    
    if (true) {
	// 3000 bytes/25us = 960 (+24+160=1144) Mbits for 1% of the time
	for(unsigned i = 5500000; i<5600000; i+= 25) {
	    packet_bw_rolling_info.push(packetTimeSize(i, 3000));
	}
    }
    summarizeBandwidthInformation();
    exit(0);
}

int
main(int argc, char **argv)
{
    if (false) testBWRolling();
    if (argc == 4 && strcmp(argv[1],"--uncompress") == 0) {
	uncompressFile(argv[2],argv[3]);
    }

    if (argc == 4 && strcmp(argv[1],"--recompress-bz2") == 0) {
	recompressFileBZ2(argv[2], argv[3]);
    }
    
    if (argc == 4 && strcmp(argv[1],"--check-erf-equal") == 0) {
	checkERFEqual(argv[2], argv[3]);
    }

    counts.resize(static_cast<unsigned>(last_count));
    INVARIANT(sizeof(count_names)/sizeof(string) == last_count, "bad");
    // Make it hard to run without encryption; don't want to
    // accidentally do that.
    INVARIANT(enable_encrypt_filenames || getenv("DISABLE_ENCRYPTION") != NULL, 
	      "enable_encrypt_filenames must be true or DISABLE_ENCRYPTION env variable set");

    if (argc >= 4) {
	bool info = strcmp(argv[1], "--info") == 0;
	bool conv = strcmp(argv[1], "--convert") == 0;

	if (info || conv) {
	    if (strcmp(argv[2], "--erf") == 0) {
		file_type = ERF;
	    } else if (strcmp(argv[2], "--pcap") == 0) {
		file_type = PCAP;
	    } else {
		FATAL_ERROR(format("expecting --erf or --pcap as second argument, not %s") % argv[2]);
	    }

	    int startFileArg = INT_MAX;
	    MultiFileReader *mfr = new MultiFileReader();

	    if (info) { 
		prepareEncrypt("abcdef0123456789","abcdef0123456789");
		first_record_id = 0;
		cur_record_id = -1;
		startFileArg = 3;
	    }
    
	    commonPackingArgs packing_args;
	    long expected_records = 0;
	    if (conv) { 
		INVARIANT(argc >= 7, "Missing arguments to --convert; try -h for usage");
		if (enable_encrypt_filenames) {
		    prepareEncryptEnvOrRandom();
		}
		getPackingArgs(&argc,argv,&packing_args);
	    
		first_record_id = stringToLongLong(argv[3]);
		cur_record_id = first_record_id - 1;
		expected_records = stringToLong(argv[4]);
		startFileArg = 6;
	    }
	    for(int i = startFileArg;i < argc; ++i) {
		if (file_type == ERF) {
		    mfr->addReader(new ERFReader(argv[i]));
		} else if (file_type == PCAP) {
		    mfr->addReader(new PCAPReader(argv[i]));
		}
	    }
	
	    if (info) {
		doInfo(mfr);
	    } else if (conv) {
		doConvert(mfr, argv[5], packing_args, expected_records);
	    }
	}
    }
    FATAL_ERROR("usage: --uncompress <input-erf> <output-erf>\n"
	    "       --info --erf <input-erf...>\n"
	    "       --info --pcap <input-pcap...>\n"
	    "       --convert --erf <first-record-num> <expected-record-count> <output-ds-name> <input-erf...>\n"
	    "       --convert --pcap <first-record-num> <expected-record-count> <output-ds-name> <input-pcap...>\n");
}

