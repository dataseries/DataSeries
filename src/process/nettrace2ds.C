/* -*-C++-*-
*******************************************************************************
*
* File:         tcpdump2ds.C
* RCS:          $Header: /mount/cello/cvs/Grizzly/cpp/new-tcpdump2ds/tcpdump2ds.C,v 1.6 2004/12/22 01:38:27 anderse Exp $
* Description:  NFS tcpdump to data series convert; derived from nfsdump
* Author:       Eric Anderson
* Created:      Sat Aug 16 19:07:09 2003
* Modified:     Thu Dec 16 21:44:16 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

// Note: this code uses two methods for connecting requests to
// replies, a specific one for handling V2 attribute operations, and a
// general one for the rest.  the specific method for handling the v2
// attr ops should eventually go away.

// TODO: decide whether we should put the output from the 
// summarizeBandwidthInformation() run in the dataseries output -- it is
// completely re-calculable from the wire-length analysis, so I think
// the answer is no.

// For TCP stream reassembly: 
// http://www.circlemud.org/~jelson/software/tcpflow/

using namespace std;

const bool warn_duplicate_reqs = false;
const bool warn_parse_failures = false;
const bool warn_unmatched_rpc_reply = false; // not watching output
const int max_mismatch_duplicate_requests = 15;
int max_missing_request_count = 20000;
int missing_request_count = 0;

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

#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/udp.h>
#include <netinet/tcp.h>

#include <string>

#include <Lintel/HashTable.H>
#include <Lintel/AssertException.H>
#include <Lintel/StringUtil.H>
#include <Lintel/PriorityQueue.H>
#include <Lintel/StatsQuantile.H>
#include <Lintel/Deque.H>

#include <DataSeries/commonargs.H>
#include <DataSeries/DataSeriesModule.H>

#include <nfs_prot.h>
#include <cryptutil.H>
extern "C" {
#include <liblzf-1.6/lzf.h>
}
// #include "known-bad.H"

#if defined(bswap_64)
inline u_int64_t ntohll(u_int64_t in)
{
    return bswap_64(in);
}
#else
#error "don't know how to do ntohll"
#endif

struct network_error_listT {
  ExtentType::int64 h_rid, v_rid;
  unsigned int h_hash, v_hash;
  unsigned int server, client;
};

// For weird reasons, traces will get re-transmissions with the same
// source, dest, xid but with different content.

vector<network_error_listT> known_bad_list; 

int exitvalue = 0;
char *tracename;

ExtentType::int64 cur_record_id = -1000000000;
ExtentType::int64 first_record_id = -1000000000;
int cur_mismatch_duplicate_requests = 0;
int tcp_rpc_message_count = 0;
int tcp_short_data_in_rpc_count = 0;

#define CONSTANTHOSTNETSWAP(v) ((((uint32_t)(v) >> 24) & 0xFF) | \
                               (((uint32_t)(v)>>8) & 0xFF00) | \
        		       (((uint32_t)(v) & 0xFF00) << 8) | \
        		       (((uint32_t)(v) & 0xFF) << 24))

#define RPCParseAssert(condition) \
  ( (condition) ? (void)0 : throw RPC::parse_exception(#condition, "", __FILE__, __LINE__) )
#define RPCParseAssertMsg(condition,message) \
  ( (condition) ? (void)0 : throw RPC::parse_exception(#condition, AssertExceptionT::stringPrintF message, __FILE__, __LINE__) )

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
    RPC(void *bytes, unsigned _len)
	: rpchdr((uint32_t *)bytes), len(_len)
    {
	RPCParseAssert(_len >= 8);
	RPCParseAssert(rpchdr[1] == 0 || rpchdr[1] == net_reply);
    }
    uint32_t &xid() { return rpchdr[0]; }
    bool is_request() { return rpchdr[1] == 0; }
    void *duppacket() { 
	u_char *ret = new u_char[len];
	memcpy(ret,rpchdr,len);
	return ret;
    }
    unsigned getlen() { 
	return len;
    }
    uint32_t *getxdr() {
	return rpchdr;
    }
protected:
    uint32_t *rpchdr;
    unsigned len;

    virtual void enable_dynamic_cast() { };
};

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
    RPCRequest(void *bytes, int _len) : RPC(bytes,_len), orig_xid(xid()) {
	RPCParseAssert(len >= 10*4);
	RPCParseAssert(rpchdr[1] == 0);
	RPCParseAssert(rpchdr[2] == net_rpc_version);
	if (rpchdr[6] == 0) {
	    // auth_none
	    RPCParseAssert(rpchdr[7] == 0 && rpchdr[8] == 0 && rpchdr[9] == 0);
	    rpcparam = rpchdr + 10;
	    auth_sys_uid = NULL;
	    auth_sys_len = 0;
	} else if (rpchdr[6] == net_auth_sys) {
	    uint32_t *cur_pos = rpchdr + 7;
	    unsigned auth_credlen = ntohl(*cur_pos);
	    ++cur_pos; 
	    RPCParseAssert(auth_credlen >= 5*4 && len >= 10*4 + auth_credlen);
	    ++cur_pos; // stamp
	    unsigned hostname_len = ntohl(*cur_pos);
	    ++cur_pos;
	    hostname_len = roundup4(hostname_len);
	    AssertAlways(auth_credlen >= 5*4 + hostname_len,("bad\n"));
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
	    printf("bytes starting @%d: ",f-(unsigned char *)rpchdr);
	    for(unsigned i=0;i<rpcparamlen;++i) {
		printf("%02x ",f[i]);
	    }
	    printf("\n");
	}
    }
	
    uint32_t net_prognum() { return rpchdr[3]; }
    uint32_t host_prognum() { return ntohl(rpchdr[3]); }
    uint32_t net_version() { return rpchdr[4]; }
    uint32_t host_version() { return ntohl(rpchdr[4]); }
    uint32_t net_procnum() { return rpchdr[5]; }
    uint32_t host_procnum() { return ntohl(net_procnum()); }
    bool isauth_none() { return auth_sys_uid == NULL; }
    uint32_t *getrpcparam() { return rpcparam; }
    unsigned getrpcparamlen() { return rpcparamlen; }
protected:
    uint32_t *auth_sys_uid, *rpcparam;
    const uint32_t orig_xid;
    unsigned auth_sys_len, rpcparamlen; // both count in bytes!
};

class RPCReply : public RPC {
public:
    static const uint32_t net_msg_denied = CONSTANTHOSTNETSWAP(1);
    static const uint32_t net_auth_error = CONSTANTHOSTNETSWAP(1);
    
    RPCReply(void *bytes, int _len) : RPC(bytes,len) {
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
    u_int32_t *getrpcresults() { return rpc_results; }
    int getrpcresultslen() { return results_len; }
protected:
    u_int32_t *rpc_results;
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

    bool mountok() { 
	return rpc_results[0] == 0;
    }
    u_int32_t mounterror() {
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

const string nfs_common_xml(
  "<ExtentType name=\"NFS trace: common\">\n"
  "  <field type=\"int64\" name=\"packet-at\" comment=\"time in nanoseconds since UNIX epoch; time is of the first fragment of this packet\" pack_relative=\"packet-at\" print_divisor=\"1000\" />\n"
  "  <field type=\"int32\" name=\"source\" comment=\"32 bit packed IPV4 address\" print_format=\"%08x\" />\n"
  "  <field type=\"int32\" name=\"source-port\" />\n"
  "  <field type=\"int32\" name=\"dest\" comment=\"32 bit packed IPV4 address\" print_format=\"%08x\" />\n"
  "  <field type=\"int32\" name=\"dest-port\" />\n"
  "  <field type=\"bool\" name=\"is-udp\" print_true=\"UDP\" print_false=\"TCP\" />\n"
  "  <field type=\"bool\" name=\"is-request\" print_true=\"request\" print_false=\"response\" />\n"
  "  <field type=\"byte\" name=\"nfs-version\" print_format=\"V%d\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"transaction-id\" print_format=\"%08x\" />\n"
  //  "  <field type=\"int32\" name=\"euid\" opt_nullable=\"yes\" />\n"
  //  "  <field type=\"int32\" name=\"egid\" opt_nullable=\"yes\" />\n"
  "  <field type=\"byte\" name=\"op-id\" opt_nullable=\"yes\" note=\"op-id is nfs-version dependent\" />\n"
  "  <field type=\"variable32\" name=\"operation\" pack_unique=\"yes\" />\n"
  "  <field type=\"int32\" name=\"rpc-status\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"payload-length\" />\n"
  "  <field type=\"int64\" name=\"record-id\" comment=\"for correlating with the records in other extent types\" pack_relative=\"record-id\" />\n"
  "</ExtentType>\n"
);

ExtentSeries nfs_common_series;
OutputModule *nfs_common_outmodule;
Int64Field packet_at(nfs_common_series,"packet-at");
Int32Field source(nfs_common_series,"source");
Int32Field source_port(nfs_common_series,"source-port");
Int32Field dest(nfs_common_series,"dest");
Int32Field dest_port(nfs_common_series,"dest-port");
BoolField is_udp(nfs_common_series,"is-udp");
BoolField is_request(nfs_common_series,"is-request");
ByteField nfs_version(nfs_common_series,"nfs-version",Field::flag_nullable);
Int32Field xid(nfs_common_series,"transaction-id");
// Int32Field euid(nfs_common_series,"euid",Field::flag_nullable);
// Int32Field egid(nfs_common_series,"egid",Field::flag_nullable);
ByteField opid(nfs_common_series,"op-id",Field::flag_nullable);
Variable32Field operation(nfs_common_series,"operation");
Int32Field rpc_status(nfs_common_series,"rpc-status",Field::flag_nullable);
Int32Field payload_length(nfs_common_series,"payload-length");
Int64Field common_record_id(nfs_common_series,"record-id");

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
  "<ExtentType name=\"NFS trace: attr-ops\">\n"
  "  <field type=\"int64\" name=\"request-id\" comment=\"for correlating with the records in other extent types\" pack_relative=\"request-id\" />\n"
  "  <field type=\"int64\" name=\"reply-id\" comment=\"for correlating with the records in other extent types\" pack_relative=\"request-id\" />\n"
  "  <field type=\"variable32\" name=\"filename\" opt_nullable=\"yes\" pack_unique=\"yes\" print_style=\"maybehex\" />\n"
  "  <field type=\"variable32\" name=\"filehandle\" print_style=\"hex\" pack_unique=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"lookup-dir-filehandle\" print_style=\"hex\" pack_unique=\"yes\" opt_nullable=\"yes\" />\n"
  "  <field type=\"byte\" name=\"typeid\" />\n"
  "  <field type=\"variable32\" name=\"type\" pack_unique=\"yes\" />\n"
  "  <field type=\"int32\" name=\"mode\" comment=\"only includes bits from 0xFFF down, so type is not reproduced here\" print_format=\"%x\" />\n"
  "  <field type=\"int32\" name=\"uid\" />\n"
  "  <field type=\"int32\" name=\"gid\" />\n"
  "  <field type=\"int64\" name=\"file-size\" />\n"
  "  <field type=\"int64\" name=\"used-bytes\" />\n"
  "  <field type=\"int64\" name=\"modify-time\" pack_relative=\"modify-time\" comment=\"time in ns since Unix epoch; doubles don't have enough precision to represent a year in ns and NFSv3 gives us ns precision modify times\" />\n"
  "</ExtentType>\n");

ExtentSeries nfs_attrops_series;
OutputModule *nfs_attrops_outmodule;
Int64Field attrops_request_id(nfs_attrops_series,"request-id");
Int64Field attrops_reply_id(nfs_attrops_series,"reply-id");
Variable32Field attrops_filename(nfs_attrops_series,"filename", Field::flag_nullable);
Variable32Field attrops_filehandle(nfs_attrops_series,"filehandle");
Variable32Field attrops_lookupdirfilehandle(nfs_attrops_series,"lookup-dir-filehandle", Field::flag_nullable);
ByteField attrops_typeid(nfs_attrops_series,"typeid");
Variable32Field attrops_type(nfs_attrops_series,"type");
Int32Field attrops_mode(nfs_attrops_series,"mode");
Int32Field attrops_uid(nfs_attrops_series,"uid");
Int32Field attrops_gid(nfs_attrops_series,"gid");
Int64Field attrops_filesize(nfs_attrops_series,"file-size");
Int64Field attrops_used_bytes(nfs_attrops_series,"used-bytes");
Int64Field attrops_modify_time(nfs_attrops_series,"modify-time",
				DoubleField::flag_allownonzerobase);

// struct attropData {
//     ExtentType::int32 server_id, client_id, xid;
//     ExtentType::int64 request_at;
//     string filehandle, lookup_directory_filehandle;
//     ExtentType::int64 request_id;
//     string filename;
//     unsigned int rpcreqhashval; // for sanity checking of duplicate requests
//     unsigned int ipchecksum, l4checksum;
//     void set(ExtentType::int32 a, ExtentType::int32 b, ExtentType::int32 c)
//     { server_id = a; client_id = b; xid = c; 
//       rpcreqhashval = (unsigned int)this;}
//     void set(ExtentType::int32 a, ExtentType::int32 b, ExtentType::int32 c,
// 	     ExtentType::int64 d, u_int32_t *e, int elen, ExtentType::int64 f,
// 	     const string &g)
//     { server_id = a; client_id = b; xid = c; request_at = d; 
//       filehandle.assign((char *)e,elen); request_id = f; filename = g;
//       rpcreqhashval = (unsigned int)this;}
//       
// };

// class attropDataHash {
// public:
//     unsigned int operator()(const attropData &k) {
// 	    unsigned ret,a,b;
// 	    ret = k.xid;
// 	    a = k.server_id;
// 	    b = k.client_id;
// 	    BobJenkinsHashMix(a,b,ret);
// 	    return ret;
//     }
// };
// 
// class attropDataEqual {
// public:
//     bool operator()(const attropData &a, const attropData &b) {
// 	return a.server_id == b.server_id && a.client_id == b.client_id &&
// 	    a.xid == b.xid;
//     }
// };
// 
// HashTable<attropData,attropDataHash,attropDataEqual> attropHashTable;

const string nfs_readwrite_xml(
  "<ExtentType name=\"NFS trace: read-write\">\n"
  "  <field type=\"int64\" name=\"request-id\" comment=\"for correlating with the records in other extent types\" pack_relative=\"request-id\" />\n"
  "  <field type=\"int64\" name=\"reply-id\" comment=\"for correlating with the records in other extent types\" pack_relative=\"request-id\" />\n"
  "  <field type=\"variable32\" name=\"filehandle\" print_style=\"hex\" pack_unique=\"yes\" />\n"
  "  <field type=\"bool\" name=\"is-read\" />\n"
  "  <field type=\"int64\" name=\"offset\" />\n"
  "  <field type=\"int32\" name=\"bytes\" />\n"
  "</ExtentType>\n");

ExtentSeries nfs_readwrite_series;
OutputModule *nfs_readwrite_outmodule;
Int64Field readwrite_request_id(nfs_readwrite_series,"request-id");
Int64Field readwrite_reply_id(nfs_readwrite_series,"reply-id");
Variable32Field readwrite_filehandle(nfs_readwrite_series,"filehandle");
BoolField readwrite_is_read(nfs_readwrite_series,"is-read");
Int64Field readwrite_offset(nfs_readwrite_series,"offset");
Int32Field readwrite_bytes(nfs_readwrite_series,"bytes");

const string ippacket_xml(
  "<ExtentType name=\"Network trace: IP packets\">\n"
  "  <field type=\"int64\" name=\"packet-at\" pack_relative=\"packet-at\" comment=\"in nanoseconds since unix epoch\" print_divisor=\"1000\" />\n"
  "  <field type=\"int32\" name=\"source\" print_format=\"%08x\" />\n"
  "  <field type=\"int32\" name=\"destination\" print_format=\"%08x\" />\n"
  "  <field type=\"int32\" name=\"wire-length\" />\n"
  "  <field type=\"bool\" name=\"udp-tcp\" opt_nullable=\"yes\" comment=\"true on udp, false on tcp, null on neither\" />\n"
  "  <field type=\"int32\" name=\"source-port\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"destination-port\" opt_nullable=\"yes\" />\n"
  "  <field type=\"bool\" name=\"is-fragment\" />\n"
  "  <field type=\"int32\" name=\"tcp-seqnum\" opt_nullable=\"yes\" />\n"
  "</ExtentType>\n");

ExtentSeries ippacket_series;
OutputModule *ippacket_outmodule;
Int64Field ippacket_packet_at(ippacket_series,"packet-at");
Int32Field ippacket_source(ippacket_series,"source");
Int32Field ippacket_destination(ippacket_series,"destination");
Int32Field ippacket_wire_length(ippacket_series,"wire-length");
BoolField ippacket_udp_tcp(ippacket_series,"udp-tcp",Field::flag_nullable);
Int32Field ippacket_source_port(ippacket_series,"source-port",Field::flag_nullable);
Int32Field ippacket_destination_port(ippacket_series,"destination-port",Field::flag_nullable);
BoolField ippacket_is_fragment(ippacket_series,"is-fragment");
Int32Field ippacket_tcp_seqnum(ippacket_series,"tcp-seqnum",Field::flag_nullable);

const string nfs_mount_xml(
  "<ExtentType name=\"NFS trace: mount\">\n"
  "  <field type=\"int64\" name=\"request-at\" pack_relative=\"request-at\" comment=\"in nanoseconds since unix epoch\" print_divisor=\"1000\" />\n"
  "  <field type=\"int64\" name=\"reply-at\" pack_relative=\"request-at\" comment=\"in nanoseconds since unix epoch\" print_divisor=\"1000\" />\n"
  "  <field type=\"int32\" name=\"server\" print_format=\"%08x\" />\n"
  "  <field type=\"int32\" name=\"client\" print_format=\"%08x\" />\n"
  "  <field type=\"variable32\" name=\"pathname\" pack_unique=\"yes\" print_style=\"maybehex\" />\n"
  "  <field type=\"variable32\" name=\"filehandle\" print_style=\"hex\" pack_unique=\"yes\" />\n"
  "</ExtentType>\n");

ExtentSeries nfs_mount_series;
OutputModule *nfs_mount_outmodule;
Int64Field nfs_mount_request_at(nfs_mount_series,"request-at");
Int64Field nfs_mount_reply_at(nfs_mount_series,"reply-at");
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
    ExtentType::int64 interval_width, update_step, cur_time;
    double dbl_interval_width, cur_bytes_in_queue;
    Deque<packetTimeSize> packets_in_flight;
    StatsQuantile bytes_per_second;
    void update(ExtentType::int64 packet_us, int packet_size) {
	AssertAlways(packets_in_flight.empty() || 
		     packet_us >= packets_in_flight.back().timestamp_us,("internal"));
	while ((packet_us - cur_time) > interval_width) {
	    // update statistics for the interval from cur_time to cur_time + interval_width
	    // all packets in p_i_f must have been recieved in that interval
	    bytes_per_second.add(cur_bytes_in_queue/dbl_interval_width);
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
    bandwidth_rolling(ExtentType::int64 a, ExtentType::int64 start_time, 
		      int substep_count = 20) 
	       : interval_width(a), update_step(a/substep_count), cur_time(start_time), 
	dbl_interval_width(1024.0*1024.0*(double)a/1.0e6), cur_bytes_in_queue(0) { 
	AssertAlways(substep_count > 0,("internal"));
    }
};

void
summarizeBandwidthInformation()
{
    vector<bandwidth_rolling *> bw_info;
    bw_info.push_back(new bandwidth_rolling(1000,packet_bw_rolling_info.top().timestamp_us));
    bw_info.push_back(new bandwidth_rolling(10000,packet_bw_rolling_info.top().timestamp_us));
    bw_info.push_back(new bandwidth_rolling(100000,packet_bw_rolling_info.top().timestamp_us));
    bw_info.push_back(new bandwidth_rolling(1000000,packet_bw_rolling_info.top().timestamp_us));
    bw_info.push_back(new bandwidth_rolling(5000000,packet_bw_rolling_info.top().timestamp_us));
    bw_info.push_back(new bandwidth_rolling(15000000,packet_bw_rolling_info.top().timestamp_us));
    bw_info.push_back(new bandwidth_rolling(60000000,packet_bw_rolling_info.top().timestamp_us));

    while(packet_bw_rolling_info.empty() == false) {
	for(unsigned i = 0;i<bw_info.size();++i) {
	    bw_info[i]->update(packet_bw_rolling_info.top().timestamp_us,packet_bw_rolling_info.top().packetsize);
	}
	packet_bw_rolling_info.pop();
    }
    for(unsigned i = 0;i<bw_info.size();++i) {
	if (bw_info[i]->bytes_per_second.count() > 0) {
	    printf("MB/s for interval len of %lldus with samples every %lldus\n",
		   bw_info[i]->interval_width, bw_info[i]->update_step);
	    bw_info[i]->bytes_per_second.printFile(stdout);
	    bw_info[i]->bytes_per_second.printTail(stdout);
	    printf("\n");
	}
    }
}

void testBWRolling()
{
    if (true)
	return;
    for(unsigned i = 0;i<10000000;i+= 333) {
	packet_bw_rolling_info.push(packetTimeSize(i,1000));
    }
    for(unsigned i = 500000;i<600000;i+= 333) {
	packet_bw_rolling_info.push(packetTimeSize(i,1000));
    }
    summarizeBandwidthInformation();
    exit(0);
}

inline ExtentType::int64 xdr_ll(u_int32_t *xdr,int offset)
{
    return ntohll(*(u_int64_t *)(xdr + offset));
}

string 
getLookupFilename(u_int32_t *xdr, int remain_len)
{
    AssertAlways(remain_len >= 4,("bad1 %d\n",remain_len));
    u_int32_t strlen = ntohl(xdr[0]);
    // Changed to an assertion to handle set-6/cqracks.19352
    RPCParseAssertMsg(remain_len == (int)(strlen + (4 - (strlen % 4))%4 + 4),
		      ("bad2 %d != roundup4(%d) @ record %lld\n",
		       remain_len,strlen,common_record_id.val()));
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
getNFS2Time(u_int32_t *xdr_time)
{
    ExtentType::int64 seconds = ntohl(xdr_time[0]);
    ExtentType::int64 useconds = ntohl(xdr_time[1]);
    return seconds * 1000 * 1000 * 1000 + useconds * 1000;
}

ExtentType::int64
getNFS3Time(u_int32_t *xdr_time)
{
    ExtentType::int64 seconds = ntohl(xdr_time[0]);
    ExtentType::int64 nseconds = ntohl(xdr_time[1]);
    return seconds * 1000 * 1000 * 1000 + nseconds;
}

class RPCReplyHandler;

struct RPCRequestData {
    u_int32_t client, server, xid;
    ExtentType::int64 request_id;
    u_int32_t program, version, procnum;
    ExtentType::int64 request_at;
    unsigned int rpcreqhashval; // for sanity checking of duplicate requests
    unsigned int ipchecksum, l4checksum;
    RPCRequest *reqdata;
    RPCReplyHandler *replyhandler;
    RPCRequestData(u_int32_t a, u_int32_t b, u_int32_t c) 
      : client(a), server(b), xid(c) {}
};

class RPCRequestDataHash {
public:
    unsigned int operator()(const RPCRequestData &k) {
      unsigned ret,a,b;
      ret = k.xid;
      a = k.server;
      b = k.client;
      BobJenkinsHashMix(a,b,ret);
      return ret;
    }
};

class RPCRequestDataEqual {
public:
    bool operator()(const RPCRequestData &a, const RPCRequestData &b) {
	return a.client == b.client && a.server == b.server && a.xid == b.xid;
    }
};

HashTable<RPCRequestData, RPCRequestDataHash, RPCRequestDataEqual> rpcHashTable;

class RPCReplyHandler {
public:
    RPCReplyHandler() { }

    virtual ~RPCReplyHandler() {
    }
    virtual void handleReply(RPCRequestData *reqdata, 
			     const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
			     int source_port, int dest_port, int l4checksum, 
			     int payload_len, RPCReply &reply) = 0;
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
			   const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
			   int source_port, int dest_port, int l4checksum, int payload_len,
			   RPCReply &reply)
  {
    u_int32_t *xdr = reply.getrpcresults();
    int actual_len = reply.getrpcresultslen();
    AssertAlways(reply.status() == 0,("request not accepted?!\n"));
    u_int32_t op_status = ntohl(*xdr);
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
      readwrite_reply_id.set(common_record_id.val());
      readwrite_filehandle.set(filehandle);
      readwrite_is_read.set(is_read);
      readwrite_offset.set(offset);
      readwrite_bytes.set(reqbytes);
      
      if (is_read) {
	AssertAlways(actual_len >= 17*4 + 4,("bad %d",actual_len));
	u_int32_t actual_bytes = ntohl(xdr[17]);
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
			     const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
			     int source_port, int dest_port, int l4checksum, int payload_len,
			     RPCReply &reply) 
    {
	AssertAlways(reply.status() == 0,("request not accepted?!\n"));
	int v3fattroffset = getfattroffset(reqdata,reply);
	if (v3fattroffset >= 0) {
	    nfs_attrops_outmodule->newRecord();
	    attrops_request_id.set(reqdata->request_id);
	    attrops_reply_id.set(common_record_id.val());
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
	    ShortDataAssertMsg(v3fattroffset * 4 + fattr3_len <= reply.getrpcresultslen(),
			       "NFSv3 attribute op",
			       ("%d * 4 + %d <= %d",v3fattroffset,fattr3_len,reply.getrpcresultslen()));
	    u_int32_t *xdr = reply.getrpcresults();
	    xdr += v3fattroffset;
	    int type = ntohl(xdr[0]);
	    AssertAlways(type >= 1 && type < 8,("bad"));
	    attrops_typeid.set(type);
	    attrops_type.set(NFSV3_typelist[type]);
	    attrops_mode.set(ntohl(xdr[1] & 0xFFF));
	    attrops_uid.set(ntohl(xdr[3]));
	    attrops_gid.set(ntohl(xdr[4]));
	    attrops_filesize.set(xdr_ll(xdr,5));
	    attrops_used_bytes.set(xdr_ll(xdr,7));
	    attrops_modify_time.set(getNFS3Time(xdr+17));
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
	u_int32_t *xdr = reply.getrpcresults();
	int actual_len = reply.getrpcresultslen();
	ShortDataAssertMsg(actual_len >= 4,"NFSv3 getattr reply",
			   ("bad %d", actual_len));
	u_int32_t op_status = ntohl(*xdr);
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

    void checkOpStatus(u_int32_t op_status) {
	AssertAlways(op_status == 2 || // enoent
		     op_status == 13 || // eaccess
		     op_status == 70, // estale
		     ("bad11 %d",op_status)); 
	// might at some point want to record failed lookups, as per the NFSV2 
	// decode also
    }

    virtual void handleReply(RPCRequestData *reqdata, 
			     const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
			     int source_port, int dest_port, int l4checksum, int payload_len,
			     RPCReply &reply) 
    {
	u_int32_t *xdr = reply.getrpcresults();
	int actual_len = reply.getrpcresultslen();
	AssertAlways(actual_len >= 4,("bad"));
	u_int32_t op_status = ntohl(*xdr);
	if (op_status != 0) {
	    checkOpStatus(op_status);
	    return;
	}
	int fhlen = ntohl(xdr[1]);
	AssertAlways(fhlen >= 4 && (fhlen % 4) == 0,("bad"));
	ShortDataAssertMsg(actual_len >= 4 + 4 + fhlen,"NFSv3 lookup reply",("bad"));
	filehandle.assign((char *)xdr + 8,fhlen);
	NFSV3AttrOpReplyHandler::handleReply(reqdata,h,ip_hdr,source_port,dest_port,
					     l4checksum,payload_len,reply);
    }

    virtual int getfattroffset(RPCRequestData *reqdata,
			       RPCReply &reply) 
    {
	u_int32_t *xdr = reply.getrpcresults();
	int actual_len = reply.getrpcresultslen();
	AssertAlways(actual_len >= 4,("bad"));
	u_int32_t op_status = ntohl(*xdr);
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
	u_int32_t *xdr = reply.getrpcresults();
	int actual_len = reply.getrpcresultslen();

	ShortDataAssertMsg(actual_len >= 4,"NFSv3 R/W reply",
			   ("actual_len %d",actual_len));
	u_int32_t op_status = ntohl(*xdr);
	xdr += 1; 
	actual_len -= 4;

	if (op_status != 0) {
	    if (is_read) {
		  AssertAlways(op_status == 70 || // stale file handle
			       op_status == 13, // permission denied
			       ("bad12 %d", op_status)); 
	    } else {
		  AssertAlways(op_status == 70 || // stale file handle
			       op_status == 28 || // out of space
			       op_status == 69, // disk quota exceeded
			       ("bad12 op_status = %d",op_status)); 
	    }
	    return -1;
	}
	if (is_read) {
	    ShortDataAssertMsg(actual_len >= 4,is_read ? "NFSv3 read reply" : "NFSv3 write reply",
			       ("actual len %d",actual_len));
	    AssertAlways(ntohl(*xdr) == 1,("bad"));
	    ShortDataAssertMsg(actual_len >= 4 + fattr3_len,is_read ? "NFSv3 read reply" : "NFSv3 write reply",
			       ("actual len %d",actual_len));
	    return 2;
	} else {
	    AssertAlways(actual_len == 4 + 3*8 + 4 + fattr3_len + 4 + 4 + 8,
			 ("bad"));
	    return 1 + 1 + 3*2 + 1;
	}
    }

    virtual void handleReply(RPCRequestData *reqdata, 
			     const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
			     int source_port, int dest_port, int l4checksum, int payload_len,
			     RPCReply &reply) 
    {
	NFSV3AttrOpReplyHandler::handleReply(reqdata,h,ip_hdr,
					     source_port,dest_port,l4checksum,
					     payload_len, reply);
	u_int32_t *xdr = reply.getrpcresults();
	int actual_len = reply.getrpcresultslen();
	AssertAlways(reply.status() == 0,("request not accepted?!\n"));
	u_int32_t op_status = ntohl(*xdr);
	xdr += 1;
	actual_len -= 4;
	if (op_status != 0) {
	    if (is_read) {
		AssertAlways(op_status == 70 || // stale file handle
			     op_status == 13, // permission denied
			     ("bad12 %d", op_status)); 
	    } else {
		AssertAlways(op_status == 70 || // stale file handle
			     op_status == 28 || // out of space
			     op_status == 69, // disk quota exceeded
			     ("bad12 op_status = %d",op_status)); 
	    }
	} else {
	    nfs_readwrite_outmodule->newRecord();
	    readwrite_request_id.set(reqdata->request_id);
	    readwrite_reply_id.set(common_record_id.val());
	    readwrite_filehandle.set(filehandle);
	    readwrite_is_read.set(is_read);
	    readwrite_offset.set(offset);
	    readwrite_bytes.set(reqbytes);
	 
	    if (is_read) {
		ShortDataAssertMsg(actual_len >= 4 + fattr3_len + 4 + 4 + 4,
				   "NFSv3 Read Reply",("bad %d",actual_len));
		u_int32_t actual_bytes = ntohl(xdr[1+fattr3_len/4]);
		AssertAlways(reqbytes >= (ExtentType::int32)actual_bytes,
			     ("wrong %d %d\n",reqbytes,actual_bytes));
		readwrite_bytes.set(actual_bytes);
	    } else {
		AssertAlways(actual_len == 4 + 3*8 + 4 + fattr3_len + 4 + 4 + 8,
			     ("bad"));
		u_int32_t actual_bytes = ntohl(xdr[1+3*2+1+fattr3_len/4]);
		AssertAlways((int)actual_bytes == reqbytes,("bad\n"));
		readwrite_bytes.set(reqbytes);
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
maybeDumpRecord(uint32_t *xdr, int actual_len)
{
    if (common_record_id.val() == hex_dump_id_1 ||
	common_record_id.val() == hex_dump_id_2) {
      printf("hex dump request op = %d; %lld %d:\n",
	     opid.val(),common_record_id.val(),actual_len);
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
handleNFSV2Request(const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
		   int ipchecksum, int l4checksum, RPCRequest &req, RPCRequestData &d)
{
    FATAL_ERROR("unimplemented"); // needs to be re-done with the correct new style...

//    uint32_t *xdr = req.getrpcparam();
//    int actual_len = req.getrpcparamlen();
//
//    attropData val;
//    val.server_id = -1;
//    maybeDumpRecord(xdr,actual_len);
//    switch(opid.val()) 
//	{
//	case NFSPROC_GETATTR: 
//	    {
//		if (false) printf("v2GetAttr %lld %8x -> %8x; %d\n",
//				  packet_at.val(),source.val(),dest.val(),actual_len);
//		// Changed to assertion to handle set-6/cqracks.23919
//		RPCParseAssertMsg(actual_len == 32,
//				  ("bad getattr request @%ld.%06ld rec#%lld: %d",
//				   h->ts.tv_sec,h->ts.tv_usec,
//				   common_record_id.val(),actual_len));
//		val.set(dest.val(),source.val(),xid.val(),
//			packet_at.val(),xdr,32,common_record_id.val(),
//			empty_string);
//	    }
//	    break;
//	case NFSPROC_LOOKUP: 
//	    {
//		ShortDataAssertMsg(actual_len >= 32,
//				   "NFSv2 Lookup Request",
//				   ("bad read len %d @%ld.%06ld/%d; %d %d",actual_len,
//				    h->ts.tv_sec,h->ts.tv_usec,h->len,
//				    ip_hdr->protocol,IPPROTO_UDP));
//		string filename = getLookupFilename(xdr+8,actual_len - 32);
//		if (false) printf("v2Lookup %lld %8x -> %8x; %d; %s\n",
//				  packet_at.val(),source.val(),dest.val(),
//				  actual_len,filename.c_str());
//		val.set(dest.val(),source.val(),xid.val(),
//			packet_at.val(),xdr,0,common_record_id.val(),
//			filename);
//		val.lookup_directory_filehandle.assign((char *)xdr,32);
//	    }
//	    break;
//	case NFSPROC_READ:
//	    { 
//		ShortDataAssertMsg(actual_len >= (32+4+4+4),
//				   "NFSv2 Read Request",
//				   ("bad read len %d @%ld.%06ld/%d; %d %d",actual_len,
//				    h->ts.tv_sec,h->ts.tv_usec,h->len,
//				    ip_hdr->protocol,IPPROTO_UDP));
//		val.set(dest.val(),source.val(),xid.val(),
//			packet_at.val(),xdr,32,common_record_id.val(),
//			empty_string);
//		string foo;
//		foo.assign((char *)xdr,32);
//		d.replyhandler = new NFSV2ReadWriteReplyHandler(common_record_id.val(),
//								foo, true,
//								ntohl(xdr[8]), ntohl(xdr[9]));
//	    }
//	    break;
//	case NFSPROC_WRITE:
//	    {
//		ShortDataAssertMsg(actual_len >= 32+4+4+4+4,
//				   "NFSv2 Write Request",
//				   ("bad read len %d @%ld.%06ld/%d; %d %d",actual_len,
//				    h->ts.tv_sec,h->ts.tv_usec,h->len,
//				    ip_hdr->protocol,IPPROTO_UDP));
//		val.set(dest.val(),source.val(),xid.val(),
//			packet_at.val(),xdr,32,common_record_id.val(),
//			empty_string);
//		if (false) printf("v2Write %lld %8x -> %8x; %d\n",
//				 packet_at.val(),source.val(),dest.val(),
//				 actual_len);
//		string foo;
//		foo.assign((char *)xdr,32);
//		d.replyhandler = new NFSV2ReadWriteReplyHandler(common_record_id.val(),
//								foo, false,
//								ntohl(xdr[9]), ntohl(xdr[11]));
//	    }
//	    break;
//	default:
//	    return;
//	}
//    val.rpcreqhashval = BobJenkinsHash(1972, xdr, actual_len);
//    val.ipchecksum = ipchecksum;
//    val.l4checksum = l4checksum;
//
//    AssertAlways(val.server_id != -1,("bad6"));
//    attropData *hval = attropHashTable.lookup(val);
//    if (hval != NULL) {
//	AssertAlways(hval->server_id == val.server_id &&
//		     hval->client_id == val.client_id &&
//		     hval->xid == val.xid,("internal error\n"));
//	if (warn_duplicate_reqs) { // disabled because of tons of them showing up in set-8
//	  printf("Probable duplicate request detected s=%08x c=%08x xid=%08x; #%lld duped by #%lld\n",
//		 hval->server_id, hval->client_id, hval->xid, 
//		 hval->request_id,val.request_id);
//	  printf("  Checksums are %d/%d vs %d/%d\n",
//		 hval->ipchecksum,hval->l4checksum,val.ipchecksum,val.l4checksum);
//	}
//	if (hval->rpcreqhashval != val.rpcreqhashval) {
//	  for(vector<network_error_listT>::iterator i = known_bad_list.begin();
//	      i != known_bad_list.end(); ++i) {
//	    if ((hval->request_id - first_record_id) == i->h_rid &&
//		(val.request_id - first_record_id) == i->v_rid &&
//		hval->rpcreqhashval == i->h_hash &&
//		val.rpcreqhashval == i->v_hash &&
//		(unsigned)hval->server_id == i->server &&
//		(unsigned)hval->client_id == i->client) {
//	      fprintf(stderr,"speculate bad network transmission/lack of locking in linux write??\n");
//	      hval->rpcreqhashval = val.rpcreqhashval;
//	    }
//	  }
//	}
//	if (hval->rpcreqhashval != val.rpcreqhashval) {
//	  fprintf(stderr, "bad7.1 { %lld, %lld, 0x%08x, 0x%08x, 0x%08x, 0x%08x }, // %s\n",
//		 hval->request_id - first_record_id,
//		 val.request_id - first_record_id,
//		 hval->rpcreqhashval, val.rpcreqhashval,
//		 hval->server_id, hval->client_id,
//		 tracename);
//	  exitvalue = 1;
//	}
//	// can't remove the original request in case we get a pair of retransmissions; 
//	// matching is a litle bit odd in this case, since a response will match to the nearest request.
//	//	attropHashTable.remove(val); 
//    }
//		     
//    attropHashTable.add(val);
}

void
handleNFSV3Request(const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
		   int ipchecksum, int l4checksum, RPCRequest &req, RPCRequestData &d)
{
    uint32_t *xdr = req.getrpcparam();
    int actual_len = req.getrpcparamlen();

    maybeDumpRecord(xdr,actual_len);
    switch(opid.val()) 
	{
	case NFSPROC3_GETATTR: 
	    {
		if (false) printf("v3GetAttr %lld %8x -> %8x; %d\n",
				  packet_at.val(),source.val(),dest.val(),actual_len);
		AssertAlways(actual_len >= 8,
			     ("bad getattr request @%lld: %d",common_record_id.val(),actual_len));
		int fhlen = ntohl(xdr[0]);
		AssertAlways(fhlen % 4 == 0 && fhlen > 0 && fhlen <= 64,
			     ("bad"));
		ShortDataAssertMsg(actual_len == 4+fhlen,"NFSv3 getattr request",("bad"));
//		val.set(dest.val(),source.val(),xid.val(),
//			packet_at.val(),xdr+1,fhlen,common_record_id.val(),
//			empty_string);
		string filehandle((char *)(xdr+1), fhlen);
		d.replyhandler =
		    new NFSV3GetAttrReplyHandler(filehandle);
	    }
	    break;
	case NFSPROC3_LOOKUP: 
	    {
		ShortDataAssertMsg(actual_len >= 12,
				   "NFSv3 lookup request",
				   ("bad read len %d @%ld.%06ld/%d; %d %d",actual_len,
				    h->ts.tv_sec,h->ts.tv_usec,h->len,
				    ip_hdr->protocol,IPPROTO_UDP));
		int fhlen = ntohl(xdr[0]);
		AssertAlways(fhlen % 4 == 0 && fhlen > 0 && fhlen <= 64,
			     ("bad"));
		ShortDataAssertMsg(actual_len >= 4+fhlen+4,"NFSv3 lookup request",("bad"));
		string filename = getLookupFilename(xdr+1+fhlen/4,actual_len - (4+fhlen));
		if (false) printf("v3Lookup %lld %8x -> %8x; %d; %s\n",
				  packet_at.val(),source.val(),dest.val(),
				  actual_len,filename.c_str());
//		val.set(dest.val(),source.val(),xid.val(),
//			packet_at.val(),xdr,0,common_record_id.val(),
//			filename);
		string lookup_directory_filehandle((char *)(xdr+1),fhlen);
		d.replyhandler =
		    new NFSV3LookupReplyHandler(lookup_directory_filehandle,filename);
						
	    }
	    break;
	case NFSPROC3_READ:
	    { 
		ShortDataAssertMsg(actual_len >= (8+8+4),
				   "NFSv3 read request",
				   ("bad read len %d @%ld.%06ld/%d; %d %d",actual_len,
				    h->ts.tv_sec,h->ts.tv_usec,h->len,
				    ip_hdr->protocol,IPPROTO_UDP));
		int fhlen = ntohl(xdr[0]);
		AssertAlways(fhlen % 4 == 0 && fhlen > 0 && fhlen <= 64,
			     ("bad fhlen1"));
		RPCParseAssertMsg(actual_len == 4 + fhlen + 8 + 4,("bad actual_len %d fhlen %d +=16",actual_len,fhlen));
//		val.set(dest.val(),source.val(),xid.val(),
//			packet_at.val(),xdr+1,fhlen,common_record_id.val(),
//			empty_string);
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
				   ("bad read len %d @%ld.%06ld/%d; %d %d",actual_len,
				    h->ts.tv_sec,h->ts.tv_usec,h->len,
				    ip_hdr->protocol,IPPROTO_UDP));
		int fhlen = ntohl(xdr[0]);
		AssertAlways(fhlen % 4 == 0 && fhlen > 0 && fhlen <= 64,
			     ("bad"));
		ShortDataAssertMsg(actual_len >= (4+ fhlen + 8 + 4),
				   "NFSv3 Write Request",
				   ("bad read len %d @%ld.%06ld/%d; %d %d",actual_len,
				    h->ts.tv_sec,h->ts.tv_usec,h->len,
				    ip_hdr->protocol,IPPROTO_UDP));   

//		val.set(dest.val(),source.val(),xid.val(),
//			packet_at.val(),xdr+1,fhlen,common_record_id.val(),
//			empty_string);
		string filehandle((char *)(xdr+1), fhlen);
		if (false) printf("v2Write %lld %8x -> %8x; %d\n",
				 packet_at.val(),source.val(),dest.val(),
				 actual_len);
		unsigned len = ntohl(xdr[1+ fhlen/4 + 2]);
		AssertAlways(len < 65536,("bad"));
		d.replyhandler = 
		    new NFSV3ReadWriteReplyHandler(filehandle, 
						   xdr_ll(xdr,1+fhlen/4),
						   len,false);
	    }
	    break;
	default:
	    return;
	}
//    val.rpcreqhashval = BobJenkinsHash(1972, xdr, actual_len);
//    val.ipchecksum = ipchecksum;
//    val.l4checksum = l4checksum;
//
//    AssertAlways(val.server_id != -1,("bad6"));
//    attropData *hval = attropHashTable.lookup(val);
//    if (hval != NULL) {
//	AssertAlways(hval->server_id == val.server_id &&
//		     hval->client_id == val.client_id &&
//		     hval->xid == val.xid,("internal error\n"));
//	if (warn_duplicate_reqs) { // disabled because of tons of them showing up in set-8
//	  printf("Probable duplicate request detected s=%08x c=%08x xid=%08x; #%lld duped by #%lld\n",
//		 hval->server_id, hval->client_id, hval->xid, 
//		 hval->request_id,val.request_id);
//	  printf("  Checksums are %d/%d vs %d/%d\n",
//		 hval->ipchecksum,hval->l4checksum,val.ipchecksum,val.l4checksum);
//	}
//	if (hval->rpcreqhashval != val.rpcreqhashval) {
//	  for(vector<network_error_listT>::iterator i = known_bad_list.begin(); 
//	      i != known_bad_list.end(); ++i) {
//	    if ((hval->request_id - first_record_id) == i->h_rid &&
//		(val.request_id - first_record_id) == i->v_rid &&
//		hval->rpcreqhashval == i->h_hash &&
//		val.rpcreqhashval == i->v_hash &&
//		(unsigned)hval->server_id == i->server &&
//		(unsigned)hval->client_id == i->client) {
//	      fprintf(stderr,"speculate bad network transmission/lack of locking in linux write??\n");
//	      hval->rpcreqhashval = val.rpcreqhashval;
//	    }
//	  }
//	}
//	if (hval->rpcreqhashval != val.rpcreqhashval) {
//	  fprintf(stderr, "bad7.2 { %lld, %lld, 0x%08x, 0x%08x, 0x%08x, 0x%08x }, // %s\n",
//		 hval->request_id - first_record_id,
//		 val.request_id - first_record_id,
//		 hval->rpcreqhashval, val.rpcreqhashval,
//		 hval->server_id, hval->client_id,
//		 tracename);
//	  exitvalue = 1;
//	}
//	// can't remove the original request in case we get a pair of retransmissions; 
//	// matching is a litle bit odd in this case, since a response will match to the nearest request.
//	//	attropHashTable.remove(val); 
//    }
//		     
//    attropHashTable.add(val);
}

void
setNFS2attrops(u_int32_t *xdr)
{
    int type = ntohl(xdr[0]);
    AssertAlways(type >= 1 && type < 9 && type != 7,("bad8\n"));
    attrops_typeid.set(type);
    attrops_type.set(NFSV2_typelist[type]);
    attrops_mode.set(ntohl(xdr[1]) & 0xFFF);
    attrops_uid.set(ntohl(xdr[3]));
    attrops_gid.set(ntohl(xdr[4]));
    unsigned int filesize = ntohl(xdr[5]);
    attrops_filesize.set((ExtentType::int64)filesize);
    ExtentType::int32 blksize = ntohl(xdr[6]);
    ExtentType::int32 blocks = ntohl(xdr[8]);
    ExtentType::int64 est_used = (ExtentType::int64)blksize * (ExtentType::int64)blocks;
    // The results from looking at the traces seems to indicate that
    // the filers are setting the NFS block size to 8192, but on disk
    // they actually use only 512 byte blocks; as it's really unlikely
    // that the 1 GB file is using up 16GB, and it's really consistent
    // across all of the attribute entries, the ratio slightly
    // increases as the file size decreases, which is to be expected.
    if (((double)est_used / (double)filesize) > 16.0) {
	est_used = (ExtentType::int64)blocks * 512;
    }
    attrops_used_bytes.set(est_used);
    attrops_modify_time.set(getNFS2Time(xdr+13));
}

// attropData *
// getAttrOpDataEnt()
// {
//     attropData key,*val;
//     key.set(source.val(),dest.val(),xid.val());
//     val = attropHashTable.lookup(key);
//     AssertAlways(val != NULL,
// 		 ("bad9 src=%08x dest=%08x xid=%08x\n",
// 		  source.val(),dest.val(),xid.val()));
//     AssertAlways(packet_at.val() - val->request_at < (ExtentType::int64)300*1000*1000*1000,("slow\n"));
//     return val;
// }

void
handleNFSV2Reply(int ipchecksum, int l4checksum, RPCRequestData *d, RPCReply &reply,
		 bool is_tcp)
{
    FATAL_ERROR("unimplemented"); // needs to be re-done with the correct new style
//    u_int32_t *xdr = reply.getrpcresults();
//    int actual_len = reply.getrpcresultslen();
//    AssertAlways(reply.status() == 0,("request not accepted?!\n"));
//    AssertAlways(opid.isNull() == false,("bad"));
//    int op = opid.val();
//    if (op == 0) {
//	rpc_status.set(0);
//	return;
//    }
//    ShortDataAssertMsg(actual_len >= 4,
//		       "NFSv2 *unknown* reply",
//		       ("actual len %d",actual_len));
//    u_int32_t op_status = ntohl(*xdr);
//    xdr += 1;
//    actual_len -= 4;
//    rpc_status.set(op_status);
//    if (common_record_id.val() == hex_dump_id_1 ||
//	common_record_id.val() == hex_dump_id_2) {
//      printf("hex dump reply op = %d, status %d; %lld %d:\n",
//	     op,op_status,common_record_id.val(),actual_len);
//      unsigned char *f = (unsigned char *)xdr;
//      for(int i=0;i<actual_len;++i) {
//	printf("%02x ",f[i]);
//	if ((i % 26) == 25) {
//	  printf("\n");
//	}
//      }
//      printf("\n");
//    }
//    switch(op) 
//	{
//	case NFSPROC_GETATTR: 
//	    {
//		if (false) printf("v2GetAttrReply %lld %8x -> %8x; %d\n",
//				  packet_at.val(),source.val(),dest.val(),actual_len);
//		attropData *val = getAttrOpDataEnt();
//		if (actual_len == 0) {
//		  // for now, we don't put anything in the attrops series 
//		  // because there is no useful content in here.
//		  AssertAlways(op_status == 70,
//			       ("bad 11 %d\n",op_status));
//		} else {
//		  AssertAlways(actual_len == 68,("bad10 %d",actual_len));
//		  nfs_attrops_outmodule->newRecord();
//		  attrops_request_id.set(val->request_id);
//		  attrops_reply_id.set(common_record_id.val());
//		  attrops_filename.setNull(true);
//		  attrops_filehandle.set(val->filehandle);
//		  attrops_lookupdirfilehandle.setNull();
//		  setNFS2attrops(xdr);
//		}
//		attropHashTable.remove(*val);
//	    }
//	break;
//	case NFSPROC_LOOKUP: 
//	    {	    
//		attropData *val = getAttrOpDataEnt();
//		if (false) printf("v2LookupReply(%s) %lld %8x -> %8x; %d // %d\n",
//				 val->filename.c_str(),
//				 packet_at.val(),source.val(),dest.val(),
//				 op_status,actual_len);
//		
//		if (actual_len == 0) {
//		    AssertAlways(op_status == 2 || // enoent
//				 op_status == 13 || // eaccess
//				 op_status == 70, // estale
//				 ("bad117 %d",op_status)); 
//		    // don't generate a record here, later make an entry
//		    // in an extentseries of failed lookups
//		} else {
//		    AssertAlways(actual_len == 100,("bad\n"));
//		    nfs_attrops_outmodule->newRecord();
//		    attrops_request_id.set(val->request_id);
//		    attrops_reply_id.set(common_record_id.val());
//		    attrops_filename.set(val->filename);
//		    attrops_filehandle.set(xdr,4*8);
//		    AssertAlways(val->lookup_directory_filehandle.size() == 32,("bad"));
//		    attrops_lookupdirfilehandle.set(val->lookup_directory_filehandle.data(),32);
//		    setNFS2attrops(xdr + 8);
//		}
//		attropHashTable.remove(*val);
//	    }
//	    break;
//	case NFSPROC_READ:
//	    {
//		attropData *val = getAttrOpDataEnt();
//		if (false) printf("v2ReadReply %lld %8x -> %8x; %d // %d\n",
//				 packet_at.val(),source.val(),dest.val(),
//				 op_status,actual_len);
//		if (actual_len == 0) {
//		    AssertAlways(op_status == 70 || // stale file handle
//				 op_status == 13, // permission denied
//				 ("bad12 %d", op_status)); 
//		    // don't generate a record here; should update extents 
//		    // to store the return value for rpc replys
//		} else {
//		    ShortDataAssertMsg(actual_len >= 68,"NFSv2 Read Reply",
//				       ("actual len %d",actual_len));
//		    
//		    nfs_attrops_outmodule->newRecord();
//		    attrops_request_id.set(val->request_id);
//		    attrops_reply_id.set(common_record_id.val());
//		    attrops_filename.setNull();
//		    attrops_filehandle.set(val->filehandle);
//		    attrops_lookupdirfilehandle.setNull();
//		    setNFS2attrops(xdr);
//		}
//	    }
//	    break;
//	case NFSPROC_WRITE:
//	    {
//		attropData *val = getAttrOpDataEnt();
//		if (false) printf("v2WriteReply %lld %8x -> %8x; %d // %d\n",
//				 packet_at.val(),source.val(),dest.val(),
//				 op_status,actual_len);
//		if (actual_len == 0) {
//		    AssertAlways(op_status == 13 || // permission denied
//			       op_status == 28 || // out of space
//			       op_status == 69 || // disk quota exceeded
//			       op_status == 70, // stale file handle
//			       ("bad12 op_status = %d",op_status)); 
//		  // don't generate a record here; should update extents 
//		  // to store the return value for rpc replys
//		} else {
//		  AssertAlways(actual_len == 68,("bad %d ;; %d",actual_len,op_status));
//		  nfs_attrops_outmodule->newRecord();
//		  attrops_request_id.set(val->request_id);
//		  attrops_reply_id.set(common_record_id.val());
//		  attrops_filename.setNull();
//		  attrops_filehandle.set(val->filehandle);
//		  attrops_lookupdirfilehandle.setNull();
//		  setNFS2attrops(xdr);
//		}
//		attropHashTable.remove(*val);
//	    }
//	    break;
//	}
}

void
doIPPacket(const struct timeval &ts, u_int32_t srcHost, u_int32_t destHost, 
	   u_int32_t wire_len, struct udphdr *udp_hdr, struct tcphdr *tcp_hdr,
	   bool is_fragment)
{
    ippacket_outmodule->newRecord();
    ippacket_packet_at.set((ExtentType::int64)ts.tv_sec * 1000 * 1000 * 1000 + ts.tv_usec * 1000);
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
fillcommonNFSRecord(const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
		    int v_source_port, int v_dest_port, int payload_len,
		    int procnum, int v_nfsversion, u_int32_t v_xid)
{
    nfs_common_outmodule->newRecord();
    packet_at.set(timeval2ns(h->ts));
    source.set(ntohl(ip_hdr->saddr));
    source_port.set(v_source_port);
    dest.set(ntohl(ip_hdr->daddr));
    dest_port.set(v_dest_port);
    is_udp.set(ip_hdr->protocol == IPPROTO_UDP);
    nfs_version.set(v_nfsversion);
    xid.set(htonl(v_xid));
    opid.set(procnum);
    if (v_nfsversion == 2) {
	AssertAlways(procnum >= 0 && procnum < n_nfsv2ops,("bad"));
	operation.set(nfsv2ops[procnum]);
    } else if (v_nfsversion == 3) {
	AssertAlways(procnum >= 0 && procnum < n_nfsv3ops,("bad"));
	operation.set(nfsv3ops[procnum]);
    } else if (v_nfsversion == 1) {
        // caches seem to use NFS version 1 null op occasionally
        AssertAlways(procnum == 0,("bad"));
    } else {
	AssertFatal(("bad; nfs version %d",v_nfsversion));
    }
    payload_length.set(payload_len);
    common_record_id.set(cur_record_id);
    ++cur_record_id;
}

void
handleNFSRequest(const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
		 int source_port, int dest_port, int l4checksum, int payload_len,
		 RPCRequest &req, RPCRequestData &d)
{
    fillcommonNFSRecord(h,ip_hdr,source_port,dest_port,payload_len,
			d.procnum,d.version,req.xid());
    d.request_id = common_record_id.val();
    is_request.set(true);
    rpc_status.setNull();
    if (d.version == 2) {
	handleNFSV2Request(h,ip_hdr,ntohs(ip_hdr->check),l4checksum,req,d);
    } else if (d.version == 3) {
	handleNFSV3Request(h,ip_hdr,ntohs(ip_hdr->check),l4checksum,req,d);
    }
}

void
handleNFSReply(const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
	       int source_port, int dest_port, int l4checksum, int payload_len,
	       RPCRequestData *req, RPCReply &reply)
{
    fillcommonNFSRecord(h,ip_hdr,source_port,dest_port,payload_len,
			req->procnum,req->version,reply.xid());
    is_request.set(false);
    rpc_status.setNull(); // don't know yet
    if (req->version == 2) {
	handleNFSV2Reply(htons(ip_hdr->check), l4checksum, req, reply,
			 ip_hdr->protocol == IPPROTO_TCP);
    }
}

void
handleMountRequest(const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
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
	default:
	    printf("skipping mount request type %d\n",d.procnum);
	}
}

void
handleMountReply(const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
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
		nfs_mount_outmodule->newRecord();
		nfs_mount_request_at.set(req->request_at);
		nfs_mount_reply_at.set(timeval2ns(h->ts));
		nfs_mount_server.set(ntohl(ip_hdr->saddr));
		nfs_mount_client.set(ntohl(ip_hdr->daddr));
		nfs_mount_pathname.set(pathname);
		nfs_mount_filehandle.set(m_rep.rawfilehandle(),m_rep.rawfilehandlelen());
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
    if (hval == NULL)
	return;
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
	AssertAlways(hval->request_id >= first_record_id &&
		     d.request_id >= first_record_id,
		     ("whoa %lld %lld >= %lld\n",hval->request_id, d.request_id, first_record_id));
	fprintf(stderr, "bad7.3 { %lld, %lld, 0x%08x, 0x%08x, 0x%08x, 0x%08x }, // %s\n",
		hval->request_id - first_record_id,
		d.request_id - first_record_id,
		hval->rpcreqhashval, d.rpcreqhashval,
		hval->server, hval->client,
		tracename);
	fprintf(stderr, " // bad7 prog=%d/%d, ver=%d/%d, proc=%d/%d\n",d.program,hval->program,
		d.version,hval->version,d.procnum,hval->procnum);
	if (++cur_mismatch_duplicate_requests < max_mismatch_duplicate_requests) {
	    fprintf(stderr," // bad7 automatically tolerating :) %d < %d\n",
		    cur_mismatch_duplicate_requests, max_mismatch_duplicate_requests);
	} else {
	    fprintf(stderr," // bad7 NOT automatically tolerating :) %d < %d\n",
		    cur_mismatch_duplicate_requests, max_mismatch_duplicate_requests);
	    exitvalue = 1;
	}
	fflush(stderr);
    }
    // can't remove the original request in case we get a pair of
    // retransmissions; matching is a litle bit odd in this case,
    // since a response will match to the nearest request.
}

void
handleRPCRequest(const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
		 int source_port, int dest_port, int l4checksum, int payload_len,
		 u_char *p, u_char *pend)
{
    RPCRequest req(p,pend-p);
	
    payload_len -= 4*(req.getrpcparam() - req.getxdr());
    if (false) printf("RPCRequest for prog %d, version %d, proc %d\n",
		      req.host_prognum(),req.host_version(),
		      req.host_procnum());
    RPCRequestData d(ip_hdr->saddr,ip_hdr->daddr,req.xid());
    d.request_id = -1;
    d.program = req.host_prognum();
    d.version = req.host_version();
    d.procnum = req.host_procnum();
    d.request_at = timeval2ns(h->ts);
    d.rpcreqhashval = BobJenkinsHash(1972,p,pend-p);
    d.ipchecksum = ntohs(ip_hdr->check);
    d.l4checksum = l4checksum;
    d.reqdata = NULL;
    d.replyhandler = NULL;
    if (d.program == RPCRequest::host_prog_nfs) {
	handleNFSRequest(h,ip_hdr,source_port,dest_port,l4checksum,payload_len,req,d);
	RPCRequestData *hval = rpcHashTable.lookup(d);
	duplicateRequestCheck(d,hval);
    } else if (d.program == RPCRequest::host_prog_mount) {
	handleMountRequest(h,ip_hdr,source_port,dest_port,payload_len,req,d);
    } else if (d.program == RPCRequest::host_prog_yp) {
	// ignore
    } else if (d.program == RPCRequest::host_prog_portmap) {
	// ignore
    } else {
	// unknown program, just retain request...
	printf("unrecognized rpc request program %d, version %d, procedure %d\n",
	       d.program,d.version,d.procnum);
    }
    rpcHashTable.add(d); // only add if successful parse...
}

void
handleRPCReply(const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
	       int source_port, int dest_port, int l4checksum, int payload_len,
	       u_char *p, u_char *pend)
{
    RPCReply reply(p,pend-p);
    // RPC reply parsing should handle all underflows    
//    if (payload_len < 1000) {
//	  if (payload_len != pend - p) {
//	      AssertAlways(ip_hdr->protocol == IPPROTO_TCP,
//			   ("bad %d %d",payload_len,pend - p));
//	      fprintf(stderr,"Warning: skipping short TCP reply at end of packet (?)\n");
//	      RPCParseAssertMsg(false,("skip"));
//	  }
//    }
    payload_len -= 4*(reply.getrpcresults() - reply.getxdr());
    RPCRequestData *req = rpcHashTable.lookup(RPCRequestData(ip_hdr->daddr,ip_hdr->saddr,reply.xid()));
	
    if (req != NULL) {
	if (req->program == RPCRequest::host_prog_nfs) {
	    handleNFSReply(h,ip_hdr,source_port,dest_port,l4checksum,payload_len,req,reply);
	} else if (req->program == RPCRequest::host_prog_mount) {
	    handleMountReply(h,ip_hdr,source_port,dest_port,payload_len,req,reply);
	} else {
	    // unknown program; ignore
	}
	if (req->replyhandler != NULL) {
	    req->replyhandler->handleReply(req,h,ip_hdr,source_port, dest_port, l4checksum, payload_len,reply);
	}
	if (false) 
	    printf("rpc reply for prog %d, version %d, proc %d?\n",
		   req->program,req->version,req->procnum);
	delete req->replyhandler;
	rpcHashTable.remove(*req);
    } else {
	++missing_request_count;
	// really ought to do this as a fraction of total requests
	AssertAlways(missing_request_count < max_missing_request_count,
		     ("whoa, more than %d possible reply packets without the request %lld packets so far; you need to tcpdump -s 256+; on %s\n",
		      max_missing_request_count, cur_record_id - first_record_id,tracename));
	if (warn_unmatched_rpc_reply) // many of these appear to be spurious, e.g. not really an rpc reply
	    printf("%ld.%06ld: unmatched rpc reply xid %08x client %08x\n",h->ts.tv_sec,h->ts.tv_usec,
		   ntohl(reply.xid()),ntohl(ip_hdr->daddr)); 
    }
}


void
handleUDPPacket(const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
	        u_char *p, u_char *pend)
{
    struct udphdr *udp_hdr = (struct udphdr *)p;
    p += 8;

    AssertAlways(p < pend,("short capture?"));
    doIPPacket(h->ts, ntohl(ip_hdr->saddr), ntohl(ip_hdr->daddr), h->len,
	       udp_hdr, NULL, false);
    u_int32_t *rpcmsg = (u_int32_t *)p;
    if ((p+2*4+2*4) > pend) {
	printf("short packet?!\n");
	return; // can't be RPC, short (error) reply is at least this long
    }
    if (rpcmsg[1] == 0) {
	handleRPCRequest(h,ip_hdr,ntohs(udp_hdr->source),
			 ntohs(udp_hdr->dest),ntohs(udp_hdr->check),
			 ntohs(udp_hdr->len) - 8,
			 p,pend);
    } else if (rpcmsg[1] == RPC::net_reply) {
	handleRPCReply(h,ip_hdr,ntohs(udp_hdr->source),
		       ntohs(udp_hdr->dest),ntohs(udp_hdr->check),
		       ntohs(udp_hdr->len) - 8,
		       p,pend);
    } else {
	if (false) printf("unknown\n");
	return; // can't be RPC
    }
}

void 
handleTCPPacket(const struct pcap_pkthdr *h, struct iphdr *ip_hdr,
		u_char *p, u_char *pend)
{
    struct tcphdr *tcp_hdr = (struct tcphdr *)p;

    if (false) printf("tcp packet at %ld.%06ld\n",h->ts.tv_sec,h->ts.tv_usec);
    AssertAlways((int)tcp_hdr->doff * 4 >= (int)sizeof(struct tcphdr),
		 ("bad doff %d %d\n",
		  tcp_hdr->doff * 4, sizeof(struct tcphdr)));
    p += tcp_hdr->doff * 4;
    AssertAlways(p <= pend,("short capture? %p %p",p,pend));
    doIPPacket(h->ts, ntohl(ip_hdr->saddr), ntohl(ip_hdr->daddr), h->len,
	       NULL, tcp_hdr, false);
    while((pend-p) >= 4) { // handle multiple RPCs in single TCP message; hope they are aligned to start
	++tcp_rpc_message_count;
	u_int32_t rpclen = ntohl(*(u_int32_t *)p);
	if (false) printf("  rpclen %x\n",rpclen);
	if ((rpclen & 0x80000000) == 0) {
	    return; // can't be RPC, high bit supposed to be set in length!
	}
	rpclen &= 0x7FFFFFFF;
	if (false) printf("  rpclen %d\n",rpclen);
	p += 4;
	u_int32_t *rpcmsg = (u_int32_t *)p;
	u_char *thismsgend;
	if ((p + rpclen) > pend) { 
	    thismsgend = pend;
	} else {
	    thismsgend = p + rpclen;
	}
	try {
	    if (rpcmsg[1] == 0) {
		if (false) printf("tcprpcreq\n");
		handleRPCRequest(h,ip_hdr,ntohs(tcp_hdr->source),
				 ntohs(tcp_hdr->dest),ntohs(tcp_hdr->check),
				 rpclen,p,thismsgend);
	    } else if (rpcmsg[1] == RPC::net_reply) {
		if (false) printf("tcprpcrep\n");
		handleRPCReply(h,ip_hdr,ntohs(tcp_hdr->source),
			       ntohs(tcp_hdr->dest),ntohs(tcp_hdr->check),
			       rpclen,p,thismsgend);
	    } else {
		if (false) printf("not tcp rpcthing?\n");
	    }
	} catch (ShortDataInRPCException &err) {
	    // TODO: count all the occurences of this based on the file,line,message in err and print out
	    // a summary at the end of processing
	    AssertAlways(thismsgend == pend,("Error, got short data error, but not at end of TCP segment"));
	    ++tcp_short_data_in_rpc_count;
	}
	p = thismsgend;
    }    
}

const int min_ethernet_header_length = 14;
const int min_ip_header_length = 20;

void 
packetHandler (u_char *user, const struct pcap_pkthdr *h, const u_char *packetdata)
{
    ExtentType::int64 timestamp_us = (ExtentType::int64)h->ts.tv_sec * 1000000 + h->ts.tv_usec;

    AssertAlways(h->len >= h->caplen,("bad lengths\n"));
    packet_bw_rolling_info.push(packetTimeSize(timestamp_us, h->len));
    const u_int32_t capture_remain = h->caplen;
    AssertAlways(capture_remain >= min_ethernet_header_length + min_ip_header_length,
		 ("whoa tiny packet %d\n",capture_remain));
    u_char *p = (u_char *)packetdata; // yea, this isn't quite right... 
    u_char *pend = p + h->caplen;

    int wire_length = h->len;
    int ethtype = (p[12] << 8) | p[13];
    AssertAlways(wire_length <= 1514,("whoa, long packet %d\n",wire_length));
    if (ethtype < 1500) {
        int protonum = (p[20] << 8) | p[21];
	bool print_warning = true;
	if (protonum == 0 && ethtype == 38)
	  print_warning = false;
	if (print_warning) {
	    printf("Weird ethernet type in packet @%ld.%06ld len=%d, wire length %d; proto %d jumbo? \n",
		   h->ts.tv_sec,h->ts.tv_usec,ethtype,wire_length,
		   protonum);
	}
	return;
    }

    int ethernet_header_len = 14;
    p += ethernet_header_len;
    if (ethtype != 0x800) { // IP type, the only one we care about
	return;
    }

    struct iphdr *ip_hdr = (struct iphdr *)p;
    AssertAlways(ip_hdr->version == 4,("Whoa, not IPV4 (V%d)\n",ip_hdr->version));
    int ip_hdrlen = ip_hdr->ihl * 4;
    p += ip_hdrlen;
    AssertAlways(p < pend,("short capture?!\n"));
    
    if (false) printf("%ld.%06ld: ",h->ts.tv_sec,h->ts.tv_usec);
    if ((ntohs(ip_hdr->frag_off) & 0x1FFF) != 0) { // mask off flags
	if (false) printf("fragment %d?\n",ntohs(ip_hdr->frag_off));
	if (ip_hdr->protocol == IPPROTO_UDP && ((p+8) <= pend)) {
	  struct udphdr *udp_hdr = (struct udphdr *)p;
	  doIPPacket(h->ts,ntohl(ip_hdr->saddr), ntohl(ip_hdr->daddr), 
		     wire_length,udp_hdr,NULL,true);
	} else if (ip_hdr->protocol == IPPROTO_TCP && ((p+sizeof(struct tcphdr)) <= pend)) {
	  struct tcphdr *tcp_hdr = (struct tcphdr *)p;
	  doIPPacket(h->ts,ntohl(ip_hdr->saddr), ntohl(ip_hdr->daddr), 
		     wire_length,NULL,tcp_hdr,true);
	} else {
	  doIPPacket(h->ts,ntohl(ip_hdr->saddr), ntohl(ip_hdr->daddr), 
		     wire_length,NULL,NULL,true);
	}
	  
	return; // fragment; no reassembly for now
    }
    try {
	if (ip_hdr->protocol == IPPROTO_TCP) {
	    handleTCPPacket(h,ip_hdr, p,pend); 
	} else if (ip_hdr->protocol == IPPROTO_UDP) {
	    handleUDPPacket(h,ip_hdr, p,pend);
	} else {
	    doIPPacket(h->ts, ntohl(ip_hdr->saddr), ntohl(ip_hdr->daddr), 
		       wire_length, NULL, NULL, false);
	    if (false) printf("unknown packet\n");
	}
    } catch (ShortDataInRPCException &err) {
	printf("parse failed on request at %s:%d (%s) was false: %s\n",
	       err.filename,err.lineno,err.condition.c_str(),
	       err.message.c_str()); // ignore
	AssertAlways(false,("got Short Data Error unexpectedly"));
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

void
throwassertcheck()
{
    AssertException(true && false && 1 == 0);
}

void
doassertcheck()
{
    bool got_assertion = false;
    try {
	throwassertcheck();
    } catch (AssertExceptionT &err) {
	got_assertion = true;
    }
    AssertAlways(got_assertion,("whoa, didn't get assertion?!\n"));
}

void
doSampleEncrypt()
{
    string encrypted = encryptString("aaaaaa");
    printf("aaaaaa -> %s\n",hexstring(encrypted).c_str());
    encrypted = encryptString("aaaaaaaaaaaaaaaa");
    printf("a x 16 -> %s\n",hexstring(encrypted).c_str());
    encrypted = encryptString("abcdefghijklmnopqrstuvwxyz");
    printf("a-z -> %s\n",hexstring(encrypted).c_str());
}

void
check_file_missing(const string &filename)
{
    struct stat statbuf;
    int ret = stat(filename.c_str(), &statbuf);
    INVARIANT(ret == -1 && errno == ENOENT,
	      boost::format("refusing to run with existing file %s: %s")
	      % filename % strerror(errno));
}

void
uncompressFile(const string &src, const string &dest)
{
    check_file_missing(dest);

    struct stat statbuf;
    int ret = stat(src.c_str(), &statbuf);
    
    INVARIANT(ret == 0, boost::format("could not stat source file %s: %s")
	      % src % strerror(errno));

    int fd = open(src.c_str(), O_RDONLY);
    INVARIANT(fd > 0, boost::format("could not open source file %s: %s")
	      % src % strerror(errno));
    
    unsigned char *buf = static_cast<unsigned char *>(mmap(NULL, statbuf.st_size, PROT_READ, MAP_SHARED, fd, 0));
    INVARIANT(buf != MAP_FAILED, boost::format("could not mmap source file %s: %s")
	      % src % strerror(errno));

    unsigned char *outbuf = NULL;
    unsigned int outsize = 0;
    if (suffixequal(src,".128MiB.lzf")) {
	outbuf = new unsigned char[128*1024*1024];
	outsize = lzf_decompress(buf, statbuf.st_size, outbuf, 128*1024*1024);
	INVARIANT(outsize > 0 && outsize <= 128*1024*1024, "bad");
    } else if (suffixequal(src,".128MiB.zlib1") || suffixequal(src, ".128MiB.zlib6") || suffixequal(src, ".128MiB.zlib9")) {
	outbuf = new unsigned char[128*1024*1024];
	uLongf destlen = 128*1024*1024;
	int ret = uncompress((Bytef *)outbuf,
			     &destlen,(const Bytef *)buf,
			     statbuf.st_size);
	INVARIANT(ret == Z_OK && destlen > 0 && destlen <= 128*1024*1024, "bad");
	outsize = destlen;
    } else {
	FATAL_ERROR(boost::format("Don't know how to unpack %s") % src);
    }
    int outfd = open(dest.c_str(), O_WRONLY | O_CREAT, 0664);
    INVARIANT(outfd > 0, boost::format("can not open %s for write: %s") % dest % strerror(errno));
    INVARIANT(outbuf != NULL && outsize > 0, "internal");
    ssize_t write_amt = write(outfd, outbuf, outsize);
    ret = close(outfd);
    INVARIANT(ret == 0, "bad close");
}

int
get_max_missing_request_count(const char *tracename)
{
    return 1000;
}

int
main(int argc, char **argv)
{
    if (argc == 4 && strcmp(argv[1],"--uncompress") == 0) {
	uncompressFile(argv[2],argv[3]);
	exit(0);
    }

    doassertcheck();

    testBWRolling();
    char errbuf[PCAP_ERRBUF_SIZE];
    
    if (enable_encrypt_filenames) {
	prepareEncryptEnvOrRandom();
    }

    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);
    
    int expect_records = -1;

    if (argc == 6) {
      expect_records = atoi(argv[5]);
      argc = 5;
    }
    INVARIANT(argc == 5,
	      boost::format("Usage: %s <tracename> <record-id-start> <tcpdump input, - ok> <dataseries file> [<expected-records>]\n")
	      % argv[0]);
    
    tracename = argv[1];

    if (strncmp(tracename,"/mnt/trace-",strlen("/mnt/trace-")) == 0 &&
	(tracename[11] >= '0' && tracename[11] <= '7' ||
	 tracename[11] >= 'a' && tracename[11] <= 'n') &&
	tracename[12] == '/') {
      tracename += 13;
    }
    max_missing_request_count = get_max_missing_request_count(tracename);
    cur_record_id = strtoll(argv[2], NULL, 10);
    first_record_id = cur_record_id;
    pcap_t *pd = pcap_open_offline(argv[3], errbuf);
    if (pd == NULL) {
	fprintf(stderr,"Error opening offline pcap file: %s\n", errbuf);
	exit(1);
    }

#if 0
    DataSeriesSink nfsdsout(argv[4],packing_args.compress_modes,packing_args.compress_level);
    ExtentTypeLibrary library;
    ExtentType *nfs_common_type = library.registerType(nfs_common_xml);
    nfs_common_series.setType(nfs_common_type);
    nfs_common_outmodule = new OutputModule(nfsdsout,nfs_common_series,
					    nfs_common_type,packing_args.extent_size);

    ExtentType *nfs_attrops_type = library.registerType(nfs_attrops_xml);
    nfs_attrops_series.setType(nfs_attrops_type);
    nfs_attrops_outmodule = new OutputModule(nfsdsout, nfs_attrops_series,
					     nfs_attrops_type,packing_args.extent_size);

    ExtentType *nfs_readwrite_type = library.registerType(nfs_readwrite_xml);
    nfs_readwrite_series.setType(nfs_readwrite_type);
    nfs_readwrite_outmodule = new OutputModule(nfsdsout, nfs_readwrite_series,
					       nfs_readwrite_type,packing_args.extent_size);

    ExtentType *ippacket_type = library.registerType(ippacket_xml);
    ippacket_series.setType(ippacket_type);
    ippacket_outmodule = new OutputModule(nfsdsout, ippacket_series,
					       ippacket_type,packing_args.extent_size);

    ExtentType *nfs_mount_type = library.registerType(nfs_mount_xml);
    nfs_mount_series.setType(nfs_mount_type);
    nfs_mount_outmodule = new OutputModule(nfsdsout, nfs_mount_series,
					       nfs_mount_type,packing_args.extent_size);

    nfsdsout.writeExtentLibrary(library);
    
    AssertAlways(pcap_datalink(pd) == DLT_EN10MB,("unsupported\n"));
    if (pcap_loop(pd, -1, packetHandler, NULL) < 0) {
	if (strcmp(pcap_geterr(pd),"truncated dump file") == 0) {
	  if (truncated_trace(tracename) ||
	      getenv("IGNORE_TRUNCATED_DUMPS") != NULL) {
	    // ignore
	  } else {
	    fprintf(stderr,"set the env variable IGNORE_TRUNCATED_DUMPS to tolerate silently on %s\n",tracename);
	    exitvalue = 1;  
	  }
	} else {
	    exitvalue = 1;  
	}	  
	if (exitvalue != 0) {
	  fprintf(stderr, "%s: pcap_loop: %s\n",
		  argv[0], pcap_geterr(pd));
        }
    }
    
    pcap_close(pd);
    nfs_common_outmodule->flushExtent();
    delete nfs_common_outmodule;
    nfs_attrops_outmodule->flushExtent();
    delete nfs_attrops_outmodule;
    nfs_readwrite_outmodule->flushExtent();
    delete nfs_readwrite_outmodule;
    ippacket_outmodule->flushExtent();
    delete ippacket_outmodule;
    nfs_mount_outmodule->flushExtent();
    delete nfs_mount_outmodule;

    printf("next record id: %lld\n",cur_record_id);
    fprintf(stderr,"possible replies with missing request: %d/%lld records\n",
	    missing_request_count,cur_record_id - first_record_id);
    if (tcp_rpc_message_count > 0) {
	fprintf(stderr,"short data in tcp rpcs: %d/%d\n",tcp_short_data_in_rpc_count,tcp_rpc_message_count);
	AssertAlways(tcp_short_data_in_rpc_count * 1000 < tcp_rpc_message_count,
		     ("too many"));
    }
    AssertAlways(expect_records == -1 || (cur_record_id - first_record_id) == expect_records,
		 ("mismatch on expected # records: %lld - %lld != %d\n",
		  cur_record_id,first_record_id,expect_records));
    summarizeBandwidthInformation();
    return exitvalue;
#endif
}

