/*
   (c) Copyright 2003-2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <getopt.h>
#include <ctype.h>
#include <signal.h>
#include <setjmp.h>
#include <limits.h>

#include <list>
#include <ostream>
#include <algorithm>

#include <Lintel/AssertBoost.hpp>
#include <Lintel/ConstantString.hpp>
#include <Lintel/HashTable.hpp>
#include <Lintel/HashUnique.hpp>
#include <Lintel/HashMap.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/RotatingHashMap.hpp>
#include <Lintel/Stats.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/DStoTextModule.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/PrefetchBufferModule.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

#include <analysis/nfs/join.hpp>
#include "analysis/nfs/mod1.hpp"
#include "analysis/nfs/mod2.hpp"
#include "analysis/nfs/mod3.hpp"
#include "analysis/nfs/mod4.hpp"
#include <analysis/nfs/HostInfo.hpp>
#include "process/sourcebyrange.hpp"

/*
=pod

=head1 NAME 

   nfsdsanalysis - perform analysis on NFS dataseries files

=head1 SYNOPSIS

   nfsdsanalysis [OPTION]... FILE...
   nfsdsanalysis [OPTION]... index.ds start-time end-time

=head1 DESCRIPTION

Each of the different options adds a module to the list of ones that
will be run over the set of files to be analyzed.  The set of files
may be selected either by just listing them, or by generating an index
file and listing a start and end time in seconds.  The latter form is
useful when the number of files gets large because they may not all be
listable on a single command line.  

Classnames are given for each option to make it easier to find the
implementation in the code.

=head2 -a # Operation count by filehandle (class OperationByFileHandle)

This analysis is useful for finding files which are very commonly
accessed, the operations used on those files, and if the underlying
fileservers are NetApps, the filesystems which are commonly accessed.

In particular, the analysis counts the number of operations by
(operation,filename), (filename), (filesystem).  If no name is found,
the filehandle is substituted for the filename.  The filesystem
version of this rollup currently assumes the form of filesystem naming
that is used by the NetApp filers wherein the filesystem can be
determined by certain of the bytes in the filehandle.  That part of
the results will be garbage unless the underlying filesystem was
netapp.

This analysis can use a ton of memory because it's memory usage is
proportional to the number of unique (server, operation, filehandle)
pairs.  

=head3 Example output

	FileSystem Rollup:
	Too many filesystems (>= 100000) filesystems found, assuming that fh->fs mapping is wrong
	
	FH Rollup:
	fhrollup: server 10.11.192.51, fh 12345608000003e7000a00000001d6ba20fd3d75000a00000000000200006765, fn *unknown*: 16 ops 0.102 MB, 398.312 us avg lat
	fhrollup: server 10.11.192.3, fh 12345634000003e7000a00000002fc06020f7777000a00000000000200000000, fn *unknown*: 6 ops 0.003 MB, 1103.167 us avg lat
	fhrollup: server 10.11.192.32, fh 1234562064c2bd0035df56002000000000251c212d495100c83700000138c000, fn 2f8f2f56269e884bf1e95bd4fc5d4fa7: 6 ops 0.001 MB, 1602.167 us avg lat
	fhrollup: server 10.11.192.110, fh 123456f000003e7000a000000033e976812f470000a0000000000020000604a, fn *unknown*: 3 ops 0.004 MB, 915.000 us avg lat
	fhrollup: server 10.11.192.110, fh 123456f000003e7000a00000003709c5acf3e18000a0000000000020000604a, fn *unknown*: 3 ops 0.004 MB, 1374.000 us avg lat
	...
	
	FH/Op Rollup:
	fhoprollup: server 10.11.224.74, op read, fh 123456aa050000008230303006002f0c011700028292c2f9dbdf97d707ce49c50200bf0c8d1e000603005f0cb51d0006, fn *unknown*: 8 ops 0.007 MB, 139.125 us avg lat
	fhoprollup: server 10.11.192.4, op write, fh 123456a000003e7000a000000025ac37224787b000a000000000002000007c3, fn *unknown*: 69 ops 0.009 MB, 2306.565 us avg lat
	fhoprollup: server 10.11.224.10, op read, fh 123456050000008230303004002f0e2d1c00027bbab53417280d9b7a5a591f00003f0ed91f000600003f0e4f1f0006, fn *unknown*: 46 ops 0.176 MB, 192.565 us avg lat
	...

=head2 -b # Server latency by operation (classname ServerLatency)

This analysis is useful for determining the number of operations and
latency for each of the various servers in the trace.  In particular,
for each request/response pair, it accumulates the latency of the
operation for each server and operation.  It rolls the results up into
by server, by operation, and overall.  The data under the duplicates
line tells you quantiles for the delay between duplicates if any
existed.  The code currently calculates the latency from the first
request to each reply.  This emulates the delay seen by the client.
The code has support for calculating the latency from the last
request, which provides more information on how the server generating
latency, but that code is currently disabled.

=head3 Example Output

Begin-virtual void ServerLatency::printResult()
 id: server ip
001: 10.11.8.37
002: 10.11.9.21

0 missing requests, 0 missing replies; 0.00ms mean est firstlat, 0.00ms mean est lastlat
duplicates: 0 replies, 0 requests; delays min=infms
0 data points, mean 0 +- 0 [inf,-inf]

Grouped by (Server,Operation) pair:
server operation   #ops  #dups mean ; firstlat mean 50% 90% 
001   getattr         2      0 0.00 ;   1.208   1.21   1.21 
001    statfs         7      0 0.00 ;   0.349   0.23   1.25 
002    statfs         3      0 0.00 ;   0.989   1.35   1.35 

Grouped by Server:
server operation   #ops  #dups mean ; firstlat mean 50% 90% 
001         *         9      0 0.00 ;   0.540   0.27   1.25 
002         *         3      0 0.00 ;   0.989   1.35   1.35 

Grouped by Operation:
server operation   #ops  #dups mean ; firstlat mean 50% 90% 
 *    getattr      3358      0 0.00 ;   0.064   0.06   0.09 
 *     lookup      1124      0 0.00 ;   0.165   0.15   0.22 

Complete rollup:
server operation   #ops  #dups mean ; firstlat mean 50% 90% 
 *          *     10000      0 0.00 ;   0.135   0.10   0.20 
End-virtual void ServerLatency::printResult()

=head2 -c I<seconds>[,I<options>] # Host analysis (classname HostInfo)

This analysis looks at the amount of data and operations going either
in or out of each of the hosts.  It is useful for identifying heavily
used hosts.  In particular for each access, the payload size is
accumulated for both the source host and the destination host. 

=head3 Options

There are a number of options that you can specify in order to control
host the HostInfo analysis will be performed.  They are separated by
,'s after the mandatory seconds option that specifies the time
interval for the basic grouping.

=over 4

=item B<no_cube_time>: Do not cube over the time column, i.e. in the
cube output, this column will always be "*".  Unless you have a
relatively high grouping for seconds, failing to specify this will
generate an astonishing amount of output for the cube.

=item B<no_cube_host>: Do not cube over the host colum, i.e. in the
cube output, this column will always be "*".  This column tends to be
the second most common, and hence skipping the cube over this column
can reduce the amount of output.

=item B<no_print_base>: Do not print the base data, i.e. at least one
column in the output will include a "*".

=item B<no_print_cube>: Do not print the cubed data, i.e. no column
in the output will include a "*".  If you specify this option, the
options controlling how the cube operates are irrelevant.

=back

=head3 Example Output

    Begin-virtual void HostInfo::printResult()
    host     time        dir          op    op-dir   count mean-payload
    0a0b0915 1063931160 send       fsstat response      3    24.00
    0a0b0915 1063931160 send            * response      3    24.00
    0a0b0915 1063931160 recv       fsstat  request      3    32.00
    0a0b0915 1063931160 recv            *  request      3    32.00
    0a0b0915          * send            * response      3    24.00
    0a0b0915          * recv            *  request      3    32.00
    0a0c0b0b 1063931160 send      getattr  request      2    32.00
    0a0c0b0b 1063931160 send         read  request      4    44.00
    0a0c0b0b 1063931160 send            *  request      6    40.00
    0a0c0b0b 1063931160 recv      getattr response      2    72.00
    0a0c0b0b 1063931160 recv         read response      4  7244.00
    0a0c0b0b 1063931160 recv            * response      6  4853.33
    0a0c0b0b          * send            *  request      6    40.00
    0a0c0b0b          * recv            * response      6  4853.33
    ...
           * 1063931160  send           *        *  20000  1545.67
           *          *  send           *        *  20000  1545.67
    End-virtual void HostInfo::printResult()

=head2 -d I<filehandle or path to file of filehandles> # Directory path lookup

This analysis attempts to trace from each of the given filehandles up
to the root of their filesystem.  Each output line consists of an
original filehandle and as much of the path as could be reconstructed.
If the analysis does not reach the root, then the first filehandle
will have a FH:... prefix.  The remaining values on the line will be
the filenames for that part of the path.  Components in a path are
separated by /'s.  The analysis keeps a bounded amount of memory for
storing previously seen filehandle, parent-fh, filename entries.
Therefore, there is a chance for large datasets that re-running the
analysis requesting some of the early filehandles will make additional
progress in reconstructing the entire path.  The base path can only be
reconstructed if the dataset includes the original mount request.

=head3 Example Output

    Begin-virtual void DirectoryPathLookup::printResult()
    e8535201c2cca305200000000079de25139fc708d6390035400000006bfc0800: FH:e8535201c2cca3052000000001d63ba0074e0209d6390035400000006bfc0800/0f96481c6462479c5d0ec6a8c8ae4de3be167bea59064c541a5fd74f86d0767a
    a63c6021c20f0300200000000117721df1e3040064000000400000009a871600: FH:a63c6021c20f03002000000000b931882fd9040064000000400000009a871600/a7df16a9a4d78d0fff7370c4db658524/6bd595a355c6e6b695315994eeae03268630d4db3ec297a911950a0542627477
    64000000c20f0300200000000127ef590364240c4fdf0058400000009a871600: FH:64000000c20f03002000000000a4d165e8e304004fdf0058400000009a871600/bb2531e57d5bc4f1f062ca919177d84c/c73a170c9d445b5f5787cad97e2253b4/3cccf0547171de3fc38438085d40074b62fa9aa11ab6004fb2b6f78b7e9ac130
    End-virtual void DirectoryPathLookup::printResult()

=head2 -e I<filehandle | fh-list filename> # File Handle Operation Lookup

This analysis finds all of the operations associated with the specified 
filehandles and prints out information about each operation.  To eliminate
buffering, the output is printed out with a FHOL: prefix rather than within
a Begin-... End-... block.


=head3 Example Output

    FHOL: reply-at client server ; operation type filehandle filename; file-size modify-time
    FHOL: 1063931188.266179000 10.12.11.75 10.110.1.14 ; getattr file 25cccd0040f47d05200000000160929015f77d0578e4003e1630670064637600 '' ; 325880 1023314262.000000000
    FHOL: 1063931190.068588000 10.12.11.77 10.110.1.14 ; getattr directory 0138c000948119052000000000c03801948119050b71002c40000000ffa72e00 '' ; 4096 1050428920.444579000

=head1 DISABLED ANALYSIS

Most of the analysis were writen in early 2003, they do not have
regression tests, and fixing some of the other ones has shown subtle
bugs.  Therefore, for now, the below analysis are disabled in the
code.

=head2 -b # classname UniqueBytesInFilehandles

This analysis finds bytes which have a small number of unique values
in filehandles.  This is important if we are going to do a re-encoding
of filehandles for a caching system because we would need to store
additional data (such as the origin server), and if there are bytes
with few unique values, then we could use those bytes for encoding the
additional information.

=head2 -c <recent-age-seconds> # classname FileageByFilehandle

This analysis is useful for estimating how rapidly data is changing.
In particular, it examines all of the filehandles in the trace, and
for each one finds the maximum size of the file, and the most recent
modify time relative to the packet arrival time, or alternately, the
age of the file at the time of the operation.  It then prints out the
top 20 most recently modified files, and a summary of the number of
filehandles found, the number of recent ones, and the maximum MB of
source data that could have been accessed and the fraction of that
which is recent. It also prints out the first filename associated with
a filehandle in the trace (if any).

Note that this analysis has the downside that if a filehandle is being
repeatedly overwritten (either by deletes and re-use of the
filehandle, or by explicit overwrites) that the analysis could
underestimate the amount of data that needs to be transferred.  Also
note that you may get negative times for recent accesses; this is
because the timestamp on the packet is generated by the tracing
machine, but the timestamp in the file is generated by the client or
server.  Clock skew effectively causes an offset to these numbers.

This operation can use a ton of memory because the memory usage is
proportional to the number of unique filehandles.  A possible bug in
this implementation is that it doesn't include the server as part of
the uniquification of file handles.  This is correct for NetApp
systems with caches as the caches just pass through the filehandle,
but is incorrect in other systems.

Example output:

	Begin-virtual void FileageByFilehandle::printResult()
	   -44.499 secs 024da5e0000003e7000a000000058d4c56d7c9db000a00000000000200006b86            *unknown*     file 532480
	   -44.499 secs 024cfdea000003e7000a0000000230637e996877000a000000000002000007c3            *unknown*     file 11186176
	   -44.499 secs 024cfdea000003e7000a00000002ea4c1f0c7043000a000000000002000007c3            *unknown*     file 1802240
	   -44.499 secs 024cfdea000003e7000a00000002e9cb45b23cde000a000000000002000007c3            *unknown*     file 11084
	   ...
	271007 unique filehandles, 50609 recent: 189443.23 MB total files accessed; 59311.59 MB recent, or 31.31%
	End-virtual void FileageByFilehandle::printResult()

=head2 -d

Large file write analysis by file handle

=head2 -e

Large file write analysis by file name

=head2 -f

Large file analysis by file handle

=head2 -g

Large file analysis by file name

=head2 -h

Help -- Generate usage summary

=head2 -i # classname NFSOpPayload

This analysis is useful for examining how many requests and bytes each
of the different nfs operations are transferring, for example to
estimate how fast a proxy/cache would have to be to keep up.  In
particular, for each request or response, this analysis accumulates
the payload size separately for the TCP/UDP transport choice,
operation type, and request/reply direction.

Example Output:

	Begin-virtual void NFSOpPayload::printResult()
	TCP         read   Reply: 248724.709 MB, 17555.72 bytes/op, 32872 max bytes/op, 14855939 ops
	TCP        write Request: 30236.439 MB, 6058.29 bytes/op, 32824 max bytes/op, 5233360 ops
	TCP      getattr   Reply: 2804.244 MB,  88.00 bytes/op,   88 max bytes/op, 33414358 ops


=head2 -k # classname ClientServerPairInfo

This analysis is useful for determining if the usage between clients
and servers is balanced, and if different clients use the same servers
in similar amounts.  In particular, for each Client/Server pair, this
analysis accumulates the payload size between the client and server.
It currently sorts the output by total payload size.

Example Output:

	protocol client server: total data ...
	TCP 10.11.220.132 10.11.192.110: 10510.290 MB, 7421.21 bytes/op, 32872 max bytes/op, 1485046 ops
	TCP 10.11.220.124 10.11.197.24: 3263.072 MB, 12325.62 bytes/op, 32872 max bytes/op, 277599 ops
	TCP 10.11.220.133 10.11.224.52: 1412.919 MB, 1900.68 bytes/op, 32872 max bytes/op, 779485 ops
	TCP 10.11.220.4 10.11.224.30: 1219.811 MB, 1765.50 bytes/op, 32872 max bytes/op, 724479 ops
	TCP 10.11.220.118 10.11.224.30: 1170.995 MB, 1584.63 bytes/op, 32872 max bytes/op, 774868 ops
	...

=head2 -m # classname PayloadInfo

This analysis looks at which transport protocols are primarily being
used to move the data in the system.  In particular for each request,
it accumulates the total bytes moved, and breaks this out into tcp and
udp transports.

Example Output:

	Begin-virtual void PayloadInfo::printResult()
	time range: 1091578148.32s .. 1091602813.35s = 24665.02s, 6.85hours
	payload_length: avg = 1717.90 bytes, stddev = 6556.1648 bytes, sum = 283.9577 GB, 11.7889MB/s, count=177.48 million 7.20 k/s
	payload_length(udp): avg = 99.06 bytes, stddev = 280.9744 bytes, sum = 0.9220 GB, 0.0383MB/s, count=9.99 million 0.41 k/s
	payload_length(tcp): avg = 1814.50 bytes, stddev = 6736.3013 bytes, sum = 283.0356 GB, 11.7506MB/s, count=167.49 million 6.79 k/s
	End-virtual void PayloadInfo::printResult()

=head2 -n # classname FileSizeByType

This analysis makes no sense; for each record, it adds up the size of
the thing referenced based on the type of the thing.  This stunningly
overcounts the size of files and directories, and at best is good for
telling you how many accesses were made to different types of things.

=head2 -o

Unbalanced operations analysis

=head2 -p

Time range analysis

=head2 -q

Read analysis

=head2 -r

Common bytes between filehandle and dirhandle

=head2 -s # classname SequentialWholeAccess

This analysis looks for patterns in how files are being accessed, in
particular, how many clients access the files, whether their accesses
are sequential or random, how many repeat accesses, and an estimate of
cache efficiency.

TODO: fill in details of analysis

This analysis is stunningly expensive in terms of memory because it
needs to maintain separate entries for every
(client,server,fh,read/write) pair to track all of the different
access patterns.

The code can print out complete individual client details, or do
rollups based on (server,filehandle) and based on mountpoint if the
fh->mountpoint mapping is correct.

Example output:


=head2 -t

Strange write search

=head2 -v I<1-5>

print series: 1 = common, 2 = attrops, 3 = rw, 4 = common/attr join, 5
= common/attr/rw join

=head2 -w

servers per filehandle

=head2 -x 

transactions

=head2 -y

outstanding requests

=head1 SEE ALSO

indexnfscommon(1), lindump-mmap(1), /usr/share/doc/DataSeries/fast2009-nfs-analysis.pdf

=head1 BUGS

Most of the modules are commented out because they have not been re-validated and do 
not have regression tests.

=cut
*/

using namespace NFSDSAnalysisMod;    
using namespace std;
using boost::format;
using dataseries::TFixedField;

namespace NFSDSAnalysisMod {
    RowAnalysisModule *newReadWriteExtentAnalysis(DataSeriesModule &prev);
    RowAnalysisModule *newUniqueFileHandles(DataSeriesModule &prev);
    RowAnalysisModule *newSequentiality(DataSeriesModule &prev, const string &arg);
    RowAnalysisModule *newServerLatency(DataSeriesModule &prev, const string &arg);
    RowAnalysisModule *newMissingOps(DataSeriesModule &prev);
}

// needed to make g++-3.3 not suck.
extern int printf (__const char *__restrict __format, ...) 
   __attribute__ ((format (printf, 1, 2)));

// TODO: consider whether we need to make the attribute rollups apply
// only after the common/attr join as we now prune common rows based
// on timestamps, but are not pruning the attr op rollups in the same
// way.  Probably doesn't matter given the current analysis that are done.

static const string str_read("read");
static const string str_write("write");

class SequentialWholeAccess : public NFSDSModule {
public:
    SequentialWholeAccess(DataSeriesModule &_source)
	: source(_source),
	  serverip(s,"server"), clientip(s,"client"),
	  operation(s,"operation"), filehandle(s,"filehandle"),
	  offset(s,"offset"), filesize(s,"file-size"), 
	  packetat(s,"packet-at"), bytes(s,"bytes"),
	  first_time((ExtentType::int64)1 << 62), last_time(0)
    {
    }
    virtual ~SequentialWholeAccess() { }

    // 2 assumes that the cache discards data on writes; 1 assumes that the cache maintains data on writes
    static const double write_cache_inefficiency = 2;

    ExtentType::int64 cacheEfficiencyBytes(ExtentType::int64 read_bytes, ExtentType::int64 write_bytes,
					   ExtentType::int64 max_file_size) {
//	  if ((read_bytes + write_bytes) > max_file_size) {
//	      printf("XXX %lld %lld %lld\n",read_bytes, write_bytes, max_file_size);
//	  }
	if (read_bytes < max_file_size) {
	    max_file_size = read_bytes;
	}
	return read_bytes - (ExtentType::int64)(write_cache_inefficiency * write_bytes) - max_file_size;
    }

    struct hteData {
	ExtentType::int32 server, client;
	string filehandle;
	bool is_read;
	ExtentType::int64 cur_offset, sequential_access_bytes, total_access_bytes, max_file_size;
	ExtentType::int64 total_read_bytes, total_write_bytes, cache_efficiency_bytes, total_file_bytes;
	int zero_starts, eof_restarts, total_starts, nclients, client_read_count, client_write_count;
    };

    struct hteHash {
	unsigned operator()(const hteData &k) const {
	    return lintel::hashBytes(k.filehandle.data(),k.filehandle.size(),
				     lintel::BobJenkinsHashMix3(k.server,k.client,
								(k.is_read ? 0x5555 : 0xAAAA)));
	}
    };

    struct hteEqual {
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.server == b.server && a.client == b.client && a.is_read == b.is_read &&
		a.filehandle == b.filehandle;
	}
    };

    HashTable<hteData, hteHash, hteEqual> seqaccessexact;

    virtual Extent::Ptr getSharedExtent() {
        Extent::Ptr e = source.getSharedExtent();
	if (e == NULL)
	    return e;
	INVARIANT(e->type.getName() == "common-attr-rw-join","bad");
	hteData k;
	for(s.setExtent(e);s.morerecords();++s) {
	    if (packetat.val() < first_time) {
		first_time = packetat.val();
	    } 
	    if (packetat.val() > last_time) {
		last_time = packetat.val();
	    }
	    //	    printf("server %08x ; client %08x\n",serverip.val(),clientip.val());
	    k.server = serverip.val();
	    k.client = clientip.val();
	    if (operation.equal(str_read)) {
		k.is_read = 1;
	    } else if (operation.equal(str_write)) {
		k.is_read = 0;
	    } else {
		FATAL_ERROR("internal");
	    }
	    k.filehandle = filehandle.stringval();
	    hteData *v = seqaccessexact.lookup(k);
	    if (v != NULL) {
		//		printf("found value\n");
	    } else {
		k.cur_offset = -1;
		k.sequential_access_bytes = 0;
		k.total_access_bytes = 0;
		k.max_file_size = 0;
		k.total_read_bytes = 0;
		k.total_write_bytes = 0;
		k.cache_efficiency_bytes = 0;
		k.total_file_bytes = 0;
		k.zero_starts = 0;
		k.eof_restarts = 0;
		k.total_starts = 1;
		k.nclients = 1;
		if (k.is_read) {
		    k.client_read_count = 1;
		    k.client_write_count = 0;
		} else {
		    k.client_read_count = 0;
		    k.client_write_count = 1;
		}
		v = seqaccessexact.add(k);
	    }
	    if (filesize.val() > v->max_file_size)
		v->max_file_size = filesize.val();
	    v->total_access_bytes += bytes.val();
	    if (v->is_read) {
	        v->total_read_bytes += bytes.val();
	    } else {
		v->total_write_bytes += bytes.val();
	    }
	    if (v->cur_offset == offset.val()) { // sequential
		v->sequential_access_bytes += bytes.val();
		v->cur_offset += bytes.val();
	    } else {
		v->total_starts += 1;
		if (v->cur_offset == filesize.val()) {
		    v->eof_restarts += 1;
		}
		if (offset.val() == 0) {
		    v->zero_starts += 1;
		}
		v->cur_offset = offset.val() + bytes.val();
	    }
	    if (v->cur_offset >= filesize.val()) {
		// this happens because we may hit EOF reading, but the data series
		// reports the number of bytes we tried to read, not the number that
		// we succeeded in reading; this should of course be fixed in the
		// converter.
		if (offset.val() > filesize.val()) {
		    string *filename = fnByFileHandle(v->filehandle);
		    if (filename == NULL) filename = &v->filehandle;
		    if (offset.val() > filesize.val()) {
			INVARIANT(bytes.val() == 0, "whoa, read beyond file size got bytes");
			cout << format("tolerating weird over %s on %s at %lld from %08x? %lld > %lld ; %d\n")
			    % (v->is_read ? "read" : "write")
			    % maybehexstring(*filename) % packetat.val()
			    % clientip.val() % offset.val() % filesize.val()
			    % bytes.val();
		    }
		}
		v->cur_offset = filesize.val();
	    }
	}
	return e;
    }

    struct rollupHash {
	unsigned operator()(const hteData &k) const {
	    return lintel::hashBytes(k.filehandle.data(),k.filehandle.size(),k.server);
	}
    };

    struct rollupEqual {
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.server == b.server && a.filehandle == b.filehandle;
	}
    };

    struct mountRollupHash {
	unsigned operator()(const hteData &k) const {
	    return lintel::hashBytes(k.filehandle.data(),k.filehandle.size(),k.server);
	}
    };

    struct mountRollupEqual {
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.server == b.server && a.filehandle == b.filehandle;
	}
    };

    static const bool print_per_client_fh = false;
    static const bool print_per_fh = true;
    static const bool print_per_mount = false;

    virtual void printResult() {
	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;
	cout << format("time range is %d .. %d\n") % first_time % last_time;
	if (print_per_client_fh) printf("exact client/server/filehandle/rw info:\n");
	// calculate per-(server,fh) rollup while doing the client/server/fh/rw report
	HashTable<hteData, rollupHash, rollupEqual> rollup;

	for(HashTable<hteData, hteHash, hteEqual>::iterator i = seqaccessexact.begin();
	    i != seqaccessexact.end();++i) {
	    if (print_per_client_fh) {
		string *filename = fnByFileHandle(i->filehandle);
		if (filename == NULL) filename = &i->filehandle;
		cout << format("  %08x %08x %5s: %9d %9d %8d ; %3d %3d %3d; %s\n")
		    % i->server % i->client % (i->is_read ? "read" : "write")
		    % i->sequential_access_bytes % i->total_access_bytes % i->max_file_size
		    % i->zero_starts % i->eof_restarts % i->total_starts
		    % maybehexstring(*filename);
	    }
	    hteData *rv = rollup.lookup(*i);
	    if (rv == NULL) {
		rv = rollup.add(*i);
	    } else {
		rv->sequential_access_bytes += i->sequential_access_bytes;
		rv->total_access_bytes += i->total_access_bytes;
		if (i->max_file_size > rv->max_file_size &&
		    i->client != rv->client) { // tolerate read/modify/write on one client silently
		    string *filename = fnByFileHandle(i->filehandle);
		    if (filename == NULL) filename = &i->filehandle;
		    if (false) {
			cout << format("odd, %s changed max size from %d (on %08x/%08x a %s) to %d via a %s of %08x/%08x\n")
			    % maybehexstring(*filename) % rv->max_file_size % rv->client
			    % rv->server % (rv->is_read ? "read" : "write") % i->max_file_size
			    % (i->is_read ? "read" : "write") % i->client % i->server;
		    }
		    rv->max_file_size = i->max_file_size;
		}
		rv->total_read_bytes += i->total_read_bytes;
		rv->total_write_bytes += i->total_write_bytes;
		rv->zero_starts += i->zero_starts;
		rv->eof_restarts += i->eof_restarts;
		rv->total_starts += i->total_starts;
		rv->nclients += i->nclients;
		if (i->is_read) {
		    rv->client_read_count += 1;
		} else {
		    rv->client_write_count += 1;
		}
	    }
	}
	if (print_per_fh) {
	    printf("server/filehandle rollup (%d unique entries):\n", rollup.size());
	    printf("             clients     frac. relative to    maxfsize ; start/restart acceses\n");
	    printf("srvr      # read write:  seq.   random total  in bytes ; zero eof  #; filename or filehandle\n");
	    fflush(stdout);
	}
	// calculate the mount point rollup while printing out per-fh information
	HashTable<hteData, mountRollupHash, mountRollupEqual> mountRollup;

	for(HashTable<hteData, rollupHash, rollupEqual>::iterator i = rollup.begin();
	    i != rollup.end();++i) {
	    string *filename = fnByFileHandle(i->filehandle);
	    if (i->max_file_size == 0) {
		i->max_file_size = 1;
	    }
	    INVARIANT(i->total_access_bytes == i->total_write_bytes + i->total_read_bytes,
		      format("bad %d %d %d") % i->total_access_bytes % i->total_read_bytes
		      % i->total_write_bytes);
	    if (print_per_fh) {
		cout << format("%08x %3d %3d %3d: %7.4f %7.4f %7.4f %9d ; %3d %3d %3d; %s\n")
		    % i->server % i->nclients % i->client_read_count % i->client_write_count
		    % ((double)i->sequential_access_bytes/(double)i->max_file_size)
		    % ((double)(i->total_access_bytes - i->sequential_access_bytes)/(double)i->max_file_size)
		    % ((double)i->total_access_bytes/(double)i->max_file_size) % i->max_file_size
		    % i->zero_starts % i->eof_restarts % i->total_starts
		    % (filename == NULL ? maybehexstring(i->filehandle) : maybehexstring(*filename));
	    }
	    if (mountRollup.size() <= NFSDSAnalysisMod::max_mount_points_expected) {
		fh2mountData::pruneToMountPart(i->filehandle);
		hteData *mtrv = mountRollup.lookup(*i);
		if (mtrv == NULL) {
		    mtrv = mountRollup.add(*i);
		    mtrv->cache_efficiency_bytes = cacheEfficiencyBytes(i->total_read_bytes,i->total_write_bytes, i->max_file_size);
		    mtrv->total_file_bytes = i->max_file_size;
		} else {
		    mtrv->sequential_access_bytes += i->sequential_access_bytes;
		    mtrv->total_access_bytes += i->total_access_bytes;
		    mtrv->total_read_bytes += i->total_read_bytes;
		    mtrv->total_write_bytes += i->total_write_bytes;
		    mtrv->zero_starts += i->zero_starts;
		    mtrv->eof_restarts += i->eof_restarts;
		    mtrv->total_starts += i->total_starts;
		    mtrv->nclients += i->nclients;
		    mtrv->client_read_count += i->client_read_count;
		    mtrv->client_write_count += i->client_write_count;
		    mtrv->cache_efficiency_bytes += cacheEfficiencyBytes(i->total_read_bytes,i->total_write_bytes,i->max_file_size);
		    mtrv->total_file_bytes += i->max_file_size;
		}
	    }
	}

	if (print_per_mount) {
	    printf("server/mountpoint rollup:\n");
	    if (mountRollup.size() > NFSDSAnalysisMod::max_mount_points_expected) {
		printf("Skipped -- exceeded %d 'mounts' -- fh -> mount mapping probably wrong\n",
		       NFSDSAnalysisMod::max_mount_points_expected);
	    } else {
		printf("             clients*fh          access total         ;      cache stuff (MB)  ; start/restart acceses\n");
		printf("srvr          #  read write ; seq. MB  rand. MB  total MB ; efficiency file-size;  zero   eof     #; \n");
		
		for(HashTable<hteData, mountRollupHash, mountRollupEqual>::iterator i = mountRollup.begin();
		    i != mountRollup.end();++i) {
		    fh2mountData d(i->filehandle);
		    fh2mountData *v = fh2mount.lookup(d);
		    string pathname;
		    if (v == NULL) {
			pathname = "unknown:";
			pathname.append(maybehexstring(i->filehandle));
		    } else {
			pathname = maybehexstring(v->pathname);
		    }
		    printf("%08x %6d %6d %5d ; %8.2f %8.2f %8.2f ; %8.2f %8.2f ; %6d %6d %6d; %s\n",
			   i->server, i->nclients, i->client_read_count, i->client_write_count,
			   (double)i->sequential_access_bytes/(double)(1024*1024),
			   (double)(i->total_access_bytes - i->sequential_access_bytes)/(double)(1024*1024),
			   (double)i->total_access_bytes/(double)(1024*1024),
			   (double)i->cache_efficiency_bytes/(double)(1024*1024),
			   (double)i->total_file_bytes/(double)(1024*1024),
			   i->zero_starts, i->eof_restarts, i->total_starts,
			   pathname.c_str());
		}
	    }
	}

	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    DataSeriesModule &source;
    ExtentSeries s;
    Int32Field serverip, clientip;
    Variable32Field operation, filehandle;
    Int64Field offset, filesize, packetat;
    Int32Field bytes;

    ExtentType::int64 first_time, last_time;
};

struct swsSearchFor {
    const string full_fh;
    string mountpartfh;
};

swsSearchFor sws_searchfor[] = {
    { "a63c6021c20f03002000000000000064c20f030064000000400000009a871600", "" },
};
int nsws_searchfor = sizeof(sws_searchfor)/sizeof(swsSearchFor);

static const bool sws_filehandle = false;

class StrangeWriteSearch : public NFSDSModule {
public:
    StrangeWriteSearch(DataSeriesModule &_source)
	: source(_source), 
	opat(s,"packet-at"), server(s,"source"),
	client(s,"dest"),
	operation(s,"operation"), filehandle(s,"filehandle")
    {
	for(int i=0;i<nsws_searchfor;++i) {
	    if (sws_searchfor[i].mountpartfh == "") {
		string tmp = hex2raw(sws_searchfor[i].full_fh);
		fh2mountData::pruneToMountPart(tmp);
		sws_searchfor[i].mountpartfh = tmp;
		printf("sws: searchfor %s\n",hexstring(tmp).c_str());
	    }
	}
    }
    virtual ~StrangeWriteSearch() { }
    
    virtual Extent::Ptr getSharedExtent() {
	if (sws_filehandle) {
	    return source.getSharedExtent();
	} else {
            Extent::Ptr e = source.getSharedExtent();
	    if (e == NULL) return e;
	    for(s.setExtent(e);s.morerecords();++s) {
		if(operation.equal(str_write)) {
		    for(int i=0;i<nsws_searchfor;++i) {
			if (fh2mountData::equalMountParts(filehandle.stringval(),
							  sws_searchfor[i].mountpartfh)) {
			    cout << format("sws: (%s) %s at %d from %08x\n")
				% operation.stringval() % maybehexstring(filehandle.stringval())
				% opat.val() % client.val();
			}
		    }
		}
	    }
	    fflush(stdout);
	    return e;
	}
    }
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	if (sws_filehandle) {
	    int nmountents = 0;
	    string pathwant = hex2raw("e3b9b5e14bb42d52b023bcac3084c5d98df4907152cc34aae66e5f9d878f9549");
	    for(fh2mountT::iterator i = fh2mount.begin();
		i != fh2mount.end();++i) {
		++nmountents;
		if (i->pathname == pathwant) {
		    printf("foundpath %08x:%s\n",i->server,
			   maybehexstring(i->fullfh).c_str());
		} else {
		    printf("notpath %08x:%s\n",i->server,maybehexstring(i->pathname).c_str());
		}
	    }
	    printf("%d mount entries\n",nmountents);
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    DataSeriesModule &source;
    ExtentSeries s;
    Int64Field opat;
    Int32Field server, client;
    Variable32Field operation, filehandle;
};

struct fsKeep {
    const string full_fh;
    string mountpartfh;
    fsKeep() {}
};

class OperationByFileHandle : public RowAnalysisModule {
public:
    OperationByFileHandle(DataSeriesModule &_source)
	: RowAnalysisModule(_source),
	  server(series,"server"),
	  unified_op_id(series,"unified-op-id"), 
	  filehandle(series,"filehandle"),
	  request_at(series,"request-at"), reply_at(series,"reply-at"),
	  payload_len(series,"payload-length"), 
	  min_time(numeric_limits<int64_t>::max()),
	  max_time(numeric_limits<int64_t>::min()),
	  out_of_bounds_count(0)
    {
    }
    virtual ~OperationByFileHandle() { }
    
    struct hteData {
	ExtentType::int32 server;
	uint32_t opcount;
	ConstantString filehandle;
	uint8_t unified_op_id;
	int64_t payload_sum, latency_sum_raw;
	void zero() { opcount = 0; payload_sum = 0; latency_sum_raw = 0; }
    };

    struct byServerFhOp {
	bool operator() (const HashTable_hte<hteData> &a, 
			 const HashTable_hte<hteData> &b) const {
	    if (a.data.server != b.data.server) {
		return a.data.server < b.data.server;
	    }
	    if (a.data.filehandle != b.data.filehandle) {
		return a.data.filehandle < b.data.filehandle;
	    }
	    if (a.data.unified_op_id != b.data.unified_op_id) {
		return unifiedIdToName(a.data.unified_op_id)
		    < unifiedIdToName(b.data.unified_op_id);
	    }
	    return false;
	}
    };

    struct fhOpHash {
	unsigned operator()(const hteData &k) const {
	    unsigned a = lintel::hashBytes(k.filehandle.data(),
					     k.filehandle.size(), 1492);
	    return lintel::BobJenkinsHashMix3(a, k.server, k.unified_op_id);
	}
    };

    struct fhOpEqual {
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.server == b.server && a.filehandle == b.filehandle 
		&& a.unified_op_id == b.unified_op_id;
	}
    };

    struct fhHash {
	unsigned operator()(const hteData &k) const {
	    size_t dataaddr = reinterpret_cast<size_t>(k.filehandle.data());
	    return static_cast<unsigned>(dataaddr) ^ k.server;
	}
    };

    struct fhEqual {
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.server == b.server && a.filehandle == b.filehandle;
	}
    };

    typedef HashTable<hteData, fhOpHash, fhOpEqual> fhOpRollupT;
    fhOpRollupT fhOpRollup;
    typedef HashTable<hteData, fhHash, fhEqual> fhRollupT;
    fhRollupT fhRollup, fsRollup;

    virtual void prepareForProcessing() {
	SINVARIANT(request_at.getType() == reply_at.getType());
    }

    virtual void processRow() {
	unique_fhs.add(ConstantString(filehandle.val(), filehandle.size()));

	hteData k;
	if (request_at.valRaw() < min_time) {
	    min_time = request_at.valRaw();
	} 
	if (reply_at.valRaw() > max_time) {
	    max_time = reply_at.valRaw();
	}
	k.server = server.val();
	k.filehandle = filehandle.stringval();
	INVARIANT(k.filehandle.compare(filehandle.stringval()) == 0, 
		  boost::format("bad %s != %s") 
		  % maybehexstring(k.filehandle) % maybehexstring(filehandle.stringval()));
	k.unified_op_id = unified_op_id.val();
	hteData *v = fhOpRollup.lookup(k);
	if (v == NULL) {
	    k.zero();
	    v = fhOpRollup.add(k);
	}
	++v->opcount;
	v->payload_sum += payload_len.val();
	if (reply_at.valRaw() == request_at.valRaw()) {
	    // TODO: count the number of these "0-latency" events; note with
	    // 10GbE you can see this in the traces as two packets can arrive
	    // with < 1us between them.
	    v->latency_sum_raw += 1;
	} else {
	    INVARIANT(reply_at.valRaw() > request_at.valRaw(), format("no %d %d.\n") 
		      % reply_at.valRaw() % request_at.valRaw());
	    v->latency_sum_raw += reply_at.valRaw() - request_at.valRaw();
	}

	v = fhRollup.lookup(k);
	if (v == NULL) {
	    k.zero();
	    v = fhRollup.add(k);
	}
	INVARIANT(v->filehandle.compare(filehandle.stringval()) == 0, 
		  boost::format("bad %s != %s") 
		  % maybehexstring(v->filehandle) % maybehexstring(filehandle.stringval()));
	++v->opcount;
	v->payload_sum += payload_len.val();
	v->latency_sum_raw += reply_at.valRaw() - request_at.valRaw();

	if (fsRollup.size() < NFSDSAnalysisMod::max_mount_points_expected) {
	    k.filehandle = fh2mountData::pruneToMountPart(k.filehandle);
	    v = fsRollup.lookup(k);
	    if (v == NULL) {
		k.zero();
		v = fsRollup.add(k);
	    }
	    ++v->opcount;
	    v->payload_sum += payload_len.val();
	    v->latency_sum_raw += reply_at.valRaw() - request_at.valRaw();
	}
    }

    // sorting so the regression tests give stable results.

    void printFSRollup() {
	SINVARIANT(request_at.getType() == reply_at.getType());
	printf("FileSystem Rollup:\n");
	if (fsRollup.size() >= NFSDSAnalysisMod::max_mount_points_expected) {
	    printf("Too many filesystems (>= %d) filesystems found, assuming that fh->fs mapping is wrong\n", NFSDSAnalysisMod::max_mount_points_expected);
	} else {
	    fhRollupT::hte_vectorT &raw = fsRollup.unsafeGetRawDataVector();
	    sort(raw.begin(), raw.end(), byServerFhOp());
	    for(fhRollupT::hte_vectorT::iterator i = raw.begin();
		i != raw.end(); ++i) {
		hteData &d(i->data);
		double lat_sum_sec = 
		    request_at.rawToDoubleSeconds(d.latency_sum_raw, false);
		double avglat_us = 1.0e6 * lat_sum_sec / d.opcount;
		fh2mountData fhdata(d.filehandle);
		fh2mountData *v = fh2mount.lookup(fhdata);
		string pathname;
		if (v == NULL) {
		    pathname = "unknown:";
		    pathname.append(maybehexstring(d.filehandle));
		} else {
		    pathname = maybehexstring(v->pathname);
		}
		printf("fsrollup: server %s, fs %s: %d ops %.3f MB, %.3f us avg lat\n",
		       ipv4tostring(d.server).c_str(), pathname.c_str(),
		       d.opcount, d.payload_sum/(1024.0*1024.0), avglat_us);
	    }
	}
    }

    void printFHRollup() {
	printf("\nFH Rollup (%d,%d):\n", fhRollup.size(), unique_fhs.size());
	fhRollupT::hte_vectorT &raw = fhRollup.unsafeGetRawDataVector();
	sort(raw.begin(), raw.end(), byServerFhOp());
	for(fhRollupT::hte_vectorT::iterator i = raw.begin();
	    i != raw.end(); ++i) {
	    // TODO: eliminate duplicate code with printFSRollup?
	    hteData &d(i->data);
	    double lat_sum_sec = 
		request_at.rawToDoubleSeconds(d.latency_sum_raw, false);
	    double avglat_us = 1.0e6 * lat_sum_sec / d.opcount;
	    printf("fhrollup: server %s, fh %s: %d ops %.3f MB, %.3f us avg lat\n",
		   ipv4tostring(d.server).c_str(),
		   maybehexstring(d.filehandle).c_str(),
		   d.opcount,d.payload_sum/(1024.0*1024.0),avglat_us);
	}

    }

    void printFHOpRollup() {
	printf("\nFH/Op Rollup:\n");

	fhOpRollupT::hte_vectorT &raw = fhOpRollup.unsafeGetRawDataVector();
	sort(raw.begin(), raw.end(), byServerFhOp());
	for(fhOpRollupT::hte_vectorT::iterator i = raw.begin();
	    i != raw.end(); ++i) {
	    hteData &d(i->data);
	    // TODO: eliminate duplicate code with printFSRollup?
	    double lat_sum_sec = 
		request_at.rawToDoubleSeconds(d.latency_sum_raw, false);
	    double avglat_us = 1.0e6 * lat_sum_sec / d.opcount;
	    cout << format("fhoprollup: server %s, op %s, fh %s: %d ops %.3f MB, %.3f us avg lat\n")
		% ipv4tostring(d.server) % unifiedIdToName(d.unified_op_id)
		% maybehexstring(d.filehandle)
		% d.opcount % (d.payload_sum/(1024.0*1024.0)) % avglat_us;
	}
    }

    virtual void printResult() {
	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;

	cout << format("Time range: %.3f .. %.3f; %d out of bounds\n")
	    % ((double)min_time/1.0e9) % ((double)max_time/1.0e9)
	    % out_of_bounds_count;

	printFSRollup();
	printFHRollup();
	printFHOpRollup();

	if (false) ConstantString::dumpInfo();
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

    TFixedField<int32_t> server;
    TFixedField<uint8_t> unified_op_id;
    Variable32Field filehandle;
    Int64TimeField request_at, reply_at;
    TFixedField<int32_t> payload_len;

    int64_t min_time, max_time;
    uint64_t out_of_bounds_count;
    HashUnique<ConstantString> unique_fhs;
};

class DirectoryPathLookup : public RowAnalysisModule {
public:
    DirectoryPathLookup(DataSeriesModule &_source)
	: RowAnalysisModule(_source), 
	  filename(series,"filename", Field::flag_nullable), 
	  filehandle(series,"filehandle"),
	  lookup_dir_filehandle(series,"", Field::flag_nullable)
    {
	any_wanted = wanted;
    }

    virtual ~DirectoryPathLookup() { }

    static void processArg(const string &arg) {
	ifstream in(arg.c_str());
	if (in.good()) {
	    while(in.good()) {
		string aline;
		in >> aline;
		if (in.eof()) 
		    break;
		SINVARIANT(in.good());
		wanted.add(hex2raw(aline));
	    }
	} else {
	    wanted.add(hex2raw(arg));
	}
    }
    
    void setBuf(string &buf, Variable32Field &field) {
	if (buf.capacity() < static_cast<uint32_t>(field.size())) {
	    buf.resize(field.size());
	}
	buf.assign(reinterpret_cast<const char *>(field.val()), field.size());
    }

    // 2*FH 32-64 bytes FN 80 bytes = 200 bytes/entry
    // at most 1M saved (0.5M recent, 0.5M old) => 200MB cache.
    // TODO: make it possible to configure this.
    static const unsigned max_recent_cache_size = 500*1000;

    virtual void newExtentHook(const Extent &e) {
	if (lookup_dir_filehandle.getName().empty()) {
	    if (e.getType().hasColumn("lookup_dir_filehandle")) {
		lookup_dir_filehandle.setFieldName("lookup_dir_filehandle");
	    } else if (e.getType().hasColumn("lookup-dir-filehandle")) {
		lookup_dir_filehandle.setFieldName("lookup-dir-filehandle");
	    } else {
		FATAL_ERROR("can't figure out lookup dir filehandle field name");
	    }
	} 

	if (tmpfh2info.size_recent() >= max_recent_cache_size) {
	    tmpfh2info.rotate();
	}
    }

    virtual void processRow() {
	if (filename.isNull() && lookup_dir_filehandle.isNull()) {
	    return; // nothing useful
	}
	SINVARIANT(!filehandle.isNull() && !lookup_dir_filehandle.isNull());

	setBuf(fhbuf, filehandle);
	setBuf(ldfhbuf, lookup_dir_filehandle);
	setBuf(fnbuf, filename);

	if (any_wanted.exists(fhbuf)) {
	    fhinfo *v = fh2info.lookup(fhbuf);
	    if (v == NULL) {
		fh2info[fhbuf] = fhinfo(fnbuf, ldfhbuf);
		if (false) {
		    cout << format("add %s %s %s\n") % hexstring(fhbuf)
			% hexstring(ldfhbuf) % hexstring(fnbuf);
		}
		any_wanted.add(ldfhbuf);
		while(tmpfh2info.exists(ldfhbuf)) {
		    fhinfo &w(tmpfh2info[ldfhbuf]);
		    if (false) {
			cout << format("chain-add %s %s %s\n") % hexstring(ldfhbuf)
			    % hexstring(w.lookup_dir_filehandle) 
			    % hexstring(w.filename);
		    }
		    fh2info[ldfhbuf] = w;
		    any_wanted.add(w.lookup_dir_filehandle);
		    ldfhbuf = w.lookup_dir_filehandle;
		}
	    } else {
		// INVARIANT(v->filename == fnbuf && v->lookup_dir_filehandle == ldfhbuf);
	    }
	} else if (!tmpfh2info.exists(fhbuf)) {
	    if (false) {
		cout << format("stash %s %s %s\n") % hexstring(fhbuf)
		    % hexstring(ldfhbuf) % hexstring(fnbuf);
	    }
	    tmpfh2info[fhbuf] = fhinfo(fnbuf, ldfhbuf);
	}
	    
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	for(HashUnique<string>::iterator i = wanted.begin();
	    i != wanted.end(); ++i) {
	    string curfh = *i;
	    vector<string> path;
	    while(fh2info.exists(curfh)) {
		fhinfo &fhi(fh2info[curfh]);
		path.push_back(maybehexstring(fhi.filename));
		curfh = fhi.lookup_dir_filehandle;
	    }

	    ConstantString cs_curfh(curfh);
	    fh2mountData mountkey(cs_curfh);
	    fh2mountData *mount = fh2mount.lookup(mountkey);
	    if (mount == NULL) {
		path.push_back(string("FH:") + hexstring(curfh));
	    } else {
		path.push_back(hexstring(mount->pathname));
	    }
	    
	    reverse(path.begin(), path.end());
	    cout << format("%s: %s\n") % hexstring(*i)
		% join("/", path);
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    Variable32Field filename, filehandle, lookup_dir_filehandle;

    struct fhinfo {
	string filename, lookup_dir_filehandle;
	fhinfo(const string &a, const string &b)
	    : filename(a), lookup_dir_filehandle(b) 
	{ }
	fhinfo() { }
    };
    HashMap<string, fhinfo> fh2info;
    static HashUnique<string> wanted;
    HashUnique<string> any_wanted; // wanted + any intermediates
    string fhbuf, ldfhbuf, fnbuf;
    RotatingHashMap<string, fhinfo> tmpfh2info;
};

HashUnique<string> DirectoryPathLookup::wanted;

class TimeBoundPrune : public NFSDSModule {
public:
    TimeBoundPrune(DataSeriesModule &_source, int min_sec, int max_sec)
	: source(_source),
	  min_time((ExtentType::int64)min_sec * 1000000000),
	  max_time((ExtentType::int64)max_sec * 1000000000),
	  packet_at(s,"packet-at"),
	  pass_through_extents(0), partial_extents(0),
	  total_kept_records(0), partial_kept_records(0), partial_pruned_records(0),
	  dest_series(NULL),
	  copier(NULL) {
    }

    DataSeriesModule &source;
    const ExtentType::int64 min_time, max_time;
    ExtentSeries s;
    Int64Field packet_at;

    ExtentType::int64 pass_through_extents, partial_extents, 
	total_kept_records, partial_kept_records, partial_pruned_records;
    
    ExtentSeries *dest_series;
    ExtentRecordCopy *copier;

    virtual ~TimeBoundPrune() { 
	delete dest_series;
	delete copier;
    }

    virtual Extent::Ptr getSharedExtent() {
        Extent::Ptr e = source.getSharedExtent();
	if (e == NULL) return e;
	// expect out of bounds case to be rare.
	bool any_out_of_bounds = false;
	int record_count = 0;
	for(s.setExtent(e);s.morerecords();++s) {
	    if (packet_at.val() < min_time || packet_at.val() >= max_time) {
		any_out_of_bounds = true;
		break;
	    }
	    ++record_count;
	}
	if (any_out_of_bounds == false) {
	    ++pass_through_extents;
	    total_kept_records += record_count;
	    return e;
	}

	++partial_extents;
	if (dest_series == NULL) {
	    dest_series = new ExtentSeries(e->type);
	    copier = new ExtentRecordCopy(s,*dest_series);
	}
	
        Extent::Ptr out_extent(new Extent(*dest_series));

	// need to copy all of the rows that we want to pass on
	dest_series->setExtent(out_extent);
	// count in an int as that should be faster.
	int kept_records = 0, pruned_records = 0;
	for(s.setExtent(e);s.morerecords();++s) {
	    if (packet_at.val() < min_time || packet_at.val() >= max_time) {
		++pruned_records;
	    } else {
		++kept_records;
		dest_series->newRecord();
		copier->copyRecord();
	    }
	}

	total_kept_records += kept_records;
	partial_kept_records += kept_records;
	partial_pruned_records += pruned_records;
	if (kept_records == 0) { 
	    // rare case that we didn't keep any of the records, make
	    // sure we return a non-empty extent; simplifies
	    // downstream code which can now assume extents are
	    // non-empty
	    return getSharedExtent();
	}
	return out_extent;
    }
    
    virtual void printResult() {
	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;
	cout << format("  %d total extents, %d unchanged, %d partials\n")
	    % (pass_through_extents + partial_extents) % pass_through_extents % partial_extents;
	cout << format("  %d total records kept, %d records in partials, %d kept, %d pruned\n")
	    % total_kept_records % ( partial_kept_records + partial_pruned_records) 
	    % partial_kept_records % partial_pruned_records;
	cout << format("End-%s\n") % __PRETTY_FUNCTION__;
    }

};

class FileHandleOperationLookup : public RowAnalysisModule {
public:
    FileHandleOperationLookup(DataSeriesModule &_source)
	: RowAnalysisModule(_source),
	  filename(series,"filename", Field::flag_nullable), 
	  filehandle(series,"filehandle"),
	  file_size(series,"file-size"),
	  modify_time(series,"modify-time"),
	  client(series,"client"),
	  server(series,"server"),
	  replyat(series,"reply-at"),
	  unified_op_id(series,"unified-op-id"),
	  type(series,"type"),
	  printed_header(false)
    {
    }
    
    virtual ~FileHandleOperationLookup() { }

    virtual void processRow() {
	if (wanted.exists(filehandle.stringval())) {
	    if (!printed_header) {
		cout << "FHOL: reply-at client server ; operation type filehandle filename; file-size modify-time\n";
		printed_header = true;
	    }
	    cout << format("FHOL: %s %s %s ; %s %s %s '%s' ; %d %s\n")
		% replyat.valStrSecNano() % ipv4tostring(client.val()) 
		% ipv4tostring(server.val()) 

		% unifiedIdToName(unified_op_id.val()) % type.stringval() 
		% hexstring(filehandle.stringval()) 
		% maybehexstring(filename.stringval())

		% file_size.val() % modify_time.valStrSecNano();
	}
    }
    virtual void printResult() {
	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;
	cout << "  (already printed everything with FHOL prefix)\n";
	cout << format("End-%s\n") % __PRETTY_FUNCTION__;
    }

    static void processArg(const string &arg) {
	ifstream in(arg.c_str());
	if (in.good()) {
	    while(in.good()) {
		string aline;
		in >> aline;
		if (in.eof()) 
		    break;
		SINVARIANT(in.good());
		wanted.add(hex2raw(aline));
	    }
	} else {
	    wanted.add(hex2raw(arg));
	}
    }
	
    Variable32Field filename, filehandle;
    TFixedField<int64_t> file_size;
    Int64TimeField modify_time;
    TFixedField<int32_t> client, server;
    Int64TimeField replyat;
    TFixedField<uint8_t> unified_op_id;
    Variable32Field type;

    bool printed_header;
    static HashUnique<string> wanted;    
};

HashUnique<string> FileHandleOperationLookup::wanted;

static bool need_mount_by_filehandle = false;
static bool need_filename_by_filehandle = false;
static bool late_filename_by_filehandle_ok = true;

void
usage(char *progname) 
{
    cout << "Usage: " << progname << " flags... (file...)|(index.ds start-time end-time)\n";
    cout << " flags\n";
    cout << "    Note, most of the options are disabled because they haven't been tested.\n";
    cout << "    However, it is trivial to change the source to re-enable them, but people\n";
    cout << "    doing so should make an effort to verify the code and results.\n";
    cout << "    It's probably also worth cleaning up the code at the same time, e.g.\n";
    cout << "    replacing printfs with boost::format, int with [u]int{32,64}_t as\n";
    cout << "    appropriate, and any other cleanups that seem appropriate.\n";
    cout << "    please send in patches (software@cello.hpl.hp.com) if you do this.\n";
	
    cout << "    -h # display this help\n";
    cout << "    -a # Operation count by filehandle\n";
    cout << "    -b {,output_sql} # Server latency analysis\n";
    cout << "    -c <seconds> # Host analysis\n";
    cout << "    -d <fh|fh-list pathname> # directory path lookup\n";
    cout << "    -e <fh | fh-list pathname> # look up all the operations associated with a filehandle\n";
    cout << "    -f # Read/Write Extent analysis\n";
    cout << "    -g # Attr-Ops Extent analysis\n";
    cout << "    -i # Sequentiality analysis\n";
    cout << "    -Z <series-to-print> # common, attr-ops, rw, merge12, merge123\n";

//    cerr << "    #-b # Unique bytes in file handles analysis\n";
//    cerr << "    #-c <recent-age-in-seconds> # File age by file handle analysis\n";
//    cerr << "    #-d # Large file write analysis by file handle\n";
//    cerr << "    #-e # Large file write analysis by file name\n";
//    cerr << "    #-f # Large file analysis by file handle\n";
//    cerr << "    #-g # Large file analysis by file name\n";
//    cerr << "    #-i # NFS operation/payload analysis\n";
//    cerr << "    #-k # Client-server pair payload analysis\n";
//    cerr << "    #-m # Overall payload analysis\n";
//    cerr << "    #-n # File size analysis\n";
//    cerr << "    #-o # Unbalanced operations analysis\n";
//    cerr << "    #-p <time-gap in ms to treat as skip> # Time range analysis\n";
//    cerr << "    #-q <sampling interval in ms> # Read analysis\n";
//    cerr << "    #-r # Common bytes between filehandle and dirhandle\n";
//    cerr << "    #-s # Sequential whole access\n";
//    cerr << "    #-t # Strange write search\n";
//    cerr << "    #-v <series> # select series to print 1 = common, 2 = attrops, 3 = rw, 4 = common/attr join, 5 = common/attr/rw join\n";
//    cerr << "    #-w # servers per filehandle\n";
//    cerr << "    #-x # transactions\n";
//    cerr << "    #-y # outstanding requests\n";
    exit(1);
}

void addDSToText(SequenceModule &to) {
    to.addModule(new DStoTextModule(to.tail()));
}

int parseopts(int argc, char *argv[], SequenceModule &commonSequence,
	      SequenceModule &attrOpsSequence, SequenceModule &rwSequence,
	      SequenceModule &merge12Sequence, 
	      SequenceModule &merge123Sequence) {
    bool any_selected;

    // TODO: redo this so it gets passed the sequences and just stuffs
    // things into them.  


    any_selected = false;
    bool add_directory_path_lookup = false;
    bool add_file_handle_operation_lookup = false;

    while (1) {
	int opt = getopt(argc, argv, "hab:c:d:e:fgi:jZ:");
	if (opt == -1) break;
	any_selected = true;
	switch(opt){
	case 'h': usage(argv[0]); break;
	case 'a': merge12Sequence.addModule
		      (new OperationByFileHandle(merge12Sequence.tail()));
	    need_mount_by_filehandle = 1;
	    break;
	case 'b': 
	    commonSequence.addModule
		(NFSDSAnalysisMod::newServerLatency(commonSequence.tail(), optarg));
	    break;
	case 'c': 
	    commonSequence.addModule
		(NFSDSAnalysisMod::newHostInfo(commonSequence.tail(), optarg));
	    break;
	case 'd': 
	    need_mount_by_filehandle = true;
	    DirectoryPathLookup::processArg(optarg);
	    // will add module once at the end of processing arguments
	    add_directory_path_lookup = true;
	    break;
	case 'e': 
	    FileHandleOperationLookup::processArg(optarg);
	    // will add module once at the end of processing arguments
	    add_file_handle_operation_lookup = true;
	    break;
	case 'f':
	    rwSequence.addModule
		(newReadWriteExtentAnalysis(rwSequence.tail()));
	    break;
	case 'g':
	    attrOpsSequence.addModule
		(newUniqueFileHandles(attrOpsSequence.tail()));
	    break;
	case 'i':
#if 0
	    // experiment, <10% difference
	    merge123Sequence.addModule
		(new PrefetchBufferModule(merge123Sequence.tail(), 1024*1024));
#endif
	    merge123Sequence.addModule
		(newSequentiality(merge123Sequence.tail(), optarg));
	    break;
	case 'j':
	    commonSequence.addModule(newMissingOps(commonSequence.tail()));
	    break;
	case 'Z': {
	    string arg = optarg;
	    if (arg == "common") {
		addDSToText(commonSequence);
	    } else if (arg == "attr-ops") {
		addDSToText(attrOpsSequence);
	    } else if (arg == "rw") {
		addDSToText(rwSequence);
	    } else if (arg == "merge12") {
		addDSToText(merge12Sequence);
	    } else if (arg == "merge123") {
		addDSToText(merge123Sequence);
	    } else {
		FATAL_ERROR(format("Unknown choice to print '%s'") % arg);
	    }
	    break;
	}
	    
#if UNTESTED_ANALYSIS_DISABLED
	case 'b': FATAL_ERROR("untested");options[Unique] = 1; break;
	case 'c': FATAL_ERROR("untested");{
	    options[optFileageByFilehandle] = 1;
	    int tmp = atoi(optarg);
	    INVARIANT(tmp > 0, format("invalid -c %d") % tmp);
	    FileageByFilehandle_recent_secs.push_back(tmp);
	    break;
	}
	case 'd': FATAL_ERROR("untested");options[Largewrite_Handle] = 1; break;
	case 'e': FATAL_ERROR("untested");options[Largewrite_Name] = 1;
	          need_filename_by_filehandle = true;
		  late_filename_by_filehandle_ok = false;
		  break;
	case 'f': FATAL_ERROR("untested");options[Largefile_Handle] = 1; break;
	case 'g': FATAL_ERROR("untested");options[Largefile_Name] = 1;
	          need_filename_by_filehandle = true;
		  late_filename_by_filehandle_ok = false;
		  break;
	case 'i': FATAL_ERROR("untested");options[optNFSOpPayload] = 1; break;
	case 'k': FATAL_ERROR("untested");options[optClientServerPairInfo] = 1; break;
	case 'm': FATAL_ERROR("untested");options[optPayloadInfo] = 1; break;
	case 'n': FATAL_ERROR("untested");options[optFileSizeByType] = 1; break;
	case 'o': FATAL_ERROR("untested");options[optUnbalancedOps] = 1; break;
	case 'p': FATAL_ERROR("untested");options[optNFSTimeGaps] = 1; 
	    NFSDSAnalysisMod::gap_parm = atof(optarg); 
	    break;
	case 'q': FATAL_ERROR("untested");options[File_Read] = 1; 
	    NFSDSAnalysisMod::read_sampling = atof(optarg); 
	    break;
	case 'r': FATAL_ERROR("untested");options[Commonbytes] = 1;
	      	  need_mount_by_filehandle = true;
		  need_filename_by_filehandle = true;
		  late_filename_by_filehandle_ok = false;
	      	  break;
	case 's': FATAL_ERROR("untested");options[optSequentialWholeAccess] = 1;
	    need_filename_by_filehandle = true;
	    need_mount_by_filehandle = true;
	    break;
	case 't': FATAL_ERROR("untested");options[strangeWriteSearch] = 1;
	    if (sws_filehandle) {
		need_mount_by_filehandle = true;
	    }
	    break;
	case 'v': print_input_series = atoi(optarg);
	    INVARIANT(print_input_series >= 1 && print_input_series <= 5,
		      format("invalid print input series '%s'") % optarg);
	    break;
	case 'w': FATAL_ERROR("untested");options[optServersPerFilehandle] = 1; break;
	case 'x': FATAL_ERROR("untested");options[optTransactions] = 1; break;
	case 'y': FATAL_ERROR("untested");options[optOutstandingRequests] = 1; 
	  latency_offset = atoi(optarg);
	  break;
#endif

	case '?': FATAL_ERROR("invalid option");
	default:
	    FATAL_ERROR(format("getopt returned '%c'\n") % opt);
	}
    }
    if (any_selected == false) {
	cerr << "need to select some modules\n";
	usage(argv[0]);
    }

    if (add_directory_path_lookup) {
	attrOpsSequence.addModule
	    (new DirectoryPathLookup(attrOpsSequence.tail()));
    }
    if (add_file_handle_operation_lookup) {
	merge12Sequence.addModule
	    (new FileHandleOperationLookup(merge12Sequence.tail()));
    }
    return optind;
}

void printResult(SequenceModule::DsmPtr mod) {
    if (mod == NULL) {
	return;
    }
    NFSDSModule *nfsdsmod = dynamic_cast<NFSDSModule *>(mod.get());
    RowAnalysisModule *rowmod = dynamic_cast<RowAnalysisModule *>(mod.get());
    if (nfsdsmod != NULL) {
	nfsdsmod->printResult();
    } else if (rowmod != NULL) {
	rowmod->printResult();
    } else {
	INVARIANT(dynamic_cast<DStoTextModule *>(mod.get()) != NULL
		  || dynamic_cast<PrefetchBufferModule *>(mod.get()) != NULL,
		  "Found unexpected module in chain");
    }

    printf("\n");
}

bool isNumber(const char *v) {
    for(; *v != '\0'; ++v) {
	if (!isdigit(*v)) {
	    return false;
	}
    }
    return true;
}

static int32_t timebound_start = 0;
static int32_t timebound_end = numeric_limits<int32_t>::max();

void setupInputs(int first, int argc, char *argv[], TypeIndexModule *sourcea,
		 TypeIndexModule *sourceb, TypeIndexModule *sourcec,
		 TypeIndexModule *sourced, SequenceModule &commonSequence) {
    bool timebound_set = false;
    if ((argc - first) == 3 && isNumber(argv[first+1]) && isNumber(argv[first+2])) {
	timebound_set = true;
	timebound_start = atoi(argv[first+1]);
	timebound_end = atoi(argv[first+2]);

	// TODO: replace this with minmaxindexmodule; will need to extend it
	// so that the first round of indexing can look up the range for the
	// common attributes and from the min and max record ids use that to
	// look up values in the remaining indices.  Will also end up wanting
	// more than one index to be allowed in a single file.

	sourceByIndex(sourcea,argv[first],timebound_start,timebound_end);
    } else {
	for(int i=first; i<argc; ++i) {
	    sourcea->addSource(argv[i]); 
	}
    }

    // Don't start prefetching, will cause us to read some things we
    // may not have to read.

    sourceb->sameInputFiles(*sourcea);
    sourcec->sameInputFiles(*sourcea);
    sourced->sameInputFiles(*sourcea);

    if (timebound_set) {
	commonSequence.addModule(new TimeBoundPrune(commonSequence.tail(),
						    timebound_start, 
						    timebound_end));
    }
}

int main(int argc, char *argv[]) {
    registerUnitsEpoch();

    LintelLog::parseEnv();
    // TODO: make sources an array/vector.
    TypeIndexModule *sourcea = new TypeIndexModule("NFS trace: common");
    sourcea->setSecondMatch("Trace::NFS::common");
    TypeIndexModule *sourceb = new TypeIndexModule("NFS trace: attr-ops");
    sourceb->setSecondMatch("Trace::NFS::attr-ops");
    TypeIndexModule *sourcec = new TypeIndexModule("NFS trace: read-write");
    sourcec->setSecondMatch("Trace::NFS::read-write");
    TypeIndexModule *sourced = new TypeIndexModule("NFS trace: mount");
    sourced->setSecondMatch("Trace::NFS::mount");

    SequenceModule commonSequence(sourcea);
    SequenceModule attrOpsSequence(sourceb);
    SequenceModule rwSequence(sourcec);
    SequenceModule merge12Sequence(NFSDSAnalysisMod::newAttrOpsCommonJoin());
    SequenceModule merge123Sequence(NFSDSAnalysisMod::newCommonAttrRWJoin());

    int first = parseopts(argc, argv, commonSequence, attrOpsSequence, 
			  rwSequence, merge12Sequence, merge123Sequence);

    if (argc - first < 1) {
	usage(argv[0]);
    }

    setupInputs(first, argc, argv, sourcea, sourceb,
		sourcec, sourced, commonSequence);

    // these are the three threads that we will build according to the
    // selected analyses

    // need to pre-build the table before we can do the write-filename
    // analysis after the merge join.

    if (need_filename_by_filehandle) {
	if (late_filename_by_filehandle_ok) {
	    attrOpsSequence.addModule(NFSDSAnalysisMod::newFillFH2FN_HashTable(attrOpsSequence.tail()));
	} else {
	    PrefetchBufferModule *ptmp = new PrefetchBufferModule(*sourceb,32*1024*1024);
	    NFSDSModule *tmp = NFSDSAnalysisMod::newFillFH2FN_HashTable(*ptmp);
	    tmp->getAndDeleteShared();
	    
	    sourceb->resetPos();
	    delete tmp;
	    delete ptmp;
	}
    }

    if (need_mount_by_filehandle) {
	NFSDSModule *tmp = NFSDSAnalysisMod::newFillMount_HashTable(*sourced);
	tmp->getAndDeleteShared();
	delete tmp;
    }

    // TODO: remove the ->get(), use shared pointers throughout; check all the other
    // changes from this commit as well.
    // merge join with attributes
    NFSDSAnalysisMod::setAttrOpsSources
	(merge12Sequence.begin()->get(), commonSequence, attrOpsSequence);

    // merge join with read-write data
    NFSDSAnalysisMod::setCommonAttrRWSources
	(merge123Sequence.begin()->get(), merge12Sequence, rwSequence);


    // 2004-09-02: tried experiment of putting some of the
    // bigger-memory bits into their own threads using a prefetch
    // buffer -- performance got much worse with way more time being
    // spent in the kernel.  Possibly hypothesis is that this was
    // malloc library issues as both those modules did lots of
    // malloc/free.

    // only pull through what we actually need to pull through.
    if (merge123Sequence.size() > 1) {
	sourcea->startPrefetching(32*1024*1024, 96*1024*1024);
	sourceb->startPrefetching(32*1024*1024, 96*1024*1024);
	sourcec->startPrefetching(32*1024*1024, 96*1024*1024);
	merge123Sequence.getAndDeleteShared();
    } else if (merge12Sequence.size()> 1) {
	sourcea->startPrefetching(32*1024*1024, 96*1024*1024);
	sourceb->startPrefetching(32*1024*1024, 96*1024*1024);
	merge12Sequence.getAndDeleteShared();
	if (rwSequence.size() > 1) {
	    rwSequence.getAndDeleteShared();
	}
    } else {
	if (commonSequence.size() > 1) {
	    sourcea->startPrefetching(8*32*1024*1024, 8*96*1024*1024);
	    commonSequence.getAndDeleteShared();
	}
	if (attrOpsSequence.size() > 1) {
	    sourceb->startPrefetching(32*1024*1024, 96*1024*1024);
	    attrOpsSequence.getAndDeleteShared();
	}
	if (rwSequence.size() > 1) {
	    sourcec->startPrefetching(32*1024*1024, 96*1024*1024);
	    rwSequence.getAndDeleteShared();
	}
    }

    // print all results ...

    for(SequenceModule::iterator i = commonSequence.begin() + 1;
	i != commonSequence.end(); ++i) {
	printResult(*i);
    }
    for(SequenceModule::iterator i = attrOpsSequence.begin() + 1;
	i != attrOpsSequence.end(); ++i) {
	printResult(*i);
    }
    for(SequenceModule::iterator i = rwSequence.begin() + 1;
	i != rwSequence.end(); ++i) {
	printResult(*i);
    }
    for(SequenceModule::iterator i = merge12Sequence.begin();
	i != merge12Sequence.end(); ++i) {
	printResult(*i);
    }
    for(SequenceModule::iterator i = merge123Sequence.begin();
	i != merge123Sequence.end(); ++i) {
	printResult(*i);
    }
	
    printf("extents: %.2f MB -> %.2f MB\n",
	   (double)(sourcea->total_compressed_bytes + sourceb->total_compressed_bytes + sourcec->total_compressed_bytes + sourced->total_compressed_bytes)/(1024.0*1024),
	   (double)(sourcea->total_uncompressed_bytes + sourceb->total_uncompressed_bytes + sourcec->total_uncompressed_bytes + sourced->total_uncompressed_bytes)/(1024.0*1024));

    printf("                   common  attr-ops read-write  mount\n");
    printf("MB compressed:   %8.2f %8.2f %8.2f %8.2f\n",
	   (double)sourcea->total_compressed_bytes/(1024.0*1024),
	   (double)sourceb->total_compressed_bytes/(1024.0*1024),
	   (double)sourcec->total_compressed_bytes/(1024.0*1024),
	   (double)sourced->total_compressed_bytes/(1024.0*1024));
    printf("MB uncompressed: %8.2f %8.2f %8.2f %8.2f\n",
	   (double)sourcea->total_uncompressed_bytes/(1024.0*1024),
	   (double)sourceb->total_uncompressed_bytes/(1024.0*1024),
	   (double)sourcec->total_uncompressed_bytes/(1024.0*1024),
	   (double)sourced->total_uncompressed_bytes/(1024.0*1024));
    printf("wait fraction :  %8.2f %8.2f %8.2f %8.2f\n",
	   sourcea->waitFraction(),
	   sourceb->waitFraction(),
	   sourcec->waitFraction(),
	   sourced->waitFraction());
    sourcea->close();
    sourceb->close();
    sourcec->close();
    sourced->close();
    delete sourced; // a-c deleted by their SequenceModules
    return 0;
}

