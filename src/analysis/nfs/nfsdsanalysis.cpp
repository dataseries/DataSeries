/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

using namespace std;

#include <getopt.h>
#include <ctype.h>
#include <signal.h>
#include <setjmp.h>
#include <limits.h>

#include <list>
#include <ostream>
#include <algorithm>

#include <Lintel/AssertBoost.H>
#include <Lintel/ConstantString.H>
#include <Lintel/HashTable.H>
#include <Lintel/HashUnique.H>
#include <Lintel/HashMap.H>
#include <Lintel/Stats.H>
#include <Lintel/StringUtil.H>

#include <DataSeries/DStoTextModule.H>
#include <DataSeries/GeneralField.H>
#include <DataSeries/PrefetchBufferModule.H>
#include <DataSeries/SequenceModule.H>
#include <DataSeries/TypeIndexModule.H>

#include "analysis/nfs/mod1.hpp"
#include "analysis/nfs/mod2.hpp"
#include "analysis/nfs/mod3.hpp"
#include "analysis/nfs/mod4.hpp"
#include "process/sourcebyrange.H"

// needed to make g++-3.3 not suck.
extern int printf (__const char *__restrict __format, ...) 
   __attribute__ ((format (printf, 1, 2)));

// TODO: consider whether we need to make the attribute rollups apply
// only after the common/attr join as we now prune common rows based
// on timestamps, but are not pruning the attr op rollups in the same
// way.  Probably doesn't matter given the current analysis that are done.

enum optionsEnum {
    // Common rollups
    optNFSOpPayload = 0,     // server payload analysis
    optClientServerPairInfo, // client-server pair payload analysis
    optHostInfo,             // host analysis
    optPayloadInfo,          // overall payload analysis
    optServerLatency,        // server latency analysis
    optUnbalancedOps,        // unbalanced operations analysis
    optNFSTimeGaps,          // time gap analysis
    optTransactions,         // transaction analysis
    optOutstandingRequests,  // outstanding requests analysis

    // Attribute rollups
    optFileSizeByType,       // file size analysis
    Commonbytes,           // common bytes in dir/fh analysis
    Unique,                // unique bytes in file handles analysis
    fileHandleLookup,      // lookup a bunch of file handles given in a file

    // Common + Attribute rollups
    optFileageByFilehandle,               // file age by file handle analysis
    Largewrite_Handle,     // large file write analysis by file handle
    Largewrite_Name,       // large file write analysis by file name
    Largefile_Handle,      // large file analysis by file handle
    Largefile_Name,        // large file analysis by file name
    strangeWriteSearch,    // search for strange write operations
    optOperationByFileHandle, // per-operation information grouped by filename (or filehandle if name is unknown)
    optServersPerFilehandle, //how many servers for each file handle?
    // Common + Attribute + RW rollups
    File_Read,               // files read analysis
    optSequentialWholeAccess, // count files accessed sequentially and completely
    optTmpFilehandleLookup, // look for info specific to collision search

    LastOption
};

static bool options[LastOption];

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
	unsigned operator()(const hteData &k) {
	    return HashTable_hashbytes(k.filehandle.data(),k.filehandle.size(),
				       BobJenkinsHashMix3(k.server,k.client,(k.is_read ? 0x5555 : 0xAAAA)));
	}
    };

    struct hteEqual {
	bool operator()(const hteData &a, const hteData &b) {
	    return a.server == b.server && a.client == b.client && a.is_read == b.is_read &&
		a.filehandle == b.filehandle;
	}
    };

    HashTable<hteData, hteHash, hteEqual> seqaccessexact;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL)
	    return NULL;
	INVARIANT(e->type.getName() == "common-attr-rw-join","bad");
	hteData k;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
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
		AssertFatal(("internal"));
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
			AssertAlways(bytes.val() == 0,("whoa, read beyond file size got bytes"));
			printf("tolerating weird over %s on %s at %lld from %08x? %lld > %lld ; %d\n",
			       v->is_read ? "read" : "write",
			       maybehexstring(*filename).c_str(),
			       packetat.val(), clientip.val(),
			       offset.val(),filesize.val(),
			       bytes.val());
		    }
		}
		v->cur_offset = filesize.val();
	    }
	}
	return e;
    }

    struct rollupHash {
	unsigned operator()(const hteData &k) {
	    return HashTable_hashbytes(k.filehandle.data(),k.filehandle.size(),k.server);
	}
    };

    struct rollupEqual {
	bool operator()(const hteData &a, const hteData &b) {
	    return a.server == b.server && a.filehandle == b.filehandle;
	}
    };

    struct mountRollupHash {
	unsigned operator()(const hteData &k) {
	    return HashTable_hashbytes(k.filehandle.data(),k.filehandle.size(),k.server);
	}
    };

    struct mountRollupEqual {
	bool operator()(const hteData &a, const hteData &b) {
	    return a.server == b.server && a.filehandle == b.filehandle;
	}
    };

    static const bool print_per_client_fh = false;
    static const bool print_per_fh = true;
    static const bool print_per_mount = false;

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	printf("time range is %lld .. %lld\n",first_time,last_time);
	if (print_per_client_fh) printf("exact client/server/filehandle/rw info:\n");
	// calculate per-(server,fh) rollup while doing the client/server/fh/rw report
	HashTable<hteData, rollupHash, rollupEqual> rollup;

	for(HashTable<hteData, hteHash, hteEqual>::iterator i = seqaccessexact.begin();
	    i != seqaccessexact.end();++i) {
	    if (print_per_client_fh) {
		string *filename = fnByFileHandle(i->filehandle);
		if (filename == NULL) filename = &i->filehandle;
		printf("  %08x %08x %5s: %9lld %9lld %8lld ; %3d %3d %3d; %s\n",
		       i->server,i->client,i->is_read ? "read" : "write",
		       i->sequential_access_bytes, i->total_access_bytes, i->max_file_size,
		       i->zero_starts, i->eof_restarts, i->total_starts,
		       maybehexstring(*filename).c_str());
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
			printf("odd, %s changed max size from %lld (on %08x/%08x a %s) to %lld via a %s of %08x/%08x\n",
			       maybehexstring(*filename).c_str(),
			       rv->max_file_size,rv->client,rv->server,
			       rv->is_read ? "read" : "write", i->max_file_size,
			       i->is_read ? "read" : "write",i->client,i->server);
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
	    AssertAlways(i->total_access_bytes == i->total_write_bytes + i->total_read_bytes,
			 ("bad %lld %lld %lld",i->total_access_bytes,i->total_read_bytes,i->total_write_bytes));
	    if (print_per_fh) {
		printf("%08x %3d %3d %3d: %7.4f %7.4f %7.4f %9lld ; %3d %3d %3d; %s\n",
		       i->server, i->nclients, i->client_read_count, i->client_write_count,
		       (double)i->sequential_access_bytes/(double)i->max_file_size,
		       (double)(i->total_access_bytes - i->sequential_access_bytes)/(double)i->max_file_size,
		       (double)i->total_access_bytes/(double)i->max_file_size, i->max_file_size,
		       i->zero_starts, i->eof_restarts, i->total_starts,
		       filename == NULL ? maybehexstring(i->filehandle).c_str() : maybehexstring(*filename).c_str());
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
    
    virtual Extent *getExtent() {
	if (sws_filehandle) {
	    return source.getExtent();
	} else {
	    Extent *e = source.getExtent();
	    if (e == NULL) return NULL;
	    for(s.setExtent(e);s.pos.morerecords();++s.pos) {
		if(operation.equal(str_write)) {
		    for(int i=0;i<nsws_searchfor;++i) {
			if (fh2mountData::equalMountParts(filehandle.stringval(),
							  sws_searchfor[i].mountpartfh)) {
				printf("sws: (%s) %s at %lld from %08x\n",
				       operation.stringval().c_str(),
				       maybehexstring(filehandle.stringval()).c_str(),
				       opat.val(),client.val());
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

class OperationByFileHandle : public NFSDSModule {
public:
    OperationByFileHandle(DataSeriesModule &_source, int _timebound_start, 
			  int _timebound_end)
	: timebound_start((ExtentType::int64)_timebound_start 
			  * 1000 * 1000 * 1000), 
	  timebound_end((ExtentType::int64)_timebound_end 
			* 1000 * 1000 * 1000),
	  source(_source), server(s,"server"),
	  operation(s,"operation"), filehandle(s,"filehandle"),
	  request_at(s,"request-at"), reply_at(s,"reply-at"),
	  payload_len(s,"payload-length")
    {
	min_time = 0x7FFFFFFFLL << 32;
	max_time = 0;
    }
    virtual ~OperationByFileHandle() { }
    
    struct hteData {
	ExtentType::int32 server;
	uint32_t opcount;
	ConstantString filehandle, operation;
	ExtentType::int64 payload_sum, latency_sum;
	void zero() { opcount = 0; payload_sum = 0; latency_sum = 0; }
    };

    struct byServerFhOp {
	bool operator() (const HashTable_hte<hteData> &a, 
			 const HashTable_hte<hteData> &b) {
	    if (a.data.server != b.data.server) {
		return a.data.server < b.data.server;
	    }
	    if (a.data.filehandle != b.data.filehandle) {
		return a.data.filehandle < b.data.filehandle;
	    }
	    if (a.data.operation != b.data.operation) {
		return a.data.operation < b.data.operation;
	    }
	    return false;
	}
    };

    struct fhOpHash {
	unsigned operator()(const hteData &k) {
	    unsigned ret = HashTable_hashbytes(k.filehandle.data(),k.filehandle.size(),k.server);
	    return HashTable_hashbytes(k.operation.data(),k.operation.size(),ret);
	}
    };

    struct fhOpEqual {
	bool operator()(const hteData &a, const hteData &b) {
	    return a.server == b.server && 
		a.filehandle == b.filehandle && a.operation == b.operation;
	}
    };

    struct fhHash {
	unsigned operator()(const hteData &k) {
	    size_t dataaddr = reinterpret_cast<size_t>(k.filehandle.data());
	    return static_cast<unsigned>(dataaddr) ^ k.server;
	}
    };

    struct fhEqual {
	bool operator()(const hteData &a, const hteData &b) {
	    return a.server == b.server && a.filehandle == b.filehandle;
	}
    };

    typedef HashTable<hteData, fhOpHash, fhOpEqual> fhOpRollupT;
    fhOpRollupT fhOpRollup;
    typedef HashTable<hteData, fhHash, fhEqual> fhRollupT;
    fhRollupT fhRollup, fsRollup;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) return NULL;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    unique_fhs.add(filehandle.stringval());
	    hteData k;
	    if (request_at.val() < timebound_start ||
		reply_at.val() > timebound_end) {
		continue;
	    }
	    if (request_at.val() < min_time) {
		min_time = request_at.val();
	    } 
	    if (reply_at.val() > max_time) {
		max_time = reply_at.val();
	    }
	    k.server = server.val();
	    k.filehandle = filehandle.stringval();
	    INVARIANT(k.filehandle.compare(filehandle.stringval()) == 0, 
		      boost::format("bad %s != %s") 
		      % maybehexstring(k.filehandle) % maybehexstring(filehandle.stringval()));
	    k.operation = operation.stringval();
	    hteData *v = fhOpRollup.lookup(k);
	    if (v == NULL) {
		k.zero();
		v = fhOpRollup.add(k);
	    }
	    ++v->opcount;
	    v->payload_sum += payload_len.val();
	    if (reply_at.val() == request_at.val()) {
		v->latency_sum += 1;
	    } else {
		AssertAlways(reply_at.val() > request_at.val(),
			     ("no %lld %lld.\n",reply_at.val(),request_at.val()));
		v->latency_sum += reply_at.val() - request_at.val();
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
	    v->latency_sum += reply_at.val() - request_at.val();

	    if (fsRollup.size() < NFSDSAnalysisMod::max_mount_points_expected) {
		k.filehandle = fh2mountData::pruneToMountPart(k.filehandle);
		v = fsRollup.lookup(k);
		if (v == NULL) {
		    k.zero();
		    v = fsRollup.add(k);
		}
		++v->opcount;
		v->payload_sum += payload_len.val();
		v->latency_sum += reply_at.val() - request_at.val();
	    }
	}	    
	return e;
    }

    // sorting so the regression tests give stable results.

    void printFSRollup() {
	printf("FileSystem Rollup:\n");
	if (fsRollup.size() >= NFSDSAnalysisMod::max_mount_points_expected) {
	    printf("Too many filesystems (>= %d) filesystems found, assuming that fh->fs mapping is wrong\n", NFSDSAnalysisMod::max_mount_points_expected);
	} else {
	    fhRollupT::hte_vectorT &raw = fsRollup.unsafeGetRawDataVector();
	    sort(raw.begin(), raw.end(), byServerFhOp());
	    for(fhRollupT::hte_vectorT::iterator i = raw.begin();
		i != raw.end(); ++i) {
		hteData &d(i->data);
		double avglat = d.latency_sum / (1.0e3 * d.opcount);
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
		       ipv4tostring(d.server).c_str(),pathname.c_str(),
		       d.opcount,d.payload_sum/(1024.0*1024.0),avglat);
	    }
	}
    }

    void printFHRollup() {
	printf("\nFH Rollup (%d,%d):\n", fhRollup.size(), unique_fhs.size());
	fhRollupT::hte_vectorT &raw = fhRollup.unsafeGetRawDataVector();
	sort(raw.begin(), raw.end(), byServerFhOp());
	for(fhRollupT::hte_vectorT::iterator i = raw.begin();
	    i != raw.end(); ++i) {
	    hteData &d(i->data);
	    double avglat = d.latency_sum / (1.0e3 * d.opcount);
	    printf("fhrollup: server %s, fh %s: %d ops %.3f MB, %.3f us avg lat\n",
		   ipv4tostring(d.server).c_str(),
		   maybehexstring(d.filehandle).c_str(),
		   d.opcount,d.payload_sum/(1024.0*1024.0),avglat);
	}

    }

    void printFHOpRollup() {
	printf("\nFH/Op Rollup:\n");

	fhOpRollupT::hte_vectorT &raw = fhOpRollup.unsafeGetRawDataVector();
	sort(raw.begin(), raw.end(), byServerFhOp());
	for(fhOpRollupT::hte_vectorT::iterator i = raw.begin();
	    i != raw.end(); ++i) {
	    hteData &d(i->data);
	    double avglat = d.latency_sum / (1.0e3 * d.opcount);
	    printf("fhoprollup: server %s, op %s, fh %s: %d ops %.3f MB, %.3f us avg lat\n",
		   ipv4tostring(d.server).c_str(),d.operation.c_str(),
		   maybehexstring(d.filehandle).c_str(),
		   d.opcount,d.payload_sum/(1024.0*1024.0),avglat);
	}
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);

	printf("Time range: %.3f .. %.3f\n", (double)min_time/1.0e9,
	       (double)max_time/1.0e9);

	printFSRollup();
	printFHRollup();
	printFHOpRollup();

	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

    const ExtentType::int64 timebound_start, timebound_end;
    DataSeriesModule &source;
    ExtentSeries s;
    Int32Field server;
    Variable32Field operation, filehandle;
    Int64Field request_at, reply_at;
    Int32Field payload_len;

    ExtentType::int64 min_time, max_time;
    HashUnique<string> unique_fhs;
};

static string FHL_inputfilename;
class FileHandleLookup : public NFSDSModule {
public:
    FileHandleLookup(DataSeriesModule &_source)
	: source(_source), 
	filename(s,"filename", Field::flag_nullable), 
	filehandle(s,"filehandle"),
	lookup_dir_filehandle(s,"lookup-dir-filehandle", Field::flag_nullable),
	type(s,"type"),
	uid(s,"uid"),
	gid(s,"gid"),
	file_size(s,"file-size"),
	modify_time(s,"modify-time")
    {
	ifstream in(FHL_inputfilename.c_str());
	// todo: error on can't open file
	int nsearch = 0;
	while(in.good()) {
	    string foo;
	    in >> foo;
	    if (in.eof()) 
		break;
	    AssertAlways(in.good(),("internal"));
	    string fhwant = hex2raw(foo);
	    mymap[fhwant].file_size = -1;
	    ++nsearch;
	}
	printf("fhl: searching for %d filehandles\n",nsearch);
    }
    
    virtual ~FileHandleLookup() { }
    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) return NULL;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    fhinfo *v = mymap.lookup(filehandle.stringval());
	    if (v != NULL) {
		if (filename.isNull() == false) {
		    AssertAlways(v->filename == filename.stringval() || v->filename == "",
				 ("filehandle changed names?\n"));
		    v->filename = filename.stringval();
		}
		if (lookup_dir_filehandle.isNull() == false) {
		    AssertAlways(v->lookup_dir_filehandle == lookup_dir_filehandle.stringval() || v->lookup_dir_filehandle == "",
				 ("filehandle changed names?\n"));
		    v->lookup_dir_filehandle = lookup_dir_filehandle.stringval();
		}
		if (v->file_size == -1) {
		    v->uid = uid.val();
		    v->gid = gid.val();
		    v->type = type.stringval();
		    v->file_size = file_size.val();
		    v->modify_time = modify_time.val();
		} else {
		    AssertAlways(v->uid == uid.val() && v->gid == gid.val() &&
				 v->modify_time <= modify_time.val() &&
				 v->type == type.stringval(),
				 ("bad %d != %d || %d != %d || %lld > %lld ;; %s",
				  v->uid,uid.val(),v->gid,gid.val(),
				  v->modify_time,modify_time.val(),
				  maybehexstring(v->filename).c_str()));
		    if (file_size.val() >= v->file_size) {
			v->file_size = file_size.val();
		    } else {
			printf("fhl: warning %s got smaller %lld -> %lld\n",
			       maybehexstring(v->filename).c_str(),
			       v->file_size,file_size.val());
		    }
		    v->modify_time = modify_time.val();
		}
	    }
	}
	return e;
    }
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	for(HashMap<string, fhinfo>::iterator i = mymap.begin();
	    i != mymap.end();++i) {
	    printf("%s: ",hexstring(i->first).c_str());
	    if (i->second.file_size == -1) {
		printf("not found\n");
	    } else {
		printf("filename %s, parentfh %s, type %s, uid %d, gid %d, maxsize %lld, modifyns %lld\n",
		       maybehexstring(i->second.filename).c_str(), 
		       hexstring(i->second.lookup_dir_filehandle).c_str(),
		       i->second.type.c_str(),i->second.uid,i->second.gid,
		       i->second.file_size,i->second.modify_time);
	    }
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    DataSeriesModule &source;
    ExtentSeries s;
    Variable32Field filename, filehandle, lookup_dir_filehandle, type;
    Int32Field uid, gid;
    Int64Field file_size, modify_time;

    struct fhinfo {
	string filename, lookup_dir_filehandle, type;
	ExtentType::int32 uid, gid;
	ExtentType::int64 file_size, modify_time;
    };
    HashMap<string, fhinfo> mymap;
};

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

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) return NULL;
	// expect out of bounds case to be rare.
	bool any_out_of_bounds = false;
	int record_count = 0;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
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
	
	Extent *out_extent = new Extent(*dest_series);

	// need to copy all of the rows that we want to pass on
	dest_series->setExtent(out_extent);
	// count in an int as that should be faster.
	int kept_records = 0, pruned_records = 0;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (packet_at.val() < min_time || packet_at.val() >= max_time) {
		++pruned_records;
	    } else {
		++kept_records;
		dest_series->newRecord();
		copier->copyRecord();
	    }
	}

	delete e;
	total_kept_records += kept_records;
	partial_kept_records += kept_records;
	partial_pruned_records += pruned_records;
	if (kept_records == 0) { 
	    // rare case that we didn't keep any of the records, make
	    // sure we return a non-empty extent; simplifies
	    // downstream code which can now assume extents are
	    // non-empty
	    delete out_extent; 
	    return getExtent();
	}
	return out_extent;
    }
    
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	printf("  %lld total extents, %lld unchanged, %lld partials\n",
	       pass_through_extents + partial_extents,
	       pass_through_extents, partial_extents);
	printf("  %lld total records kept, %lld records in partials, %lld kept, %lld pruned\n",
	       total_kept_records,
	       partial_kept_records + partial_pruned_records,
	       partial_kept_records, partial_pruned_records);
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

};

static string TFHL_inputfilename;
class TmpFilehandleLookup : public NFSDSModule {
public:
  TmpFilehandleLookup(DataSeriesModule &_source)
    : source(_source), 
    filename(s,"filename", Field::flag_nullable), 
    filehandle(s,"filehandle"),
    file_size(s,"file-size"),
    modify_time(s,"modify-time"),
    client(s,"client"),
    server(s,"server"),
    replyat(s,"reply-at"),
    operation(s,"operation")
  {
	ifstream in(TFHL_inputfilename.c_str());
	// todo: error on can't open file
	int nsearch = 0;
	while(in.good()) {
	    string foo;
	    in >> foo;
	    if (in.eof()) 
		break;
	    AssertAlways(in.good(),("internal"));
	    string fhwant = hex2raw(foo);
	    mymap[fhwant].file_size = -1;
	    mymap[fhwant].ID = nsearch;
	    ++nsearch;
	}
	//	printf("fhl: searching for %d filehandles\n",nsearch);
    }
    
    virtual ~TmpFilehandleLookup() { }
    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) return NULL;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    fhinfo *v = mymap.lookup(filehandle.stringval());
	    if (v != NULL) {
	      v->operation = operation.stringval();

	      printf("%d %s %lld %s %s %lld %lld %s\n",
		     v->ID,
		     hexstring(filehandle.stringval()).c_str(),
		     replyat.val(),
		     ipv4tostring(client.val()).c_str(),
		     ipv4tostring(server.val()).c_str(),
		     file_size.val(),
		     modify_time.val(),
		     v->operation.c_str());
	    }
	}
	return e;
    }
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

  DataSeriesModule &source;
  ExtentSeries s;
  Variable32Field filename, filehandle;
  Int64Field file_size, modify_time;
  Int32Field client, server;
  Int64Field replyat;
  Variable32Field operation;

    struct fhinfo {
      string filename;
      ExtentType::int64 file_size, modify_time;
      int ID;
      string operation;
    };
    HashMap<string, fhinfo> mymap;
};

static bool need_mount_by_filehandle = false;
static bool need_filename_by_filehandle = false;
static bool late_filename_by_filehandle_ok = true;
static vector<int> FileageByFilehandle_recent_secs;
static int print_input_series;
static int latency_offset = 0;

void
usage(char *progname) 
{
    cerr << "Usage: " << progname << " flags... (file...)|(index.ds start-time end-time)\n";
    cerr << " flags\n";
    cerr << "    Note, most of the options are disabled because they haven't been tested.\n";
    cerr << "    However, it is trivial to change the source to re-enable them, but people\n";
    cerr << "    doing so should make an effort to verify the code and results.\n";
    cerr << "    It's probably also worth cleaning up the code at the same time, e.g.\n";
    cerr << "    replacing printfs with boost::format, int with [u]int{32,64}_t as\n";
    cerr << "    appropriate, and any other cleanups that seem appropriate.\n";
    cerr << "    please send in patches (software@cello.hpl.hp.com) if you do this.\n";
	
    cerr << "    -h # display this help\n";
    cerr << "    -a # Operation count by filehandle\n";
    cerr << "    #-b # Unique bytes in file handles analysis\n";
    cerr << "    #-c <recent-age-in-seconds> # File age by file handle analysis\n";
    cerr << "    #-d # Large file write analysis by file handle\n";
    cerr << "    #-e # Large file write analysis by file name\n";
    cerr << "    #-f # Large file analysis by file handle\n";
    cerr << "    #-g # Large file analysis by file name\n";
    cerr << "    #-i # NFS operation/payload analysis\n";
    cerr << "    -j # Server latency analysis\n";
    cerr << "    #-k # Client-server pair payload analysis\n";
    cerr << "    -l <seconds> # Host analysis\n";
    cerr << "    #-m # Overall payload analysis\n";
    cerr << "    #-n # File size analysis\n";
    cerr << "    #-o # Unbalanced operations analysis\n";
    cerr << "    #-p <time-gap in ms to treat as skip> # Time range analysis\n";
    cerr << "    #-q <sampling interval in ms> # Read analysis\n";
    cerr << "    #-r # Common bytes between filehandle and dirhandle\n";
    cerr << "    #-s # Sequential whole access\n";
    cerr << "    #-t # Strange write search\n";
    cerr << "    #-u <fh-list filename> # file handle lookup\n";
    cerr << "    #-v <series> # select series to print 1 = common, 2 = attrops, 3 = rw, 4 = common/attr join, 5 = common/attr/rw join\n";
    cerr << "    #-w # servers per filehandle\n";
    cerr << "    #-x # transactions\n";
    cerr << "    #-y # outstanding requests\n";
    cerr << "    #-z <fh-list filename> # tmp file handle lookup\n";
    exit(1);
}

static char *host_info_arg;
int
parseopts(int argc, char *argv[])
{
    bool any_selected;

    // TODO: redo this so it gets passed the sequences and just stuffs
    // things into them.

    any_selected = false;
    while (1) {
	int opt = getopt(argc, argv, "abc:defghijkl:mnop:q:r:stu:v:wxy:z:");
	if (opt == -1) break;
	any_selected = true;
	switch(opt){
	case 'a': options[optOperationByFileHandle] = 1;
	    need_mount_by_filehandle = 1;
	    break;
	case 'b': FATAL_ERROR("untested");options[Unique] = 1; break;
	case 'c': FATAL_ERROR("untested");{
	    options[optFileageByFilehandle] = 1;
	    int tmp = atoi(optarg);
	    AssertAlways(tmp > 0,("invalid -c %d\n",tmp));
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
	case 'h': usage(argv[0]); break;
	case 'i': FATAL_ERROR("untested");options[optNFSOpPayload] = 1; break;
	case 'j': options[optServerLatency] = 1; break;
	case 'k': FATAL_ERROR("untested");options[optClientServerPairInfo] = 1; break;
	case 'l': options[optHostInfo] = 1; host_info_arg = optarg; break;
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
	case 'u': FATAL_ERROR("untested");options[fileHandleLookup] = 1;
	    FHL_inputfilename = optarg;
	    break;
	case 'v': print_input_series = atoi(optarg);
	    AssertAlways(print_input_series >= 1 && print_input_series <= 5,
			 ("invalid print input series '%s'\n",optarg));
	    break;
	case 'w': FATAL_ERROR("untested");options[optServersPerFilehandle] = 1; break;
	case 'x': FATAL_ERROR("untested");options[optTransactions] = 1; break;
	case 'y': FATAL_ERROR("untested");options[optOutstandingRequests] = 1; 
	  latency_offset = atoi(optarg);
	  break;
	case 'z': FATAL_ERROR("untested");options[optTmpFilehandleLookup] = 1;
	    TFHL_inputfilename = optarg;
	    break;

	case '?': AssertFatal(("invalid option"));
	default:
	    AssertFatal(("getopt returned '%c'\n",opt));
	}
    }
    if (any_selected == false) {
	cerr << "need to select some modules\n";
	usage(argv[0]);
    }

    return optind;
}

void
printResult(DataSeriesModule *mod)
{
    if (mod == NULL) {
	return;
    }
    NFSDSModule *nfsdsmod = dynamic_cast<NFSDSModule *>(mod);
    RowAnalysisModule *rowmod = dynamic_cast<RowAnalysisModule *>(mod);
    if (nfsdsmod != NULL) {
	nfsdsmod->printResult();
    } else if (rowmod != NULL) {
	rowmod->printResult();
    } else {
	INVARIANT(dynamic_cast<DStoTextModule *>(mod) != NULL,
		  "Found unexpected module in chain");
    }

    printf("\n");
}

bool
isnumber(const char *v)
{
    for(; *v != '\0'; ++v) {
	if (!isdigit(*v)) {
	    return false;
	}
    }
    return true;
}

int
main(int argc, char *argv[])
{
    int first = parseopts(argc, argv);

    if (argc - first < 1) {
	usage(argv[0]);
    }

    TypeIndexModule *sourcea = 
	new TypeIndexModule("NFS trace: common");
    sourcea->setSecondMatch("Trace::NFS::common");
    TypeIndexModule *sourceb = 
	new TypeIndexModule("NFS trace: attr-ops");
    TypeIndexModule *sourcec = 
	new TypeIndexModule("NFS trace: read-write");
    TypeIndexModule *sourced = 
	new TypeIndexModule("NFS trace: mount");
    bool timebound_set = false;
    int timebound_start = 0;
    int timebound_end = INT_MAX;
    if ((argc - first) == 3 && isnumber(argv[first+1]) && isnumber(argv[first+2])) {
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

    // these are the three threads that we will build according to the
    // selected analyses

    SequenceModule commonSequence(sourcea);
    SequenceModule attrOpsSequence(sourceb);
    SequenceModule rwSequence(sourcec);

    if (timebound_set) {
	commonSequence.addModule(new TimeBoundPrune(commonSequence.tail(),
						    timebound_start, 
						    timebound_end));
    }

    // need to pre-build the table before we can do the write-filename
    // analysis after the merge join.

    if (need_filename_by_filehandle) {
	if (late_filename_by_filehandle_ok) {
	    attrOpsSequence.addModule(NFSDSAnalysisMod::newFillFH2FN_HashTable(attrOpsSequence.tail()));
	} else {
	    PrefetchBufferModule *ptmp = new PrefetchBufferModule(*sourceb,32*1024*1024);
	    NFSDSModule *tmp = NFSDSAnalysisMod::newFillFH2FN_HashTable(*ptmp);
	    DataSeriesModule::getAndDelete(*tmp);
	    
	    sourceb->resetPos();
	    delete tmp;
	    delete ptmp;
	}
    }

    if (need_mount_by_filehandle) {
	PrefetchBufferModule prefetch(*sourced,32*1024*1024);
	NFSDSModule *tmp = NFSDSAnalysisMod::newFillMount_HashTable(prefetch);
	DataSeriesModule::getAndDelete(*tmp);
	delete tmp;
    }

    // 2004-09-02: tried experiment of putting some of the
    // bigger-memory bits into their own threads using a prefetch
    // buffer -- performance got much worse with way more time being
    // spent in the kernel.  Possibly hypothesis is that this was
    // malloc library issues as both those modules did lots of
    // malloc/free.

    // common file rollups
    if (options[optNFSOpPayload]) {
	commonSequence.addModule(NFSDSAnalysisMod::newNFSOpPayload(commonSequence.tail()));
    }

    if (options[optClientServerPairInfo]) {
	commonSequence.addModule(NFSDSAnalysisMod::newClientServerPairInfo(commonSequence.tail()));
    }

    if (options[optHostInfo]) {
	commonSequence.addModule(NFSDSAnalysisMod::newHostInfo(commonSequence.tail(), host_info_arg));
    }

    if (options[optPayloadInfo]) {
	commonSequence.addModule(NFSDSAnalysisMod::newPayloadInfo(commonSequence.tail()));
    }

    if (options[optServerLatency]) {
	commonSequence.addModule(NFSDSAnalysisMod::newServerLatency(commonSequence.tail()));
    }

    if (options[optUnbalancedOps]) {
	commonSequence.addModule(NFSDSAnalysisMod::newUnbalancedOps(commonSequence.tail()));
    }

    if (options[optNFSTimeGaps]) {
	commonSequence.addModule(NFSDSAnalysisMod::newNFSTimeGaps(commonSequence.tail()));
    }

    if (options[optTransactions]) {
	commonSequence.addModule(NFSDSAnalysisMod::newTransactions(commonSequence.tail()));
    }

    if (options[optOutstandingRequests]) {
	commonSequence.addModule(NFSDSAnalysisMod::newOutstandingRequests(commonSequence.tail(), latency_offset));
    }

    // attribute rollups
    if (options[optFileSizeByType]) {
	attrOpsSequence.addModule(NFSDSAnalysisMod::newFileSizeByType(attrOpsSequence.tail()));
    }

    if (options[Commonbytes]) {
	attrOpsSequence.addModule(NFSDSAnalysisMod::newCommonBytesInFilehandles(attrOpsSequence.tail()));
    }

    if (options[Unique]) {
	attrOpsSequence.addModule(NFSDSAnalysisMod::newUniqueBytesInFilehandles(attrOpsSequence.tail()));
    }

    if (options[fileHandleLookup]) {
	attrOpsSequence.addModule(new FileHandleLookup(attrOpsSequence.tail()));
    }

    // merge join with attributes
    SequenceModule merge12Sequence(NFSDSAnalysisMod::newAttrOpsCommonJoin(commonSequence.tail(),attrOpsSequence.tail()));

    // merge join rollups
    if (options[optFileageByFilehandle]) {
	for(vector<int>::iterator i = FileageByFilehandle_recent_secs.begin();
	    i != FileageByFilehandle_recent_secs.end(); ++i) {
	    merge12Sequence.addModule(NFSDSAnalysisMod::newFileageByFilehandle(merge12Sequence.tail(),20,*i));
	}
    }

    if (options[Largewrite_Handle]) {
	merge12Sequence.addModule(NFSDSAnalysisMod::newLargeSizeFilehandleWrite(merge12Sequence.tail(),20));
    }

    if (options[Largewrite_Name]) {
	merge12Sequence.addModule(NFSDSAnalysisMod::newLargeSizeFilenameWrite(merge12Sequence.tail(),20));
    }

    if (options[Largefile_Handle]) {
	merge12Sequence.addModule(NFSDSAnalysisMod::newLargeSizeFileHandle(merge12Sequence.tail(),20));
    }

    if (options[Largefile_Name]) {
	merge12Sequence.addModule(NFSDSAnalysisMod::newLargeSizeFilename(merge12Sequence.tail(),20));
    }
    
    if (options[strangeWriteSearch] && sws_filehandle == false) {
	merge12Sequence.addModule(new StrangeWriteSearch(merge12Sequence.tail()));
    }

    if (options[optOperationByFileHandle]) {
	merge12Sequence.addModule(new OperationByFileHandle(merge12Sequence.tail(),timebound_start,timebound_end));
    }

    if (options[optServersPerFilehandle]) {
	merge12Sequence.addModule(NFSDSAnalysisMod::newServersPerFilehandle(merge12Sequence.tail()));
    }

    if (options[optTmpFilehandleLookup]) {
	merge12Sequence.addModule(new TmpFilehandleLookup(merge12Sequence.tail()));
    }

    // merge join with read-write data
    SequenceModule merge123Sequence(NFSDSAnalysisMod::newCommonAttrRWJoin(merge12Sequence.tail(),
									  rwSequence.tail()));
    if (options[File_Read]) {
	merge123Sequence.addModule(NFSDSAnalysisMod::newFilesRead(merge123Sequence.tail()));
    }

    if (options[optSequentialWholeAccess]) {
	merge123Sequence.addModule(new SequentialWholeAccess(merge123Sequence.tail()));
    }

    switch (print_input_series) 
	{
	case 0: // nothing, ok
	    break;
	case 1: 
	    commonSequence.addModule(new DStoTextModule(commonSequence.tail()));
	    break;
	case 2:
	    attrOpsSequence.addModule(new DStoTextModule(attrOpsSequence.tail()));
	    break;
	case 3:
	    rwSequence.addModule(new DStoTextModule(rwSequence.tail()));
	    break;
	case 4:
	    merge12Sequence.addModule(new DStoTextModule(merge12Sequence.tail()));
	    break;
	case 5:
	    merge123Sequence.addModule(new DStoTextModule(merge123Sequence.tail()));
	    break;
	default:
	    AssertFatal(("internal"));
	}
    // only pull through what we actually need to pull through.

    if (merge123Sequence.size() > 1) {
	DataSeriesModule::getAndDelete(merge123Sequence);
    } else if (merge12Sequence.size()> 1) {
	DataSeriesModule::getAndDelete(merge12Sequence);
    } else {
	if (commonSequence.size() > 1) {
	    DataSeriesModule::getAndDelete(commonSequence);
	}
	if (attrOpsSequence.size() > 1) {
	    DataSeriesModule::getAndDelete(attrOpsSequence);
	}
	if (rwSequence.size() > 1) {
	    DataSeriesModule::getAndDelete(rwSequence);
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
    if (options[strangeWriteSearch] && sws_filehandle) {
	DataSeriesModule *m = new StrangeWriteSearch(commonSequence.tail());
	printResult(m);
    }
	
    printf("extents: %.2f MB -> %.2f MB in %.2f secs decode time\n",
	   (double)(sourcea->total_compressed_bytes + sourceb->total_compressed_bytes + sourcec->total_compressed_bytes + sourced->total_compressed_bytes)/(1024.0*1024),
	   (double)(sourcea->total_uncompressed_bytes + sourceb->total_uncompressed_bytes + sourcec->total_uncompressed_bytes + sourced->total_uncompressed_bytes)/(1024.0*1024),
	   sourcea->decode_time + sourceb->decode_time + sourcec->decode_time + sourced->decode_time);
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
    printf("decode seconds:  %8.2f %8.2f %8.2f %8.2f\n",
	   sourcea->decode_time,
	   sourceb->decode_time,
	   sourcec->decode_time,
	   sourced->decode_time);
    printf("wait fraction :  %8.2f %8.2f %8.2f %8.2f\n",
	   sourcea->waitFraction(),
	   sourceb->waitFraction(),
	   sourcec->waitFraction(),
	   sourced->waitFraction());
//    DataSeriesModule *foo = prefetcha;
//    delete foo;
//    delete prefetchb;
//    delete prefetchc;
//    delete sourcea;
//    delete sourceb;
//    delete sourcec;
    return 0;
}

