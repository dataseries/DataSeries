// -*-C++-*-
/*
   (c) Copyright 2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Convert dataseries files back into the Ellard text format
*/

/* 
=pod

=head1 NAME

ds2ellardnfs - convert the Ellard-style NFS files from dataseries back to the original text format

=head1 SYNOPSIS

  % ds2ellardnfs file-1.ds [file-2.ds] >files.txt

=head1 DESCRIPTION

The ds2ellardnfs program converts files in dataseries format back to the original text format.
This allows for easy verification that the files were converted correctly because you can run
ellardnfs2ds and then take the resulting ds file and run it through ds2ellardnfs and compare
that the resulting output is the same as the original input.  The batch-parallel module
ellardnfs2ds will perform this check by default.

=head1 SEE ALSO

ellardnfs2ds(1)

=cut

*/

#include <Lintel/StringUtil.hpp>

#include <DataSeries/PrefetchBufferModule.hpp>
#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

using namespace std;
using boost::format;

class DSToEllardNFS : public RowAnalysisModule {
public:
    DSToEllardNFS(DataSeriesModule &source)
        : RowAnalysisModule(source),
	  time(series, "time"),
	  source_ip(series, "source_ip"),
	  source_port(series, "source_port"),
	  dest_ip(series, "dest_ip"),
	  dest_port(series, "dest_port"),
	  is_udp(series, "is_udp"),
	  is_call(series, "is_call"),
	  nfs_version(series, "nfs_version"),
	  rpc_transaction_id(series, "rpc_transaction_id"),
	  rpc_function_id(series, "rpc_function_id"),
	  rpc_function(series, "rpc_function"),
	  return_value(series, "return_value", Field::flag_nullable),
	  fh(series, "fh", Field::flag_nullable),
	  mode(series, "mode", Field::flag_nullable),
	  name(series, "name", Field::flag_nullable),
	  ftype(series, "ftype", Field::flag_nullable),
	  nlink(series, "nlink", Field::flag_nullable),
	  uid(series, "uid", Field::flag_nullable),
	  gid(series, "gid", Field::flag_nullable),
	  size(series, "size", Field::flag_nullable),
	  used(series, "used", Field::flag_nullable),
	  rdev(series, "rdev", Field::flag_nullable),
	  rdev2(series, "rdev2", Field::flag_nullable),
	  fsid(series, "fsid", Field::flag_nullable),
	  fileid(series, "fileid", Field::flag_nullable),
	  mtime(series, "mtime", Field::flag_nullable),
	  ctime(series, "ctime", Field::flag_nullable),
	  atime(series, "atime", Field::flag_nullable),
	  ftype_dup(series, "ftype_dup", Field::flag_nullable),
	  mode_dup(series, "mode_dup", Field::flag_nullable),
	  nlink_dup(series, "nlink_dup", Field::flag_nullable),
	  uid_dup(series, "uid_dup", Field::flag_nullable),
	  gid_dup(series, "gid_dup", Field::flag_nullable),
	  size_dup(series, "size_dup", Field::flag_nullable),
	  used_dup(series, "used_dup", Field::flag_nullable),
	  rdev_dup(series, "rdev_dup", Field::flag_nullable),
	  rdev2_dup(series, "rdev2_dup", Field::flag_nullable),
	  fsid_dup(series, "fsid_dup", Field::flag_nullable),
	  fileid_dup(series, "fileid_dup", Field::flag_nullable),
	  mtime_dup(series, "mtime_dup", Field::flag_nullable),
	  ctime_dup(series, "ctime_dup", Field::flag_nullable),
	  atime_dup(series, "atime_dup", Field::flag_nullable),
	  acc(series, "acc", Field::flag_nullable),
	  off(series, "off", Field::flag_nullable),
	  count(series, "count", Field::flag_nullable),
	  eof(series, "eof", Field::flag_nullable),
	  how(series, "how", Field::flag_nullable),
	  fh2(series, "fh2", Field::flag_nullable),
	  cookie(series, "cookie", Field::flag_nullable),
	  maxcnt(series, "maxcnt", Field::flag_nullable),
	  stable(series, "stable", Field::flag_nullable),
	  file(series, "file", Field::flag_nullable),
	  name2(series, "name2", Field::flag_nullable),
	  sdata(series, "sdata", Field::flag_nullable),
	  pre_size(series, "pre-size", Field::flag_nullable), 
	  pre_mtime(series, "pre-mtime", Field::flag_nullable),
	  pre_ctime(series, "pre-ctime", Field::flag_nullable),
	  euid(series, "euid", Field::flag_nullable),
	  egid(series, "egid", Field::flag_nullable),
	  blksize(series, "blksize", Field::flag_nullable),
	  blocks(series, "blocks", Field::flag_nullable),
	  tsize(series, "tsize", Field::flag_nullable),
	  bsize(series, "bsize", Field::flag_nullable),
	  bfree(series, "bfree", Field::flag_nullable),
	  bavail(series, "bavail", Field::flag_nullable),
	  fn(series, "fn", Field::flag_nullable),
	  offset(series, "offset", Field::flag_nullable),
	  tcount(series, "tcount", Field::flag_nullable),
	  nfsstat(series, "nfsstat", Field::flag_nullable),
	  short_packet(series, "short_packet"),
	  fn2(series, "fn2", Field::flag_nullable),
	  begoff(series, "begoff", Field::flag_nullable),
	  garbage(series, "garbage", Field::flag_nullable)
    {
    }

    virtual ~DSToEllardNFS() { }

    string stripDup(const string &in) {
	if (suffixequal(in, "_dup")) {
	    return in.substr(0,in.size()-4);
	} else {
	    return in;
	}
    }

    string timeConv(int64_t time, bool proper_conv) {
	if (time == -1) {
	    return "SERVER";
	}
	uint32_t seconds = time / 1000000;
	uint32_t useconds = time % 1000000;
	if (nfs_version.val() == 3 || proper_conv) {
	    return (format("%d.%06d") % seconds % useconds).str();
	} else {
	    return (format("%d.%d") % seconds % useconds).str();
	}
    }
    
    void printMaybe(BoolField &f) {
	if (f.isNull())
	    return;
	printany = true;
	cout << format(" %s %x")
	    % f.getName() % (f.val() ? 1 : 0);
    }

    void printMaybe(ByteField &f) {
	if (f.isNull())
	    return;
	printany = true;
	cout << format(" %s %x")
	    % stripDup(f.getName()) % static_cast<int>(f.val());
    }
	
    void printMaybeAcc(ByteField &f) {
	if (f.isNull())
	    return;
	printany = true;
	if (rpc_function.equal("access") && 
	    (!is_call.val() || time.val() >= 1043870000000000LL)) {
	    cout << format(" acc %x") % static_cast<int>(f.val());
	} else if (f.val() == 1) {
	    cout << " acc R";
	} else if (f.val() == 2) {
	    cout << " acc L";
	} else if (f.val() == 32) {
	    cout << " acc X";
	} else if (f.val() == 64) {
	    cout << " acc U";
	} else {
	    cout << format(" acc %x") % static_cast<int>(f.val());
	}
    }

    void printMaybeChar(ByteField &f) {
	if (f.isNull())
	    return;
	printany = true;
	cout << format(" %s %c")
	    % f.getName() % f.val();
    }
	
    void printMaybe(Int32Field &f) {
	if (f.isNull())
	    return;
	printany = true;
	cout << format(" %s %x")
	    % stripDup(f.getName()) % f.val();
    }
	
    void printMaybe(Int64Field &f) {
	if (f.isNull())
	    return;
	printany = true;
	if (nfs_version.val() == 2 && rpc_function_id.val() == 16
	    && f.getName() == "cookie") {
	    cout << format(" cookie %08x") % f.val();
	} else {
	    cout << format(" %s %x")
		% stripDup(f.getName()) % f.val();
	}
    }

    void printMaybeTime(Int64Field &f) {
	if (f.isNull())
	    return;
	printany = true;
	cout << format(" %s %s")
	    % stripDup(f.getName()) % timeConv(f.val(), false);
    }
	
    void printMaybe(Variable32Field &f) {
	if (f.isNull())
	    return;
	printany = true;
	cout << format(" %s %s")
	    % f.getName() % hexstring(f.stringval());
    }

    void printMaybeString(Variable32Field &f) {
	if (f.isNull())
	    return;
	printany = true;
	cout << format(" %s \"%s\"")
	    % f.getName() % f.stringval();
    }

    virtual void processRow() {
	if (!garbage.isNull()) {
	    SINVARIANT(garbage.size() > 0);
	    cout << garbage.stringval();
	    return;
	}
	cout << format("%s %x.%04x %x.%04x %c %c%d %x %x %s")
	    % timeConv(time.val(), true) 
	    % source_ip.val() % source_port.val()
	    % dest_ip.val() % dest_port.val() 
	    % (is_udp.val() ? 'U' : 'T')
	    % (is_call.val() ? 'C' : 'R') 
	    % static_cast<int>(nfs_version.val())
	    % rpc_transaction_id.val() 
	    % static_cast<int>(rpc_function_id.val()) 
	    % rpc_function.stringval();

	if (!is_call.val()) {
	    INVARIANT(!return_value.isNull(), "?");
	    if (return_value.val() == 0) {
		if (rpc_function_id.val() == 0) {
		    // do nothing, nulls don't print out OK
		} else {
		    cout << " OK";
		}
	    } else {
		cout << format(" %x") % return_value.val();
	    }
	}
	
	printany = false;

	printMaybe(fh);
	if (rpc_function.equal("rename")) { 
	    // rename prints name between fh and fh2; most print it after
	    printMaybeString(name);
	}

	if (nfs_version.val() == 2 && rpc_function_id.val() == 11) {
	    printMaybeString(fn);
	}

	printMaybe(fh2);

	if (nfs_version.val() == 2 && rpc_function_id.val() != 11) {
	    printMaybeString(fn);
	}

	printMaybe(pre_size);
	printMaybeTime(pre_mtime);
	printMaybeTime(pre_ctime);

	if (!rpc_function.equal("rename") && !rpc_function.equal("readlink")) {
	    printMaybeString(name);
	}
	printMaybeString(name2);
	printMaybeChar(how);

	printMaybe(tsize);
	printMaybe(bsize);

	printMaybeString(fn2);

	printMaybe(ftype);
	printMaybe(mode);
	printMaybe(nlink);
	printMaybe(uid);
	printMaybe(gid);
	printMaybe(size);
	printMaybe(blksize);
	printMaybe(used);
	printMaybe(rdev);
	printMaybe(blocks);
	printMaybe(rdev2);
	printMaybe(fsid);
	printMaybe(fileid);
	printMaybeTime(atime);
	printMaybeTime(mtime);
	printMaybeTime(ctime);
	
	printMaybe(bfree);
	printMaybe(bavail);

	if (rpc_function.equal("readlink")) {
	    printMaybeString(name);
	}

	printMaybe(ftype_dup);
	printMaybe(mode_dup);
	printMaybe(nlink_dup);
	printMaybe(uid_dup);
	printMaybe(gid_dup);
	printMaybe(size_dup);
	printMaybe(used_dup);
	printMaybe(rdev_dup);
	printMaybe(rdev2_dup);
	printMaybe(fsid_dup);
	printMaybe(fileid_dup);
	printMaybeTime(atime_dup);
	printMaybeTime(mtime_dup);
	printMaybeTime(ctime_dup);
	
	printMaybeAcc(acc);

	printMaybe(file);
	printMaybe(off);
	printMaybe(begoff);
	printMaybe(offset);
	printMaybe(cookie);
	printMaybe(count);
	printMaybe(tcount);
	printMaybe(maxcnt);
	printMaybe(eof);

	printMaybeString(sdata);
	printMaybeChar(stable);
	printMaybe(euid);
	printMaybe(egid);

	printMaybe(nfsstat);

	if (rpc_function_id.val() == 0) {
	    printany = true;
	    if (is_call.val()) {
		cout << " con = 82 len = 97";
	    } else {
		cout << " status=0 pl = 0 con = 70 len = 70";
	    }
	}
	if (printany == false) {
	    cout << " ";
	}
	if (short_packet.val()) {
	    cout << "SHORT PACKETcon = 130 len = 400";
	}
	if (is_call.val()) {
	    cout << " con = XXX len = XXX\n";
	} else {
	    cout << " status=XXX pl = XXX con = XXX len = XXX\n";
	}
    }

private:
    bool printany;

    Int64Field time;
    Int32Field source_ip;
    Int32Field source_port;
    Int32Field dest_ip;
    Int32Field dest_port;
    BoolField is_udp;
    BoolField is_call;
    ByteField nfs_version;
    Int32Field rpc_transaction_id;
    ByteField rpc_function_id;
    Variable32Field rpc_function;
    Int32Field return_value;
    Variable32Field fh;
    Int32Field mode;
    Variable32Field name;
    ByteField ftype;
    Int32Field nlink;
    Int32Field uid;
    Int32Field gid;
    Int64Field size;
    Int64Field used;
    Int32Field rdev;
    Int32Field rdev2;
    Int64Field fsid;
    Int64Field fileid;
    Int64Field mtime;
    Int64Field ctime;
    Int64Field atime;
    ByteField ftype_dup;
    Int32Field mode_dup;
    Int32Field nlink_dup;
    Int32Field uid_dup;
    Int32Field gid_dup;
    Int64Field size_dup;
    Int64Field used_dup;
    Int32Field rdev_dup;
    Int32Field rdev2_dup;
    Int64Field fsid_dup;
    Int64Field fileid_dup;
    Int64Field mtime_dup;
    Int64Field ctime_dup;
    Int64Field atime_dup;
    ByteField acc;
    Int64Field off;
    Int32Field count;
    BoolField eof;
    ByteField how;
    Variable32Field fh2;
    Int64Field cookie;
    Int32Field maxcnt;
    ByteField stable;
    Variable32Field file;
    Variable32Field name2;
    Variable32Field sdata;
    Int64Field pre_size;
    Int64Field pre_mtime;
    Int64Field pre_ctime;
    Int32Field euid;
    Int32Field egid;
    Int64Field blksize;
    Int32Field blocks;
    Int32Field tsize;
    Int32Field bsize;
    Int32Field bfree;
    Int32Field bavail;
    Variable32Field fn;
    Int32Field offset;
    Int32Field tcount;
    Int32Field nfsstat;
    BoolField short_packet;
    Variable32Field fn2;
    Int32Field begoff;
    Variable32Field garbage;
};

int
main(int argc, char *argv[]) 
{
    TypeIndexModule source("Trace::NFS::Ellard");
    PrefetchBufferModule *prefetch
	= new PrefetchBufferModule(source, 64*1024*1024);

    INVARIANT(argc >= 2 && strcmp(argv[1], "-h") != 0,
	      boost::format("Usage: %s <file...>\n") % argv[0]);

    for(int i = 1; i < argc; ++i) {
	source.addSource(argv[i]);
    }

    SequenceModule seq(prefetch);
    
    seq.addModule(new DSToEllardNFS(seq.tail()));
    
    seq.getAndDeleteShared();
    return 0;
}


