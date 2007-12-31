// -*-C++-*-
/*
   (c) Copyright 2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Convert dataseries files back into the Ellard text format
*/

#include <Lintel/StringUtil.H>

#include <DataSeries/PrefetchBufferModule.H>
#include <DataSeries/RowAnalysisModule.H>
#include <DataSeries/SequenceModule.H>
#include <DataSeries/TypeIndexModule.H>

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
	  sdata(series, "sdata", Field::flag_nullable)
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

    string timeConv(int64_t time) {
	if (time == -1) {
	    return "SERVER";
	}
	uint32_t seconds = time / 1000000;
	uint32_t useconds = time % 1000000;
	return (format("%d.%06d") % seconds % useconds).str();
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
	if (rpc_function.equal("access") && !is_call.val()) {
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
	cout << format(" %s %x")
	    % stripDup(f.getName()) % f.val();
    }

    void printMaybeTime(Int64Field &f) {
	if (f.isNull())
	    return;
	printany = true;
	cout << format(" %s %s")
	    % stripDup(f.getName()) % timeConv(f.val());
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
	cout << format("%s %x.%04x %x.%04x %c %c%d %x %x %s")
	    % timeConv(time.val()) % source_ip.val() % source_port.val()
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
		cout << " OK";
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
	printMaybe(fh2);

	if (!rpc_function.equal("rename") && !rpc_function.equal("readlink")) {
	    printMaybeString(name);
	}
	printMaybeString(name2);
	printMaybeChar(how);

	printMaybe(ftype);
	printMaybe(mode);
	printMaybe(nlink);
	printMaybe(uid);
	printMaybe(gid);
	printMaybe(size);
	printMaybe(used);
	printMaybe(rdev);
	printMaybe(rdev2);
	printMaybe(fsid);
	printMaybe(fileid);
	printMaybeTime(atime);
	printMaybeTime(mtime);
	printMaybeTime(ctime);
	
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
	printMaybe(cookie);
	printMaybe(count);
	printMaybe(maxcnt);
	printMaybe(eof);

	printMaybeString(sdata);
	printMaybeChar(stable);
	if (printany == false) {
	    cout << " ";
	}
	if (is_call.val()) {
	    cout << " con = XXX len = XXX\n";
	} else {
	    cout << " status=XXX pl = XXX con = XXX len = XXX\n";
	}
	if (time.val() == 1004562602020930LL && 
	    static_cast<uint32_t>(rpc_transaction_id.val()) == 0x9366f750) {
	    // These lines are garbage, we reconstruct them here as
	    // it's not clear how we should choose to translate them.
	    cout << "1004562602.021187 30.0801 31.03e4 T R3 9d66f750 7 write OK ftype 1 mode 180 nlink 1 uid 18aff gid 18a88 size ee62bc used eec000 rdev 0 rdev2 0 fsid ffffffff8465bccf fileid aa68e03b atime 3427926016.2516582 mtime 2516582400.301989 ctime 1830883825.006319 count 11b10800 stable ? status=XXX pl = XXX con = XXX len = XXX\n";
	    cout << "1004562602.021196 30.0801 31.03e4 T R3 9d66f750 7 write OK ftype 1 mode 180 nlink 1 uid 18aff gid 18a88 size ee62bc used eec000 rdev 0 rdev2 0 fsid ffffffff8465bccf fileid aa68e03b atime 811139072.2415984 mtime 3122659328.301989 ctime 1830883761.006319 count 11f10800 stable ? status=XXX pl = XXX con = XXX len = XXX\n";
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
    
    seq.getAndDelete();
    return 0;
}


