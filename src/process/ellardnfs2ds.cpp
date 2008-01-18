// -*-C++-*-
/*
   (c) Copyright 2007 Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file 
    Convert the ellard NFS traces to dataseries.  
*/

#include <boost/algorithm/string.hpp>

#include <Lintel/HashMap.H>
#include <Lintel/StringUtil.H>
#include <Lintel/HashUnique.H>

#include <DataSeries/DataSeriesModule.H>
#include <DataSeries/commonargs.H>

using namespace std;
using boost::format;

// Our copy of a number of the files include a large block of nulls,
// these files therefore can't be easily converted.  The code gets
// confused because a null is normally a end of string terminator.
//
// The files are: anon-lair62-010903-1600.txt.gz,
// anon-lair62-010904-1600.txt.gz, anon-lair62b-030222-0900.txt.gz,
// anon-lair62-010911-1600.txt.gz, anon-lair62b-030208-0900.txt.gz, 
// anon-lair62b-030215-0900.txt.gz, anon-lair62-010918-1600.txt.gz
// anon-lair62-011110-0600.txt.gz, anon-lair62-011109-0600.txt.gz

/*
Compression ratio:
  home04, compressed with gz, 8Mextent size: ds/txt.gz = 1.40945108

Sizing experiments, turning on options is cumulative; the big win is
  pack_unique, then relative time, then non-bool null compaction;
  options turned on and off by changing pack into xack; these
  experiments were done before the _dup series of fields and later
  were added.  To replicate, remove the _dup fields, remove the code
  that uses them, and turn off the invariant that checks.

3990407 anon-home04-011119-2345.txt.gz -- original text file size

5393860 anon-home04-011119-2345.txt.gz.ds -- basic
3699644 anon-home04-011119-2345.txt.unique.gz.ds -- turn on pack_unique
3511828 anon-home04-011119-2345.txt.unique.nonbool.gz.ds -- turn on non-bool null compaction
3189196 anon-home04-011119-2345.txt.unique.nonbool.relt.gz.ds -- turn on self-relative packing of the time field
3137304 anon-home04-011119-2345.txt.unique.nonbool.relt,mc.gz.ds -- turn on packing of ctime relative to mtime
2914988 anon-home04-011119-2345.txt.unique.nonbool.relt,mc,rela.gz.ds -- turn on self-relative packing of atime
2912424 anon-home04-011119-2345.txt.unique.nonbool.relt,mc,rela,relm.gz.ds -- turn on self-relative packing of mtime
*/

const string ellard_nfs_expanded_xml(
  "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Trace::NFS::Ellard\" version=\"1.0\" pack_null_compact=\"non_bool\"\n"
  " comment=\"note, if !garbage.isNull() then only the time field is valid.\" >\n"
  // Possible we should make all the fixed fields nullable so that
  // when we have garbage we can properly mark it; probably the better
  // choice would be to remove the garbage entries, but that would
  // mean we don't have a 1 to 1 mapping between the ellard text files
  // and the dataseries ones.
  "  <field type=\"int64\" name=\"time\" units=\"microseconds\" epoch=\"unix\" pack_relative=\"time\" />\n"
  "  <field type=\"int32\" name=\"source_ip\" />\n"
  "  <field type=\"int32\" name=\"source_port\" />\n"
  "  <field type=\"int32\" name=\"dest_ip\" />\n"
  "  <field type=\"int32\" name=\"dest_port\" />\n"
  "  <field type=\"bool\" name=\"is_udp\" print_true=\"UDP\" print_false=\"TCP\" />\n"
  "  <field type=\"bool\" name=\"is_call\" print_true=\"call\" print_false=\"reply\" />\n"
  "  <field type=\"byte\" name=\"nfs_version\" />\n"
  "  <field type=\"int32\" name=\"rpc_transaction_id\" />\n"
  "  <field type=\"byte\" name=\"rpc_function_id\" />\n"
  "  <field type=\"variable32\" name=\"rpc_function\" pack_unique=\"yes\" />\n"
  "  <field type=\"int32\" name=\"return_value\" opt_nullable=\"yes\" comment=\"null for calls, 0 = ok\" />\n"
  "  <field type=\"variable32\" name=\"fh\" opt_nullable=\"yes\" pack_unique=\"yes\" />\n"
  "  <field type=\"int32\" name=\"mode\" opt_nullable=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"name\" opt_nullable=\"yes\" pack_unique=\"yes\" />\n"
  "  <field type=\"byte\" name=\"ftype\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"nlink\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"uid\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"gid\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int64\" name=\"size\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int64\" name=\"used\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"rdev\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"rdev2\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int64\" name=\"fsid\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int64\" name=\"fileid\" opt_nullable=\"yes\" />\n"
  // mtime and ctime are useful relative to each other, in a few simple tests,
  // it doesn't matter which way they are relative.
  "  <field type=\"int64\" name=\"mtime\" opt_nullable=\"yes\" pack_relative=\"mtime\" comment=\"-1 means set to server\" units=\"microseconds\" epoch=\"unix\" />\n"
  "  <field type=\"int64\" name=\"ctime\" opt_nullable=\"yes\" pack_relative=\"mtime\" comment=\"-1 means set to server\" units=\"microseconds\" epoch=\"unix\" />\n"
  // packing this relative to either mtime or ctime makes things larger in a few simple tests.
  "  <field type=\"int64\" name=\"atime\" opt_nullable=\"yes\" pack_relative=\"atime\" comment=\"-1 means set to server\" units=\"microseconds\" epoch=\"unix\" />\n"
  "  <field type=\"byte\" name=\"ftype_dup\" opt_nullable=\"yes\" comment=\"all of the dup fields are the second occurance of an identically named field\" />\n"
  "  <field type=\"int32\" name=\"mode_dup\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"nlink_dup\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"uid_dup\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"gid_dup\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int64\" name=\"size_dup\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int64\" name=\"used_dup\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"rdev_dup\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"rdev2_dup\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int64\" name=\"fsid_dup\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int64\" name=\"fileid_dup\" opt_nullable=\"yes\" />\n"

  "  <field type=\"int64\" name=\"mtime_dup\" opt_nullable=\"yes\" pack_relative=\"mtime_dup\" comment=\"-1 means set to server\" units=\"microseconds\" epoch=\"unix\" />\n"
  "  <field type=\"int64\" name=\"ctime_dup\" opt_nullable=\"yes\" pack_relative=\"mtime_dup\" comment=\"-1 means set to server\" units=\"microseconds\" epoch=\"unix\" />\n"
  // packing this relative to either mtime or ctime makes things larger in a few simple tests.
  "  <field type=\"int64\" name=\"atime_dup\" opt_nullable=\"yes\" pack_relative=\"atime_dup\" comment=\"-1 means set to server\" units=\"microseconds\" epoch=\"unix\" />\n"


  "  <field type=\"byte\" name=\"acc\" opt_nullable=\"yes\" comment=\"bitmas, bit 0 = read, bit 1 = lookup, bit 2 = modify, bit 3 = extend, bit 4 = delete, bit 5 = execute; ellard traces also have U, traslating that as bit 6\" />\n"
  "  <field type=\"int64\" name=\"off\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"count\" opt_nullable=\"yes\" />\n"
  "  <field type=\"bool\" name=\"eof\" opt_nullable=\"yes\" />\n"
  "  <field type=\"byte\" name=\"how\" opt_nullable=\"yes\" comment=\"for create, U = unchecked, G = guarded, X = exclusive; for stable U = unstable, D = data_sync, F = file_sync\" />\n"
  "  <field type=\"variable32\" name=\"fh2\" opt_nullable=\"yes\" pack_unique=\"yes\" />\n"
  "  <field type=\"int64\" name=\"cookie\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"maxcnt\" opt_nullable=\"yes\" />\n"
  "  <field type=\"byte\" name=\"stable\" opt_nullable=\"yes\" comment=\"for create, U = unchecked, G = guarded, X = exclusive; for stable U = unstable, D = data_sync, F = file_sync\" />\n"
  "  <field type=\"variable32\" name=\"file\" opt_nullable=\"yes\" pack_unique=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"name2\" opt_nullable=\"yes\" pack_unique=\"yes\" />\n"

  "  <field type=\"variable32\" name=\"sdata\" opt_nullable=\"yes\" pack_unique=\"yes\" comment=\"symlink data, appears to be the target of the symlink\" />\n"
  "  <field type=\"int64\" name=\"pre-size\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int64\" name=\"pre-mtime\" opt_nullable=\"yes\" units=\"microseconds\" epoch=\"unix\" />\n"
  "  <field type=\"int64\" name=\"pre-ctime\" opt_nullable=\"yes\" units=\"microseconds\" epoch=\"unix\" />\n"
  "  <field type=\"int32\" name=\"euid\" opt_nullable=\"yes\" pack_relative=\"euid\" />\n"
  "  <field type=\"int32\" name=\"egid\" opt_nullable=\"yes\" pack_relative=\"egid\" />\n"
  "  <field type=\"int64\" name=\"blksize\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"blocks\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"tsize\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"bsize\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"bfree\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"bavail\" opt_nullable=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"fn\" opt_nullable=\"yes\" comment=\"name for V2 lookups\" />\n"
  "  <field type=\"int32\" name=\"offset\" opt_nullable=\"yes\" comment=\"off for V2 reads\" />\n"
  "  <field type=\"int32\" name=\"tcount\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"nfsstat\" opt_nullable=\"yes\" comment=\"may be garbage, values don't match legal values for nfsstat from nfs_prot.x\" />\n"
  "  <field type=\"bool\" name=\"short_packet\" print_true=\"SHORT_PACKET\" print_false=\"OK_PACKET\" />\n"
  "  <field type=\"variable32\" name=\"fn2\" opt_nullable=\"yes\" comment=\"second name for V2 renames\" />\n"
  "  <field type=\"int32\" name=\"begoff\" opt_nullable=\"yes\" comment=\"beginning offset for V2 write\" />\n"
  "  <field type=\"variable32\" name=\"garbage\" opt_nullable=\"yes\" comment=\"a few lines are garbage, we simply pass through the entire contents here\" />\n"
  "</ExtentType>\n"
  );

ExtentSeries series;
OutputModule *outmodule;
Int64Field time_field(series, "time");
Int32Field source_ip(series, "source_ip");
Int32Field source_port(series, "source_port");
Int32Field dest_ip(series, "dest_ip");
Int32Field dest_port(series, "dest_port");
BoolField is_udp(series, "is_udp");
BoolField is_call(series, "is_call");
ByteField nfs_version(series, "nfs_version");
Int32Field rpc_transaction_id(series, "rpc_transaction_id");
ByteField rpc_function_id(series, "rpc_function_id");
Variable32Field rpc_function(series, "rpc_function");
Int32Field return_value(series, "return_value", Field::flag_nullable);
BoolField short_packet(series, "short_packet");
Variable32Field garbage(series, "garbage", Field::flag_nullable);

class KVParser {
public:
    virtual ~KVParser() { };
    virtual void parse(const string &val) = 0;
    virtual void setNull() = 0;
};

HashMap<string, KVParser *> kv_parsers;
unsigned nlines;

int64_t 
parseTime(const string &field)
{
    vector<string> timeparts;
    boost::split(timeparts, field, boost::is_any_of("."));
    // Check on size of timeparts is to try to make sure that 
    // we have a "sane" time
    INVARIANT((timeparts.size() == 2 && timeparts[0].size() >= 9 &&
	       timeparts[0].size() <= 10 
	       && timeparts[1].size() == 6) 
	      // V2 times seem to have been printed as %d.%d; 
	      // examination of anon-deasna-021104-1600.txt.gz shows some
	      // values that are going up at about the same rate as actual
	      // time, and progress from .9004 to .10007
	      || (nfs_version.val() == 2 && 
		  timeparts[1].size() >= 1 && timeparts[1].size() <= 6)
	      // A collection of misc times that don't make a lot of
	      // sense, but occurred in the traces; might want to make
	      // a warning flag or something like that.
	      || field == "0.000000" || field == "1000000.000000" 
	      || field == "4288942.000000" || field == "4096.000001"
	      || field == "18000.000000" || field == "2671200.000000"
	      || field == "125.000000" || field == "17999.000000"
	      || field == "21655309.000000" || field == "3600.000000"
	      || field == "11873905.000000" || field == "37026228.000000"
	      || field == "12458006.000000" || field == "1.000024"
	      || field == "800.000000" || field == "7139658.000000" 
	      || field == "2696400.000000",
	      format("error parsing time '%s' (%d,%d,%d) in line %d\n"
		     "garbage_lines.push_back(GarbageLine(%dLL, \"%x\")); // %s\n") 
	      % field % timeparts.size() % timeparts[0].size() 
	      % timeparts[1].size() % nlines
	      % time_field.val() % rpc_transaction_id.val() % field);
    return
	static_cast<int64_t>(stringToUInt32(timeparts[0])) * 1000000 
	+ stringToUInt32(timeparts[1]);
}

class KVParserFH : public KVParser {
public:
    KVParserFH(const string &fieldname)
	: field(series, fieldname, Field::flag_nullable)
    { }

    virtual ~KVParserFH() { }

    virtual void parse(const string &val) {
	INVARIANT(field.isNull(), "?");
	field.set(hex2raw(val));
    }
    
    virtual void setNull() {
	field.setNull();
    }
    
    Variable32Field field;
};

class KVParserString : public KVParser {
public:
    KVParserString(const string &fieldname)
	: field(series, fieldname, Field::flag_nullable)
    { }

    virtual ~KVParserString() { }

    virtual void parse(const string &val) {
	// tolearate empty strings, even though that seems unlikely to
	// be correct.
	INVARIANT(val.size() >= 2, format("? %s '%s'")
		  % field.getName() % val);
	SINVARIANT(val[0] == '"' && val[val.size()-1] == '"');
	
	SINVARIANT(field.isNull());
	field.set(val.substr(1,val.size()-2));
	SINVARIANT(!field.isNull());
    }
    
    virtual void setNull() {
	field.setNull();
    }
    
    Variable32Field field;
};

class KVParserByte : public KVParser {
public:
    KVParserByte(const string &fieldname, KVParserByte *_dup = NULL)
	: field(series, fieldname, Field::flag_nullable), dup(_dup)
    { }

    virtual ~KVParserByte() { }

    virtual void parse(const string &val) {
	if (field.isNull()) {
	    uint32_t v = stringToUInt32(val);
	    INVARIANT(v < 256, "bad");
	    field.set(v);
	} else {
	    INVARIANT(dup != NULL, 
		      format("? %d %s") % nlines % field.getName());
	    dup->parse(val);
	}
    }
    
    virtual void setNull() {
	field.setNull();
	if (dup) {
	    dup->setNull();
	}
    }
    
    ByteField field;
    KVParserByte *dup;
};

class KVParserHexInt32 : public KVParser {
public:
    KVParserHexInt32(const string &fieldname, KVParserHexInt32 *_dup = NULL)
	: field(series, fieldname, Field::flag_nullable), dup(_dup)
    { }

    virtual ~KVParserHexInt32() { }

    virtual void parse(const string &val) {
	if (field.isNull()) {
	    field.set(stringToUInt32(val, 16));
	} else {
	    INVARIANT(dup != NULL, 
		      format("? %d %s") % nlines % field.getName());
	    dup->parse(val);
	}
    }
    
    virtual void setNull() {
	field.setNull();
	if (dup) {
	    dup->setNull();
	}
    }
    
    Int32Field field;
    KVParserHexInt32 *dup;
};

class KVParserHexInt64 : public KVParser {
public:
    KVParserHexInt64(const string &fieldname, KVParserHexInt64 *_dup = NULL)
	: field(series, fieldname, Field::flag_nullable), dup(_dup)
    { }

    virtual ~KVParserHexInt64() { }

    virtual void parse(const string &val) {
	if (field.isNull()) {
	    field.set(stringToUInt64(val, 16));
	} else {
	    INVARIANT(dup != NULL, 
		      format("? %d %s") % nlines % field.getName());
	    dup->parse(val);
	}
    }
    
    virtual void setNull() {
	field.setNull();
	if (dup) {
	    dup->setNull();
	}
    }
    
    Int64Field field;
    KVParserHexInt64 *dup;
};

class KVParserTime : public KVParser {
public:
    KVParserTime(const string &fieldname, KVParserTime *_dup = NULL)
	: field(series, fieldname, Field::flag_nullable), dup(_dup)
    { }

    virtual ~KVParserTime() { }

    virtual void parse(const string &val) {
	if (field.isNull()) {
	    if (val == "SERVER") {
		field.set(-1);
	    } else {
		field.set(parseTime(val));
	    }
	} else {
	    INVARIANT(dup != NULL, 
		      format("? %d %s") % nlines % field.getName());
	    dup->parse(val);
	}
    }
    
    virtual void setNull() {
	field.setNull();
	if (dup) {
	    dup->setNull();
	}
    }
    
    Int64Field field;
    KVParserTime *dup;
};

class KVParserACC : public KVParser {
public:
    KVParserACC(const string &fieldname)
	: field(series, fieldname, Field::flag_nullable)
    { }

    virtual ~KVParserACC() { }

    virtual void parse(const string &val) {
	INVARIANT(field.isNull(), "?");
	if (val == "L") {
	    field.set(1 << 1);
	} else if (val == "R") {
	    field.set(1 << 0);
	} else if (val == "X") { // assume this is execute as opposed to extend
	    field.set(1 << 5); 
	} else if (val == "U") {
	    // No clue as to what this is.
	    field.set(1 << 6); 
	} else if (isdigit(val[0]) || islower(val[0]) && isxdigit(val[0])) {
	    uint32_t v = stringToUInt32(val, 16);
	    INVARIANT(v <= 63, "bad");
	    field.set(v);
	} else {
	    FATAL_ERROR(format("can't handle acc %s on line %d") 
			% val % nlines);
	}
    }
    
    virtual void setNull() {
	field.setNull();
    }
    
    ByteField field;
};

class KVParserBool : public KVParser {
public:
    KVParserBool(const string &fieldname)
	: field(series, fieldname, Field::flag_nullable)
    { }

    virtual ~KVParserBool() { }

    virtual void parse(const string &val) {
	INVARIANT(field.isNull(), "?");
	INVARIANT(val.size() == 1, format("bad boolean on line %d") % nlines);
	if (val[0] == '0') {
	    field.set(false);
	} else if (val[0] == '1') {
	    field.set(true);
	} else {
	    FATAL_ERROR("bad");
	}
    }
    
    virtual void setNull() {
	field.setNull();
    }
    
    BoolField field;
};

class KVParserHow : public KVParser {
public:
    KVParserHow(const string &fieldname)
	: field(series, fieldname, Field::flag_nullable)
    { }

    virtual ~KVParserHow() { }

    virtual void parse(const string &val) {
	INVARIANT(field.isNull(), "?");
	INVARIANT(val.size() == 1, "bad");
	INVARIANT(val[0] == 'U' || val[0] == 'G' || val[0] == 'X' ||
		  val[0] == 'D' || val[0] == 'F', 
		  format("bad how '%s' on line %d") % val % nlines);
	field.set(val[0]);
    }
    
    virtual void setNull() {
	field.setNull();
    }
    
    ByteField field;
};


void
parseIPPort(const string &field, Int32Field &ip_field, Int32Field &port_field)
{
    vector<string> parts;
    boost::split(parts, field, boost::is_any_of("."));
    INVARIANT(parts.size() == 2 && parts[1].size() == 4,
	      format("error parsing line %d") % nlines);

    ip_field.set(stringToUInt32(parts[0], 16));
    port_field.set(stringToUInt32(parts[1], 16));
}

void
parseTCPUDP(const string &field)
{
    INVARIANT(field.size() == 1, format("error parsing line %d") % nlines);
    if (field[0] == 'T') {
	is_udp.set(false);
    } else if (field[0] == 'U') {
	is_udp.set(true);
    } else {
	FATAL_ERROR(format("error parsing line %d") % nlines);
    }
}

void
parseCallReplyVersion(const string &field)
{
    INVARIANT(field.size() == 2, format("error parsing line %d") % nlines);
    if (field[0] == 'C') {
	is_call.set(true);
    } else if (field[0] == 'R') {
	is_call.set(false);
    } else {
	FATAL_ERROR(format("error parsing line %d") % nlines);
    }

    if (field[1] == '2') {
	nfs_version.set(2);
    } else if (field[1] == '3') {
	nfs_version.set(3);
    } else {
	FATAL_ERROR(format("error parsing line %d") % nlines);
    }
}

struct GarbageLine {
    int64_t time_val;
    string xid;
    bool short_packet;
    GarbageLine(int64_t a, const string &b, bool c = false)
	: time_val(a), xid(b), short_packet(c) {}
};

vector<GarbageLine> garbage_lines;
HashUnique<uint64_t> garbage_times;

bool
parseCommon(vector<string> &fields)
{
    INVARIANT(fields.size() >= 8, format("error parsing line %d") % nlines);

    time_field.set(parseTime(fields[0]));
    
    if (garbage_times.exists(time_field.val())) {
	// Have to return early since a few of the garbage lines don't parse through the 
	// remainder.
	return false;
    }
    parseIPPort(fields[1], source_ip, source_port);
    parseIPPort(fields[2], dest_ip, dest_port);
    
    parseTCPUDP(fields[3]);
    parseCallReplyVersion(fields[4]);

    rpc_transaction_id.set(stringToUInt32(fields[5], 16));
    rpc_function_id.set(stringToUInt32(fields[6], 16));
    rpc_function.set(fields[7]);
    short_packet.set(false);
    garbage.setNull(true);
    return true;
}

void
parseKVPair(const string &key, const string &value)
{
    KVParser *p = kv_parsers[key];
    INVARIANT(p != NULL, 
	      format("error parsing line %d don't have parser for key %s value %s")
	      % nlines % key % value);
    p->parse(value);
}

void
checkTailCall(vector<string> &fields, unsigned kvpairs)
{
    INVARIANT(kvpairs + 6 == fields.size(),
	      format("error parsing line %d; %d + 6 != %d") 
	      % nlines % kvpairs % fields.size());
    INVARIANT(fields[kvpairs] == "con" &&
	      fields[kvpairs+1] == "=" &&
	      fields[kvpairs+2] == "XXX" &&
	      fields[kvpairs+3] == "len" &&
	      fields[kvpairs+4] == "=" &&
	      fields[kvpairs+5] == "XXX\n",
	      format("error parsing line %d %s,%s,%s,%s,%s,%s") 
	      % nlines % fields[kvpairs] % fields[kvpairs+1]
	      % fields[kvpairs+2] % fields[kvpairs+3] 
	      % fields[kvpairs+4] % fields[kvpairs+5]);
}

void
checkTailReply(vector<string> &fields, unsigned kvpairs)
{
    INVARIANT(kvpairs + 10 == fields.size(),
	      format("error parsing line %d") % nlines);
    INVARIANT(fields[kvpairs] == "status=XXX" &&
	      fields[kvpairs+1] == "pl" &&
	      fields[kvpairs+2] == "=" &&
	      fields[kvpairs+3] == "XXX" &&
	      fields[kvpairs+4] == "con" &&
	      fields[kvpairs+5] == "=" &&
	      fields[kvpairs+6] == "XXX" &&
	      fields[kvpairs+7] == "len" &&
	      fields[kvpairs+8] == "=" &&
	      fields[kvpairs+9] == "XXX\n",
	      format("error parsing line %d") % nlines);
}

void
initGarbageLines()
{
    // If I'd known there would be this many garbage lines, I probably
    // would have modified the code to throw an exception anytime
    // there is a parsing error and in those cases, stuff the data
    // into the garbage field.  We're close enough to the end now that
    // it's not worth going back though.  This also lets us see some
    // patterns in the errors, although I have no idea why those
    // patterns exist.

    // Some lines have garbage in them, the times have 7 digits
    // in the microsecond field and the stable field is '?'; the
    // count is also garbage as it is claiming we have written
    // more than could be carried in a write request.
    garbage_lines.push_back(GarbageLine(1004562602021187LL, "9d66f750"));
    garbage_lines.push_back(GarbageLine(1004562602021196LL, "9d66f750"));
    garbage_lines.push_back(GarbageLine(1004391592471374LL, "25e02642"));
    garbage_lines.push_back(GarbageLine(1004391592471382LL, "25e02642"));
    garbage_lines.push_back(GarbageLine(1004554395617822LL, "35aeaf4f"));
    garbage_lines.push_back(GarbageLine(1002660539928279LL, "32136c8"));

    // More 7 digit numbers, not inspecting to figure out if other parts
    // of line look bad
    garbage_lines.push_back(GarbageLine(1000221413955706LL, "c7ffbdc6"));
    garbage_lines.push_back(GarbageLine(1000320518315046LL, "8021d1cc"));
    garbage_lines.push_back(GarbageLine(1000492652749891LL, "815b14d9"));
    garbage_lines.push_back(GarbageLine(1000499794952747LL, "4efeb2db"));
    garbage_lines.push_back(GarbageLine(1000747179024730LL, "12aee5e8"));
    garbage_lines.push_back(GarbageLine(1001106847789435LL, "a090f16a"));
    garbage_lines.push_back(GarbageLine(1001377905911480LL, "8284d478"));
    garbage_lines.push_back(GarbageLine(1001449644132284LL, "8fcba7d"));
    garbage_lines.push_back(GarbageLine(1000492653024779LL, "46114d9"));
    garbage_lines.push_back(GarbageLine(1001526976180832LL, "5c6bcf19"));
    garbage_lines.push_back(GarbageLine(1001526976180840LL, "5c6bcf19"));
    garbage_lines.push_back(GarbageLine(1001606263249714LL, "9551311f"));
    garbage_lines.push_back(GarbageLine(1001606263249724LL, "9551311f"));
    garbage_lines.push_back(GarbageLine(998927238429938LL, "3e26c948")); // 2255194683.3448702
    garbage_lines.push_back(GarbageLine(998927241309488LL, "1141c948")); // 4122477568.2516582
    garbage_lines.push_back(GarbageLine(998927238429938LL, "3e26c948")); // 2255194683.3448702
    garbage_lines.push_back(GarbageLine(1004639115274740LL, "3febbc18")); // 1004639115.2341724
    garbage_lines.push_back(GarbageLine(1004639955607259LL, "7ce7e518")); // 301989984.1830883
    garbage_lines.push_back(GarbageLine(1004636299176050LL, "63aa0ee")); // 301989984.1830881
    garbage_lines.push_back(GarbageLine(1004636299176063LL, "63aa0ee")); // 301989984.1830884
    garbage_lines.push_back(GarbageLine(1004639955607268LL, "7ce7e518")); // 301989984.1830892
    garbage_lines.push_back(GarbageLine(1004639115274755LL, "3febbc18")); // 1004639115.2341724
    garbage_lines.push_back(GarbageLine(1004642066119750LL, "ec9c3119")); // 301989984.1830883
    garbage_lines.push_back(GarbageLine(1005691932540232LL, "aa5601e0")); // 840828928.1157628
    garbage_lines.push_back(GarbageLine(1005256554580105LL, "1d67f794")); // 980310523.1795156
    garbage_lines.push_back(GarbageLine(1004642066119759LL, "ec9c3119")); // 301989984.1830892
    garbage_lines.push_back(GarbageLine(1005256554580119LL, "1d67f794")); // 980310523.1005256
    garbage_lines.push_back(GarbageLine(1005691932540243LL, "aa5601e0")); // 126814208.1157628
    garbage_lines.push_back(GarbageLine(1004639119489430LL, "9b08bd18")); // 3849783040.2516582
    garbage_lines.push_back(GarbageLine(1004639119489445LL, "9b08bd18")); // 3750496000.2415984
    garbage_lines.push_back(GarbageLine(1004639121697534LL, "a611bd18")); // 1004638846.2442387
    garbage_lines.push_back(GarbageLine(1045587326380320LL, "4a3e3dec")); // 1025711813.4075311
    garbage_lines.push_back(GarbageLine(1004639121697543LL, "a611bd18")); // 1004638846.2442387
    garbage_lines.push_back(GarbageLine(1044597353223926LL, "43a8a24d")); // 1022432832.2168971
    garbage_lines.push_back(GarbageLine(1044663900617111LL, "5bfea6f3")); // 1022432832.2168971
    garbage_lines.push_back(GarbageLine(1047096382697179LL, "56fafe75")); // 1022432832.2168971
    garbage_lines.push_back(GarbageLine(1044676504592523LL, "5c5ec461")); // 1022432832.2168971
    garbage_lines.push_back(GarbageLine(1044597353224513LL, "43a8a24f")); // 1028736550.1441504
    garbage_lines.push_back(GarbageLine(1047096382697568LL, "56fafe76")); // 1025711813.4075311
    garbage_lines.push_back(GarbageLine(1044663900617495LL, "5bfea6f4")); // 1025711813.4075311
    garbage_lines.push_back(GarbageLine(1044676504593001LL, "5c5ec462")); // 1025711813.4075311
    garbage_lines.push_back(GarbageLine(1047096382697936LL, "56fafe77")); // 1028736550.1441504
    garbage_lines.push_back(GarbageLine(1044663900617889LL, "5bfea6f5")); // 1028736550.1441504
    garbage_lines.push_back(GarbageLine(1044676504593454LL, "5c5ec463")); // 1028736550.1441504
    garbage_lines.push_back(GarbageLine(1045602743234634LL, "4a6d9a43")); // 1025710163.2383279
    garbage_lines.push_back(GarbageLine(1003853108865607LL, "53cc718d")); // 1002829854.55001
    garbage_lines.push_back(GarbageLine(1001822700334307LL, "d37bd187")); // 860
    garbage_lines.push_back(GarbageLine(1004634002979962LL, "6d9d17ee")); // 301989984.1830892
    garbage_lines.push_back(GarbageLine(1004636062591124LL, "24994a18")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1004637703366018LL, "bec68618")); // 3499427072.2583691
    garbage_lines.push_back(GarbageLine(1004638697449998LL, "555aae18")); // 1004638697.3918651
    garbage_lines.push_back(GarbageLine(1004633989564904LL, "c0ba16ee")); // 1004633989.2239750
    garbage_lines.push_back(GarbageLine(1004635764200030LL, "582e4218")); // 2449473536.2449473
    garbage_lines.push_back(GarbageLine(1004632983289302LL, "d9dedced")); // 1004632983.2541478
    garbage_lines.push_back(GarbageLine(1004639599693412LL, "687fd818")); // 1872093499.2979269
    garbage_lines.push_back(GarbageLine(1004980755403486LL, "1fe72531")); // 656934400.2516582
    garbage_lines.push_back(GarbageLine(1004641127513862LL, "a8161119")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1005114020821985LL, "958e7f3e")); // 3816081390.3841096
    garbage_lines.push_back(GarbageLine(1005111716574701LL, "b7f27f3f")); // 1343261696.3897360
    garbage_lines.push_back(GarbageLine(1005155473926803LL, "aca7f041")); // 4258712770.2496907
    garbage_lines.push_back(GarbageLine(1005156407814002LL, "887b1d42")); // 2449473536.2449473
    garbage_lines.push_back(GarbageLine(1005169739133894LL, "4a0fed44")); // 301989984.1830886
    garbage_lines.push_back(GarbageLine(1005164746141439LL, "8d78cf43")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1005168186832752LL, "6b959644")); // 1152715776.2516582
    garbage_lines.push_back(GarbageLine(1005252726693616LL, "3d6fd0ab")); // 1995500091.2140473
    garbage_lines.push_back(GarbageLine(1005942761463257LL, "d76d63de")); // 301989984.1830886
    garbage_lines.push_back(GarbageLine(1005192675075567LL, "6cc2de8e")); // 3808815675.4096196
    garbage_lines.push_back(GarbageLine(1006283821421384LL, "b48042e8")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1006207545390700LL, "49a80ced")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1006895260317066LL, "d9a26c23")); // 1343253376.2490171
    garbage_lines.push_back(GarbageLine(1007046422477924LL, "659fac18")); // 2449473536.2449473
    garbage_lines.push_back(GarbageLine(1006878434624527LL, "41bc5021")); // 2255772962.3891935
    garbage_lines.push_back(GarbageLine(1004634003028689LL, "2e9f17ee")); // 1343261696.3764977
    garbage_lines.push_back(GarbageLine(1004636062591208LL, "24994a18")); // 301008896.1157636
    garbage_lines.push_back(GarbageLine(1004637703366032LL, "bec68618")); // 2157315328.2919235
    garbage_lines.push_back(GarbageLine(1004638697450010LL, "555aae18")); // 1004638697.3918651
    garbage_lines.push_back(GarbageLine(1004633989564992LL, "c0ba16ee")); // 1004633989.2239750
    garbage_lines.push_back(GarbageLine(1004635764200114LL, "582e4218")); // 2919235584.2919235
    garbage_lines.push_back(GarbageLine(1004639599693425LL, "687fd818")); // 1872093499.4287891
    garbage_lines.push_back(GarbageLine(1004980755403495LL, "1fe72531")); // 824706560.1006632
    garbage_lines.push_back(GarbageLine(1004641127513875LL, "a8161119")); // 301008896.1157636
    garbage_lines.push_back(GarbageLine(1005114020822001LL, "958e7f3e")); // 3841096890.3816081
    garbage_lines.push_back(GarbageLine(1005111716574712LL, "b7f27f3f")); // 488306253.1658448
    garbage_lines.push_back(GarbageLine(1005155473926886LL, "aca7f041")); // 2122779393.4075619
    garbage_lines.push_back(GarbageLine(1005156407814013LL, "887b1d42")); // 2415984640.3055550
    garbage_lines.push_back(GarbageLine(1005169739133902LL, "4a0fed44")); // 301989984.1830884
    garbage_lines.push_back(GarbageLine(1005164746141453LL, "8d78cf43")); // 126814208.1157636
    garbage_lines.push_back(GarbageLine(1005168186832836LL, "6b959644")); // 2645888000.2919235
    garbage_lines.push_back(GarbageLine(1005192675075764LL, "6cc2de8e")); // 3808815675.1026228
    garbage_lines.push_back(GarbageLine(1005252726693631LL, "3d6fd0ab")); // 1995500091.3549760
    garbage_lines.push_back(GarbageLine(1006207545390715LL, "49a80ced")); // 443680768.1157636
    garbage_lines.push_back(GarbageLine(1006283821421394LL, "b48042e8")); // 126814208.1157628
    garbage_lines.push_back(GarbageLine(1006895260317075LL, "d9a26c23")); // 564424699.1515901
    garbage_lines.push_back(GarbageLine(1006878434624538LL, "41bc5021")); // 3891943979.2255773
    garbage_lines.push_back(GarbageLine(1004634003028702LL, "2e9f17ee")); // 2821336744.4283649
    garbage_lines.push_back(GarbageLine(1004637703906764LL, "75cc8618")); // 301989984.1830883
    garbage_lines.push_back(GarbageLine(1004636062665674LL, "77994a18")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1004638697523916LL, "ec5fae18")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1004639874848416LL, "75a3e218")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1004635764457072LL, "3d314218")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1004641173415319LL, "f98b1219")); // 1004641173.2510020
    garbage_lines.push_back(GarbageLine(1005164746516335LL, "457bcf43")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1005169739474018LL, "7713ed44")); // 1005169739.1269623
    garbage_lines.push_back(GarbageLine(1006207545433097LL, "b9a80ced")); // 964884795.3550152
    garbage_lines.push_back(GarbageLine(1004634003050562LL, "e99f17ee")); // 2919235584.2919235
    garbage_lines.push_back(GarbageLine(1004637703906786LL, "75cc8618")); // 301989984.1830892
    garbage_lines.push_back(GarbageLine(1004636062665682LL, "77994a18")); // 301008896.1157628
    garbage_lines.push_back(GarbageLine(1004638698055049LL, "f26aae18")); // 301989984.1830883
    garbage_lines.push_back(GarbageLine(1004639874848424LL, "75a3e218")); // 301008896.1157636
    garbage_lines.push_back(GarbageLine(1004635764457081LL, "3d314218")); // 126814208.1157628
    garbage_lines.push_back(GarbageLine(1004641173415339LL, "f98b1219")); // 1004641173.2510020
    garbage_lines.push_back(GarbageLine(1005169739474028LL, "7713ed44")); // 1005169739.1269623
    garbage_lines.push_back(GarbageLine(1006207545433107LL, "b9a80ced")); // 964884795.3080521
    garbage_lines.push_back(GarbageLine(1005164746516344LL, "457bcf43")); // 443680768.1157628
    garbage_lines.push_back(GarbageLine(1005084752361209LL, "fd3dbd35")); // 3915368706.2145773
    garbage_lines.push_back(GarbageLine(1004634012282584LL, "f63a18ee")); // 1004634012.2625626
    garbage_lines.push_back(GarbageLine(1004638005940914LL, "750d9018")); // 2449473536.2449473
    garbage_lines.push_back(GarbageLine(1004638698055119LL, "f26aae18")); // 301989984.1830884
    garbage_lines.push_back(GarbageLine(1004636236262019LL, "e0865218")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1004641173467065LL, "358c1219")); // 1004641173.2510020
    garbage_lines.push_back(GarbageLine(1005169739496716LL, "ec13ed44")); // 1269623099.1452541
    garbage_lines.push_back(GarbageLine(1004634012282596LL, "f63a18ee")); // 1004634012.2625626
    garbage_lines.push_back(GarbageLine(1004638698415598LL, "8375ae18")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1004638005940990LL, "750d9018")); // 2415984640.3122659
    garbage_lines.push_back(GarbageLine(1004636236262027LL, "e0865218")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1004641929647224LL, "11cd2b19")); // 2308890939.4108978
    garbage_lines.push_back(GarbageLine(1004641173996144LL, "a58e1219")); // 993201920.2516582
    garbage_lines.push_back(GarbageLine(1004638698415612LL, "8375ae18")); // 301008896.1157628
    garbage_lines.push_back(GarbageLine(1004638006336467LL, "f9109018")); // 2449473536.2449473
    garbage_lines.push_back(GarbageLine(1004634359407966LL, "e3692eee")); // 1343261696.1724055
    garbage_lines.push_back(GarbageLine(1005170265114034LL, "d47f0b45")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1004641173996155LL, "a58e1219")); // 2318667520.2415984
    garbage_lines.push_back(GarbageLine(1004638006336475LL, "f9109018")); // 2919235584.2919235
    garbage_lines.push_back(GarbageLine(1004638872971444LL, "be3db418")); // 2559762747.3234991
    garbage_lines.push_back(GarbageLine(1005170265114043LL, "d47f0b45")); // 313067520.1157636
    garbage_lines.push_back(GarbageLine(1002033023312806LL, "afaac037")); // 2144319803.1069941
    garbage_lines.push_back(GarbageLine(1003785530915153LL, "6a9c8eac")); // 301989984.1830881
    garbage_lines.push_back(GarbageLine(1003334888806384LL, "2cf19988")); // 4216130560.2516582
    garbage_lines.push_back(GarbageLine(1003516789410431LL, "9350fb96")); // 2449473536.2449473
    garbage_lines.push_back(GarbageLine(1003932925709369LL, "7c3fbeb2")); // 2449473536.2449473
    garbage_lines.push_back(GarbageLine(1004564743565665LL, "f0516e3")); // 3013675008.2449473
    garbage_lines.push_back(GarbageLine(1003882239929972LL, "6e6378b4")); // 4187477657.2262930
    garbage_lines.push_back(GarbageLine(1003892223700745LL, "832037b1")); // 2449473536.2449473
    garbage_lines.push_back(GarbageLine(1004555497930382LL, "9b3ccbe5")); // 301989984.1830883
    garbage_lines.push_back(GarbageLine(1002033023312895LL, "afaac037")); // 2415984640.3055550
    garbage_lines.push_back(GarbageLine(1003334888806395LL, "2cf19988")); // 2803567616.3254779
    garbage_lines.push_back(GarbageLine(1003516789410440LL, "9350fb96")); // 2919235584.2919235
    garbage_lines.push_back(GarbageLine(1003785530915189LL, "6a9c8eac")); // 301989984.1830884
    garbage_lines.push_back(GarbageLine(1003932925709382LL, "7c3fbeb2")); // 2415984640.3055550
    garbage_lines.push_back(GarbageLine(1004564743565683LL, "f0516e3")); // 849479680.2852126
    garbage_lines.push_back(GarbageLine(1003882239929980LL, "6e6378b4")); // 671040139.1142022
    garbage_lines.push_back(GarbageLine(1003892223700839LL, "832037b1")); // 2919235584.2919235
    garbage_lines.push_back(GarbageLine(1004555497930473LL, "9b3ccbe5")); // 301989984.1830892
    garbage_lines.push_back(GarbageLine(1004555498025027LL, "9b3dcbe5")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1004555498025038LL, "9b3dcbe5")); // 840828928.1157636
    garbage_lines.push_back(GarbageLine(1004487254047915LL, "561033dc")); // 1002898469.000999ctime
    garbage_lines.push_back(GarbageLine(1004555498121793LL, "ed3ecbe5")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1004555498121887LL, "ed3ecbe5")); // 840828928.1157636
    garbage_lines.push_back(GarbageLine(1004638996326081LL, "1e51b918")); // 3505980416.2650800
    garbage_lines.push_back(GarbageLine(1004636916872941LL, "7e576618")); // 1004636746.4102742
    garbage_lines.push_back(GarbageLine(1004639287162818LL, "8e2856ef")); // 1585185280.2516582
    garbage_lines.push_back(GarbageLine(1004636916872948LL, "7e576618")); // 1004636746.4102742
    garbage_lines.push_back(GarbageLine(1004639287162910LL, "8e2856ef")); // 813498880.2415984
    garbage_lines.push_back(GarbageLine(1004636941265995LL, "f62f6718")); // 337183744.2415984
    garbage_lines.push_back(GarbageLine(1004636941265981LL, "f62f6718")); // 185533440.2516582
    garbage_lines.push_back(GarbageLine(1004637070232212LL, "ccf66b18")); // 2717908992.2717908
    garbage_lines.push_back(GarbageLine(1004637070232225LL, "ccf66b18")); // 2391531835.3751084
    garbage_lines.push_back(GarbageLine(1004637071401490LL, "b8fc6b18")); // 1004637071.2408309
    garbage_lines.push_back(GarbageLine(1004637071401503LL, "b8fc6b18")); // 1004637071.2408309
    garbage_lines.push_back(GarbageLine(1004637277119652LL, "6e207618")); // 1825767680.2516582
    garbage_lines.push_back(GarbageLine(1004637277119660LL, "6e207618")); // 1960116480.2415984
    garbage_lines.push_back(GarbageLine(1004637310016378LL, "b84a7818")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1004637310016388LL, "b84a7818")); // 126814208.1157628
    garbage_lines.push_back(GarbageLine(1004637399256053LL, "96b7c18")); // 313067520.1157628
    garbage_lines.push_back(GarbageLine(1004637399256067LL, "96b7c18")); // 126814208.1157628
    garbage_lines.push_back(GarbageLine(1044597353223574LL, "43a8a24c")); // 1025710163.2383279
    garbage_lines.push_back(GarbageLine(1047096382696791LL, "56fafe74")); // 1025710163.2383279
    garbage_lines.push_back(GarbageLine(1044663900616725LL, "5bfea6f2")); // 1025710163.2383279
    garbage_lines.push_back(GarbageLine(1044676504592083LL, "5c5ec460")); // 1025710163.2383279

    // Automatically made short lines
    garbage_lines.push_back(GarbageLine(1003429220137370LL, "d3cebfdd", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1005230236836724LL, "63bec1f0", true)); // SHORT - 150
    garbage_lines.push_back(GarbageLine(1003434217988156LL, "f6e0bfdd", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1005232431794051LL, "7f80caf0", true)); // SHORT - 150
    garbage_lines.push_back(GarbageLine(1006977186643486LL, "4dd89106", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1001610469341859LL, "dc9e4025", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1001535583919140LL, "ffe67123", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1006977186663031LL, "6cd89106", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1005234074613256LL, "6498d1f0", true)); // SHORT - 150
    garbage_lines.push_back(GarbageLine(1006977274174174LL, "7e09106", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1003434218043283LL, "fde0bfdd", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1001535892069287LL, "28f07123", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1003429220139550LL, "d6cebfdd", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1001610469528550LL, "fa9e4025", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1003434218813525LL, "16e1bfdd", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1003429220139550LL, "d6cebfdd", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1001610471767838LL, "409f4025", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1003429273373328LL, "a9d0bfdd", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1003434286425485LL, "dfe6bfdd", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1001535944925478LL, "6ef17123", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1003434306536012LL, "39e8bfdd", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1003434306538940LL, "3be8bfdd", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1003434306717767LL, "67e8bfdd", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1003434306742061LL, "6be8bfdd", true)); // SHORT - 134
    garbage_lines.push_back(GarbageLine(1005148078456740LL, "bb1ae741", true)); // SHORT - 126
    garbage_lines.push_back(GarbageLine(1005756441953199LL, "818989c3", true)); // SHORT - 126
    garbage_lines.push_back(GarbageLine(1002828437658521LL, "90e4b967", true)); // SHORT - 126

    // missing fields?
    garbage_lines.push_back(GarbageLine(1001819098701115LL, "4920d087")); 
    garbage_lines.push_back(GarbageLine(1004641929647217LL, "11cd2b19"));

    // stable ?
    garbage_lines.push_back(GarbageLine(1005163315478874LL, "f6bc8043"));
    garbage_lines.push_back(GarbageLine(1004636875995561LL, "5e276518"));
    garbage_lines.push_back(GarbageLine(1005164772998660LL, "7270d143"));
    garbage_lines.push_back(GarbageLine(1005169739496726LL, "ec13ed44"));
    garbage_lines.push_back(GarbageLine(1004028529705656LL, "bc4479bf"));
    garbage_lines.push_back(GarbageLine(1004636876032719LL, "59276518"));
    garbage_lines.push_back(GarbageLine(1005164772998676LL, "7270d143"));
    garbage_lines.push_back(GarbageLine(1004636916815233LL, "1c566618"));
    garbage_lines.push_back(GarbageLine(1004638996326096LL, "1e51b918"));

    // "...ok... fsid ze 368184 ..ok...
    garbage_lines.push_back(GarbageLine(999551098438206LL, "5f877f83"));

    // Got mangled missing a bunch in the middle
    garbage_lines.push_back(GarbageLine(999464715460608LL, "30b76883"));

    // Garbage packet, stable ?, mode, nlink, size insane
    garbage_lines.push_back(GarbageLine(1005720202993479LL, "436ec1cb")); 
    // Looks kinda ok, but count and stable are garbage
    garbage_lines.push_back(GarbageLine(1004639118500428LL, "1704bd18"));

    // These are missing a key; probably fh, but can't tell for sure      
    garbage_lines.push_back(GarbageLine(1004486490806623LL, "18e520dc"));
    garbage_lines.push_back(GarbageLine(1004487246423963LL, "d9d732dc"));
    // Missing a space?
    garbage_lines.push_back(GarbageLine(1004487248910780LL, "247744e0"));
    // filehandles have '.' in them??
    garbage_lines.push_back(GarbageLine(1004487249188942LL, "ae855e4a"));
    // missing keys?
    garbage_lines.push_back(GarbageLine(1004487246432890LL, "ead732dc"));
    // short packet with con = 126; translate as garbage
    garbage_lines.push_back(GarbageLine(1000818821199260LL, "f48ab557", true));
    garbage_lines.push_back(GarbageLine(1005232945781235LL, "be686a9e", true));
    // short pack with con = 114
    garbage_lines.push_back(GarbageLine(1037402177970404LL, "79c477b0", true));
    garbage_lines.push_back(GarbageLine(1037402177971361LL, "79c477b1", true));
    // short con == 150
    garbage_lines.push_back(GarbageLine(1005240784958398LL, "682ffff0", true));
    // "...ok... write O  ...endstuff...
    garbage_lines.push_back(GarbageLine(1001455501691682LL, "630d5f7b"));
    // rdev2 shows up 3 times on this line
    garbage_lines.push_back(GarbageLine(1004487254072837LL, "921033dc"));
    // rdev2 shows up twice in a row in this line
    garbage_lines.push_back(GarbageLine(1004487254461187LL, "59e44e0"));
    // 11con
    garbage_lines.push_back(GarbageLine(1004487253737494LL, "ec9744e0"));
    // fh 2fb392840a00000con
    garbage_lines.push_back(GarbageLine(1004487253666126LL, "6e9744e0"));
    // missing fields
    garbage_lines.push_back(GarbageLine(1004487252515372LL, "7d8d44e0"));
    garbage_lines.push_back(GarbageLine(1004487252169500LL, "44945e4a"));
    garbage_lines.push_back(GarbageLine(1004487250492633LL, "568444e0"));
    // 1b6mtime
    garbage_lines.push_back(GarbageLine(1004487250653436LL, "b2e732dc"));
    // c4db32d1004487248.727481
    garbage_lines.push_back(GarbageLine(1004487248709749LL, "c4db32d1004487248.727481"));
    // fh 2fb392840a0000000c0000006blookup
    garbage_lines.push_back(GarbageLine(1004487250533621LL, "cc8444e0"));
    // status=0 pl = 212
    garbage_lines.push_back(GarbageLine(1004487248736569LL, "f7db32dc"));
    // R3C3
    garbage_lines.push_back(GarbageLine(1004487248607712LL, "5ceb99db"));
    // eof 0access
    garbage_lines.push_back(GarbageLine(1004487246743842LL, "31d832dc"));
    // R3 bcd732dc 4 acc.03ce
    garbage_lines.push_back(GarbageLine(1004487246378698LL, "bcd732dc"));
    //    garbage_lines.push_back(GarbageLine());
    
    
    for(vector<GarbageLine>::iterator i = garbage_lines.begin();
	i != garbage_lines.end(); ++i) {
	garbage_times.add(i->time_val);
    }
}

void
processLine(const string &buf)
{
    INVARIANT(buf[buf.size()-1] == '\n',
	      format("line %d doesn't end with a newline\n") % nlines);
    vector<string> fields;
    boost::split(fields, buf, boost::is_any_of(" "));
    
    outmodule->newRecord();
    for(HashMap<string, KVParser *>::iterator i = kv_parsers.begin();
	i != kv_parsers.end(); ++i) {
	i->second->setNull();
    }
    if (buf[0] == 'X' && buf[1] == 'X' && buf[2] == ' ') {
	// XX Funny line (#)
	garbage.set(buf);
	return;
    }
    bool ok = parseCommon(fields);

    if (garbage_times.exists(time_field.val())) {
	for(vector<GarbageLine>::iterator i = garbage_lines.begin();
	    i != garbage_lines.end(); ++i) {
	    if (time_field.val() == i->time_val) {
		SINVARIANT(fields[5] == i->xid);
		garbage.set(buf);
		short_packet.set(i->short_packet);
		return;
	    }
	}
    }
    SINVARIANT(ok);

    if (source_ip.val() == 0x30 && dest_ip.val() == 0x33 
	&& dest_port.val() == 0x039b && 
	(rpc_function_id.val() == 4 || rpc_function_id.val() == 6)
	&& fields.size() > 34 && 
	(fields[34] == "1025711813.4075311" 
	 || fields[34] == "1025710163.2383279")) {
	// lair62b traces have many of these all with the same bad
	// time value.
	garbage.set(buf);
	return;
    }

    if (rpc_function_id.val() == 0) {
	// special case null; the cleanup of the con = ## len = ## didn't
	// go right for that one.
	if (is_call.val()) {
	    SINVARIANT(fields.size() == 20);
	    SINVARIANT(fields[8] == "con" && fields[9] == "=");
	    SINVARIANT(fields[10] == "82" && fields[11] == "len");
	    SINVARIANT(fields[12] == "=" && fields[13] == "97");
	    SINVARIANT(fields[14] == "con" && fields[15] == "=");
	    SINVARIANT(fields[16] == "XXX" && fields[17] == "len");
	    SINVARIANT(fields[18] == "=" && fields[19] == "XXX\n");
	} else {
	    SINVARIANT(fields.size() == 28 &&
		       fields[8] == "status=0" && fields[9] == "pl" &&
		       fields[10] == "=" && fields[11] == "0" && 
		       fields[12] == "con" && fields[13] == "=" &&
		       fields[14] == "70" && fields[15] == "len" &&
		       fields[16] == "=" && fields[17] == "70" &&
		       fields[18] == "status=XXX" && fields[19] == "pl" &&
		       fields[20] == "=" && fields[21] == "XXX" &&
		       fields[22] == "con" && fields[23] == "=" &&
		       fields[24] == "XXX" && fields[25] == "len" &&
		       fields[26] == "=" && fields[27] == "XXX\n");
	}
	return;
    }
	    
    unsigned kvpairs;

    if (is_call.val()) {
	return_value.setNull();
	kvpairs = 8;
    } else {
	INVARIANT(fields.size() >= 9, 
		  format("error parsing line %d") % nlines);
	if (fields[8] == "OK") {
	    return_value.set(0);
	} else {
	    return_value.set(stringToInt32(fields[8],16));
	}
	kvpairs = 9;
    }
    
    if (fields[kvpairs] == "SHORT") {
	SINVARIANT(fields[kvpairs+1] == "PACKETcon");
	SINVARIANT(fields[kvpairs+2] == "=");
	INVARIANT(fields[kvpairs+3] == "130",
		  format("Got %s instead of 130 for the short packet value, need to just pass through\n"
			 "garbage_lines.push_back(GarbageLine(%dLL, \"%x\", true)); // SHORT - %s\n") % fields[kvpairs+3] % time_field.val() % rpc_transaction_id.val() % fields[kvpairs+3]);

	SINVARIANT(fields[kvpairs+4] == "len");
	SINVARIANT(fields[kvpairs+5] == "=");
	SINVARIANT(fields[kvpairs+6] == "400");
	SINVARIANT(fields[kvpairs+7] == "con");
	SINVARIANT(fields[kvpairs+8] == "=");
	SINVARIANT(fields[kvpairs+9] == "XXX");
	SINVARIANT(fields[kvpairs+10] == "len");
	SINVARIANT(fields[kvpairs+11] == "=");
	SINVARIANT(fields[kvpairs+12] == "XXX\n");
	short_packet.set(true);
	return;
    }

    unsigned end_at = fields.size() - (is_call.val() ? 6 : 10);
    if (false) cout << format("line %d (%s) %d fields end at %d\n") % nlines % fields[0] % fields.size() % end_at;
    for(; kvpairs < end_at; ) {
	if (fields[kvpairs].empty()) {
	    ++kvpairs;
	    continue;
	}
	INVARIANT(fields.size() >= kvpairs + 2, 
		  format("error parsing line %d") % nlines);		  
	parseKVPair(fields[kvpairs], fields[kvpairs+1]);
	kvpairs += 2;
    }
    
    if (is_call.val()) {
	checkTailCall(fields, kvpairs);
    } else {
	checkTailReply(fields, kvpairs);
    }
}

void
setupKVParsers()
{
    kv_parsers["fh"] = new KVParserFH("fh");
    kv_parsers["mode"] = 
	new KVParserHexInt32("mode", new KVParserHexInt32("mode_dup"));
    kv_parsers["name"] = new KVParserString("name");
    kv_parsers["ftype"] = new KVParserByte("ftype", 
					   new KVParserByte("ftype_dup"));
    kv_parsers["nlink"] = 
	new KVParserHexInt32("nlink", new KVParserHexInt32("nlink_dup"));
    kv_parsers["uid"] = 
	new KVParserHexInt32("uid", new KVParserHexInt32("uid_dup"));
    kv_parsers["gid"] = 
	new KVParserHexInt32("gid", new KVParserHexInt32("gid_dup"));
    kv_parsers["size"] = 
	new KVParserHexInt64("size", new KVParserHexInt64("size_dup"));
    kv_parsers["used"] = 
	new KVParserHexInt64("used", new KVParserHexInt64("used_dup"));
    kv_parsers["rdev"] = 
	new KVParserHexInt32("rdev", new KVParserHexInt32("rdev_dup"));
    kv_parsers["rdev2"] = 
	new KVParserHexInt32("rdev2", new KVParserHexInt32("rdev2_dup"));
    kv_parsers["fsid"] = 
	new KVParserHexInt64("fsid", new KVParserHexInt64("fsid_dup"));
    kv_parsers["fileid"] = 
	new KVParserHexInt64("fileid", new KVParserHexInt64("fileid_dup"));
    kv_parsers["atime"] = 
	new KVParserTime("atime", new KVParserTime("atime_dup"));
    kv_parsers["mtime"] = 
	new KVParserTime("mtime", new KVParserTime("mtime_dup"));
    kv_parsers["ctime"] = 
	new KVParserTime("ctime", new KVParserTime("ctime_dup"));
    kv_parsers["acc"] = new KVParserACC("acc");
    kv_parsers["off"] = new KVParserHexInt64("off");
    kv_parsers["count"] = new KVParserHexInt32("count");
    kv_parsers["eof"] = new KVParserBool("eof");
    kv_parsers["how"] = new KVParserHow("how");
    kv_parsers["fh2"] = new KVParserFH("fh2");
    kv_parsers["cookie"] = new KVParserHexInt64("cookie");
    kv_parsers["maxcnt"] = new KVParserHexInt32("maxcnt");
    kv_parsers["stable"] = new KVParserHow("stable");
    kv_parsers["file"] = new KVParserFH("file"); // should merge with fh
    kv_parsers["name2"] = new KVParserString("name2");
    kv_parsers["sdata"] = new KVParserString("sdata");
    kv_parsers["pre-size"] = new KVParserHexInt64("pre-size");
    kv_parsers["pre-mtime"] = new KVParserTime("pre-mtime");
    kv_parsers["pre-ctime"] = new KVParserTime("pre-ctime");
    kv_parsers["euid"] = new KVParserHexInt32("euid");
    kv_parsers["egid"] = new KVParserHexInt32("egid");
    kv_parsers["blksize"] = new KVParserHexInt64("blksize");
    kv_parsers["blocks"] = new KVParserHexInt32("blocks");
    kv_parsers["tsize"] = new KVParserHexInt32("tsize");
    kv_parsers["bsize"] = new KVParserHexInt32("bsize");
    kv_parsers["bfree"] = new KVParserHexInt32("bfree");
    kv_parsers["bavail"] = new KVParserHexInt32("bavail");
    kv_parsers["fn"] = new KVParserString("fn");
    kv_parsers["offset"] = new KVParserHexInt32("offset");
    kv_parsers["tcount"] = new KVParserHexInt32("tcount");
    kv_parsers["nfsstat"] = new KVParserHexInt32("nfsstat");
    kv_parsers["fn2"] = new KVParserString("fn2");
    kv_parsers["begoff"] = new KVParserHexInt32("begoff");
}

int
main(int argc,char *argv[])
{
    commonPackingArgs packing_args;
    //    packing_args.extent_size = 8*1024*1024;
    getPackingArgs(&argc,argv,&packing_args);

    INVARIANT(argc == 3,
	      format("Usage: %s inname outdsname; - valid for inname")
	      % argv[0]);
    FILE *infile;
    if (strcmp(argv[1],"-")==0) {
	infile = stdin;
    } else {
	infile = fopen(argv[1],"r");
	INVARIANT(infile != NULL, format("Unable to open file %s: %s")
		  % argv[1] % strerror(errno));
    }

    DataSeriesSink outds(argv[2],
			 packing_args.compress_modes,
			 packing_args.compress_level);
    ExtentTypeLibrary library;
    const ExtentType *type = library.registerType(ellard_nfs_expanded_xml);
    series.setType(*type);
    outmodule = new OutputModule(outds, series, type, 
				 packing_args.extent_size);
    outds.writeExtentLibrary(library);

    setupKVParsers();
    initGarbageLines();

    // STL is slow 
    const unsigned bufsize = 1024*1024;
    char buf[bufsize];
    while(true) {
	buf[0] = '\0';
	fgets(buf,bufsize,infile);
	++nlines;
	if (buf[0] == '#') {
	    continue;
	}
	if (buf[0] == '\0') {
	    break;
	}
	AssertAlways(strlen(buf) < (bufsize - 1),
		     ("increase bufsize constant.\n"));
	processLine(buf);
    }
    cout << format("Processed %d lines\n") % nlines;

    delete outmodule;
    outds.close();
    return 0;
}
