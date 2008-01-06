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

#include <DataSeries/DataSeriesModule.H>
#include <DataSeries/commonargs.H>

using namespace std;
using boost::format;

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
  " comment=\"see ellardnfs2ds.cpp:processLine for notes on how a few lines containing garbage were translated\" >\n"
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
  "  <field type=\"int32\" name=\"nfsstat\" opt_nullable=\"yes\" comment=\"may be garbage, values don't match legal values for nfsstat from nfs_prot.x\" />"
  "  <field type=\"bool\" name=\"short_packet\" print_true=\"SHORT_PACKET\" print_false=\"OK_PACKET\" />"
  "  <field type=\"variable32\" name=\"fn2\" opt_nullable=\"yes\" comment=\"second name for V2 renames\" />"
  "  <field type=\"int32\" name=\"begoff\" opt_nullable=\"yes\" comment=\"beginning offset for V2 write\" />"
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
	      || field == "0.000000" || field == "1000000.000000" 
	      || field == "4288942.000000" || field == "4096.000001", 
	      format("error parsing time '%s' (%d,%d,%d) in line %d") 
	      % field % timeparts.size() % timeparts[0].size() 
	      % timeparts[1].size() % nlines);
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
	INVARIANT(val.size() > 2, format("? %s '%s'")
		  % field.getName() % val);
	INVARIANT(val[0] == '"' && val[val.size()-1] == '"', "?");
	
	INVARIANT(field.isNull(), "?");
	field.set(val.substr(1,val.size()-2));
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
	INVARIANT(val.size() == 1, "bad");
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
		  val[0] == 'D' || val[0] == 'F', "bad");
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

void
parseCommon(vector<string> &fields)
{
    INVARIANT(fields.size() >= 8, format("error parsing line %d") % nlines);

    time_field.set(parseTime(fields[0]));
    
    parseIPPort(fields[1], source_ip, source_port);
    parseIPPort(fields[2], dest_ip, dest_port);
    
    parseTCPUDP(fields[3]);
    parseCallReplyVersion(fields[4]);

    rpc_transaction_id.set(stringToUInt32(fields[5], 16));
    rpc_function_id.set(stringToUInt32(fields[6], 16));
    rpc_function.set(fields[7]);
    short_packet.set(false);
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
processLine(const string &buf)
{
    vector<string> fields;
    boost::split(fields, buf, boost::is_any_of(" "));
    
    if (fields[0] == "1004562602.021187" ||
	fields[0] == "1004562602.021196") {
	INVARIANT(fields[5] == "9d66f750", "?");
	// These lines have garbage in them, the times have 7 digits
	// in the microsecond field and the stable field is '?'; the
	// count is also garbage as it is claiming we have written
	// more than could be carried in a write request.
	return;
    }
    outmodule->newRecord();
    parseCommon(fields);
	
    for(HashMap<string, KVParser *>::iterator i = kv_parsers.begin();
	i != kv_parsers.end(); ++i) {
	i->second->setNull();
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
	SINVARIANT(fields[kvpairs+3] == "130");
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
    ExtentType *type = library.registerType(ellard_nfs_expanded_xml);
    series.setType(*type);
    outmodule = new OutputModule(outds, series, type, 
				 packing_args.extent_size);
    outds.writeExtentLibrary(library);

    setupKVParsers();

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
