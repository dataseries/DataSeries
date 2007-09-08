// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    DataSeriesFile implementation
*/

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/resource.h>

#include <ostream>

#include <boost/static_assert.hpp>

using namespace std;

#include <Lintel/Double.H>
#include <Lintel/HashTable.H>

#include <DataSeries/DataSeriesFile.H>
#include <DataSeries/ExtentField.H>

#if _FILE_OFFSET_BITS == 64 && !defined(_LARGEFILE64_SOURCE)
#define _LARGEFILE64_SOURCE
#define lseek64 lseek
#endif

#ifndef _LARGEFILE64_SOURCE
#error "Must compile with -D_LARGEFILE64_SOURCE"
#endif

static const string dataseries_type_xml = 
  "<ExtentType name=\"DataSeries: XmlType\">\n"
  "  <field type=\"variable32\" name=\"xmltype\" />\n"
  "</ExtentType>\n";

static ExtentType global_dataseries_type(dataseries_type_xml);

const string dataseries_type_index =
  "<ExtentType name=\"DataSeries: ExtentIndex\">\n"
  "  <field type=\"int64\" name=\"offset\" />\n"
  "  <field type=\"variable32\" name=\"extenttype\" />\n"
  "</ExtentType>\n";

const string dataseries_type_index_v2 =
  "<ExtentType name=\"DataSeries::ExtentIndex\" >\n"
  "  <!-- next fields are necessary/useful for finding the extents that\n"
  "       a program wants to process without having a separate index file -->\n"
  "  <field type=\"int64\" name=\"offset\" />\n"
  "  <field type=\"variable32\" name=\"extenttype\" />\n"
  "  <field type=\"variable32\" name=\"namespace\" />\n"
  "  <field type=\"variable32\" name=\"version\" />\n"
  "  <!-- technically the next bits are in the header for each extent; \n"
  "       this would allow for the possibility of reading the files without\n"
  "       reading the index at the end, although this is not currently\n"
  "       supported.  However, these end up being useful for figuring\n"
  "       out properties of a given DS file without having to write\n"
  "       a separate interface that can skitter through a file and extract\n"
  "       all of this information. -->\n"
  "  <field type=\"byte\" name=\"fixed_compress_mode\" />\n"
  "  <field type=\"int32\" name=\"fixed_uncompressed_size\" />\n"
  "  <field type=\"int32\" name=\"fixed_compressed_size\" />\n"
  "  <field type=\"byte\" name=\"variable_compress_mode\" />\n"
  "  <field type=\"int32\" name=\"variable_uncompressed_size\" />\n"
  "  <field type=\"int32\" name=\"variable_compressed_size\" />\n"
  "</ExtentType>\n";

static ExtentType global_dataseries_indextype(dataseries_type_index);

DataSeriesSource::DataSeriesSource(const string &_filename)
    : filename(_filename), fd(-1), cur_offset(0)
{
    reopenfile();
    mylibrary.registerType(dataseries_type_xml);
    mylibrary.registerType(dataseries_type_index);
    dataseries_type = mylibrary.getTypeByName("DataSeries: XmlType");
    Extent::ByteArray data;
    const int file_header_size = 2*4 + 4*8;
    data.resize(file_header_size);
    Extent::checkedPread(fd,0,data.begin(),file_header_size);
    cur_offset = file_header_size;
    AssertAlways(data[0] == 'D' && data[1] == 'S' &&
		 data[2] == 'v' && data[3] == '1',
		 ("Invalid data series source, not DSv1\n"));
    typedef ExtentType::int32 int32;
    typedef ExtentType::int64 int64;
    int32 check_int = *(int32 *)(data.begin() + 4);
    if (check_int == 0x12345678) {
	need_bitflip = false;
    } else if (check_int == 0x78563412) {
	need_bitflip = true;
    } else {
	AssertFatal(("Unable to interpret check integer %x\n",check_int));
    }
    if (need_bitflip) {
	Extent::flip4bytes(data.begin()+4);
	Extent::flip8bytes(data.begin()+8);
	Extent::flip8bytes(data.begin()+16);
	Extent::flip8bytes(data.begin()+24);
	Extent::flip8bytes(data.begin()+32);
    }
    AssertAlways(*(int32 *)(data.begin() + 4) == 0x12345678,
		 ("int32 check failed\n"));
    AssertAlways(*(int64 *)(data.begin() + 8) == 0x123456789ABCDEF0LL,
		 ("int64 check failed\n"));
    AssertAlways(fabs(3.1415926535897932384 - *(double *)(data.begin() + 16)) < 1e-18,
		 ("fixed double check failed\n"));
    AssertAlways(*(double *)(data.begin() + 24) == Double::Inf,
		 ("infinity double check failed\n"));
    AssertAlways(*(double *)(data.begin() + 32) != *(double *)(data.begin() + 32),
		 ("NaN double check failed\n"));
    Extent::ByteArray extentdata;
    AssertAlways(Extent::preadExtent(fd,cur_offset,extentdata,need_bitflip),
		 ("Invalid file, must have a first extent\n"));
    Extent *e = new Extent(mylibrary,extentdata,need_bitflip);
    AssertAlways(e->type == dataseries_type,
		 ("Whoa, first extent must be the type defining extent\n"));

    ExtentSeries type_extent_series(e);
    Variable32Field typevar(type_extent_series,"xmltype");
    for(;type_extent_series.pos.morerecords();++type_extent_series.pos) {
	string v = typevar.stringval();
	mylibrary.registerType(v);
    }
    delete e;
    struct stat ds_file_stats;
    int ret_val = fstat(fd,&ds_file_stats);
    INVARIANT(ret_val == 0,
	      boost::format("fstat failed: %s")
	      % strerror(errno));
    BOOST_STATIC_ASSERT(sizeof(ds_file_stats.st_size) >= 8); // won't handle large files correctly unless this is true.
    off64_t tailoffset = ds_file_stats.st_size-7*4;
    INVARIANT(tailoffset > 0, "file is too small to be a dataseries file??");
    byte tail[7*4];
    Extent::checkedPread(fd,tailoffset,tail,7*4);
    DataSeriesSink::verifyTail(tail,need_bitflip,filename);
    if (need_bitflip) {
	Extent::flip4bytes(tail+4);
	Extent::flip8bytes(tail+16);
    }
    int32 packedsize = *(int32 *)(tail + 4);
    off64_t indexoffset = *(int64 *)(tail + 16);
    INVARIANT(tailoffset - packedsize == indexoffset,
	      boost::format("mismatch on index offset %d - %d != %d!")
	      % tailoffset % packedsize % indexoffset);
    indexExtent = preadExtent(indexoffset);
    INVARIANT(indexExtent != NULL, "index extent read failed");
}

DataSeriesSource::~DataSeriesSource()
{
    if (isactive()) {
	closefile();
    }
    delete indexExtent;
}

void
DataSeriesSource::closefile()
{
    AssertAlways(close(fd) == 0,("close failed: %s\n",strerror(errno)));
    fd = -1;
}

void
DataSeriesSource::reopenfile()
{
    AssertAlways(fd == -1,("trying to reopen non-closed source?!\n"));
    fd = open(filename.c_str(), O_RDONLY | O_LARGEFILE);
    AssertAlways(fd >= 0,("error opening %s for read: %s\n",filename.c_str(),
			  strerror(errno)));
}

Extent *
DataSeriesSource::preadExtent(off64_t &offset, unsigned *compressedSize)
{
    Extent::ByteArray extentdata;
    
    if (Extent::preadExtent(fd, offset, extentdata, need_bitflip) == false) {
	return NULL;
    }
    if (compressedSize) *compressedSize = extentdata.size();
    Extent *ret = new Extent(mylibrary,extentdata,need_bitflip);
    AssertAlways(ret->type != dataseries_type,
		 ("Invalid to have a type extent after the first extent!\n"));
    return ret;
}

DataSeriesSink::DataSeriesSink(const string &filename,
			       int _compression_modes,
			       int _compression_level)
    : index_series(global_dataseries_indextype),
      index_extent(index_series),
      field_extentOffset(index_series,"offset"),
      field_extentType(index_series,"extenttype"),
      wrote_library(false),
      compression_modes(_compression_modes),
      compression_level(_compression_level),
      chained_checksum(0)
{
    stats.packed_size += 2*4 + 4*8;
    AssertAlways(global_dataseries_type.name == "DataSeries: XmlType",
		 ("internal error; c++ initializers didn't run?\n"));
    if (filename == "-") {
	fd = fileno(stdout);
    } else {
	fd = open(filename.c_str(), 
		  O_WRONLY | O_LARGEFILE | O_CREAT | O_TRUNC, 0666);
    }
    AssertAlways(fd >= 0,
		 ("Error opening %s for write: %s\n",
		  filename.c_str(),strerror(errno)));
    const string filetype = "DSv1";
    checkedWrite(filetype.data(),4);
    ExtentType::int32 int32check = 0x12345678;
    checkedWrite(&int32check,4);
    ExtentType::int64 int64check = 0x123456789ABCDEF0LL;
    checkedWrite(&int64check,8);
    double doublecheck = 3.1415926535897932384; 
    checkedWrite(&doublecheck,8);
    doublecheck = Double::Inf;
    checkedWrite(&doublecheck,8);
    doublecheck = Double::NaN;
    checkedWrite(&doublecheck,8);
    cur_offset = 2*4 + 4*8;
}

DataSeriesSink::~DataSeriesSink()
{
    if (cur_offset > 0) {
	close();
    }
}

void
DataSeriesSink::close()
{
    AssertAlways(wrote_library,
		 ("error: never wrote the extent type library?!"));
    AssertAlways(cur_offset >= 0,("error: close called twice?!"));
    ExtentType::int64 index_offset = cur_offset;
    uint32_t packed_size = doWriteExtent(index_extent, NULL);
    char *tail = new char[7*4];
    AssertAlways(((unsigned long)tail % 8) == 0,("malloc alignment glitch?!"));
    for(int i=0;i<4;i++) {
	tail[i] = 0xFF;
    }
    typedef ExtentType::int32 int32;
    *(int32 *)(tail + 4) = packed_size;
    *(int32 *)(tail + 8) = ~packed_size;
    *(int32 *)(tail + 12) = chained_checksum;
    *(ExtentType::int64 *)(tail + 16) = (ExtentType::int64)index_offset;
    *(int32 *)(tail + 24) = BobJenkinsHash(1776,tail,6*4);
    checkedWrite(tail,7*4);
    delete [] tail;
    cur_offset = 0;
    int ret = ::close(fd);
    INVARIANT(ret == 0, boost::format("close failed: %s") % strerror(errno));
    fd = -1;
    index_series.clearExtent();
    index_extent.clear();
}

void
DataSeriesSink::checkedWrite(const void *buf, int bufsize)
{
    ssize_t ret = write(fd,buf,bufsize);
    INVARIANT(ret != -1,
	      boost::format("Error on write of %d bytes: %s")
	      % bufsize % strerror(errno));
    INVARIANT(ret == bufsize,
	      boost::format("Partial write %d bytes out of %d bytes (disk full?): %s")
	      % ret % bufsize % strerror(errno));
}

void
DataSeriesSink::writeExtent(Extent &e, Stats *stats)
{
    INVARIANT(wrote_library,
	      "must write extent type library before writing extents!\n");
    INVARIANT(valid_types[e.type],
	      boost::format("type %s (%p) wasn't in your type library")
	      % e.type->name.c_str() % e.type);
    doWriteExtent(e, stats);
}

void
DataSeriesSink::finishWriting()
{
    // nothing to do right now.
}

void
DataSeriesSink::writeExtentLibrary(ExtentTypeLibrary &lib)
{
    ExtentSeries type_extent_series(global_dataseries_type);
    Extent type_extent(type_extent_series);

    Variable32Field typevar(type_extent_series,"xmltype");
    for(map<const string,ExtentType *>::iterator i = lib.name_to_type.begin();
	i != lib.name_to_type.end();++i) {
	ExtentType *et = i->second;
	if (et->name == "DataSeries: XmlType") {
	    continue; // no point of writing this out; can't use it.
	}

	type_extent_series.newRecord();
	AssertAlways(et->xmldesc.size() > 0,
		     ("whoa extenttype has no xml data?!\n"));
	typevar.set(et->xmldesc.data(),et->xmldesc.size());
	valid_types[et] = true;
	if (et->majorVersion() == 0 && et->minorVersion() == 0 ||
	    et->getNamespace().empty()) {
	    // Once we have a version of dsrepack/dsselect that can
	    // change the XML type, we can make this an error.
	    cerr << boost::format("Warning: type '%s' is missing either a version or a namespace")
		% et->name << endl;
	}
    }
    doWriteExtent(type_extent, NULL);
    wrote_library = true; 
}

void
DataSeriesSink::verifyTail(ExtentType::byte *tail,
			   bool need_bitflip,
			   const string &filename)
{
    // Only thing we can't check here is a match between the offset of
    // the tail and the offset stored in the tail.
    for(int i=0;i<4;i++) {
	AssertAlways(tail[i] == 0xFF,("bad header for the tail of %s!\n",
				      filename.c_str()));
    }
    typedef ExtentType::int32 int32;
    int32 packed_size = *(int32 *)(tail + 4);
    int32 tilde_packed_size = *(int32 *)(tail + 8);
    int32 bjhash = *(int32 *)(tail + 24);
    if (need_bitflip) {
	packed_size = Extent::flip4bytes(packed_size);
	tilde_packed_size = Extent::flip4bytes(tilde_packed_size);
	bjhash = Extent::flip4bytes(bjhash);
    }
    AssertAlways(packed_size == ~tilde_packed_size,
		 ("bad packed size in the tail!\n"));
    int32 check_bjhash = BobJenkinsHash(1776,tail,6*4);
    AssertAlways(bjhash == check_bjhash,
		 ("bad hash in the tail!\n"));
}

size_t
DataSeriesSink::doWriteExtent(Extent &e, Stats *to_update)
{
    Extent::ByteArray data;
    index_series.newRecord();
    field_extentOffset.set(cur_offset);
    field_extentType.set(e.type->name);
    AssertAlways(cur_offset > 0,("Error: doWriteExtent on closed file\n"));

    struct timespec pack_start;
    INVARIANT(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &pack_start) == 0, "??");
    
    int headersize, fixedsize, variablesize;
    uint32_t checksum = e.packData(data,compression_modes,compression_level,
				   &headersize,&fixedsize,&variablesize);
    struct timespec pack_end;
    INVARIANT(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &pack_end) == 0, "??");

    checkedWrite(data.begin(),data.size());
    cur_offset += data.size();
    double pack_extent_time = (pack_end.tv_sec - pack_start.tv_sec) 
	+ (pack_end.tv_nsec - pack_start.tv_nsec)*1e-9;
    
    chained_checksum = BobJenkinsHashMix3(checksum, chained_checksum, 1972);
    PThreadAutoLocker lock(Stats::getMutex());
    stats.update(headersize + fixedsize + variablesize, fixedsize,
		 e.variabledata.size(), variablesize, 
		 data.size(), *reinterpret_cast<uint32_t *>(data.begin()+4), 
		 pack_extent_time, data[6*4], data[6*4+1]);
    
    if (to_update != NULL) {
	to_update->update(headersize + fixedsize + variablesize, fixedsize,
			  e.variabledata.size(), variablesize, 
			  data.size(), *reinterpret_cast<uint32_t *>(data.begin()+4), 
			  pack_extent_time, data[6*4], data[6*4+1]);
    }
		 
    return data.size();
}

void
DataSeriesSink::Stats::reset()
{
    extents = compress_none = compress_lzo = compress_gzip 
	= compress_bz2 = compress_lzf = 0;
    unpacked_size = unpacked_fixed = unpacked_variable 
	= unpacked_variable_raw = packed_size = 0;
    pack_time = 0;
}

DataSeriesSink::Stats &
DataSeriesSink::Stats::operator+=(const DataSeriesSink::Stats &from)
{
    extents += from.extents;
    compress_none += from.compress_none;
    compress_lzo += from.compress_lzo;
    compress_gzip += from.compress_gzip;
    compress_bz2 += from.compress_bz2;
    compress_lzf += from.compress_lzf;
    unpacked_size += from.unpacked_size;
    unpacked_fixed += from.unpacked_fixed;
    unpacked_variable += from.unpacked_variable;
    unpacked_variable_raw += from.unpacked_variable_raw;
    packed_size += from.packed_size;
    pack_time += from.pack_time;
    return *this;
}

DataSeriesSink::Stats &
DataSeriesSink::Stats::operator-=(const DataSeriesSink::Stats &from)
{
    extents -= from.extents;
    compress_none -= from.compress_none;
    compress_lzo -= from.compress_lzo;
    compress_gzip -= from.compress_gzip;
    compress_bz2 -= from.compress_bz2;
    compress_lzf -= from.compress_lzf;
    unpacked_size -= from.unpacked_size;
    unpacked_fixed -= from.unpacked_fixed;
    unpacked_variable -= from.unpacked_variable;
    unpacked_variable_raw -= from.unpacked_variable_raw;
    packed_size -= from.packed_size;
    pack_time -= from.pack_time;

    return *this;
}

void
DataSeriesSink::Stats::update(uint32_t unp_size, uint32_t unp_fixed, 
			      uint32_t unp_var_raw, uint32_t unp_variable, 
			      uint32_t pkd_size, uint32_t pkd_var_size, 
			      double pkd_time, 
			      unsigned char fixed_compress_mode,
			      unsigned char variable_compress_mode)
{
    ++extents;
    unpacked_size += unp_size;
    unpacked_fixed += unp_fixed;
    unpacked_variable_raw += unp_var_raw;
    unpacked_variable += unp_variable;
    packed_size += pkd_size;
    pack_time += pkd_time;
    updateCompressMode(fixed_compress_mode);
    if (pkd_var_size > 0) {
	updateCompressMode(variable_compress_mode);
    }
}

void
DataSeriesSink::Stats::updateCompressMode(unsigned char compress_mode)
{
    switch(compress_mode) 
	{
	case 0: ++compress_none; break;
	case 1: ++compress_lzo; break;
	case 2: ++compress_gzip; break;
	case 3: ++compress_bz2; break;
	case 4: ++compress_lzf; break;
	default:
	    FATAL_ERROR(boost::format("whoa, unknown compress option %d\n") 
			% static_cast<unsigned>(compress_mode));
	}
}

// TODO: make this take a int64 records argument also, default -1, and
// print out record counts, etc, like logfu2ds.C
void
DataSeriesSink::Stats::printText(ostream &to, const string &extent_type)
{
    if (extent_type.empty()) {
	to << boost::format("  wrote %d extents\n")
	    % extents;
    } else {
	to << boost::format("  wrote %d extents of type %s\n")
	    % extents % extent_type;
    }

    to << boost::format("  compression (none,lzo,gzip,bz2,lzf): (%d,%d,%d,%d,%d)\n")
	% compress_none % compress_lzo % compress_gzip % compress_bz2 
	% compress_lzf;
    to << boost::format("  unpacked: %d = %d (fixed) + %d (variable, %d raw)\n")
	% unpacked_size % unpacked_fixed % unpacked_variable 
	% unpacked_variable_raw;
    to << boost::format("  packed size: %d; pack time: %.3f\n")
	% packed_size % pack_time;
}

PThreadMutex &
DataSeriesSink::Stats::getMutex()
{
    static PThreadMutex mutex;
    
    return mutex;
}
