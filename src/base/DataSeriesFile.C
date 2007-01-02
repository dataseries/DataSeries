// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    DataSeriesFile implementation
*/

#include <fcntl.h>
#include <errno.h>
#include <sys/resource.h>

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

static const std::string dataseries_type_xml = 
  "<ExtentType name=\"DataSeries: XmlType\">\n"
  "  <field type=\"variable32\" name=\"xmltype\" />\n"
  "</ExtentType>\n";

static ExtentType global_dataseries_type(dataseries_type_xml);

const std::string dataseries_type_index =
  "<ExtentType name=\"DataSeries: ExtentIndex\">\n"
  "  <field type=\"int64\" name=\"offset\" />\n"
  "  <field type=\"variable32\" name=\"extenttype\" />\n"
  "</ExtentType>\n";

static ExtentType global_dataseries_indextype(dataseries_type_index);

DataSeriesSource::DataSeriesSource(const std::string &_filename)
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
	std::string v = typevar.stringval();
	mylibrary.registerType(v);
    }
    delete e;

    off64_t tailoffset = lseek64(fd,-7*4,SEEK_END);
    AssertAlways(tailoffset > 0,("seek to end failed?!\n"));
    byte tail[7*4];
    Extent::checkedPread(fd,tailoffset,tail,7*4);
    DataSeriesSink::verifyTail(tail,need_bitflip,filename);
    if (need_bitflip) {
	Extent::flip4bytes(tail+4);
	Extent::flip8bytes(tail+16);
    }
    int32 packedsize = *(int32 *)(tail + 4);
    off64_t indexoffset = *(int64 *)(tail + 16);
    AssertAlways(tailoffset - packedsize == indexoffset,
		 ("mismatch on index offset %lld - %d != %lld!\n",
		  tailoffset,packedsize,indexoffset));
    indexExtent = preadExtent(indexoffset);
    AssertAlways(indexExtent != NULL,("index extent read failed\n"));
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
    
    if (Extent::preadExtent(fd,offset, extentdata, need_bitflip) == false) {
	return NULL;
    }
    if (compressedSize) *compressedSize = extentdata.size();
    Extent *ret = new Extent(mylibrary,extentdata,need_bitflip);
    AssertAlways(ret->type != dataseries_type,
		 ("Invalid to have a type extent after the first extent!\n"));
    return ret;
}

DataSeriesSink::DataSeriesSink(const std::string &filename,
			       int _compression_modes,
			       int _compression_level)
    : extents(0),
      compress_none(0), compress_lzo(0), compress_gzip(0), compress_bz2(0),
      unpacked_size(0), unpacked_fixed(0), unpacked_variable(0), 
      packed_size(2*4+4*8), pack_time(0),
      index_series(global_dataseries_indextype),
      index_extent(index_series),
      field_extentOffset(index_series,"offset"),
      field_extentType(index_series,"extenttype"),
      wrote_library(false),
      compression_modes(_compression_modes),
      compression_level(_compression_level)
{
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
    const std::string filetype = "DSv1";
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
    Extent::ByteArray packed;
    doWriteExtent(&index_extent,packed);
    char *tail = new char[7*4];
    AssertAlways(((unsigned long)tail % 8) == 0,("malloc alignment glitch?!"));
    for(int i=0;i<4;i++) {
	tail[i] = 0xFF;
    }
    typedef ExtentType::int32 int32;
    *(int32 *)(tail + 4) = packed.size();
    *(int32 *)(tail + 8) = ~packed.size();
    *(int32 *)(tail + 12) = BobJenkinsHash(random() ^ time(NULL),packed.begin(),packed.size());
    *(ExtentType::int64 *)(tail + 16) = (ExtentType::int64)index_offset;
    *(int32 *)(tail + 24) = BobJenkinsHash(1776,tail,6*4);
    checkedWrite(tail,7*4);
    delete [] tail;
    cur_offset = 0;
}

void
DataSeriesSink::checkedWrite(const void *buf, int bufsize)
{
    ssize_t ret = write(fd,buf,bufsize);
    AssertAlways(ret != -1,
		 ("Error on write of %d bytes: %s\n",bufsize,
		  strerror(errno)));
    AssertAlways(ret == bufsize,
		 ("Partial write %d bytes out of %d bytes (disk full?): %s\n",
		  ret,bufsize,strerror(errno)));
}

double
DataSeriesSink::writeExtent(Extent *e, Extent::ByteArray &compressed)
{
    AssertAlways(wrote_library,
		 ("must write extent type library before writing extents!\n"));
    AssertAlways(e != NULL,("bad argument to writeExtent\n"));
    AssertAlways(valid_types[e->type],
		 ("type %s (%p) wasn't in your type library\n",
		  e->type->name.c_str(),e->type));
    return doWriteExtent(e,compressed);
}

void
DataSeriesSink::writeExtentLibrary(ExtentTypeLibrary &lib)
{
    ExtentSeries type_extent_series(global_dataseries_type);
    Extent type_extent(type_extent_series);

    Variable32Field typevar(type_extent_series,"xmltype");
    for(std::map<const std::string,ExtentType *>::iterator i = lib.name_to_type.begin();
	i != lib.name_to_type.end();++i) {
	ExtentType *et = i->second;
	if (et->name != "DataSeries: XmlType") {
	    type_extent_series.newRecord();
	    AssertAlways(et->xmldesc.size() > 0,
			 ("whoa extenttype has no xml data?!\n"));
	    typevar.set(et->xmldesc.data(),et->xmldesc.size());
	    valid_types[et] = true;
	}
    }
    Extent::ByteArray foo;
    doWriteExtent(&type_extent,foo);
    wrote_library = true;
}

void
DataSeriesSink::verifyTail(ExtentType::byte *tail,
			   bool need_bitflip,
			   const std::string &filename)
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

double
DataSeriesSink::doWriteExtent(Extent *e, Extent::ByteArray &data)
{
    index_series.newRecord();
    field_extentOffset.set(cur_offset);
    field_extentType.set(e->type->name);
    AssertAlways(cur_offset > 0,("Error: doWriteExtent on closed file\n"));
    ++extents;
    struct rusage pack_start;
    AssertAlways(getrusage(RUSAGE_SELF,&pack_start)==0,
		 ("?!"));
    int headersize, fixedsize, variablesize;
    e->packData(data,compression_modes,compression_level,&headersize,&fixedsize,&variablesize);
    struct rusage pack_end;
    AssertAlways(getrusage(RUSAGE_SELF,&pack_end)==0,
		 ("?!"));
    checkedWrite(data.begin(),data.size());
    cur_offset += data.size();
    double pack_extent_time = (pack_end.ru_utime.tv_sec - pack_start.ru_utime.tv_sec) + (pack_end.ru_utime.tv_usec - pack_start.ru_utime.tv_usec)/1000000.0;
    unpacked_size += headersize + fixedsize + variablesize;
    unpacked_fixed += fixedsize;
    unpacked_variable += variablesize;
    packed_size += data.size();
    pack_time += pack_extent_time;
    if (data[6*4] == 0) {
	++compress_none;
    } else if (data[6*4] == 1) {
	++compress_lzo;
    } else if (data[6*4] == 2) {
	++compress_gzip;
    } else if (data[6*4] == 3) {
	++compress_bz2;
    } else if (data[6*4] == 4) {
	++compress_lzf;
    } else {
	AssertFatal(("whoa, unknown compress option %d\n",data[6*4]));
    }
    if (*(ExtentType::int32 *)(data.begin()+4) > 0) {
	if (data[6*4+1] == 0) {
	    ++compress_none;
	} else if (data[6*4+1] == 1) {
	    ++compress_lzo;
	} else if (data[6*4+1] == 2) {
	    ++compress_gzip;
	} else if (data[6*4+1] == 3) {
	    ++compress_bz2;
	} else if (data[6*4+1] == 4) {
	    ++compress_lzf;
	} else {
	    AssertFatal(("whoa, unknown compress option %d\n",data[6*4+1]));
	}
    }
    return pack_extent_time;
}


