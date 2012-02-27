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
#include <sys/time.h>

#include <ostream>

#include <boost/static_assert.hpp>

#include <Lintel/Double.hpp>
#include <Lintel/HashTable.hpp>
#include <Lintel/LintelLog.hpp>

#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/ExtentField.hpp>

using namespace std;
using boost::format;

#if (_FILE_OFFSET_BITS == 64 && !defined(_LARGEFILE64_SOURCE)) || defined(__CYGWIN__)
#define _LARGEFILE64_SOURCE
#define lseek64 lseek
#endif

#ifndef _LARGEFILE64_SOURCE
#error "Must compile with -D_LARGEFILE64_SOURCE"
#endif

#ifdef __CYGWIN__
#define O_LARGEFILE 0
#endif

DataSeriesSource::DataSeriesSource(const string &filename, bool read_index, bool check_tail)
    : index_extent(), filename(filename), fd(-1), cur_offset(0), read_index(read_index),
      check_tail(check_tail)
{
    mylibrary.registerType(ExtentType::getDataSeriesXMLType());
    mylibrary.registerType(ExtentType::getDataSeriesIndexTypeV0());
    SINVARIANT(mylibrary.getTypeByName("DataSeries: XmlType") 
	      == &ExtentType::getDataSeriesXMLType());
    reopenfile();
}

DataSeriesSource::~DataSeriesSource() {
    if (isactive()) {
	closefile();
    }
}

void DataSeriesSource::closefile() {
    CHECKED(close(fd) == 0,
	    boost::format("close failed: %s") % strerror(errno));
    fd = -1;
}

void DataSeriesSource::reopenfile() {
    INVARIANT(fd == -1, "trying to reopen non-closed source?!");
    fd = open(filename.c_str(), O_RDONLY | O_LARGEFILE);
    INVARIANT(fd >= 0,boost::format("error opening file '%s' for read: %s")
	      % filename % strerror(errno));

    checkHeader();
    readTypeExtent();
    readTailIndex();
}

void DataSeriesSource::checkHeader() {
    cur_offset = 0;
    Extent::ByteArray data;
    const int file_header_size = 2*4 + 4*8;
    data.resize(file_header_size);
    Extent::checkedPread(fd,0,data.begin(),file_header_size);
    cur_offset = file_header_size;
    INVARIANT(data[0] == 'D' && data[1] == 'S' &&
	      data[2] == 'v' && data[3] == '1',
	      "Invalid data series source, not DSv1");
    int32_t check_int = *(int32_t *)(data.begin() + 4);
    if (check_int == 0x12345678) {
	need_bitflip = false;
    } else if (check_int == 0x78563412) {
	need_bitflip = true;
    } else {
	FATAL_ERROR(boost::format("Unable to interpret check integer %x")
		    % check_int);
    }
    if (need_bitflip) {
	Extent::flip4bytes(data.begin()+4);
	Extent::flip8bytes(data.begin()+8);
	Extent::flip8bytes(data.begin()+16);
	Extent::flip8bytes(data.begin()+24);
	Extent::flip8bytes(data.begin()+32);
    }
    INVARIANT(*(int32_t *)(data.begin() + 4) == 0x12345678,
	      "int32 check failed");
    INVARIANT(*(int64_t *)(data.begin() + 8) == 0x123456789ABCDEF0LL,
	      "int64 check failed");
    INVARIANT(fabs(3.1415926535897932384 - *(double *)(data.begin() + 16)) 
	      < 1e-18, "fixed double check failed");
    INVARIANT(*(double *)(data.begin() + 24) == Double::Inf,
	      "infinity double check failed");
    INVARIANT(*(double *)(data.begin() + 32) != *(double *)(data.begin() + 32),
	      "NaN double check failed");
}

void DataSeriesSource::readTypeExtent() {
    Extent::ByteArray extentdata;
    INVARIANT(Extent::preadExtent(fd,cur_offset,extentdata,need_bitflip),
	      "Invalid file, must have a first extent");
    Extent *e = new Extent(mylibrary,extentdata,need_bitflip);
    INVARIANT(&e->type == &ExtentType::getDataSeriesXMLType(),
	      "First extent must be the type defining extent");

    ExtentSeries type_extent_series(e);
    Variable32Field typevar(type_extent_series,"xmltype");
    for(;type_extent_series.morerecords(); ++type_extent_series) {
	string v = typevar.stringval();
	mylibrary.registerTypeR(v);
    }
    delete e;
}

void DataSeriesSource::readTailIndex() {
    if (read_index) {
	check_tail = true;
    }

    off64_t indexoffset = -1;
    if (check_tail) {
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
	int32_t packedsize = *(int32_t *)(tail + 4);
	indexoffset = *(int64_t *)(tail + 16);
	INVARIANT(tailoffset - packedsize == indexoffset,
		boost::format("mismatch on index offset %d - %d != %d!")
		% tailoffset % packedsize % indexoffset);
    }
    index_extent.reset();
    if (read_index) {
	index_extent.reset(preadExtent(indexoffset));
	INVARIANT(index_extent != NULL, "index extent read failed");
    }
}    

Extent *DataSeriesSource::preadExtent(off64_t &offset, unsigned *compressedSize) {
    Extent::ByteArray extentdata;
    
    off64_t save_offset = offset;
    if (Extent::preadExtent(fd, offset, extentdata, need_bitflip) == false) {
	return NULL;
    }
    if (compressedSize) *compressedSize = extentdata.size();
    Extent *ret = new Extent(mylibrary,extentdata,need_bitflip);
    ret->extent_source = filename;
    ret->extent_source_offset = save_offset;
    INVARIANT(&ret->type != &ExtentType::getDataSeriesXMLType(),
	      "Invalid to have a type extent after the first extent.");
    return ret;
}

