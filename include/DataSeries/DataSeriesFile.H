/* -*-C++-*-
*******************************************************************************
*
* File:         DataSeriesFile.H
* RCS:          $Header: /mount/cello/cvs/DataSeries/include/DataSeries/DataSeriesFile.H,v 1.4 2004/09/07 21:29:28 anderse Exp $
* Description:  a dataseries file
* Author:       Eric Anderson
* Created:      Tue May 20 11:11:51 2003
* Modified:     Tue Jul  6 16:10:36 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/
#ifndef __DATASERIES_FILE_H
#define __DATASERIES_FILE_H

#include <Extent.H>
#include <ExtentField.H>
// ExtentField isn't used in this header, but makes using the library
// simpler as just including this file gets everything you need.

class DataSeriesSource {
public:
    DataSeriesSource(const std::string &filename);
    ~DataSeriesSource();
    // returns NULL at EOF
    Extent *readExtent() { return preadExtent(cur_offset); }
    // both of the pread* routines update offset to point to the start of the
    // next compressed extent on disk.
    Extent *preadExtent(off64_t &offset, unsigned *compressedSize = NULL);
    // false on EOF
    bool preadCompressed(off64_t &offset, Extent::ByteArray &bytes) {
	return Extent::preadExtent(fd,offset, bytes, need_bitflip);
    }
    bool isactive() { return fd >= 0; }
    void closefile(); // release the file descriptor
    void reopenfile(); // leaves offset unchanged; completely messy if the underlying file is being changed

    ExtentTypeLibrary mylibrary;
    Extent *indexExtent; 
    const bool needBitflip() { return need_bitflip; }
    const ExtentType *dataseriesType() { return dataseries_type; }
private:
    const std::string filename;
    typedef ExtentType::byte byte;
    ExtentType *dataseries_type;
    int fd;
    off64_t cur_offset;
    bool need_bitflip;
};

class DataSeriesSink {
public:
    // if filename is "-", will write to stdout...
    DataSeriesSink(const std::string &filename,
		   int compression_modes = Extent::compress_all,
		   int compression_level = 9);
    ~DataSeriesSink(); // automatically calls close() if needed.
    void close();
    void writeExtentLibrary(ExtentTypeLibrary &lib);
    void writeExtent(Extent *e) {
	Extent::ByteArray foo;
	writeExtent(e,foo);
    }
    // returns the time to pack the extent in seconds, and the
    // compressed extent is put into compressed
    double writeExtent(Extent *e, Extent::ByteArray &compressed);
    const off64_t cur_position() {
	return cur_offset;
    }

    // sundry statistics on this sink; expect to get one
    // non-compressed chunk (the fixed record pointers for the extent
    // type information); also, empty variable-sized extents are not
    // counted in the compress_* stats
    int extents;
    int compress_none, compress_lzo, compress_gzip, compress_bz2, compress_lzf;
    long long unpacked_size, unpacked_fixed, unpacked_variable, packed_size;
    double pack_time;

    static void verifyTail(ExtentType::byte *data, bool need_bitflip,
			   const std::string &filename);
private:
    off64_t cur_offset; // set to -1 when sink is closed
    ExtentSeries index_series;
    Extent index_extent;
    Int64Field field_extentOffset;
    Variable32Field field_extentType;
    bool wrote_library;
    double doWriteExtent(Extent *e, Extent::ByteArray &compressed);
    void checkedWrite(const void *buf, int bufsize);
    void writeExtentType(ExtentType &et);
    std::map<const ExtentType *,bool> valid_types;
    int fd;
    const int compression_modes;
    const int compression_level;
};
    
#endif
