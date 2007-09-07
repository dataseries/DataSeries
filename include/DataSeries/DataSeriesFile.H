// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    a dataseries file
*/

#ifndef __DATASERIES_FILE_H
#define __DATASERIES_FILE_H

#include <Lintel/PThread.H>

#include <DataSeries/Extent.H>
#include <DataSeries/ExtentField.H>
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
    // TODO: is the following ("-" == stdout) a good idea?  I don't
    // think anything uses it.

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
    struct Stats {
	// Single global mutex for stats right now, should be sufficient
	// given they are pretty infrequently accessed.
	static PThreadMutex &getMutex();

	uint32_t extents;
	uint32_t compress_none, compress_lzo, compress_gzip, 
	    compress_bz2, compress_lzf;
	uint64_t unpacked_size, unpacked_fixed, unpacked_variable, 
	    unpacked_variable_raw, packed_size;
	double pack_time; // This statistic appears to be generally
			  // wrong right now because of threading.

	Stats() {
	    reset();
	}
	void reset();

	Stats & operator+=(const Stats &from);
	Stats & operator-=(const Stats &from);
	// all just data so use default copy constructor

	/// specify an extent type if this statistic was entirely for one
	/// type.
	void printText(std::ostream &to, const std::string &extent_type = "");
    };

    // You are only guarenteeed to have all the statistics for writing
    // this file after you call close().
    Stats getStats() {
	// Make a copy so it's thread safe.
	PThreadAutoLocker lock(Stats::getMutex());
	Stats ret = stats; // with the auto-locker, do we have a
			   // guarentee that the copy would be made
			   // before we return?
	return ret;
    }
    static void verifyTail(ExtentType::byte *data, bool need_bitflip,
			   const std::string &filename);
private:
    Stats stats;

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
    uint32_t chained_checksum; 
};

inline DataSeriesSink::Stats 
operator-(const DataSeriesSink::Stats &a, const DataSeriesSink::Stats &b) {
    DataSeriesSink::Stats ret = a;
    ret -= b;
    return ret;
}
    
#endif
