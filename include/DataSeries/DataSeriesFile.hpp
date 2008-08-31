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

#include <Lintel/Deque.hpp>
#include <Lintel/PThread.hpp>

#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>
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

    ExtentTypeLibrary &getLibrary() { return mylibrary; }

    ExtentTypeLibrary mylibrary;
    Extent *indexExtent; 
    bool needBitflip() { return need_bitflip; }
private:
    const std::string filename;
    typedef ExtentType::byte byte;
    int fd;
    off64_t cur_offset;
    bool need_bitflip;
};

class DataSeriesSink {
public:
    // sundry statistics on this sink; expect to get one
    // non-compressed chunk (the fixed record pointers for the extent
    // type information); also, empty variable-sized extents are not
    // counted in the compress_* stats
    class Stats {
    public:
	// Single global mutex for stats right now, should be sufficient
	// given they are pretty infrequently accessed.
	static PThreadMutex &getMutex();

	Stats(const Stats &from) {
	    *this = from;
	}
	Stats & operator=(const Stats &from);
	// If anything other than plain old data gets put in here, 
	// then the assignment operator needs to be fixed.
	uint32_t extents;
	uint32_t compress_none, compress_lzo, compress_gzip, 
	    compress_bz2, compress_lzf;
	uint64_t unpacked_size, unpacked_fixed, unpacked_variable, 
	    unpacked_variable_raw, packed_size;

	// The pack_time statistic has been disabled pending figuring
	// out how to make it accurate.  For mor discussion, see
	// DataSeriesFile.C:get_thread_cputime()
	double pack_time; 

	Stats() {
	    reset();
	}
	~Stats();
	void reset();
	Stats & operator+=(const Stats &from);
	Stats & operator-=(const Stats &from);
	// all just data so use default copy constructor

	/// specify an extent type if this statistic was entirely for one
	/// type.
	void printText(std::ostream &to, const std::string &extent_type = "");

    private:
	uint32_t use_count; // how many updates are pending to this stats?
	// You should grab the lock before calling this
	void update(uint32_t unpacked_size, uint32_t unpacked_fixed,
		    uint32_t unpacked_variable_raw,
		    uint32_t unpacked_variable, uint32_t packed_size,
		    uint32_t packed_variable_size, double pack_time, 
		    unsigned char fixed_compress_mode,
		    unsigned char variable_compress_mode);
	void updateCompressMode(unsigned char compress_mode);
	friend class DataSeriesSink;
    };

    // TODO: is the following ("-" == stdout) a good idea?  I don't
    // think anything uses it.

    // if filename is "-", will write to stdout...
    DataSeriesSink(const std::string &filename,
		   int compression_modes = Extent::compress_all,
		   int compression_level = 9);
    ~DataSeriesSink(); // automatically calls close() if needed.
    void close();

    // must be called once before calling writeExtent.
    void writeExtentLibrary(ExtentTypeLibrary &lib);

    // set toUpdate to NULL if you have nothing to update.
    // writeExtent is destructive, e will be empty on return.
    // writeExtent is thread safe, although obviously the precise
    // order of writing the extents can't be guarenteed if two
    // writeExtent calls are executing at the same time.
    void writeExtent(Extent &e, Stats *toUpdate);

    // if another thread is writing extents at the same time, this could
    // wait forever.
    void flushPending();

    // You are only guaranteed to have all the statistics for writing
    // this file after you call close().
    Stats getStats();

    // If you are going to delete a Stats that you previously used to
    // write to a DataSeriesFile before you close the file, you need
    // to call this.
    void removeStatsUpdate(Stats *would_update);

    // count == -1 ==> use Lintel/PThreadMisc::getNCpus()
    // count == 0 ==> no separate thread for compressing, work done during
    // call to writeExtent()
    static void setDefaultCompressorCount(int count = -1);
    static void verifyTail(ExtentType::byte *data, bool need_bitflip,
			   const std::string &filename);
    
    /// compressor_count == -1 ==> use # cpus. 0 ==> no threading;
    // needs to be called before you create a sink to have any effect.
    static void setCompressorCount(int compressor_count = -1);

    const std::string &getFilename() const {
	return filename;
    }

    void setMaxBytesInProgress(size_t nbytes);
private:
    struct toCompress {
	Extent extent;
	Stats *to_update;
	bool in_progress;
	uint32_t checksum;
	Extent::ByteArray compressed;
	toCompress(Extent &e, Stats *_to_update)
	    : extent(e.type), to_update(_to_update), 
	      in_progress(false), checksum(0) 
	{
	    extent.swap(e);
	}
	void wipeExtent() {
	    Extent tmp(extent.type);
	    extent.swap(tmp);
	}
	bool readyToWrite() {
	    return compressed.size() > 0 && !in_progress;
	}
    };
    Stats stats;

    off64_t cur_offset; // set to -1 when sink is closed
    ExtentSeries index_series;
    Extent index_extent;
    Int64Field field_extentOffset;
    Variable32Field field_extentType;
    bool wrote_library;
    // returns the size of the compressed extent, needed for writing the
    // tail of the dataseries file.
    void checkedWrite(const void *buf, int bufsize);
    void writeExtentType(ExtentType &et);
    std::map<const ExtentType *,bool> valid_types;
    int fd;
    const int compression_modes;
    const int compression_level;
    uint32_t chained_checksum; 

    bool canQueueWork() {
	return bytes_in_progress < max_bytes_in_progress &&
	    pending_work.size() < 2 * compressors.size();
    }
    void queueWriteExtent(Extent &e, Stats *to_update);
    void lockedProcessToCompress(toCompress *work);
    void writeOutPending(bool have_lock = false); // always exits without lock held.
    static int compressor_count;
    std::vector<PThread *> compressors;
    PThread *writer;
    size_t bytes_in_progress, max_bytes_in_progress;
    bool shutdown_workers;
    Deque<toCompress *> pending_work;
    PThreadMutex mutex; // this mutex is ordered after Stats::getMutex(), so grab it second if you need both.
    PThreadCond available_queue_cond, available_work_cond, 
				   available_write_cond;

    const std::string filename;
    friend class DataSeriesSinkPThreadCompressor;
    void compressorThread();
    friend class DataSeriesSinkPThreadWriter;
    void writerThread();
};

inline DataSeriesSink::Stats 
operator-(const DataSeriesSink::Stats &a, const DataSeriesSink::Stats &b) {
    DataSeriesSink::Stats ret = a;
    ret -= b;
    return ret;
}

#endif
