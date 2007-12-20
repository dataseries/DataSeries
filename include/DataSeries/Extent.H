// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Variable sized chunks of data; containers for the individual
    records that are stored within, typed, and aligned to 8 byte
    boundaries
*/

#ifndef __DATASERIES_EXTENT_H
#define __DATASERIES_EXTENT_H

extern "C" {
    char *dataseriesVersion();
}

#include <unistd.h>
#include <inttypes.h>
#if defined(__linux__) && defined(__GNUC__) && __GNUC__ >= 2 
#  ifdef __i386__
#    if defined __i486__ || defined __pentium__ || defined __pentiumpro__ || defined __pentium4__ || defined __k6__ || defined __k8__ || defined __athlon__ 
       // ok, will get the good byteswap
#    else
       // Note that in particular __i386__ is not enough to have defined here
       // the swap routine is significantly worse than the i486 version; see
       // /usr/include/bits/byteswap.h.  See the test_byteflip part of test.C
       // for the performance test, it will fail unless you've selected the
       // best of the routines
#      warning "Automatically defining __i486__ on the assumption you have at least that type of CPU"
#      warning "Not doing this gets a much slower byte swap routine"
#      define __i486__ 1
#      ifdef COMPILE_OPTIMIZE
#        error ".../configure --enable-optmode=optimize should have set this up"
#      endif
#    endif
#  endif
#  include <byteswap.h>
#endif

#include <Lintel/LintelAssert.H>

#include <DataSeries/ExtentType.H>

class ExtentSeries;
class Extent {
public:
    typedef ExtentType::byte byte;
    typedef ExtentType::int32 int32;
    typedef ExtentType::uint32 uint32;
    typedef ExtentType::int64 int64;
    // G++-3 changed the way vectors work to remove the clear
    // similarity between them and an array; since they could be
    // implemented as a two level lookup (that's still constant time),
    // and using them to get to the byte array is now more difficult,
    // we implement our own to guarentee the assumption that we are
    // really representing a variable sized array of bytes, which is
    // what we want.  Later change to the C++ spec clarified that 
    // arrays have to be contiguous, so we could switch back at some point.
    class ByteArray { 
    public:
	ByteArray() { beginV = endV = maxV = NULL; }
	~ByteArray();
	const size_t size() const { return endV - beginV; }
	void resize(size_t newsize, bool zero_it = true) {
	    size_t oldsize = size();
	    if (newsize <= oldsize) {
		// shrink
		endV = beginV + newsize;
	    } else if (beginV + newsize <= maxV) {
		endV = beginV + newsize;
		if (zero_it) {
		    memset(beginV + oldsize,0,newsize - oldsize);
		}
	    } else { 
		copyResize(newsize, zero_it);
	    } 
	}
	void clear();
	void reserve(size_t reserve_bytes);
	byte *begin() { return beginV; };
	byte *begin(size_t offset) { return beginV + offset; }
	byte *end() { return endV; };
	byte &operator[] (size_t offset) { return *(begin(offset)); }
    
	void swap(ByteArray &with) {
	    swap(beginV,with.beginV);
	    swap(endV,with.endV);
	    swap(maxV,with.maxV);
	}

	typedef byte * iterator;

	// glibc prefers to use mmap to allocate large memory chunks; we 
	// allocate and de-allocate those fairly regularly; so we increase
	// the threshold.  This function is automatically called once when
	// we have to resize a bytearray, if you disagree with the defaults,
	// you can call this manually and set the options yourself.
	// if you want to look at the retaining space code again, version 
	// d5bb884b572b07590a8131710f01513577e24813, prior to 2007-10-25
	// will have a copy of the old code.
	static void initMallocTuning();
    private:
	void swap(byte * &a, byte * &b) {
	    byte *tmp = a;
	    a = b;
	    b = tmp;
	}

	void copyResize(size_t newsize, bool zero_it);
	byte *beginV, *endV, *maxV;
    };

    const ExtentType &type;

    Extent(ExtentTypeLibrary &library, 
	   Extent::ByteArray &packeddata, 
	   const bool need_bitflip);
    Extent(const ExtentType &type,
	   Extent::ByteArray &packeddata, 
	   const bool need_bitflip);
    Extent(const ExtentType &type);
    Extent(const std::string &xmltype); 
    // the above constructor will leak the extenttype it creates if
    // you init with a series, and series.extent() == NULL, the Extent
    // creation will set the extent to the new extent.
    Extent(ExtentSeries &myseries);

    void swap(Extent &with);

    void clear() {
	fixeddata.clear();
	variabledata.clear();
	init();
    }
    size_t size() {
	return fixeddata.size() + variabledata.size();
    }

    unsigned int extentsize() { // TODO deprecate this function
	return size();
    }

    // The following are defined by the file format and can't be changed.
    static const Extent::byte compress_mode_none = 0;
    static const Extent::byte compress_mode_lzo = 1;
    static const Extent::byte compress_mode_zlib = 2;
    static const Extent::byte compress_mode_bz2 = 3;
    static const Extent::byte compress_mode_lzf = 4;

    static const int compress_lzo = 1;
    static const int compress_zlib = 2;
    static const int compress_bz2 = 4;
    static const int compress_lzf = 8;
    static const int compress_all = compress_bz2 | compress_zlib | compress_lzo | compress_lzf;

    // returns a "checksum" calculated from the underlying checksums in the packed extent
    uint32_t packData(Extent::ByteArray &into, 
		      int compression_modes = compress_all,
		      int compression_level = 9,
		      // sizes of following are pre-compression (if any)
		      int *header_packed = NULL, 
		      int *fixed_packed = NULL, 
		      int *variable_packed = NULL); 

    // Note you can't unpack the same data twice, it may modify the
    // input data
    void unpackData(Extent::ByteArray &from, bool need_bitflip);
		    
    static const std::string getPackedExtentType(Extent::ByteArray &from);

    // the various pack routines return false if packing the data with a 
    // particular compression algorithm wouldn't gain anything over leaving
    // the data uncompressed; if into.size > 0, will only code up to that
    // size for the BZ2 and Zlib packing options (LZO doesn't allow this)
    static bool packBZ2(byte *input, int32 inputsize, 
			Extent::ByteArray &into, int compression_level);
    static bool packZLib(byte *input, int32 inputsize, 
			 Extent::ByteArray &into, int compression_level);
    static bool packLZO(byte *input, int32 inputsize, 
			Extent::ByteArray &into, int compression_level);
    static bool packLZF(byte *input, int32 inputsize,
			Extent::ByteArray &into, int compression_level);

    static inline uint32_t flip4bytes(uint32 v) {
#if defined(bswap_32)
	return bswap_32(v);
#else
	// fastest method on PIII & PA2.0-- see test.C for a bunch of variants
	// that we chose the best will be verified by test.C
	return ((v >> 24) & 0xFF) | ((v>>8) & 0xFF00) |
	    ((v & 0xFF00) << 8) | ((v & 0xFF) << 24);
#endif
    }
    static inline void flip4bytes(byte *data) {
	*(uint32_t *)data = flip4bytes(*(uint32_t *)data);
    }
    // 
    static inline void flip8bytes(byte *data) {
#ifdef USE_X86_GCC_BYTESWAP
	*(uint64_t *)data = bswap_64(*(uint64_t *)data);
#else
	uint32 a = *(uint32 *)data;
	uint32 b = *(uint32 *)(data + 4);
	*(uint32 *)(data + 4) = flip4bytes(a);
	*(uint32 *)data = flip4bytes(b);
#endif
    }
	
    // returns true if it successfully read the extent; returns false 
    // on eof (with into.size() == 0); aborts otherwise
    // updates offset to the end of the extent
    static bool preadExtent(int fd, off64_t &offset, Extent::ByteArray &into, bool need_bitflip);

    // returns true if it read amount bytes, returns false if it read
    // 0 bytes and eof_ok; aborts otherwise
    static bool checkedPread(int fd, off64_t offset, byte *into, int amount, 
			    bool eof_ok = false);

    // you are responsible for deleting the return buffer
    static Extent::ByteArray *packAny(byte *input, int32 input_size,
				 int compression_modes,
				 int compression_level, byte *mode);
    static void unpackAny(byte *into, byte *from,
			  byte compression_mode, int32 intosize,
			  int32 fromsize);

    // The verification checks are the dataseries internal checksums
    // that verify that the files were read correctly.  The
    // pre_uncompress check verifies the checksum over the compressed
    // data.  The post_uncompress check verifies the data after it has
    // been uncompressed, and after some of the transforms
    // (e.g. relative packing) have been applied.  The
    // unpack_variable32 check verifies that
    // Variable32Field::selfcheck() passes for all of the variable32
    // fields.

    // set the environment variable to one or more of:
    // DATASERIES_READ_CHECKS=preuncompress,postuncompress,variable32,all,none
    // This function is automatically called before unpacking the first extent
    // if it hasn't already been called.
    static void setReadChecksFromEnv(bool default_with_env_unset = false);

    // be smart before directly accessing these!  here because making
    // them private and using friend class ExtentSeries::iterator
    // didn't work.
    Extent::ByteArray fixeddata;
    Extent::ByteArray variabledata;

    // This function is here to verify that we got the right
    // flip4bytes when we compiled DataSeries, it's used by
    // DataSeries/src/test.C
    static void run_flip4bytes(uint32_t *buf, unsigned buflen);
private:
    const ExtentType &packedType(ExtentTypeLibrary &library,
				 Extent::ByteArray &packeddata,
				 const bool need_bitflip);

    bool compactIsNull(const byte *fixed_record, const ExtentType::fieldInfo *f);
    void compactNulls(Extent::ByteArray &fixed_coded);

    friend class ExtentSeries;
    void createRecords(unsigned int nrecords); // will leave iterator pointing at the current record
    void init();
};
    
#endif
