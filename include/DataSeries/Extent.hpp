
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

#ifndef DATASERIES_EXTENT_H
#define DATASERIES_EXTENT_H

extern "C" {
    char *dataseriesVersion();
}

#include <unistd.h>
#include <inttypes.h>
#include <cstring>

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

#include <DataSeries/ExtentType.hpp>

/// \cond INTERNAL_ONLY
// This doesn't really belong here, but it's not clear what a better
// place is.
#ifdef __CYGWIN__
    typedef _off64_t off64_t;
#endif
/// \endcond

class ExtentSeries;

/** \brief Stores a sequence of records having the same type.
  *
  * Each @c Extent has two components
  *   - A buffer containing fixed size records.  The size of
  *     each individual record can be determined from the ExtentType,
  *     allowing random access.  Variable32 fields are represented
  *     by a 4 byte index into the string pool.
  *   - A string pool for variable32 fields. */
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
        void clear(); // frees allocated memory
        void reserve(size_t reserve_bytes);
        bool empty() { return endV == beginV; }
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

    /// \cond INTERNAL_ONLY
    const ExtentType &type;
    /// \endcond

    /** Returns the type of the Extent. */
    const ExtentType &getType() const {
        return type;
    }

    /** This constructor creates an @c Extent from raw bytes in
        the external format created by packData.  It will
        Extract the name of the appropriate @c ExtentType from
        the input bytes and look it up in the given @c ExtentTypeLibrary.
        If needs_bitflip is true, it indicates that the endianness
        of the host processor is opposite the endianness of the
        input data. */
    Extent(ExtentTypeLibrary &library, 
           Extent::ByteArray &packeddata, 
           const bool need_bitflip);
    /** Similar to the above constructor, except that
        the ExtentType is passed explicitly. */
    Extent(const ExtentType &type,
           Extent::ByteArray &packeddata, 
           const bool need_bitflip);
    /** Creates an empty @c Extent. */
    Extent(const ExtentType &type);
    /** Creates an empty @c Extent. */
    Extent(const std::string &xmltype); 
    
    /** Creates a new empty Extent. If series.extent() == NULL, the Extent
        creation will set the series' extent to the new extent. */
    Extent(ExtentSeries &myseries);

    /** Swap the contents of two &c Extent. 

        Preconditions:
          - Both Extents must have the same type. 

        Provided the preconditions are met, this
        function does not throw. */
    void swap(Extent &with);

    /** Clears the contents of the Extent.  Note that
        there is no way to clear the type. */
    void clear() {
        fixeddata.clear();
        variabledata.clear();
        init();
    }
    /** Returns the total size of the @c Extent in bytes. */
    size_t size() {
        return fixeddata.size() + variabledata.size();
    }
    
    /** Equivalent to size() 
        \deprecated */
    unsigned int extentsize() { // TODO deprecate this function
        return size();
    }

    /// \cond INTERNAL_ONLY
    // The following are defined by the file format and can't be changed.
    static const Extent::byte compress_mode_none = 0;
    static const Extent::byte compress_mode_lzo = 1;
    static const Extent::byte compress_mode_zlib = 2;
    static const Extent::byte compress_mode_bz2 = 3;
    static const Extent::byte compress_mode_lzf = 4;
    /// \endcond

    /** \defgroup Extent_compress Extent::compress
        These constants are used to indicate which compression
        algorithms will be tried when converting an Extent
        to its external representation.  Multiple flags can
        be |'ed together and the algorithm that
        yields the best compression will be chosen.  Which
        algorithms are available depends on which libraries
        are found when DataSeries is compiled.  If an algorithm
        which is not supported is requested, it will be ignored.
        @{ */

    /** Do not attempt any compression */
    static const int compress_none = 0;
    /** Try the lzo algorithm. */
    static const int compress_lzo = 1;
    /** Use zlib */
    static const int compress_zlib = 2;
    /** technically gz and zlib are different -- gz has a header, but
        everyone knows it as gz and it's the same compression algorithm */
    static const int compress_gz = 2;
    /** Use bzip2 */
    static const int compress_bz2 = 4;
    /** Use lzf (lzf is included in the DataSeries distribution) */
    static const int compress_lzf = 8;
    /** Try all available compression schemes */
    static const int compress_all = compress_bz2 | compress_zlib | compress_lzo | compress_lzf;
    /// @}

    /** \brief converts an Extent to the external representation which is stored
            in files.

        \arg into Output buffer to write the result to.

        \arg compression_modes Specifies which compression algorithms will be
            tried.  The one that yields the smallest result will be selected.

        \arg compression_level must be between 1 and 9 inclusive.  In general larger
             values give better compression but require more time/space.  See the
             documentation of the individual compression schemes for details.
             
        \arg header_packed If header_packed is not null, *header_packed will recieve
            the pre-compression size of the Extent header.
        \arg fixed_packed If fixed_packed is not null, *fixed_packed will recieve
            the pre-compression size in bytes of the fixed size records.
        \arg variable_packed If variable_packed is not null, *variable_packed
            will recieve the pre-compression size of the string pool.
    
        \return a "checksum" calculated from the underlying checksums in the packed extent */
    uint32_t packData(Extent::ByteArray &into, 
                      int compression_modes = compress_all,
                      int compression_level = 9,
                      int *header_packed = NULL, 
                      int *fixed_packed = NULL, 
                      int *variable_packed = NULL); 

    /** Loads an Extent from the external representation.

        \arg need_bitflip Indicates whether the endianness of
            from is the same as the that of the host machine.

        Preconditions:
          - The type of the data must be the type of this Extent.

        Note you can't unpack the same data twice, it may modify the
        input data */
    void unpackData(Extent::ByteArray &from, bool need_bitflip);

    /** Returns the total size in bytes that an Extent created
        using @param from will need.  (This will be the result of size() after
        unpacking.)

	\param from The byte array to use as input
	\param need_bitflip Do we need to flip the byte order when unpacking?
	\param type What type does the raw data represent?
        
        Preconditions:
          - from must be in the external representation of Extents. 
    */
    static uint32_t unpackedSize(Extent::ByteArray &from, bool need_bitflip,
                                 const ExtentType &type);
    
    /** Returns the name of the type of the Extent stored in @param from
	
        \param from What byte array should we get the type for?
        
        Preconditions:
          - from must be in the external representation of Extents. */
    static const std::string getPackedExtentType(Extent::ByteArray &from);

    /// \cond INTERNAL_ONLY
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
    /// \endcond
        
    // returns true if it successfully read the extent; returns false 
    // on eof (with into.size() == 0); aborts otherwise
    // updates offset to the end of the extent
    static bool preadExtent(int fd, off64_t &offset, Extent::ByteArray &into, bool need_bitflip);

    // returns true if it read amount bytes, returns false if it read
    // 0 bytes and eof_ok; aborts otherwise
    static bool checkedPread(int fd, off64_t offset, byte *into, int amount, 
                            bool eof_ok = false);

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

    /// \cond INTERNAL_ONLY
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
    // you are responsible for deleting the return buffer
    static Extent::ByteArray *compressBytes(byte *input, int32 input_size,
                                            int compression_modes,
                                            int compression_level, byte *mode);
    static int32 uncompressBytes(byte *into, byte *from,
                                byte compression_mode, int32 intosize,
                                int32 fromsize);

    const ExtentType &packedType(ExtentTypeLibrary &library,
                                 Extent::ByteArray &packeddata,
                                 const bool need_bitflip);

    void compactNulls(Extent::ByteArray &fixed_coded);
    void uncompactNulls(Extent::ByteArray &fixed_coded, int32_t &size);
    friend class ExtentSeries;
    void createRecords(unsigned int nrecords); // will leave iterator pointing at the current record
    void init();
    /// \endcond
};
    
#endif
