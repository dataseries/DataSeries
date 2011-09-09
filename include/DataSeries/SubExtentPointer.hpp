// -*-C++-*-
/*
   (c) Copyright 2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file

    Pointers to sub-parts of an extent.  There are two natural types of pointers, 1) those that
    point directly to an individual field 2) those that point to the beginning of a row. There are
    two natural implementations of the pointers A) as a void *, and B) as an offset relative to the
    start of the extent.  Note that for variable32 fields, you could have the direct pointer to be
    either to the fixed part, requiring the extent to translate, and allowing the variable32 value
    to be updated, or directly to the location in the variable part disallowing updates (the value
    would move) but removing an indirection. You can't have a variable32 pointing to the beginning
    of the row in the variable part since that makes no sense.

    For both types of pointers we could also include the extent they were relative to for
    cross-checking in debug mode to make sure that something weird hasn't happened.  There are
    performance and space tradeoffs among the types of pointers.  The direct pointers should be
    faster, but have the downside that for multiple pointers in a single row that multiple pointers
    would need to be stored.  

    The two implementations are again a space-time tradeoff; the void * pointer will be larger on
    64 bit machines than a 32 bit offset, but the offset requires an addition to use and the
    pointer does not.  The offset can be 32 bit because extents are effectively limited to 2^32
    bytes. the variable part explicitly by the 4 bytes in the extent header, and the the fixed part
    because the number of records is stored in 4 bytes.  Both must compress sufficiently into 4
    bytes or otherwise the data can't be stored.  In addition, based on the performance testing
    we've done you want to keep extents either < L2 cache size, for fast access or moderate sized
    (1-10MB) for parallelism and better compression.  Finally as of 2011, there are probably bugs
    with extents larger than 2^31 simply because the system is not tested or used that way.

    Since there are 5 separate options on what type of sub-extent pointer to support, we have to
    make some choices to simplify the implementation.  The pointers to the beginning of the row
    have a significant space advantage if we need pointers to multiple fields, and the cost is an
    additional highly cacheable add.  Using offset pointers halves the space on 64 bit machines,
    which are becoming the common case today reducing cache pressure at the cost of an additional
    add per operation.  Not quite as clearly a substantial win, but probably again the right choice
    for the first implementation.

    In the future additional options can be implemented in order to compare the speed; the most
    likely for the next option is to implement the direct pointer with the offset and the special
    case of that for the variable32 entry since that eliminates an indirection in the latter
    case and an add in the former for the likely common case where there is only a single field
    for the random access.

    Note that part of the point of the random access iterators as opposed to the getPos/setPos
    operations on a series is that the random access iterators don't require a series to be updated
    in order to extract values from a field.  The upside is that the performance should be faster.
    The downside will be that they are inherently less safe.
*/
/*
  TODO-eric-review:

  I prefer #2, as an offset in bytes.

  I don't think that you can do binary search given the two types of pointers proposed, without
  incurring an integer division.  There is another pointer type, row_number; row_number ==
  row_offset/row_size.  This makes binary search easy, but requires a multiply; multiply is much
  much better than divide, though.  With either pointer style #1 or #2, you can "fake" binary
  search by ensuring that the difference between your L and R pointers are always a power of 2;
  this requires special casing soem of it.
  
  As for type #1 vs. type #2, x86 natively supports base + offset + (compiled in constant).  As a
  Field or Series is implemented now, the base+offset is already being used.  However, if you
  knew the layout of a row at compile time, then offset would become free again, making #2
  cheaper.
  
  As we discussed Thursday evening, it seems unlikely that you can make a random-access iterator
  which matches the STL spec without having at least one full pointer (either to the extent base,
  or to the row); I think that a true STL random access iterator is less useful than the more
  direct ones.

  Besides thinkin of pointers, just some form of random access which only requires a single integer
  could be useful.  Maybe instantiate the [] operator on Fields, e.g.
  
  int32_t & operator[] (int);
  
  for Int32Field.  For bounds check (if wanted), extracting the nrecords field from the extent is
  nearly free.
    
*/

#ifndef DATASERIES_SUBEXTENTPOINTER_HPP
#define DATASERIES_SUBEXTENTPOINTER_HPP

#include <Lintel/AssertBoost.hpp>

#include <DataSeries/SEP_RowOffset.hpp>

namespace dataseries {
    /// For performance and space the default random access pointers do not contain the extent they
    /// are derived from.  However, for correctness the extent should be the same.  Therefore a
    /// debug compile will include the extent pointer in the SEP structures.  That change makes
    /// them a different size.  This call allows you to verify that the library sizes and the
    /// caller sizes are the same, i.e. that both libraries were compiled with
    /// -DDEBUG/-DLINTEL_DEBUG=1 or without.
    bool checkSubExtentPointerSizes(size_t sep_rowoffset = sizeof(dataseries::SEP_RowOffset));

    /// like checkSubExtentPointerSizes, but just invariants out on failure.
    inline void verifySubExtentPointerSizes() {
        INVARIANT(checkSubExtentPointerSizes(), 
                  "mismatch between -DDEBUG flags for compiling library and application");
    }
}

#endif

