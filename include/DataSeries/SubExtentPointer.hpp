// -*-C++-*-
/*
   (c) Copyright 2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file

    Pointers to sub-parts of an extent.  There are several choices of how to create these pointer:

      1a) A void * pointer directly to the fixed extent location of a field
      1b) An offset from the extent start in bytes directly to the fixed extent location of a field
      1c) A void * pointer directly to the variable32 data for a field
      1d) An offset from the variable32 part of an extent to the variable32 data

      2a) A void * pointer to the beginning of a row
      2b) An offset in bytes to the beginning of the fixed part of a row

      3) A row number

      4) An STL iterator as an extent pointer + 1[ab],2* + field offset, or 3 + field offset.

    There are several goals that we have for using these sub-extent pointers:
      A) fast direct access to a single field
      B) space-efficient storage of pointers (including for multiple fields in a row)
      C) efficient binary search in a sorted extent
      D) ability to update existing fields.
      E) work with nullable fields.

    1a, 1c are best for goal A, but are poor for goal B, and do not support goal C without a divide
    to compute the number of rows to start. 1b, 1d are better for goal B on 64 bit machines because
    the offsets can be half the size of pointers.  1c and 1d fail to support goal C at all, and
    they fail to achieve goal D (except for the weird case where the update is the same size or
    smaller than the existing things and pack_unique is off).  To support goal E, 1a and 1b will
    require an extra subtract (to calculate the relative offset between the nullable flag and the
    value; 1c/1d can't support it.

    2a and 2b are much better than 1* for goal B with multiple fields in a row, and 2b is twice as
    good as 2a for goal B on 64 bit machines.  2a and 2b as slightly worse than 1* for goal A
    unless the field offset is known at compile time on x86 (which happens to support base + offset
    + compiled-in-constant as "free").  2* can support goal C with a divide needed to calculate the
    number of rows.  These options work fine for D, E.

    3 is worse than 1*,2* for goal A because it requires a multiply to access the fields.  There is
    a potential for a slightly better binary search since you can create a type-3 pointer just by
    specifying a row number, and you can calculate the difference between pointers with just a
    subtract rather than a subtract + divide.  By adding a extent type and performing a multiply,
    the type 2* pointers can be created, and differences can be mostly avoided so option (3) is
    unlikely to be of benefit in comparison with the other choices.

    4 allows all of the standard STL algorithms to be used with dataseries iterators, so is
    probably worth creating to enable the use of those algorithms although it is significantly
    worse on goal B because it has to store multiple things.  

    For all the types of pointers, we can include the extent they were for in debug mode to 
    enable cross-checking to make sure that something weird hasn't happened.  

    The assumption that 32 bits is a sufficient offset isn't completely guaranteed, but in practice
    is guaranteed.  Extents are effectively limited to 2^32 bytes. the variable part explicitly by
    the 4 bytes in the extent header, and the the fixed part because the number of records is
    stored in 4 bytes.  Both must compress sufficiently into < 2^32 bytes or otherwise the data
    can't be stored.  In addition, based on the performance testing we've done you want to keep
    extents either < L2 cache size, for fast access or moderate sized (1-10MB) for parallelism and
    better compression.  Finally as of 2011, there are probably bugs with extents larger than 2^31
    simply because the system is not tested or used that way.

    For the first implementation, 2b is probably the best choice it's about as fast as the other
    options, much more space efficient for multiple fields, and halves cache pressure on 64 bit
    machines.  With one divide it can support binary search (provided we add an increment by
    constant function).  We're unlikely to support compiled in field offsets in the first version.

    In the future additional options can be implemented in order to compare the speed; the most
    likely for the next option is to implement the iterator even though it is slower and less space
    efficient because it enables the use of the STL algorithms.  After that the direct pointer with
    the offset and the special case of that for the variable32 entry since that eliminates an
    indirection in the latter case and an add in the former for the likely common case where there
    is only a single field for the random access.

    Note that part of the point of the random access iterators as opposed to the getPos/setPos
    operations on a series is that the random access iterators don't require a series to be updated
    in order to extract values from a field.  The upside is that the performance should be faster.
    The downside will be that they are inherently less safe.

    It may also be useful to support a ExtentSeries.getSEP_type(int32_t offset) that will calculate
    a sub extent pointer using an offset relative to the current location, and possibly an
    operator[] on fields that will use that same type of functionality to go to a random row in
    the extent.
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

