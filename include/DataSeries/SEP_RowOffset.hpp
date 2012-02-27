// -*-C++-*-
/*
   (c) Copyright 2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    See SubExtentPointer for documentation
*/
#ifndef DATASERIES_SEP_ROWOFFSET_HPP
#define DATASERIES_SEP_ROWOFFSET_HPP

#include <inttypes.h>

#include <Lintel/DebugFlag.hpp>

#include <DataSeries/Extent.hpp>

class Field;
class FixedField;
class Variable32Field;
class ExtentRecordCopy;

namespace dataseries {
    class SEP_RowOffset {
    public:
        typedef int32_t difference_type;

        // TODO: deprecate the below.
        SEP_RowOffset(uint32_t row_offset, const Extent *extent)
            : row_offset(row_offset)
#if LINTEL_DEBUG
              , extent(extent)
#endif
        { }

        SEP_RowOffset(uint32_t row_offset, const Extent::Ptr &extent)
            : row_offset(row_offset)
#if LINTEL_DEBUG
              , extent(extent.get())
#endif
        { }

        SEP_RowOffset(const SEP_RowOffset &from)
            : row_offset(from.row_offset)
#if LINTEL_DEBUG
              , extent(from.extent)
#endif
        { }

        /// Copy a row offset and then advance it by distance
        SEP_RowOffset(const SEP_RowOffset &from, 
                      difference_type distance, const Extent *within_extent)
            : row_offset(from.row_offset) IF_LINTEL_DEBUG2(, extent(from.extent))
        { 
            advance(distance, within_extent);
        }

        /// Copy a row offset and then advance it by distance
        SEP_RowOffset(const SEP_RowOffset &from, 
                      difference_type distance, const Extent &within_extent)
            : row_offset(from.row_offset) IF_LINTEL_DEBUG2(, extent(from.extent))
        { 
            advance(distance, &within_extent);
        }

        /// Copy a row offset and then advance it by distance
        SEP_RowOffset(const SEP_RowOffset &from, 
                      difference_type distance, const Extent::Ptr &within_extent)
            : row_offset(from.row_offset) IF_LINTEL_DEBUG2(, extent(from.extent))
        { 
            advance(distance, within_extent.get());
        }


        SEP_RowOffset &operator =(const SEP_RowOffset &from) {
            row_offset = from.row_offset;
            IF_LINTEL_DEBUG(extent = from.extent);
            return *this;
        }

        /// distance from a to b in rows; will be negative if a is after b.
        static difference_type
        distance(const SEP_RowOffset &a, const SEP_RowOffset &b, const Extent *extent) {
            DEBUG_SINVARIANT(a.extent == b.extent && b.extent == extent);
            return (b.row_offset - a.row_offset) / extent->type.fixedrecordsize();
        }

        /// distance from this to that
        difference_type distance(const SEP_RowOffset &that, const Extent *extent) {
            return distance(*this, that, extent);
        }

        /// advance iterator forward (or backward if negative) by amount
        void advance(int32_t amount, const Extent *within_extent) {
            DEBUG_SINVARIANT(extent == within_extent);
            row_offset += amount * within_extent->type.fixedrecordsize();
            // allow <= so that we can have a SEP_RowOffset pointing to the end of an exent.
            DEBUG_SINVARIANT(row_offset <= extent->fixeddata.size());
        }

        bool operator ==(const SEP_RowOffset &them) {
            DEBUG_SINVARIANT(extent == them.extent);
            return row_offset == them.row_offset;
        }

        bool operator !=(const SEP_RowOffset &them) {
            return !(*this == them);
        }

        bool operator <(const SEP_RowOffset &them) {
            DEBUG_SINVARIANT(extent == them.extent);
            return row_offset < them.row_offset;
        }

        bool operator <=(const SEP_RowOffset &them) {
            DEBUG_SINVARIANT(extent == them.extent);
            return row_offset <= them.row_offset;
        }

        bool operator >(const SEP_RowOffset &them) {
            DEBUG_SINVARIANT(extent == them.extent);
            return row_offset > them.row_offset;
        }

        bool operator >=(const SEP_RowOffset &them) {
            DEBUG_SINVARIANT(extent == them.extent);
            return row_offset >= them.row_offset;
        }
        
    private:
        uint8_t *rowPos(const Extent &e) const {
            uint8_t *ret = e.fixeddata.begin() + row_offset;
            DEBUG_SINVARIANT(e.insideExtentFixed(ret));
            return ret;
        }
        friend class ::Field;
        friend class ::FixedField;
        friend class ::Variable32Field;
        friend class ::ExtentRecordCopy;

        // should be a multiple of row_size.
        uint32_t row_offset;
        // for verifying that the extent was set correctly in debug mode
        IF_LINTEL_DEBUG(const Extent *extent;) 
    };
}

#endif
