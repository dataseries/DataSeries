// -*-C++-*-
/*
   (c) Copyright 2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#ifndef DATASERIES_SEP_ROWOFFSET_HPP
#define DATASERIES_SEP_ROWOFFSET_HPP

#include <inttypes.h>

#include <Lintel/DebugFlag.hpp>

class Extent;
class FixedField;
namespace dataseries {
    class SEP_RowOffset {
    public:
        SEP_RowOffset(uint32_t row_offset, const Extent *extent)
            : row_offset(row_offset)
#if LINTEL_DEBUG
              , extent(extent)
#endif
        { }
    private:
        friend class ::FixedField;

        // should be a multiple of row_size.
        uint32_t row_offset;
        // for verifying that the extent was set correctly in debug mode
        IF_LINTEL_DEBUG(const Extent *extent;) 
    };
}

#endif
