// -*-C++-*-
/*
   (c) Copyright 2003-2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    fixed size data base field
*/

#ifndef DATASERIES_FIXEDFIELD_HPP
#define DATASERIES_FIXEDFIELD_HPP

#include <DataSeries/Field.hpp>

/** \brief Base class for fixed size fields. */
class FixedField : public Field {
public:
    typedef ExtentType::byte byte;
    
protected:
    FixedField(ExtentSeries &dataseries, const std::string &field, 
	       ExtentType::fieldType ft, int flags);
    virtual ~FixedField();

    // TODO-eric: figure out how to collapse out all the duplicate code we are ending up with.
    byte *rawval() const {
	DEBUG_INVARIANT(dataseries.extent() != NULL && offset >= 0,
			"internal error; extent not set or field not ready");
	dataseries.checkOffset(offset);
	return recordStart() + offset;
    }

    byte *rawval(const Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        DEBUG_SINVARIANT(offset >= 0 && &e.getType() == dataseries.getType());
        byte *ret = e.fixeddata.begin() + row_offset.row_offset + offset;
        DEBUG_SINVARIANT(e.insideExtentFixed(ret));
        return ret;
    }

    virtual void newExtentType();

    // -1 is used as a flag for uninitialized, otherwise would be uint32_t
    int32_t field_size, offset;
    ExtentType::fieldType fieldtype;
};

#endif
