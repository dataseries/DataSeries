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

    byte *rawval() const {
	DEBUG_INVARIANT(dataseries.extent() != NULL && offset >= 0,
			"internal error; extent not set or field not ready");
	dataseries.checkOffset(offset);
	return recordStart() + offset;
    }

    virtual void newExtentType();

    // -1 is used as a flag for uninitialized, otherwise would be uint32_t
    int32_t field_size, offset;
    ExtentType::fieldType fieldtype;
};

#endif
