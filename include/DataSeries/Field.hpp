// -*-C++-*-
/*
   (c) Copyright 2003-2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    base field class
*/

#ifndef DATASERIES_FIELD_HPP
#define DATASERIES_FIELD_HPP

#include <Lintel/AssertBoost.hpp>

#include <DataSeries/ExtentSeries.hpp>
#include <DataSeries/Extent.hpp>

class Field {
public:
    /// The field name can legitimately be empty, in which case this
    /// field won't actually be activated until you later call
    /// setFieldName().  Note that you should not call any of the
    /// field operations (e.g. isNull, val() until you have set the
    /// field name.  In debugging mode this is checked, in optimized mode
    /// it may just return wrong values.
    Field(ExtentSeries &_dataseries, const std::string &_fieldname, 
	  uint32_t _flags);
    virtual ~Field();
    
    static const int flag_nullable = 1;

    // need to have these defined in here because we can't use a
    // boolField as the field may come and go as the extent changes,
    // but this should be supported as a legal change to the type.
    bool isNull() const {
	DEBUG_INVARIANT(dataseries.extent() != NULL,
			"internal error; extent not set");
	if (nullable) {
	    DEBUG_INVARIANT(null_offset >= 0, 
			    "internal error; field not ready");
	    dataseries.pos.checkOffset(null_offset);
	    return (*(dataseries.pos.record_start() + null_offset) 
		    & null_bit_mask) ? true : false;
	} else {
	    return false;
	}
    }

    void setNull(bool val = true) {
	DEBUG_INVARIANT(dataseries.extent() != NULL && null_offset >= 0,
			"internal error; extent not set or field not ready");
	if (nullable) {
	    dataseries.pos.checkOffset(null_offset);
	    ExtentType::byte *v = dataseries.pos.record_start() + null_offset;
	    if (val) {
		*v = (ExtentType::byte)(*v | null_bit_mask);
	    } else {
		*v = (ExtentType::byte)(*v & ~null_bit_mask);
	    }
	} else {
	    INVARIANT(val == false,
		      boost::format("tried to set a non-nullable field %s to null?!") % fieldname);
	}
    }

    const std::string &getName() const {
	return fieldname;
    }

    /// This function will update the current field name; it is
    /// intended to let you create field objects yet determine the
    /// actual field name after seeing the first extent.  The
    /// alternative to have the field as a pointer means there is an
    /// un-necessary dereference on every access.
    void setFieldName(const std::string &new_name);
protected:
    bool nullable;
    int32_t null_offset;
    uint32_t null_bit_mask;
    // TODO: think about making this stash away the extent in the case where
    // the name is still empty and then recover it when the name is set.
    // this increases the chances we will seg-fault out through a null
    // dereference if the user has done something bad.
    virtual void newExtentType();
    friend class ExtentSeries;
    ExtentSeries &dataseries;
    const uint32_t flags;
private:
    std::string fieldname;
};

#endif
