// -*-C++-*-
/*
   (c) Copyright 2003-2011, Hewlett-Packard Development Company, LP

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

/** \brief Base class for all Fields.

  * A @c Field is a handle to the elements of records within an @c Extent. Each
  * @c Field is associated with a particular @c ExtentSeries.  This association
  * is set up in the constructor which initializes the protected member
  * @c Field::dataseries.  Every field has a @c val() and a @c set() member
  * which operate on the underlying @c Extent.  These members always operate
  * on the current record of the associated @c ExtentSeries. It is important to
  * note that when an @c ExtentSeries is destroyed, all the associated fields
  * must have been destroyed first. */
class Field {
public:
    /** Initialize a Field associated with the specified ExtentSeries.
        The field name can legitimately be empty, in which case this
        field won't actually be activated until you later call
        setFieldName().  Note that you should not call any of the
        field operations (e.g. isNull, val) until you have set the
        field name.  In debugging mode this is checked, in optimized mode
        it may just return wrong values. flags should be either flag
        nullable or 0.  If set to flag nullable, indicates that the field
        may be nullable.  Note that If flag nullable is specified, it is
        still ok if the field is not in fact nullable.  flag_nullable
        only means that you are prepared to deal with null fields. */
    Field(ExtentSeries &_dataseries, const std::string &_fieldname, 
	  uint32_t _flags);

    /** Unregisters the @c Field object from the @c ExtentSeries. */
    virtual ~Field();
    
    /** This value of the initialization flags indicates that the field
        may be nullable. */
    static const int flag_nullable = 1;

    /** Returns true iff the field is null in the current record of the
        @c ExtentSeries.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record.

        \internal
        need to have these defined in here because we can't use a
        boolField as the field may come and go as the extent changes,
        but this should be supported as a legal change to the type. */
    bool isNull() const {
        if (!nullable) {
            return false;
        } else {
            return isNull(dataseries.pos.record_start());
        }
    }
    bool isNull(const Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        if (!nullable) {
            return false;
        } else {
            return isNull(row_offset.row_offset + e.fixeddata.begin());
        }
    }
    // TODO-eric-review: Should be private?
    bool isNull(ExtentType::byte * rawpos) const {
	DEBUG_INVARIANT(dataseries.extent() != NULL,
			"internal error; extent not set");
	if (__builtin_expect(nullable,1)) {
	    DEBUG_INVARIANT(null_offset >= 0, 
			    "internal error; field not ready");
	    dataseries.checkOffset(null_offset);
	    return (*(rawpos + null_offset) 
		    & null_bit_mask) ? true : false;
	} else {
	    return false;
	}
    }

    /** Sets the field to null in the @c ExtentSeries' current record.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record. Also,
              the flag_nullable must have been passed to the constructor. */
    void setNull(bool val = true) {
	DEBUG_INVARIANT(dataseries.extent() != NULL && null_offset >= 0,
			"internal error; extent not set or field not ready");
	if (nullable) {
	    dataseries.checkOffset(null_offset);
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

    /** Returns the name of the field. */
    const std::string &getName() const {
	return fieldname;
    }

    /** This function will update the current field name; it is
        intended to let you create field objects yet determine the
        actual field name after seeing the first extent.  The
        alternative to have the field as a pointer means there is an
        un-necessary dereference on every access. */
    void setFieldName(const std::string &new_name);
protected:
    /** True if the field is nullable. */
    bool nullable;
    /** The byte offset within a record of the hidden bool field
        which determines whether this field is null. Note that
        you need to use null bit mask as well, because of the
        packed representation.  This is only useful for nullable
        fields. */
    int32_t null_offset;
    /** A mask representing the position of the hidden bool field
        which determines whether this field is null. For example
        if it is the third boolean field, then the mask will be
        0x4. This member is only meaningful for nullable fields. 
        
        Invariants:
            - null_bit_mask has at most one bit set which must
              be one of the 8 lowest order bits. */
    uint32_t null_bit_mask;
    /** This function is called by the associated ExtentSeries when
        the type of the @c Extent it holds changes. Derived classes
        should override it to reload any information that depends on
        the @c ExtentType.  The overriden method should call the
        @c Field version before doing its own processing.

        \todo TODO: think about making this stash away the extent in the case
        where the name is still empty and then recover it when the name is set.
        this increases the chances we will seg-fault out through a null
        dereference if the user has done something bad. */
    virtual void newExtentType();

    ExtentType::byte *recordStart() const {
        return dataseries.pos.record_start();
    }

    friend class ExtentSeries;
    /** The associated @c ExtentSeries. Most of the useful stuff comes out of
        this, including the type of the extent and the actual storage for
        the fields. */
    ExtentSeries &dataseries;
    /** The flags used to initialize the Field. At present, this can be flag nullable
        or 0. */
    const uint32_t flags;
private:
    std::string fieldname;
};

#endif
