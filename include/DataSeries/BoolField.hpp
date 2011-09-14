// -*-C++-*-
/*
   (c) Copyright 2003-2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    double field class
*/

#ifndef DATASERIES_BOOLFIELD_HPP
#define DATASERIES_BOOLFIELD_HPP

#include <DataSeries/FixedField.hpp>
/** \brief Accessor for boolean fields.
  *
  */
class BoolField : public FixedField {
public:
    /** Creates a new @c BoolField associated with the specified series.
        An empty string as the field name means that the name of the
        field will be specified later.  It must be set via
        @c Field::setFieldName before the field is used. flags should be
        either @c Field::flag_nullable or 0. If set to
        @c Field::flag_nullable, indicates that the field may be nullable.
        default_value specifies a value to be returned by @c BoolField::val()
        for null fields.

        Preconditions:
            - The ExtentType of the series must have a bool field with the
              specified name. If the type of the series is not known, this
              check will be delayed until the type is set. */
    BoolField(ExtentSeries &_dataseries, const std::string &field, 
	      int flags = 0, bool default_value = false, bool auto_add = true);

    /** Returns the value of the field in the @c ExtentSeries' current record.
        If the field is null returns the default value.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record. */
    bool val() const { 
        if (isNull()) {
            return default_value;
        } else {
            return *rawval() & bit_mask ? true : false; 
        }
    }

    bool operator() () const {
	return val();
    }

    /** Sets the value of the field in the @c ExtentSeries' current record.
        The field will never be null immediately after a call to set(),
        regardless of whether the argument is the same as the default value.

        Preconditions:
            - The name of the Field must have been set and the associated
              @c ExtentSeries must have a current record. */
    void set(bool val) {
        if (val) {
            *rawval() = (byte)(*rawval() | bit_mask);
        } else {
            *rawval() = (byte)(*rawval() & ~bit_mask);
        }
        setNull(false);
    }

    bool val(Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        INVARIANT(!nullable, "unimplemented");
        return *rawval(e, row_offset) & bit_mask ? true : false;
    }

    bool operator ()(Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        return val(e, row_offset);
    }

    void set(Extent &e, const dataseries::SEP_RowOffset &row_offset, bool val) {
        INVARIANT(!nullable, "unimplemented");
        if (val) {
            *rawval(e, row_offset) = (byte)(*rawval(e, row_offset) | bit_mask);
        } else {
            *rawval(e, row_offset) = (byte)(*rawval(e, row_offset) & ~bit_mask);
        }
    }
    virtual void newExtentType();

    /** The value returned by val for null fields. */
    bool default_value;
private:
    byte bit_mask;
};

#endif
