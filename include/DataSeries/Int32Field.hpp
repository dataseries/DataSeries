// -*-C++-*-
/*
   (c) Copyright 2003-2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    int32 field class
*/

#ifndef DATASERIES_INT32FIELD_HPP
#define DATASERIES_INT32FIELD_HPP

/** \brief Accessor for int32 fields. */
class Int32Field : public FixedField {
public:
    typedef ExtentType::int32 int32;

    Int32Field(ExtentSeries &_dataseries, const std::string &field, 
	       int flags = 0, int32 default_value = 0, bool auto_add = true);

    /** Returns the value of the field in the @c ExtentSeries' current record.
        If the field is null returns the default value.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record. */
    int32 val() const { 
	if (isNull()) {
	    return default_value;
	} else {
	    return *(int32 *)rawval();
	}
    }
    /** Sets the value of the field in the @c ExtentSeries' current record.
        The field will never be null immediately after a call to set(),
        regardless of whether the argument is the same as the default value.

        Preconditions:
            - The name of the Field must have been set and the associated
              @c ExtentSeries must have a current record. */
    void set(int32 val) {
	*(int32 *)rawval() = val;
	setNull(false);
    }
    /** Sets the value of the field in the @c ExtentSeries' current record,
        unless val == null_val, in which case it sets the field to null.

        Preconditions:
            - The name of the Field must have been set and the associated
              @c ExtentSeries must have a current record. */
    void nset(int32 val, int32 null_val = -1) {
	if (val == null_val) {
	    setNull(true);
	} else {
	    set(val);
	}
    }
    int32 default_value;
};

#endif
