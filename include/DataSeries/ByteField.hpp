// -*-C++-*-
/*
   (c) Copyright 2003-2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    byte field class
*/

#ifndef DATASERIES_BYTEFIELD_HPP
#define DATASERIES_BYTEFIELD_HPP

/** \brief Accessor for byte fields
  */
class ByteField : public FixedField {
public:
    ByteField(ExtentSeries &_dataseries, const std::string &field, 
	      int flags = 0, byte default_value = 0, bool auto_add = true);

    /** Returns the value of the field in the @c ExtentSeries' current record.
        If the field is null returns the default value.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record. */
    byte val() const { 
        if (isNull()) {
            return default_value;
        } else {
            return *rawval();
        }
    }

    /** Same as val(), but fewer characters */
    byte operator() () const {
	return val();
    }

    /** Sets the value of the field in the @c ExtentSeries' current record.
        The field will never be null immediately after a call to set(),
        regardless of whether the argument is the same as the default value.

        Preconditions:
            - The name of the Field must have been set and the associated
              @c ExtentSeries must have a current record. */
    void set(byte val) {
        *rawval() = val;
        setNull(false);
    }
    byte default_value;
};

#endif
