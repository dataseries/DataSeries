// -*-C++-*-
/*
   (c) Copyright 2003-2009, Hewlett-Packard Development Company, LP

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
    Int32Field(ExtentSeries &_dataseries, const std::string &field, 
	       int flags = 0, int32_t default_value = 0, bool auto_add = true);

    /** Returns the value of the field in the @c ExtentSeries' current record.
        If the field is null returns the default value.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record. */
    int32_t val() const { 
	if (isNull()) {
	    return default_value;
	} else {
	    return *reinterpret_cast<int32_t *>(rawval());
	}
    }

    /** Same as val(), but fewer characters */
    int32_t operator() () const {
	return val();
    }

    /** Sets the value of the field in the @c ExtentSeries' current record.
        The field will never be null immediately after a call to set(),
        regardless of whether the argument is the same as the default value.

        Preconditions:
            - The name of the Field must have been set and the associated
              @c ExtentSeries must have a current record. */
    void set(int32_t val) {
	*reinterpret_cast<int32_t *>(rawval()) = val;
	setNull(false);
    }
    /** Sets the value of the field in the @c ExtentSeries' current record,
        unless val == null_val, in which case it sets the field to null.

        Preconditions:
            - The name of the Field must have been set and the associated
              @c ExtentSeries must have a current record. */
    void nset(int32_t val, int32_t null_val = -1) {
	if (val == null_val) {
	    setNull(true);
	} else {
	    set(val);
	}
    }

    int32_t val(Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        INVARIANT(!nullable, "unimplemented");
        return *reinterpret_cast<int32_t *>(rawval(e, row_offset));
    }

    int32_t operator ()(Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        return val(e, row_offset);
    }

    void set(Extent &e, const dataseries::SEP_RowOffset &row_offset, int32_t val) {
        INVARIANT(!nullable, "unimplemented");
        *reinterpret_cast<int32_t *>(rawval(e, row_offset)) = val;
    }

    int32_t default_value;
};

#endif
