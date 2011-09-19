// -*-C++-*-
/*
   (c) Copyright 2003-2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    int64 field class
*/

#ifndef DATASERIES_INT64FIELD_HPP
#define DATASERIES_INT64FIELD_HPP

#include <DataSeries/FixedField.hpp>

/** \brief Accessor for int64 fields. */
class Int64Field : public FixedField {
public:
    Int64Field(ExtentSeries &_dataseries, const std::string &field, 
	       int flags = 0, int64_t default_value = 0, bool auto_add = true);
    virtual ~Int64Field();

    int64_t val() const { 
	if (isNull()) {
	    return default_value;
	} else {
	    return *(int64_t *)rawval();
	}
    }
    
    int64_t operator() () const {
	return val();
    }

    void set(int64_t val) {
	*(int64_t *)rawval() = val;
	setNull(false);
    }
    void nset(int64_t val, int64_t null_val = -1) {
	if (val == null_val) {
	    setNull(true);
	} else {
	    set(val);
	}
    }

    int64_t val(Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        if (isNull(e, row_offset)) {
            return default_value;
        } else {
            return *reinterpret_cast<int64_t *>(rawval(e, row_offset));
        }
    }

    int64_t operator ()(Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        return val(e, row_offset);
    }

    void set(Extent &e, const dataseries::SEP_RowOffset &row_offset, int64_t val) {
        *reinterpret_cast<int64_t *>(rawval(e, row_offset)) = val;
        setNull(e, row_offset, false);
    }

    int64_t default_value;
};

#endif
