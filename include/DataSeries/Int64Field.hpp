// -*-C++-*-
/*
   (c) Copyright 2003-2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    base field class
*/

#ifndef DATASERIES_INT64FIELD_HPP
#define DATASERIES_INT64FIELD_HPP

#include <DataSeries/FixedField.hpp>

/** \brief Accessor for int64 fields. */
class Int64Field : public FixedField {
public:
    typedef int64_t int64; // Deprecating
    Int64Field(ExtentSeries &_dataseries, const std::string &field, 
	       int flags = 0, int64_t default_value = 0);
    virtual ~Int64Field();

    int64_t val() const { 
	if (isNull()) {
	    return default_value;
	} else {
	    return *(int64_t *)rawval();
	}
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

    int64_t default_value;
};

#endif
