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

class Int32Field : public FixedField {
public:
    typedef ExtentType::int32 int32;

    Int32Field(ExtentSeries &_dataseries, const std::string &field, 
	       int flags = 0, int32 default_value = 0, bool auto_add = true);

    int32 val() const { 
	if (isNull()) {
	    return default_value;
	} else {
	    return *(int32 *)rawval();
	}
    }
    void set(int32 val) {
	*(int32 *)rawval() = val;
	setNull(false);
    }
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
