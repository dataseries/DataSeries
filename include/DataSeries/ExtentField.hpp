// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    accessor classes for fields in an extent; going to become a
    meta include file for all the field files.
*/

#ifndef __EXTENT_FIELD_H
#define __EXTENT_FIELD_H

#include <DataSeries/Field.hpp>

// Note that if a sufficient case is made that the accessors which
// handle the null case inline are too slow, then special case
// accessors could be built which only handle non-null fields.
// Measurements in test.C indicate that this isn't such a big deal.

// TODO: make a private function in the various fields that accesses
// a field directly; then use it in Extent::packData rather than
// duplicating the code there to convert raw to real data.

// I think we might be able to templatize a bunch of this so that we
// can eliminate the penalty of testing isNull() for fields that can't
// be null.

#include <DataSeries/FixedField.hpp>

class BoolField : public FixedField {
public:
    BoolField(ExtentSeries &_dataseries, const std::string &field, 
	      int flags = 0, bool default_value = false, bool auto_add = true);

    bool val() const { 
	if (isNull()) {
	    return default_value;
	} else {
	    return *rawval() & bit_mask ? true : false; 
	}
    }
    void set(bool val) {
	if (val) {
	    *rawval() = (byte)(*rawval() | bit_mask);
	} else {
	    *rawval() = (byte)(*rawval() & ~bit_mask);
	}
	setNull(false);
    }

    virtual void newExtentType();
    bool default_value;
private:
    byte bit_mask;
};

class ByteField : public FixedField {
public:
    ByteField(ExtentSeries &_dataseries, const std::string &field, 
	      int flags = 0, byte default_value = 0, bool auto_add = true);

    byte val() const { 
	if (isNull()) {
	    return default_value;
	} else {
	    return *rawval();
	}
    }
    void set(byte val) {
	*rawval() = val;
	setNull(false);
    }
    byte default_value;
};

#include <DataSeries/Int32Field.hpp>
#include <DataSeries/Int64Field.hpp>
#include <DataSeries/Int64TimeField.hpp>

class DoubleField : public FixedField {
public:
    /// flag_allownonzerobase is deprecated.  It seemed like a good
    /// idea when we initially created it, but in practice it just
    /// makes writing analysis really difficult.  It was intended to
    /// deal with time fields, and the newer Int64TimeField deals with
    /// that much better.
    static const int flag_allownonzerobase = 1024;

    DoubleField(ExtentSeries &_dataseries, const std::string &field, 
		int flags = 0, double default_value = 0, bool auto_add = true);

    double val() const { 
	if (isNull()) {
	    return default_value;
	} else {
	    return *(double *)rawval();
	}
    }
  
    double absval() const { 
	if (isNull()) {
	    // 2006-06-30 EricAnderson, used to return just
	    // default_value, but that seems inconsistent with what is
	    // being done below, and different than how val() would
	    // work.  Correct behavior seems to be unclear
	    return default_value + base_val; 
	} else {
	    return val() + base_val;
	}
    }

    void set(double val) {
	*(double *)rawval() = val;
	setNull(false);
    }
  
    void setabs(double val) {
	*(double *)rawval() = val - base_val;
	setNull(false);
    }
  
    double default_value;
    double base_val;
    virtual void newExtentType();
};

#include <DataSeries/TFixedField.hpp>

class Variable32Field : public Field {
public:
    typedef ExtentType::byte byte;
    typedef ExtentType::int32 int32;
    static const std::string empty_string;

    Variable32Field(ExtentSeries &_dataseries, const std::string &field, 
		    int flags = 0, 
		    const std::string &default_value = empty_string,
		    bool auto_add = true);

    const byte *val() const {
	DEBUG_INVARIANT(dataseries.extent() != NULL && offset_pos >= 0,
			"internal error; extent not set or field not ready");
	if (isNull()) {
	    return (const byte *)default_value.c_str();
	} else {
	    return val(dataseries.extent()->variabledata,getVarOffset());
	}
    }
    int32 size() const {
	DEBUG_INVARIANT(dataseries.extent() != NULL && offset_pos >= 0,
			"internal error; extent not set or field not ready");
	if (isNull()) {
	    return default_value.size();
	} else {
	    // getVarOffset() has checked that the size is valid.
	    return size(dataseries.extent()->variabledata,getVarOffset());
	}
    }
    std::string stringval() const {
	if (isNull()) {
	    return default_value;
	} else {
	    return std::string((char *)val(),size());
	}
    }
    void set(const void *data, int32 datasize);
    void set(const std::string &data) { // note this doesn't propagate the C '\0' at the end (neither does a C++ string)
	set(data.data(),data.size());
    }
    void nset(const std::string &val, const std::string &null_val) {
	if (val == null_val) {
	    setNull(true);
	} else {
	    set(val);
	}
    }
    void set(Variable32Field &from) {
	set(from.val(),from.size());
    }
    void clear() {
	DEBUG_INVARIANT(dataseries.extent() != NULL,
			"internal error; extent not set\n");
	dataseries.pos.checkOffset(offset_pos);
	*(int32 *)(dataseries.pos.record_start() + offset_pos) = 0;
    }
    bool equal(const std::string &to) {
	if ((int)to.size() != size())
	    return false;
	return memcmp(to.data(),val(),to.size()) == 0;
    }
    std::string default_value;
protected:
    virtual void newExtentType();
    friend class Extent;
    static void *vardata(Extent::ByteArray &varbytes, int32 offset) {
	return (void *)(varbytes.begin() + offset);
    }
    static int32 size(Extent::ByteArray &varbytes, int32 varoffset) {
	return *(int32 *)(varbytes.begin() + varoffset);
    }	    
    static byte *val(Extent::ByteArray &varbytes, int32 varoffset) {
	return (byte *)vardata(varbytes,varoffset+4);
    }
    static int32 getVarOffset(byte *record, int offset) {
	return *(int32 *)(record + offset);
    }
    int32 getVarOffset() const {
	DEBUG_INVARIANT(dataseries.extent() != NULL,
			"internal error; extent not set\n");
	dataseries.pos.checkOffset(offset_pos);
	int32 varoffset = getVarOffset(dataseries.pos.record_start(),
				       offset_pos);
#if defined(COMPILE_DEBUG) || defined(DEBUG)
	selfcheck(varoffset);
#endif
	return varoffset;
    }
    void selfcheck() const {
	DEBUG_INVARIANT(dataseries.extent() != NULL,
			"internal error; extent not set\n");
	dataseries.pos.checkOffset(offset_pos);
	int32 varoffset = getVarOffset(dataseries.pos.record_start(),
				       offset_pos);
	selfcheck(varoffset);
    }
    void selfcheck(int32 varoffset) const {
	selfcheck(dataseries.extent()->variabledata,varoffset);
    }
    static void selfcheck(Extent::ByteArray &varbytes, int32 varoffset);
    static void dosetandguard(byte *vardatapos, 
			      const void *data, int32 datasize,
			      int32 roundup);
    static int32 roundupSize(int32 size) {
	return size + (12 - (size % 8)) % 8;
    }
    int offset_pos;
    bool unique;
};

#endif
