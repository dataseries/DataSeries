// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    accessor classes for fields in an extent
*/

#ifndef __EXTENT_FIELD_H
#define __EXTENT_FIELD_H

#include <Lintel/MersenneTwisterRandom.H>
#include <Lintel/AssertBoost.H>

#include <DataSeries/ExtentSeries.H>
#include <DataSeries/Extent.H>

// Note, right now you have to delete the ExtentSeries and all the
// fields at the same time.  In the future, you may be able to delete
// fields well before deleting the series, but you will need to do it
// in that order.

// Note that if a sufficient case is made that the accessors which
// handle the null case inline are too slow, then special case
// accessors could be built which only handle non-null fields.
// Measurements in test.C indicate that this isn't such a big deal.

// TODO: make a private function in the various fields that accesses
// a field directly; then use it in Extent::packData rather than
// duplicating the code there to convert raw to real data.

class Field {
public:
    Field(ExtentSeries &_dataseries, const std::string &_fieldname, 
	  int _flags);
    // TODO: remove ourselves from the series when the destructor is called.
    // Disallow destroying a series that still has fields associated with it.
    virtual ~Field() {}
    
    static const int flag_nullable = 1;

    // need to have these defined in here because we can't use a
    // boolField as the field may come and go as the extent changes,
    // but this should be supported as a legal change to the type.
    bool isNull() const {
	DEBUG_INVARIANT(dataseries.extent() != NULL,
			"internal error; extent not set\n");
	if (nullable) {
	    dataseries.pos.checkOffset(null_offset);
	    return (*(dataseries.pos.record_start() + null_offset) & null_bit_mask) ? true : false;
	} else {
	    return false;
	}
    }
    void setNull(bool val = true) {
	DEBUG_INVARIANT(dataseries.extent() != NULL,
			"internal error; extent not set\n");
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
		      boost::format("tried to set a non-nullable field %s to null?!\n") % fieldname);
	}
    }

    const std::string &getName() {
	return fieldname;
    }

protected:
    bool nullable;
    long null_offset;
    int null_bit_mask;
    virtual void newExtentType();
    friend class ExtentSeries;
    ExtentSeries &dataseries;
    const std::string fieldname;
    const int flags;
};

class FixedField : public Field {
public:
    typedef ExtentType::byte byte;
    
protected:
    FixedField(ExtentSeries &_dataseries, const std::string &field, 
	       ExtentType::fieldType ft, int flags);

    byte *rawval() const {
	DEBUG_INVARIANT(dataseries.extent() != NULL,
			"internal error; extent not set\n");
	dataseries.pos.checkOffset(offset);
	return dataseries.pos.record_start() + offset;
    }

    virtual void newExtentType();

    int size, offset;
    ExtentType::fieldType fieldtype;
};

class BoolField : public FixedField {
public:
    BoolField(ExtentSeries &_dataseries, const std::string &field, 
	      int flags = 0, bool default_value = false);

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
    ByteField(ExtentSeries &_dataseries, const std::string &field, int flags = 0,
	      byte default_value = '\0');

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

class Int32Field : public FixedField {
public:
    typedef ExtentType::int32 int32;

    Int32Field(ExtentSeries &_dataseries, const std::string &field, int flags = 0,
	       int32 default_value = 0);

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

class Int64Field : public FixedField {
public:
    typedef ExtentType::int64 int64;
    Int64Field(ExtentSeries &_dataseries, const std::string &field, int flags = 0,
	       int64 default_value = 0);

    int64 val() const { 
	if (isNull()) {
	    return default_value;
	} else {
	    return *(int64 *)rawval();
	}
    }
    void set(int64 val) {
	*(int64 *)rawval() = val;
	setNull(false);
    }
    void nset(int64 val, int64 null_val = -1) {
	if (val == null_val) {
	    setNull(true);
	} else {
	    set(val);
	}
    }

    int64 default_value;
};

class DoubleField : public FixedField {
public:
    static const int flag_allownonzerobase = 1024;

    DoubleField(ExtentSeries &_dataseries, const std::string &field, int flags = 0,
		double default_value = 0);

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

class Variable32Field : public Field {
public:
    typedef ExtentType::byte byte;
    typedef ExtentType::int32 int32;
    static const std::string empty_string;

    Variable32Field(ExtentSeries &_dataseries, const std::string &field, int flags = 0,
		    const std::string &default_value = empty_string);

    const byte *val() const {
	if (isNull()) {
	    return (const byte *)default_value.c_str();
	} else {
	    return val(dataseries.extent()->variabledata,getVarOffset());
	}
    }
    const int32 size() const {
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
