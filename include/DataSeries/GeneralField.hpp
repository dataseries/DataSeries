// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Generic field operations, slower than normal ones, so not the default
*/

#ifndef __DATASERIES_GENERALFIELD_H
#define __DATASERIES_GENERALFIELD_H

#include <stddef.h>

#include <iostream>
#include <fstream>

#include <libxml/xmlmemory.h>
#include <libxml/parser.h>
#include <boost/static_assert.hpp>

#include <Lintel/CompilerMarkup.hpp>
#include <Lintel/HashMap.hpp>

#include <DataSeries/ExtentSeries.hpp>
#include <DataSeries/ExtentField.hpp>

class GeneralField;

/** \brief Discriminated union capable of holding any value
  * that could be stored in dataseries.

 * Sometimes it is necessary to store the values associated with
 * GeneralFields so that they can be used for comparisons in the
 * future.  Rather than having programs convert to a specific type,
 * such as a double for the numeric fields, it seems better to have a
 * general value type with a collection of operations on that type
 * that would allow additional values to be added to dataseries in the
 * future. It is currently invalid to change the type of a general
 * value once it has acquired a type.
 *
 * BUG/FEATURE: doesn't correctly track nulls right now, not clear
 * exactly the right way to implement that. */

class GeneralValue {
public:
    GeneralValue()
        : gvtype(ExtentType::ft_unknown), v_variable32(NULL), v_fixedwidth(NULL)
    { }
    GeneralValue(const GeneralValue &v)
	: gvtype(v.gvtype), gvval(v.gvval) {
        switch (gvtype) {
        case ExtentType::ft_variable32:
            v_variable32 = new std::string(*v.v_variable32);
            v_fixedwidth = NULL;
            break;
        case ExtentType::ft_fixedwidth:
            v_fixedwidth = new std::vector<uint8_t>(*v.v_fixedwidth);
            v_variable32 = NULL;
            break;
        default:
            v_variable32 = NULL;
            v_fixedwidth = NULL;
        }
    }
    GeneralValue(const GeneralField &from)
	: gvtype(ExtentType::ft_unknown), v_variable32(NULL), v_fixedwidth(NULL)
    { set(from); }

    GeneralValue(const GeneralField *from)
	: gvtype(ExtentType::ft_unknown), v_variable32(NULL), v_fixedwidth(NULL)
    { set(from); }

    ~GeneralValue() {delete v_variable32; delete v_fixedwidth;}

    void set(const GeneralValue &from);
    void set(const GeneralValue *from) {
	DEBUG_INVARIANT(from != NULL, "bad"); set(*from);
    }
    void set(const GeneralField &from);
    void set(const GeneralField *from) {
	DEBUG_INVARIANT(from != NULL, "bad"); set(*from);
    }

    /** \brief return this < gv

     * for ft_unknown, always false
     * for ft_bool, true < false
     * for integer/double types, works as normal
     * for ft_variable32, works as if memcmp on values padded with '\\0' to
     *   the maximum length
     *
     * we implement strictlylessthan rather than compare as is done
     * for std::string because it wasn't clear how to implement
     * compare sufficiently efficiently that it would be as efficent
     * as implmenting strictlylessthan and using that to build the
     * comparison. */
    bool strictlylessthan(const GeneralValue &gv) const;
    /** return this == gv */
    bool equal(const GeneralValue &gv) const;

    /** allowing FILE * rather than std::ostream as std::ostream is
	much slower */
    void write(FILE *to);

    std::ostream &write(std::ostream &to) const;

    GeneralValue &operator =(const GeneralValue &from) {
	set(from);
	return *this;
    }
    GeneralValue &operator =(const GeneralField &from) {
	set(from);
	return *this;
    }
    ExtentType::fieldType getType() const { return gvtype; }
    /** calculate a hash of this value, use partial_hash as the
	starting hash. */
    uint32_t hash(uint32_t partial_hash = 1776) const;

    void setBool(bool val);
    void setByte(uint8_t val);
    void setInt32(int32_t val);
    void setInt64(int64_t val);
    void setDouble(double val);
    void setVariable32(const std::string &from);

    bool valBool();
    uint8_t valByte();
    int32_t valInt32();
    int64_t valInt64();
    double valDouble();
    const std::string valString();

protected:
    // let all of the general field classes get at our value/type
    friend class GF_Bool;
    friend class GF_Byte;
    friend class GF_Int32;
    friend class GF_Int64;
    friend class GF_Double;
    friend class GF_FixedWidth;
    friend class GF_Variable32;
    ExtentType::fieldType gvtype;
    /// \cond INTERNAL_ONLY
    union gvvalT {
	bool v_bool;
	ExtentType::byte v_byte;
	ExtentType::int32 v_int32;
	ExtentType::int64 v_int64;
	double v_double;
    } gvval;
    /// \endcond
    std::string *v_variable32; // only valid if gvtype = ft_variable32
    std::vector<uint8_t> *v_fixedwidth; // only valid if gvtype == ft_fixedwidth
};

inline bool operator < (const GeneralValue &a, const GeneralValue &b) {
    return a.strictlylessthan(b);
}

inline bool operator > (const GeneralValue &a, const GeneralValue &b) {
    return b.strictlylessthan(a);
}

inline bool operator == (const GeneralValue &a, const GeneralValue &b) {
    return a.equal(b);
}

inline bool operator != (const GeneralValue &a, const GeneralValue &b) {
    return !a.equal(b);
}

inline bool operator <= (const GeneralValue &a, const GeneralValue &b) {
    return a < b || a == b;
}

inline bool operator >= (const GeneralValue &a, const GeneralValue &b) {
    return a > b || a == b;
}

inline std::ostream & operator << (std::ostream &to, const GeneralValue &a) {
    return a.write(to);
}

template <>
struct HashMap_hash<const GeneralValue> {
    unsigned operator()(const GeneralValue &a) const {
	return a.hash();
    }
};

/** \brief accessor for fields whose type may not be known at compile time.

  * \note This is a base class and needs to be held by (smart) pointer.
  *
  * \note This class does not inherit from @c Field. */
class GeneralField {
public:
    virtual ~GeneralField();

    /** create a new general field for a particular series; assumes that
	the field type doesn't change over the course of the series. */
    static GeneralField *create(ExtentSeries &series,
				const std::string &column) {
	return create(NULL, series, column);
    }
    /** fieldxml can be null, in which case it gets it from the series type. */
    static GeneralField *create(xmlNodePtr fieldxml, ExtentSeries &series,
				const std::string &column);

    // see comment in DStoTextModule.H for why we have both
    // interfaces; summary ostream is very slow
    virtual void write(FILE *to) = 0;
    virtual void write(std::ostream &to) = 0;

    virtual bool isNull() = 0;
    virtual void setNull(bool val = true) = 0;

    // set will do conversion/fail as specified for each GF type
    virtual void set(GeneralField *from) = 0;

    void set(GeneralField &from) {
        set(&from);
    }

    virtual void set(const GeneralValue *from) = 0;

    void set(const GeneralValue &from) {
	set(&from);
    }

    GeneralValue val() const { return GeneralValue(this); }
    virtual double valDouble() = 0;

    ExtentType::fieldType getType() const { return gftype; }

    void enableCSV();

    /// Delete all the fields and clear the vector.
    static void deleteFields(std::vector<GeneralField *> &fields);
protected:
    // TODO: to go away once this moves from ExtentType to somewhere sane.
    static std::string strGetXMLProp(xmlNodePtr cur,
				     const std::string &option_name) {
	return ExtentType::strGetXMLProp(cur, option_name);
    }
    ExtentType::fieldType gftype;
    friend class GeneralValue;
    GeneralField(ExtentType::fieldType _gftype)
	: gftype(_gftype),csvEnabled(false) {  }
    bool csvEnabled;
};

class GF_Bool : public GeneralField {
public:
    GF_Bool(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column);
    virtual ~GF_Bool();

    virtual void write(FILE *to);
    virtual void write(std::ostream &to);

    virtual bool isNull();
    virtual void setNull(bool val);

    // set(bool) -> copy
    // set(byte,int32,int64,double) -> val = from->val == 0
    // set(variable32) -> fail
    virtual void set(GeneralField *from);
    virtual void set(const GeneralValue *from);

    // TODO: add the other type-specific set functions to all of the
    // GF_types.
    void set(const GF_Bool *from) {
	if (from->myfield.isNull()) {
	    myfield.setNull();
	} else {
	    myfield.set(from->myfield.val());
	}
    }

    virtual double valDouble();

    bool val() const { return myfield.val(); }

    BoolField myfield;
    std::string s_true, s_false;
};

class GF_Byte : public GeneralField {
public:
    GF_Byte(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column);
    virtual ~GF_Byte();

    virtual void write(FILE *to);
    virtual void write(std::ostream &to);

    virtual bool isNull();
    virtual void setNull(bool val);

    // set(bool) -> 1 if true, 0 if false
    // set(byte, int32, int64) -> val = from->val & 0xFF;
    // set(double) -> val = (byte)round(from->val);
    // set(variable32) -> ?
    virtual void set(GeneralField *from);
    virtual void set(const GeneralValue *from);

    virtual double valDouble();

    ExtentType::byte val() const { return myfield.val(); }

    char *printspec;
    ByteField myfield;
};

class GF_Int32 : public GeneralField {
public:
    GF_Int32(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column);
    virtual ~GF_Int32();

    virtual void write(FILE *to);
    virtual void write(std::ostream &to);

    virtual bool isNull();
    virtual void setNull(bool val);

    virtual void set(GeneralField *from);
    virtual void set(const GeneralValue *from);
    void set(const GF_Int32 *from) {
	if (from->myfield.isNull()) {
	    myfield.setNull();
	} else {
	    myfield.set(from->myfield.val());
	}
    }

    virtual double valDouble();

    ExtentType::int32 val() const { return myfield.val(); }

    char *printspec;
    ExtentType::int32 divisor;
    Int32Field myfield;
};

class GF_Int64 : public GeneralField {
public:
    GF_Int64(xmlNodePtr fieldxml, ExtentSeries &series,
	     const std::string &column);
    virtual ~GF_Int64();

    virtual void write(FILE *to);
    virtual void write(std::ostream &to);

    virtual bool isNull();
    virtual void setNull(bool val);

    virtual void set(GeneralField *from);
    virtual void set(const GeneralValue *from);
    void set(int64_t v) { myfield.set(v);}

    virtual double valDouble();

    ExtentType::int64 val() const { return myfield.val(); }

    Int64Field myfield, *relative_field;
    Int64TimeField *myfield_time; // null unless printing as a time value.
    char *printspec;
    int64_t divisor, offset;
    bool offset_first;
};

class GF_Double : public GeneralField {
public:
    GF_Double(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column);
    virtual ~GF_Double();

    virtual void write(FILE *to);
    virtual void write(std::ostream &to);

    virtual bool isNull();
    virtual void setNull(bool val);

    virtual void set(GeneralField *from);
    virtual void set(const GeneralValue *from);

    virtual double valDouble();

    double val() const { return myfield.val(); }

    DoubleField myfield;
    DoubleField *relative_field;
    char *printspec;
    double offset, multiplier;
};

class GF_FixedWidth : public GeneralField {
public:
    GF_FixedWidth(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column);
    virtual ~GF_FixedWidth();

    virtual void write(FILE *to);
    virtual void write(std::ostream &to);

    virtual bool isNull();

    virtual void set(GeneralField *from);
    virtual void set(const GeneralValue *from);

    virtual double valDouble();

    // TODO-tomer: make this return a vector<uint8_t>
    uint8_t* val() const { return myfield.val(); }

    FixedWidthField myfield;
};

class GF_Variable32 : public GeneralField {
public:
    enum printstyle_t { printnostyle, printhex, printmaybehex, printcsv, printtext };
    GF_Variable32(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column);
    virtual ~GF_Variable32();

    virtual void write(FILE *to);
    virtual void write(std::ostream &to);

    virtual bool isNull();
    virtual void setNull(bool val);

    virtual void set(GeneralField *from);
    virtual void set(const GeneralValue *from);
    void set(GF_Variable32 *from) {
	if (from->myfield.isNull()) {
	    myfield.setNull();
	} else {
	    myfield.set(from->myfield.val(),from->myfield.size());
	}
    }

    virtual double valDouble();

    /// Returns the raw string value, note that this may include nulls
    /// and hence c_str() is dangerous
    const std::string val() const;

    /// Returns the string formatted as per it's printspec
    const std::string valFormatted();

    const std::string val_formatted() FUNC_DEPRECATED { // old naming convention
	return valFormatted();
    }

    void clear() {
	myfield.clear();
    }

    char *printspec;
    printstyle_t printstyle;
    Variable32Field myfield;
};

/** \brief Copies records from one @c Extent to another.

    \todo TODO: add an output module as an optional argument; if it exists,
    automatically do the newRecord stuff. */
class ExtentRecordCopy {
public:
    ExtentRecordCopy(ExtentSeries &source, ExtentSeries &dest);
    /** Prepares the copy structure to do the copy.  Will be automatically
	called by copyRecord() if necessary. */
    void prep();
    ~ExtentRecordCopy();
    /** Copies the current record of the source @c ExtentSeries to the
        current record of the destination @c ExtentSeries. */
    void copyRecord(); // you need to create the record first.
private:
    bool did_prep;
    int fixed_copy_size;
    ExtentSeries &source, &dest;
    std::vector<GeneralField *> sourcefields, destfields; // all fields here if f_c_s == 0
    std::vector<GF_Variable32 *> sourcevarfields, destvarfields; // only used if fixed_copy_size >0
};

#endif
