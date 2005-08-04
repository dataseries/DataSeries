/* -*-C++-*-
*******************************************************************************
*
* File:         GeneralField.H
* RCS:          $Header: /mount/cello/cvs/DataSeries/include/DataSeries/GeneralField.H,v 1.10 2005/02/15 01:18:34 anderse Exp $
* Description:  Generic field operations, slower than normal ones, so not the default
* Author:       Eric Anderson
* Created:      Fri Jul 18 14:45:12 2003
* Modified:     Thu Oct 28 23:53:58 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#ifndef __DATASERIES_GENERALFIELD_H
#define __DATASERIES__GENERALFIELD_H

#include <streamcompat.H>
#include <libxml/xmlmemory.h>
#include <libxml/parser.h> 

#include <ExtentSeries.H>
#include <ExtentField.H>

class GeneralField;

/** \brief General value that could be stored in dataseries.

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
    GeneralValue();
    GeneralValue(const GeneralValue &v)
	: gvtype(v.gvtype), gvval(v.gvval) {
	if (gvtype == ExtentType::ft_variable32) {
	    v_variable32 = v.v_variable32;
	}
    }
    GeneralValue(const GeneralField &from);
    GeneralValue(const GeneralField *from);

    void set(const GeneralValue &from);
    void set(const GeneralValue *from) { set(*from); }
    void set(const GeneralField &from);
    void set(const GeneralField *from) { set(*from); }
    void setInt32(ExtentType::int32 from);
    void setVariable32(const std::string &from);

    /** \brief return this < gv 

     * for ft_unknown, always false
     * for ft_bool, true < false
     * for integer/double types, works as normal
     * for ft_variable32, works as if memcmp on values padded with '\0' to 
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
    /** using FILE * rather than std::ostream as std::ostream is much slower */
    void write(FILE *to);

    GeneralValue &operator =(const GeneralValue &from) {
	set(from);
	return *this;
    }
    GeneralValue &operator =(const GeneralField &from) {
	set(from);
	return *this;
    }
    ExtentType::fieldType getType() { return gvtype; }
    /** add this value into the hash */
    unsigned incrementalHash(unsigned hash) {
	AssertFatal(("unimplemented"));
	switch(gvtype) {
	}
	return hash;
    }

protected:
    // let all of the general field classes get at our value/type
    friend class GF_Bool;
    friend class GF_Byte;
    friend class GF_Int32;
    friend class GF_Int64;
    friend class GF_Double;
    friend class GF_Variable32;
    ExtentType::fieldType gvtype;
    union {
	bool v_bool;
	ExtentType::byte v_byte;
	ExtentType::int32 v_int32;
	ExtentType::int64 v_int64;
	double v_double;
    } gvval;
    std::string v_variable32; // only valid if gvtype = ft_variable32
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

class GeneralField {
public:
    // see comment in DStoTextModule.H for why we have both
    // interfaces; summary ostream is very slow
    virtual ~GeneralField();
    virtual void write(FILE *to) = 0;
    virtual void write(std::ostream &to) = 0;
    /** fieldxml can be null, in which case it gets it from the series type. */
    static GeneralField *create(xmlNodePtr fieldxml, ExtentSeries &series, 
				const std::string &column);
    // set will do conversion/fail as specified for each GF type
    virtual bool isNull() = 0;

    virtual void set(GeneralField *from) = 0;

    void set(GeneralField &from) {
        set(&from);
    }
 
    virtual void set(const GeneralValue *from) = 0;

    void set(const GeneralValue &from) {
	set(&from);
    }
    const ExtentType::fieldType getType() { return gftype; }
    void enableCSV();

protected:

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
    // set(bool) -> copy
    // set(byte,int32,int64,double) -> val = from->val == 0
    // set(variable32) -> fail
    virtual bool isNull();
    virtual void set(GeneralField *from);
    virtual void set(const GeneralValue *from);
    bool val() { return myfield.val(); }
    BoolField myfield;
    std::string s_true, s_false;  
};

class GF_Byte : public GeneralField {
public:
    GF_Byte(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column);
    virtual ~GF_Byte();
    virtual void write(FILE *to);
    virtual void write(std::ostream &to);
    // set(bool) -> 1 if true, 0 if false
    // set(byte, int32, int64) -> val = from->val & 0xFF;
    // set(double) -> val = (byte)round(from->val);
    // set(variable32) -> ?
    virtual bool isNull();
    virtual void set(GeneralField *from);
    virtual void set(const GeneralValue *from);
    ExtentType::byte val() { return myfield.val(); }
    char *printspec;
    ByteField myfield;
};

class GF_Int32 : public GeneralField {
public:
    GF_Int32(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column);
    virtual ~GF_Int32();
    virtual void write(FILE *to);
    virtual void write(std::ostream &to);
    virtual void set(GeneralField *from);
    virtual void set(const GeneralValue *from);
    virtual bool isNull();
    ExtentType::int32 val() { return myfield.val(); }
    char *printspec;
    ExtentType::int32 divisor;
    Int32Field myfield;
};

class GF_Int64 : public GeneralField {
public:
    GF_Int64(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column);
    virtual ~GF_Int64();
    virtual void write(FILE *to);
    virtual void write(std::ostream &to);
    virtual void set(GeneralField *from);
    virtual void set(const GeneralValue *from);
    virtual bool isNull();
    ExtentType::int64 val() { return myfield.val(); }
    char *printspec;
    ExtentType::int64 divisor;
    Int64Field myfield;
};

class GF_Double : public GeneralField {
public:
    GF_Double(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column); 
    virtual ~GF_Double();
    virtual void write(FILE *to);
    virtual void write(std::ostream &to);
    virtual bool isNull();
    virtual void set(GeneralField *from);
    virtual void set(const GeneralValue *from);
    double val() { return myfield.val(); }

    DoubleField myfield;
    DoubleField *relative_field;
    char *printspec;
    double offset, multiplier;
};

class GF_Variable32 : public GeneralField {
public:
    enum printstyle_t {printnostyle, printhex, printmaybehex, printcsv};
    GF_Variable32(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column);
    virtual ~GF_Variable32();
    virtual void write(FILE *to);
    virtual void write(std::ostream &to);
    virtual bool isNull();
    virtual void set(GeneralField *from);
    virtual void set(const GeneralValue *from);
    void set(GF_Variable32 *from) {
	myfield.set(from->myfield.val(),from->myfield.size());
    }
    void clear() {
	myfield.clear();
    }
    char *printspec;
    printstyle_t printstyle;
    Variable32Field myfield;
};

class ExtentRecordCopy {
public:
    ExtentRecordCopy(ExtentSeries &source, ExtentSeries &dest);
    ~ExtentRecordCopy();
    void copyrecord();
private:
    int fixed_copy_size;
    ExtentSeries &source, &dest;
    std::vector<GeneralField *> sourcefields, destfields; // all fields here if f_c_s == 0
    std::vector<GF_Variable32 *> sourcevarfields, destvarfields; // only used if fixed_copy_size >0
};

#endif
