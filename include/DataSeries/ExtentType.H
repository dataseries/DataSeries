/* -*-C++-*-
*******************************************************************************
*
* File:         ExtentType.H
* RCS:          $Header: /mount/cello/cvs/DataSeries/include/DataSeries/ExtentType.H,v 1.8 2004/09/28 05:08:31 anderse Exp $
* Description:  Type information for an extent
* Author:       Eric Anderson
* Created:      Thu May  8 04:43:23 2003
* Modified:     Mon Sep 27 22:00:53 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#ifndef __DATASERIES_EXTENTTYPE_H
#define __DATASERIES_EXTENTTYPE_H

#include <vector>
#include <string>
#include <map>

#include <libxml/tree.h>

#include <LintelAssert.H>
#include <Double.H>

class ExtentType {
public:
    typedef unsigned char byte;
    typedef int int32;
    typedef unsigned int uint32;
    typedef long long int64;

    static const int64 int64_min = LONG_LONG_MIN;
    static const int64 int64_max = LONG_LONG_MAX;

    // if you add a new type or type option, you should update
    // test.C:test_makecomplexfile() and the regression test.
    enum fieldType { ft_unknown = 0, ft_bool, ft_byte, ft_int32, ft_int64, 
		     ft_double, ft_variable32 };

    // we have visible and invisible fields; visible fields are
    // counted by getnfields and accessible through getfieldname;
    // invisible fields can be retrieved through getColumnNumber (and
    // things that use that), but are not explicitly listed.  The
    // library reserves names starting with a space to name invisible
    // fields.
    bool hasColumn(const std::string &column) const {
	int cnum = getColumnNumber(column);
	return cnum != -1;
    }

    fieldType getFieldType(const std::string &column) const {
	int cnum = getColumnNumber(column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getFieldType(cnum);
    }
    int32 getSize(const std::string &column) const {
	int cnum = getColumnNumber(column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getSize(cnum);
    }
    int32 getOffset(const std::string &column) const {
	int cnum = getColumnNumber(column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getOffset(cnum);
    }
    int getBitPos(const std::string &column) const {
	int cnum = getColumnNumber(column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getBitPos(cnum);
    }
    bool getUnique(const std::string &column) const {
	int cnum = getColumnNumber(column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getUnique(cnum);
    }
    bool getNullable(const std::string &column) const {
	int cnum = getColumnNumber(column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getNullable(cnum);
    }
    double getDoubleBase(const std::string &column) const {
	int cnum = getColumnNumber(column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getDoubleBase(cnum);
    }

    const std::string &getFieldName(unsigned int columnnum) const {
	AssertAlways(columnnum < visible_fields.size(),
		     ("invalid column num %d\n",columnnum));
	int i = visible_fields[columnnum];
	return field_info[i].name;
    }
    const int fixedrecordsize() const { return fixed_record_size; }

    ExtentType(const std::string &xmldesc);
    const int getNFields() const { return visible_fields.size(); };

    static std::string nullableFieldname(const std::string &fieldname);

    std::string name;
    std::string xmldesc;
    xmlDocPtr field_desc_doc;
    std::string xmlFieldDesc(const std::string &column) const {
	int cnum = getColumnNumber(column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return xmlFieldDesc(cnum);
    }
    xmlNodePtr xmlNodeFieldDesc(const std::string &column) const {
	int cnum = getColumnNumber(column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return xmlNodeFieldDesc(cnum);
    }

    static const std::string &fieldTypeString(fieldType ft);
    static bool prefixmatch(const std::string &a, const std::string &prefix);
protected:
    // a compelling case has been made that identifying fields by
    // column number is not necessary (the only use so far is for
    // generic programs that operate on anything, such as ds2txt),
    // also supporting both modes complicates the code, encourages the
    // wrong behavior, and makes it more difficult for fields to cross
    // record types.  Therefore we hide these implementations.

    fieldType getFieldType(int column) const;
    int32 getSize(int column) const;
    int32 getOffset(int column) const;
    int getBitPos(int column) const;
    bool getUnique(int column) const;
    bool getNullable(int column) const;
    double getDoubleBase(int column) const;

    int getColumnNumber(const std::string &column) const;
    std::string xmlFieldDesc(int field_num) const;
    xmlNodePtr xmlNodeFieldDesc(int field_num) const;

    struct fieldInfo {
	std::string name;
	fieldType type;
	int32 size, offset, bitpos;
	bool unique, nullable;
	double doublebase;
	xmlNodePtr xmldesc;
	fieldInfo() : type(ft_unknown), size(-1), offset(-1), bitpos(-1),
		      unique(false), nullable(false), doublebase(0), xmldesc(NULL)
	{ }
    };

    std::vector<int> visible_fields; // mapping from visible fields (defined in the XML)
    // to the actual field_info (which may have hidden fields)
    std::vector<fieldInfo> field_info;

    std::vector<int32> variable32_field_columns;

    struct pack_scaleT {
	int field_num;
	double scale, multiplier;
	pack_scaleT(int a, double b) 
	    : field_num(a), scale(b), multiplier(1.0/b) {}
    };
    std::vector<pack_scaleT> pack_scale;
    struct pack_other_relativeT {
	int field_num, base_field_num;
	pack_other_relativeT(int a, int b) : field_num(a), base_field_num(b) {}
    };
    std::vector<pack_other_relativeT> pack_other_relative;
    struct pack_self_relativeT {
	int field_num;
	double double_prev_v;
	double scale,multiplier;
	int32 int32_prev_v;
	int64 int64_prev_v;
	pack_self_relativeT(int a) 
	    : field_num(a), double_prev_v(0), scale(1), multiplier(1), int32_prev_v(0), int64_prev_v(0) {}
    };
    std::vector<pack_self_relativeT> pack_self_relative;

    int fixed_record_size;

    friend class Variable32Field;
    friend class Extent;
};

class ExtentTypeLibrary {
public:
    ExtentTypeLibrary() {};

    ExtentType *registerType(const std::string &xmldesc);
    ExtentType *getTypeByName(const std::string &name, bool null_ok = false);
    ExtentType *getTypeByPrefix(const std::string &prefix, bool null_ok = false);

    std::map<const std::string,ExtentType *> name_to_type;
    static ExtentType *sharedExtentType(const std::string &xmldesc);
    static xmlDocPtr sharedDocPtr(const std::string &xmldesc);
};

#endif
