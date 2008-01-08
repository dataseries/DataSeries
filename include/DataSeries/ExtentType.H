// -*-C++-*-
/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Type information for an extent
*/

#ifndef __DATASERIES_EXTENTTYPE_H
#define __DATASERIES_EXTENTTYPE_H

#include <vector>
#include <string>
#include <map>

#include <libxml/tree.h>

#include <boost/utility.hpp>

#include <Lintel/AssertBoost.H>
#include <Lintel/Double.H>
#include <Lintel/LintelAssert.H> // TODO: make this go away
#include <Lintel/StringUtil.H>

class ExtentType : boost::noncopyable {
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

    // Might want CompactAll, so don't want a boolean here
    enum PackNullCompact { 
	CompactNonBool, CompactNo
    };

    static ExtentType &getDataSeriesXMLType() {
	return dataseries_xml_type;
    }
    static ExtentType &getDataSeriesIndexTypeV0() {
	return dataseries_index_type_v0;
    }
    // we have visible and invisible fields; visible fields are
    // counted by getnfields and accessible through getfieldname;
    // invisible fields can be retrieved through getColumnNumber (and
    // things that use that), but are not explicitly listed.  The
    // library reserves names starting with a space to name invisible
    // fields.
    bool hasColumn(const std::string &column) const {
	int cnum = getColumnNumber(rep,column);
	return cnum != -1;
    }

    fieldType getFieldType(const std::string &column) const {
	int cnum = getColumnNumber(rep,column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getFieldType(cnum);
    }
    int32 getSize(const std::string &column) const {
	int cnum = getColumnNumber(rep,column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getSize(cnum);
    }
    int32 getOffset(const std::string &column) const {
	int cnum = getColumnNumber(rep,column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getOffset(cnum);
    }
    int getBitPos(const std::string &column) const {
	int cnum = getColumnNumber(rep,column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getBitPos(cnum);
    }
    bool getUnique(const std::string &column) const {
	int cnum = getColumnNumber(rep,column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getUnique(cnum);
    }
    bool getNullable(const std::string &column) const {
	int cnum = getColumnNumber(rep,column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getNullable(cnum);
    }
    double getDoubleBase(const std::string &column) const {
	int cnum = getColumnNumber(rep,column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return getDoubleBase(cnum);
    }

    const std::string &getFieldName(unsigned int columnnum) const {
	INVARIANT(columnnum < rep.visible_fields.size(),
		  boost::format("invalid column num %d") % columnnum);
	int i = rep.visible_fields[columnnum];
	return rep.field_info[i].name;
    }
    const unsigned fixedrecordsize() const { return rep.fixed_record_size; }

    const uint32_t getNFields() const { return rep.visible_fields.size(); };

    static std::string nullableFieldname(const std::string &fieldname);

    std::string xmlFieldDesc(const std::string &column) const {
	int cnum = getColumnNumber(rep,column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return xmlFieldDesc(cnum);
    }
    xmlNodePtr xmlNodeFieldDesc(const std::string &column) const {
	int cnum = getColumnNumber(rep,column);
	AssertAlways(cnum != -1,("Unknown column '%s'\n",column.c_str()));
	return xmlNodeFieldDesc(cnum);
    }

    static const std::string &fieldTypeString(fieldType ft);
    static bool prefixmatch(const std::string &a, const std::string &prefix) {
	// TODO: deprecate this
	return prefixequal(a, prefix);
    }

    /// compatible if app_major == majorVersion() and app_minor <=
    /// minorVersion() the former because major version changes allow
    /// for arbitrary differences the latter because minor version
    /// changes have to be backwards compatible.

    bool versionCompatible(unsigned app_major, unsigned app_minor) const {
	return app_major == majorVersion() && app_minor <= minorVersion();
    }
    unsigned majorVersion() const { return rep.major_version; }
    unsigned minorVersion() const { return rep.minor_version; }
    PackNullCompact getPackNullCompact() const { 
	return rep.pack_null_compact; 
    }
    const std::string &getName() const { return rep.name; }
    const std::string &getXmlDescriptionString() const {
	return rep.xml_description_str;
    }
    const xmlDocPtr &getXmlDescriptionDoc() const {
	return rep.xml_description_doc;
    }
    const std::string &getNamespace() const { return rep.type_namespace; }

    struct fieldInfo;
    struct nullCompactInfo {
	fieldType type;
	uint32_t field_num;
	int32 size, offset; // The nullable data
	int32 null_offset;
	int null_bitmask;
	nullCompactInfo() : type(ft_unknown), field_num(0), 
			    size(0), offset(0), 
			    null_offset(0), null_bitmask(0) { }
    };

    struct fieldInfo {
	std::string name;
	fieldType type;
	// size is byte size used in the fixed record, bitpos is only
	// valid for bool fields.
	int32 size, offset, bitpos; 
	int null_fieldnum;
	bool unique;
	nullCompactInfo *null_compact_info;
	double doublebase;
	xmlNodePtr xmldesc;
	fieldInfo() : type(ft_unknown), size(-1), offset(-1), bitpos(-1),
		      null_fieldnum(-1), unique(false), 
		      null_compact_info(NULL), doublebase(0), xmldesc(NULL)
	{ }
    };

private:
    static ExtentType &dataseries_xml_type;
    static ExtentType &dataseries_index_type_v0;

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

    std::string xmlFieldDesc(int field_num) const;
    xmlNodePtr xmlNodeFieldDesc(int field_num) const;

    struct pack_scaleT {
	int field_num;
	double scale, multiplier;
	pack_scaleT(int a, double b) 
	    : field_num(a), scale(b), multiplier(1.0/b) {}
    };
    struct pack_other_relativeT {
	int field_num, base_field_num;
	pack_other_relativeT(int a, int b) : field_num(a), base_field_num(b) {}
    };
    struct pack_self_relativeT {
	unsigned field_num;
	double double_prev_v;
	double scale,multiplier;
	int32 int32_prev_v;
	int64 int64_prev_v;
	pack_self_relativeT(int a) 
	    : field_num(a), double_prev_v(0), scale(1), multiplier(1), 
	      int32_prev_v(0), int64_prev_v(0) {}
    };

    // All the parsed state is in here so that we can have a const
    // representation, and have a single parsing function that just
    // returns the structure rather than having to deal with each
    // piece separately as C++ only allows initialization of const
    // values in the constructor.
    struct ParsedRepresentation {
	std::string name, xml_description_str;
	xmlDocPtr xml_description_doc;

	// mapping from visible fields (defined in the XML) to the actual
	// field_info (which may have hidden fields)
	std::vector<int> visible_fields; 
	std::vector<fieldInfo> field_info;
	std::vector<nullCompactInfo> nonbool_compact_info_size1,
	    nonbool_compact_info_size4, nonbool_compact_info_size8; 
	int bool_bytes;
	std::vector<int32> variable32_field_columns;
	
	std::vector<pack_scaleT> pack_scale;
	std::vector<pack_other_relativeT> pack_other_relative;
	std::vector<pack_self_relativeT> pack_self_relative;

	int fixed_record_size;
	unsigned major_version, minor_version;
	std::string type_namespace;
	PackNullCompact pack_null_compact;
	void sortAssignNCI(std::vector<nullCompactInfo> &nci);
    };
    
    static int getColumnNumber(const ParsedRepresentation &rep,
			       const std::string &column);
    static int getColumnNumber(const ParsedRepresentation &rep,
			       const xmlChar *column) {
	return getColumnNumber(rep, reinterpret_cast<const char *>(column));
    }
    static ParsedRepresentation parseXML(const std::string &xmldesc);
    const ParsedRepresentation rep;

    friend class Variable32Field;
    friend class Extent;
    friend class ExtentTypeLibrary;

    ExtentType(const std::string &xmldesc);
    ~ExtentType();
public:
    // TODO: switch users of these to getters, then eliminate duplication
    // with ParsedRepresentation
    const std::string name;
    const std::string xmldesc;
    const xmlDocPtr field_desc_doc;
};

class ExtentTypeLibrary {
public:
    ExtentTypeLibrary() {};

    // TODO: make this return a reference (and change all the users)
    const ExtentType *registerType(const std::string &xmldesc);
    void registerType(const ExtentType &type);

    // ExtentType * values returned by these functions survive the
    // deletion of the library -- they are shared across all of the
    // ExtentTypeLibraries based on the actual XML type information

    const ExtentType *getTypeByName(const std::string &name, 
				    bool null_ok = false);
    const ExtentType *getTypeByPrefix(const std::string &prefix, 
				      bool null_ok = false);
    const ExtentType *getTypeBySubstring(const std::string &substr,
					 bool null_ok = false);
    // tries ByName, ByPrefix and BySubstring in that order.
    // selects a single unique non-dataseries type if match is "*"
    const ExtentType *getTypeMatch(const std::string &match,
				   bool null_ok = false);

    std::map<const std::string, const ExtentType *> name_to_type;
    static ExtentType &sharedExtentType(const std::string &xmldesc);
};

#endif
