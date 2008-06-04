// -*-C++-*-
/*
   (c) Copyright 2003-2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Type information for an extent
*/

#ifndef __DATASERIES_EXTENTTYPE_H
#define __DATASERIES_EXTENTTYPE_H

#include <inttypes.h>

#include <vector>
#include <string>
#include <map>

#include <libxml/tree.h>

#include <boost/utility.hpp>

#include <Lintel/AssertBoost.hpp>
#include <Lintel/Double.hpp>
#include <Lintel/StringUtil.hpp>

class ExtentType : boost::noncopyable {
public:
    typedef unsigned char byte;
    typedef int32_t int32;
    typedef uint32_t uint32;
    typedef int64_t int64;

    // if you add a new type or type option, you should update
    // test.C:test_makecomplexfile() and the regression test.
    enum fieldType { ft_unknown = 0, ft_bool, ft_byte, ft_int32, ft_int64, 
		     ft_double, ft_variable32 };

    // Might want CompactAll, so don't want a boolean here
    enum PackNullCompact { 
	CompactNonBool, CompactNo
    };

    /// Original padding was a mistake, it padded to 8 bytes
    /// always. Future extent types may as well always set
    /// pack_pad_record="max_column_size"; there is no down side,
    /// although it is irrelevant (and wastes a few bytes of space to
    /// record the option in the type) if there are any 8 byte fields,
    /// or if the record size would otherwise be a multiple of 8
    /// bytes.
    ///
    /// If we want to enable use of SSE style extensions, may need an
    /// option to pad differently.  Could also do a PadTight, although
    /// that would require different fields or would fail on machines that
    /// can't do unaliged accesses.
    enum PackPadRecord {
	PadRecordOriginal, PadRecordMaxColumnSize
    };

    /// Options for ordering the fields within a record; big to small
    /// happens to compress slightly better (2-4%) for the world cup
    /// traces.  May be slightly better overall as it puts all the
    /// padding together, but further testing is required to determine
    /// if one ordering dominates the other.
    enum PackFieldOrdering {
	FieldOrderingSmallToBigSepVar32, FieldOrderingBigToSmallSepVar32,
    };

    static const ExtentType &getDataSeriesXMLType() {
	return dataseries_xml_type;
    }
    static const ExtentType &getDataSeriesIndexTypeV0() {
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
	int cnum = getColumnNumber(rep, column, false);
	return getFieldType(cnum);
    }
    int32 getSize(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return getSize(cnum);
    }
    int32 getOffset(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return getOffset(cnum);
    }
    int getBitPos(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return getBitPos(cnum);
    }
    bool getUnique(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return getUnique(cnum);
    }
    bool getNullable(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return getNullable(cnum);
    }
    double getDoubleBase(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
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
	int cnum = getColumnNumber(rep, column, false);
	return xmlFieldDesc(cnum);
    }
    xmlNodePtr xmlNodeFieldDesc(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return xmlNodeFieldDesc(cnum);
    }

    static const std::string &fieldTypeString(fieldType ft);
    static bool prefixmatch(const std::string &a, const std::string &prefix) {
	// TODO: deprecate this
	return prefixequal(a, prefix);
    }

    // Update doc/tr/design.tex if you change this.

    /// versionCompatible() checks if the versions of the extents are
    /// compatible.  The version number is of the form major.minor
    /// with the semantics that minor versions are only allowed to add
    /// new fields, whereas major versions can remove fields, rename
    /// fields or change field semantics.  This means that analysis
    /// code that can process version 1.x will work on any version 1.y
    /// for y $\geq$ x, but may or may not work on version 2.0.
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

    // utility function, should go somewhere else.
    static std::string strGetXMLProp(xmlNodePtr cur, 
				     const std::string &option_name);
private:
    static const ExtentType &dataseries_xml_type;
    static const ExtentType &dataseries_index_type_v0;

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
	PackPadRecord pad_record;
	PackFieldOrdering field_ordering;
	void sortAssignNCI(std::vector<nullCompactInfo> &nci);
    };
    
    static int getColumnNumber(const ParsedRepresentation &rep,
			       const std::string &column,
			       bool missing_ok = true);
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
private:
    static void parsePackBitFields(ParsedRepresentation &ret, 
				   int32 &byte_pos);
    static void parsePackByteFields(ParsedRepresentation &ret, 
				    int32 &byte_pos);
    static void parsePackInt32Fields(ParsedRepresentation &ret, 
				     int32 &byte_pos);
    static void parsePackVar32Fields(ParsedRepresentation &ret, 
				     int32 &byte_pos);
    static void parsePackSize8Fields(ParsedRepresentation &ret, 
				     int32 &byte_pos);
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
    static const ExtentType &sharedExtentType(const std::string &xmldesc);
};

#endif
