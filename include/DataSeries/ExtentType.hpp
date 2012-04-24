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

#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>

#include <Lintel/AssertBoost.hpp>
#include <Lintel/CompilerMarkup.hpp>
#include <Lintel/Double.hpp>
#include <Lintel/StringUtil.hpp>

namespace dataseries { 
    struct xmlDecode; // Internal class for caching extent type translations.
}
/** \brief This class describes the format of records in an @c Extent.

  * Provides the mapping from the DataSeries XML descriptions to 
  *  - the total space used by the fixed size portion record.
  *  - the offsets of each member 
  *  - options specified for each member

  Example:
\verbatim
  <ExtentType name="test-type" namespace="example.com" version="1.0" comment="an example type">
      <field type="int32" name="input1" pack_relative="input1" />
      <field type="int32" name="input2" pack_relative="input1" />
      <field type="int64" name="int64-1" pack_relative="int64-1" opt_nullable="yes" />
      <field type="int64" name="int64-2" />
      <field type="double" name="double1" pack_scale="1e-6" pack_relative="double1" />
      <field type="variable32" name="var1" pack_unique="yes"/>\n"
      <field type="variable32" name="var2"/>\n"
      <field type="fixedwidth" name="fw1" size="7" note="experimental" />
      <field type="fixedwidth" name="fw2" size="20" note="experimental" />
  </ExtentType>
\endverbatim
  */
class ExtentType : boost::noncopyable, public boost::enable_shared_from_this<const ExtentType> {
public:
    /** A type that is guaranteed to be a 1 byte unsigned integer;
	obsolete, just use uint8_t */
    typedef uint8_t byte;
    /** A type that is guaranteed to be a 32 bit signed integer;
	obsolete, just use int32_t */
    typedef int32_t int32;
    /** A type that is guaranteed to be a 32 bit unsigned integer;
	obsolete, just use uint32_t */
    typedef uint32_t uint32;
    /** A type that is guaranteed to be a 64 bit signed integer;
	obsolete, just use int64_t */
    typedef int64_t int64;

    /** Shared pointer type for an ExtentType */
    typedef boost::shared_ptr<const ExtentType> Ptr;

    /** This enumeration identifies the type of field

        \internal
        if you add a new type or type option, you should update
        test.C:test_makecomplexfile() and the regression test. */
    enum fieldType {
        /** This is used as a default value in several places where
            the type needs to be set later or to indicate a nonexistent
            value, i.e. a null value. */
        ft_unknown = 0,
        /** Indicates a field which can hold true and false values. 
            Corresponds to the C++ type @c bool. */
        ft_bool,
        /** Indicates a field which can hold values in the range [0, 256).
            Corresponds to the C++ type @c uint8_t */
        ft_byte,
        /** Indicates a signed 32 bit integer. Corresponds to
            the C++ type @c int32_t. */
        ft_int32,
        /** Indicates a 64 bit signed integer. Corresponds to
            the C++ type @c int64_t */
        ft_int64,
        /** Indicates a double precision floating point number.
            Corresponds to the C++ type @c double. */
        ft_double,
        /** Indicates a field which can store up to 2^31 bytes of data.
            Most closely corresponds to the C++ type @c string. 

	    \internal Note that for alignment purposes @c variable32
	    is considered a 4 byte type, because the fixed size
	    portion is a 32 bit index into a string pool.  The first
	    four bytes at the indicated position are the length of the
	    string. */
        ft_variable32,
        /** Indicates a fixed-width byte array. */
        ft_fixedwidth
    };

    /** Determines which fields can be removed before compression when
        writing an Extent to a file.

        \internal
        Might want CompactAll, so don't want a boolean here. */
    enum PackNullCompact { 
        /** When a field other than a boolean field is being
            compressed before being written to a file, first check to
            see whether it has been marked as null. If it is, skip
            it. */
	CompactNonBool,
        /** Do not remove any null fields when compressing before
	    writing to a file. */
        CompactNo
    };

    /** \brief Determines how much padding to add to align records properly.

        The original padding was a mistake because it always padded to 8 bytes
        alignment. Future extent types may as well always set
        pack_pad_record="max_column_size"; there is no down side,
        although it is irrelevant (and wastes a few bytes of space to
        record the option in the type) if there are any 8 byte fields,
        or if the record size would otherwise be a multiple of 8
        bytes.
       
        \internal
        If we want to enable use of SSE style extensions, may need an
        option to pad differently.  Could also do a PadTight, although
        that would require different fields or would fail on machines that
        can't do unaliged accesses. */
    enum PackPadRecord {
        /** Align all records to an 8 byte boundary. This was the
            original format and is retained as the default for
            backwards compatibility. */
	PadRecordOriginal,
        /** Align records to the alignment of the widest column. For example,
            a record that contains an int32 and a byte will be aligned to a
            4 byte boundary, but one which contains a double will be aligned
            to an 8 byte boundary. */
        PadRecordMaxColumnSize
    };

    /** \brief Specifies the order that fields are stored in.

        Options for ordering the fields within a record; big to small
        happens to compress slightly better (2-4%) for the world cup
        traces.  May be slightly better overall as it puts all the
        padding together, but further testing is required to determine
        if one ordering dominates the other. Initial experiments
        indicate that it is data-dependent. */
    enum PackFieldOrdering {
        /** Put the smallest fields first; the default option. */
	FieldOrderingSmallToBigSepVar32,
        /** Put the largest fields first. */
        FieldOrderingBigToSmallSepVar32,
    };

    /** Returns the type of the Extent that stores the XML descriptions
        of all the ExtentTypes used in a DataSeries file. */
    static const ExtentType &getDataSeriesXMLType() {
	return *dataseries_xml_type;
    }
    /** Returns the type of the index of a DataSeries file. */
    static const ExtentType &getDataSeriesIndexTypeV0() {
	return *dataseries_index_type_v0;
    }

    // we have visible and invisible fields; visible fields are
    // counted by getnfields and accessible through getfieldname;
    // invisible fields can be retrieved through getColumnNumber (and
    // things that use that), but are not explicitly listed.  The
    // library reserves names starting with a space to name invisible
    // fields.

    /** Returns true iff this ExtentType contains a field with the
        given name. */
    bool hasColumn(const std::string &column) const {
	int cnum = getColumnNumber(rep,column);
	return cnum != -1;
    }

    /** Returns the type of the field with the specified name.

        Preconditions:
        - The field must exist */
    fieldType getFieldType(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return getFieldType(cnum);
    }

    /** Converts a specified field type into the string representation
        that would be used in the xml specification */
    static const std::string &fieldTypeToStr(fieldType type);

    const std::string &getFieldTypeStr(const std::string &column) const {
        return fieldTypeToStr(getFieldType(column));
    }

    /** Returns the size of a field in bytes. For bool and byte fields
        the result is 1, for int32 fields it is 4 and for double and
        int64 fields, 8. It is an error to call this for variable32
        fields because the size can be different in each record.

        Preconditions:
        - The field with the specified name exists and is not
	  a @c variable32 field. */
    int32 getSize(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return getSize(cnum);
    }
    /** Returns the byte offset of a field in a record.

        Preconditions:
        - The field exists. */
    int32 getOffset(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return getOffset(cnum);
    }
    /** Returns the bit position of a boolean field within the
        byte determined by @c getOffset. The result is in the
        range [0, 8)

        Preconditions:
        - The field exists and is a @c bool field. */
    int getBitPos(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return getBitPos(cnum);
    }
    /** Returns true for a @c variable32 field which has been marked
        as unique. This means that identical values in the same @c
        Extent will be combined, including if they are for different
        fields that have both been marked as unique.

        Preconditions:
        - The field exists and is a @c variable32 field. */
    bool getUnique(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return getUnique(cnum);
    }
    /** Returns true if a field is nullable. A nullable field does not have
        to be present in any given record.

        Preconditions:
        - The specified field exists.*/
    bool getNullable(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return getNullable(cnum);
    }
    /** Returns the double base for the field. The functions
        @c DoubleField::getabs and @c DoubleField::setabs use this. For
        example, if the double base is 100.0 and the value of a
        particular field is 1.0 than @c DoubleField::absval will
        return 101.0. @c DoubleField::setabs does the reverse conversion. 
	
	This option was created to allow for more precision of large
	numbers, e.g. storing the current time in doubles with
	nanosecond precision.  It is retained for backwards
	compatibility, but is now considered to be a bad idea. */
    double getDoubleBase(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return getDoubleBase(cnum);
    }

    /** Returns the name of a field at a particular index.

        Preconditions:
        - columnIndex < getNFields() */
    const std::string &getFieldName(unsigned int columnnum) const {
	INVARIANT(columnnum < rep.visible_fields.size(),
		  boost::format("invalid column num %d") % columnnum);
	int i = rep.visible_fields[columnnum];
	return rep.field_info[i].name;
    }

    /** Returns the size of the fixed portion of each record. i.e.
        everything but the storage for variable32 fields. */
    unsigned fixedrecordsize() const { return rep.fixed_record_size; }

    /** Returns the number of visible fields. This does not include
        the hidden fields used to determine whether a nullable field
        has a value. */
    uint32_t getNFields() const { return rep.visible_fields.size(); };

    /** Returns the name of the hidden boolean field used to indicate
        whether the specified field is null. */
    static std::string nullableFieldname(const std::string &fieldname);

    /** Returns the XML associated with the given field as a @c std::string */
    std::string xmlFieldDesc(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return xmlFieldDesc(cnum);
    }
    /** Returns the XML associated with the given field as an @c xmlNodePtr */
    xmlNodePtr xmlNodeFieldDesc(const std::string &column) const {
	int cnum = getColumnNumber(rep, column, false);
	return xmlNodeFieldDesc(cnum);
    }

    /** Returns the string used to represent the type in XML */
    static const std::string &fieldTypeString(fieldType ft);

    /** versionCompatible() determines whether Extents created using this type
        are compatible with an application created for a possibly different
        version of the ExtentType.  The version number is of the form
        major.minor with the semantics that minor versions are only allowed
        to add new fields, whereas major versions can remove fields, rename
        fields or change field semantics.  This means that analysis
        code that can process version 1.x will work on any version 1.y
        for y >= x, but may or may not work on version 2.0.
        
        \internal
        Update doc/tr/design.tex if you change this description. */
    bool versionCompatible(unsigned app_major, unsigned app_minor) const {
	return app_major == majorVersion() && app_minor <= minorVersion();
    }
    /** Returns the major version of the ExtentType.  Note that every
        extent type is versioned separately. */
    uint32_t majorVersion() const { return rep.major_version; }
    /** Returns the minor version of the ExtentType.  Note that every
        extent type is versioned separately. */
    uint32_t minorVersion() const { return rep.minor_version; }
    /** Returns a flag indicating how null fields are compressed when
        writing to a file. */
    PackNullCompact getPackNullCompact() const { 
	return rep.pack_null_compact; 
    }
    /** Returns the name of the ExtentType. This corresponds the the "name"
        attribute in the XML. */
    const std::string &getName() const { return rep.name; }
    /** Returns the XML that describes the ExtentType as a @c std::string. */
    const std::string &getXmlDescriptionString() const {
	return rep.xml_description_str;
    }
    /** Returns the XML that describes the ExtentType as a @c xmlDocPtr. */
    const xmlDocPtr &getXmlDescriptionDoc() const {
	return rep.xml_description_doc;
    }
    /** Returns the namespace of the ExtentType. Corresponds to the
        "namespace" XML attribute.*/
    const std::string &getNamespace() const { return rep.type_namespace; }

    /// \cond INTERNAL_ONLY

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
    static std::string strGetXMLProp(xmlNodePtr cur, const std::string &option_name,
				     bool empty_ok = false);

    ~ExtentType();
private:
    static const ExtentType::Ptr dataseries_xml_type;
    static const ExtentType::Ptr dataseries_index_type_v0;

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

	int32_t fixed_record_size;
	uint32_t major_version, minor_version;
	std::string type_namespace;
	PackNullCompact pack_null_compact;
	PackPadRecord pad_record;
	PackFieldOrdering field_ordering;
	void sortAssignNCI(std::vector<nullCompactInfo> &nci);

	~ParsedRepresentation() {
	    xmlFreeDoc(xml_description_doc);
	}
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

    friend class Extent;
    friend class Variable32Field;
    friend class ExtentTypeLibrary;
    friend class dataseries::xmlDecode;

    ExtentType(const std::string &xmldesc);
private:
    static void parsePackBitFields(ParsedRepresentation &ret, 
				   int32 &byte_pos);
    static void parsePackByteAlignedFields(ParsedRepresentation &ret, 
                                           int32 &byte_pos);
    static void parsePackInt32Fields(ParsedRepresentation &ret, 
				     int32 &byte_pos);
    static void parsePackVar32Fields(ParsedRepresentation &ret, 
				     int32 &byte_pos);
    static void parsePackSize8Fields(ParsedRepresentation &ret, 
				     int32 &byte_pos);

    
    /// \endcond INTERNAL_ONLY
};

/** \brief This class represents a group of ExtentTypes keyed by name.
  
  * The primary use is to store the types from a single file.
  * All ExtentTypes are ultimately created by
  * @c ExtentTypeLibrary::sharedExtentType and thus survive the
  * deletion of the library -- they are shared across all of the
  * ExtentTypeLibraries based on the actual XML type information */
class ExtentTypeLibrary : boost::noncopyable {
public:
    ExtentTypeLibrary() {};

    // TODO: all the ExtentType *'s in here should turn into smart_ptr's so that we can
    // use weak pointers inside and the use of bare extent types should be removed.
    /** You should migrate to registerTypePtr.  This function will be removed after 2013-05-01 */
    const ExtentType *registerType(const std::string &xmldesc) FUNC_DEPRECATED {
	return registerTypePtr(xmldesc).get();
    }

    /** You should migrate to registerTypePtr.  This function will be removed after 2013-05-01 
        replace-string 'ExtentType &(.+)\.registerTypeR' 'ExtentType::Ptr $1.registerTypePtr' *.pp
    */
    const ExtentType &registerTypeR(const std::string &xmldesc) FUNC_DEPRECATED {
        return *registerTypePtr(xmldesc);
    }

    /** Creates an ExtentType from an XML description using @c sharedExtentType
        and stores it in the map.  Returns a shared pointer to the ExtentType.  This
        name is transitory, and will revert back to registerType once we have
        completed deprecating registerType()

        Preconditions:
        - xmldesc must be valid XML.
    */
    const ExtentType::Ptr registerTypePtr(const std::string &xmldesc);

    /** Adds an ExtentType to the map */
    void registerType(const ExtentType &type);

    /** Find an @c ExtentType whose name is known exactly.
        If @c null_ok is true then, if the name is not present
        in the @c ExtentTypeLibrary, returns a null pointer.
        Otherwise, it is an error if there is no @c ExtentType
        with the given name in the library */
    const ExtentType *getTypeByName(const std::string &name, 
				    bool null_ok = false) const;
    /** Find an ExtentType whose name begins with a known prefix.
        null_ok has the same meaning as in @c getTypeByName.
        Preconditions:
        - Only one type in the library matches. */
    const ExtentType *getTypeByPrefix(const std::string &prefix, 
				      bool null_ok = false) const;
    /** Find an ExtentType whose name contains some substring.
        null_ok has the same meaning as in @c getTypeByName.
        Preconditions:
        - Only one type in the library matches. */
    const ExtentType *getTypeBySubstring(const std::string &substr,
					 bool null_ok = false) const;
    /* tries ByName, ByPrefix and BySubstring in that order.
       selects a single unique non-dataseries type if match is "*" */
    const ExtentType *getTypeMatch(const std::string &match,
				   bool null_ok = false, bool skip_info=false);

    /** Stores the known @c ExtentType.  You should not modify this
        directly.  Use @c registerType.  The only time this map should
        be used directly is to iterate over the library elements. 

	\todo TODO: find some other way to do this so that people
	can't do something bogus, e.g. return a const_iterator.
    */

    typedef std::map<const std::string, ExtentType::Ptr> NameToType;
    NameToType name_to_type;

    /** Creates or looks up an ExtentType object corresponding to the given XML.  Identical XML
        descriptions will yield the same ExtentType object.  The ExtentTypes returned are valid so
        long as a pointer to the shared pointer remains. */
    static const ExtentType::Ptr sharedExtentTypePtr(const std::string &xmldesc);

    /** Reference version, prefer to use the shared pointer version */
    static const ExtentType &sharedExtentType(const std::string &xmldesc) FUNC_DEPRECATED {
        return *sharedExtentTypePtr(xmldesc);
    }
};

#endif
