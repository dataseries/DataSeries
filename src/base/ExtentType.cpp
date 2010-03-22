// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    ExtentType class implementation
*/

#include <vector>

#include <libxml/parser.h>

#include <Lintel/AssertBoost.hpp>
#include <Lintel/HashMap.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/PThread.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/ExtentType.hpp>

using namespace std;
using boost::format;

static const string dataseries_xml_type_xml = 
  "<ExtentType name=\"DataSeries: XmlType\">\n"
  "  <field type=\"variable32\" name=\"xmltype\" />\n"
  "</ExtentType>\n";

const string dataseries_index_type_v0_xml =
  "<ExtentType name=\"DataSeries: ExtentIndex\">\n"
  "  <field type=\"int64\" name=\"offset\" />\n"
  "  <field type=\"variable32\" name=\"extenttype\" />\n"
  "</ExtentType>\n";

// The following is here as we are working out what the next version
// of the extent index should look like; I think we will be able to
// get away with putting it into the xmltype index and hence be able 
// to update this as we see fit.

const string dataseries_index_type_v1_xml =
  "<ExtentType name=\"DataSeries::ExtentIndex\" >\n"
  "  <!-- next fields are necessary/useful for finding the extents that\n"
  "       a program wants to process without having a separate index file -->\n"
  "  <field type=\"int64\" name=\"offset\" pack_relative=\"offset\" />\n"
  "  <field type=\"int32\" name=\"size\" comment=\"header+data+padding\" />\n"
  "  <field type=\"variable32\" name=\"extenttype\" pack_unique=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"namespace\" pack_unique=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"version\" pack_unique=\"yes\" />\n"
// Technically the next bits are in the header for each extent; this
// would allow for the possibility of reading the files without
// reading the index at the end, although this is not currently
// supported.  However, these end up being useful for figuring out
// properties of a given DS file without having to write a separate
// interface that can skitter through a file and extract all of this
// information.
  "  <field type=\"byte\" name=\"fixed_compress_mode\" />\n"
  "  <field type=\"int32\" name=\"fixed_uncompressed_size\" />\n"
  "  <field type=\"int32\" name=\"fixed_compressed_size\" />\n"
  "  <field type=\"byte\" name=\"variable_compress_mode\" />\n"
  "  <field type=\"int32\" name=\"variable_uncompressed_size\" />\n"
  "  <field type=\"int32\" name=\"variable_compressed_size\" />\n"
  "</ExtentType>\n";


const ExtentType &ExtentType::dataseries_xml_type(ExtentTypeLibrary::sharedExtentType(dataseries_xml_type_xml));
const ExtentType &ExtentType::dataseries_index_type_v0(ExtentTypeLibrary::sharedExtentType(dataseries_index_type_v0_xml));

string ExtentType::strGetXMLProp(xmlNodePtr cur, const string &option_name, bool empty_ok) {
    xmlChar *option = xmlGetProp(cur, reinterpret_cast<const xmlChar *>(option_name.c_str()));
    if (option == NULL) {
	return string();
    } else {
	INVARIANT(empty_ok || *option != '\0', 
		  format("Invalid specification of empty property '%s'") % option_name);
	string ret(reinterpret_cast<char *>(option));
	xmlFree(option);
	return ret;
    }
}

static bool parseYesNo(xmlNodePtr cur, const string &option_name, bool default_val) {
    string option = ExtentType::strGetXMLProp(cur, option_name);
    
    if (option.empty()) {
	return default_val;
    }
    if (option == "yes") {
	return true;
    } else if (option == "no") {
	return false;
    } else {
	FATAL_ERROR(format("%s should be either 'yes' or 'no', not '%s'")
		    % option_name % option);
	return false;
    }
}

struct NonBoolCompactByPosition {
    bool operator()(const ExtentType::nullCompactInfo &a, 
		    const ExtentType::nullCompactInfo &b) const {
	return a.offset < b.offset;
    }
};

void ExtentType::ParsedRepresentation::sortAssignNCI(vector<nullCompactInfo> &nci) {
    sort(nci.begin(), nci.end(), NonBoolCompactByPosition());

    for(vector<nullCompactInfo>::iterator i = nci.begin();
	i != nci.end(); ++i) {
	INVARIANT(i->offset == field_info[i->field_num].offset, "?");
	field_info[i->field_num].null_compact_info = &*i;
    }
}

void ExtentType::parsePackBitFields(ParsedRepresentation &ret, int32 &byte_pos) {
    LintelLogDebug("ExtentType::Packing", "packing bool fields...\n");
    int32 bit_pos = 0;
    for(unsigned int i=0; i<ret.field_info.size(); i++) {
	if (ret.field_info[i].type == ft_bool) {
	    ret.field_info[i].size = 1;
	    ret.field_info[i].offset = byte_pos;
	    ret.field_info[i].bitpos = bit_pos;
	    DEBUG_INVARIANT(bit_pos < 8, "?");
            LintelLogDebug("ExtentType::Packing", boost::format("  field %s at position %d:%d\n")
                           % ret.field_info[i].name % byte_pos % bit_pos);
	    ++bit_pos;
	    if (bit_pos == 8) {
		byte_pos += 1;
		bit_pos = 0;
	    }
	} 
    }

    if (bit_pos > 0) {
	byte_pos += 1;
    }
}

void ExtentType::parsePackByteAlignedFields(ParsedRepresentation &ret, int32 &byte_pos) {
    LintelLogDebug("ExtentType::Packing", "packing byte-aligned fields...\n");
    for(unsigned int i=0; i<ret.field_info.size(); ++i) {
	if (ret.field_info[i].type == ft_byte) {
	    ret.field_info[i].size = 1;
	    ret.field_info[i].offset = byte_pos;
            LintelLogDebug("ExtentType::Packing", boost::format("  field %s at position %d\n")
                           % ret.field_info[i].name % byte_pos);
	    byte_pos += 1;
	} else if (ret.field_info[i].type == ft_fixedwidth) {
	    INVARIANT(ret.field_info[i].size > 0, "the size should have been set already");
	    ret.field_info[i].offset = byte_pos;
            LintelLogDebug("ExtentType::Packing", boost::format("  field %s at position %d\n")
                           % ret.field_info[i].name % byte_pos);
	    byte_pos += ret.field_info[i].size;
	}
    }
}

void ExtentType::parsePackInt32Fields(ParsedRepresentation &ret, int32 &byte_pos) {
    LintelLogDebug("ExtentType::Packing", "packing int32 fields...\n");
    for(unsigned int i=0; i<ret.field_info.size(); i++) {
	if (ret.field_info[i].type == ft_int32) {
	    ret.field_info[i].size = 4;
	    ret.field_info[i].offset = byte_pos;
	    SINVARIANT((byte_pos % 4) == 0);
            LintelLogDebug("ExtentType::Packing", boost::format("  field %s (#%d) at position %d\n")
                           % ret.field_info[i].name % i % byte_pos);
	    byte_pos += 4;
	}
    }
}

void ExtentType::parsePackVar32Fields(ParsedRepresentation &ret, int32 &byte_pos) {
    // these tend to have lots of different values making compression
    // worse, so we pack them after the other int32 fields, but to
    // avoid alignment glitches before the 8 byte fields
    LintelLogDebug("ExtentType::Packing", "packing variable32 fields...\n");
    for(unsigned int i=0; i<ret.field_info.size(); i++) {
	if (ret.field_info[i].type == ft_variable32) {
	    ret.field_info[i].size = 4;
	    ret.field_info[i].offset = byte_pos;
	    SINVARIANT((byte_pos % 4) == 0);
            LintelLogDebug("ExtentType::Packing", boost::format("  field %s (#%d) at position %d\n")
                           % ret.field_info[i].name % i % byte_pos);
	    byte_pos += 4;
	}
    }
}

void ExtentType::parsePackSize8Fields(ParsedRepresentation &ret, int32 &byte_pos) {
    LintelLogDebug("ExtentType::Packing", "packing int64 and double fields...\n");
    for(unsigned int i=0; i<ret.field_info.size(); i++) {
	if (ret.field_info[i].type == ft_int64 
	    || ret.field_info[i].type == ft_double) {
	    ret.field_info[i].size = 8;
	    ret.field_info[i].offset = byte_pos;
	    SINVARIANT((byte_pos % 8) == 0);
            LintelLogDebug("ExtentType::Packing", boost::format("  field %s at position %d\n")
                           % ret.field_info[i].name % byte_pos);
	    byte_pos += 8;
	}
    }
}

// TODO: figure out how to split up this function.
ExtentType::ParsedRepresentation ExtentType::parseXML(const string &xmldesc) {
    ParsedRepresentation ret;

    INVARIANT(sizeof(byte) == 1 && sizeof(int32) == 4 &&
	      sizeof(uint32) == 4 && sizeof(int64) == 8,
	      "sizeof check bad");
    ret.xml_description_str = xmldesc;

    LIBXML_TEST_VERSION;
    xmlKeepBlanksDefault(0);
    ret.xml_description_doc 
	= xmlParseMemory(xmldesc.c_str(),xmldesc.size());
    INVARIANT(ret.xml_description_doc != NULL,
	      "Error: parsing ExtentType description failed");

    xmlNodePtr cur = xmlDocGetRootElement(ret.xml_description_doc);
    INVARIANT(cur != NULL, "Error: ExtentType description missing document");
    INVARIANT(xmlStrcmp(cur->name, (const xmlChar *) "ExtentType") == 0,
	      boost::format("Error: ExtentType description has wrong type, '%s' != '%s'") 
	      % cur->name % "ExtentType");

    ret.name = strGetXMLProp(cur, "name");
    INVARIANT(!ret.name.empty(), "Error, ExtentType missing name");

    INVARIANT(ret.name.length() <= 255,
	      "invalid extent type name, max of 255 characters allowed");

    /// ************************************************************
    /// If you add in new packing options, you should update the TR to
    /// describe the options and any experiments that were used to
    /// evaluate them.
    /// ************************************************************

    ret.pack_null_compact = CompactNo;
    {
	string pack_option = strGetXMLProp(cur, "pack_null_compact");
	if (!pack_option.empty()) {
	    // TODO: remove the warning once we try this with a second data set.
	    LintelLog::warn("pack_null_compact under testing, may not be safe for use.\n");
	    if (pack_option == "non_bool") {
		ret.pack_null_compact = CompactNonBool;
	    } else if (pack_option == "no") {
		ret.pack_null_compact = CompactNo;
	    } else {
		FATAL_ERROR(format("Unknown pack_null_compact value '%s', expected non_bool or no")
			    % pack_option);
	    }
	}
    }
    ret.pad_record = PadRecordOriginal;
    {
	string pad_record_option = strGetXMLProp(cur, "pack_pad_record");
	if (!pad_record_option.empty()) {
	    LintelLog::warn("Warning, pack_pad_record under testing, may not be safe for use.\n");
	    if (pad_record_option == "original") {
		ret.pad_record = PadRecordOriginal;
	    } else if (pad_record_option == "max_column_size") {
		ret.pad_record = PadRecordMaxColumnSize;
	    } else {
		FATAL_ERROR(format("Unknown pack_pad_record value '%s', expect original or max_column_size") % pad_record_option);
	    }
	}
    }
    ret.field_ordering = FieldOrderingSmallToBigSepVar32;
    {
	string field_ordering_opt = strGetXMLProp(cur, "pack_field_ordering");
	if (!field_ordering_opt.empty()) {
	    LintelLog::warn("Warning, pack_field_ordering under testing, may not be safe for use.\n");
	    if (field_ordering_opt == "small_to_big_sep_var32") {
		ret.field_ordering = FieldOrderingSmallToBigSepVar32;
	    } else if (field_ordering_opt == "big_to_small_sep_var32") {
		ret.field_ordering = FieldOrderingBigToSmallSepVar32;
	    } else {
		FATAL_ERROR(format("Unknown pack_field_ordering value '%s', expect small_to_big_sep_var32 or big_to_small_sep_var32") % field_ordering_opt);
	    }
	}
    }

    for(xmlAttr *prop = cur->properties; prop != NULL; prop = prop->next) {
	string opt(reinterpret_cast<const char *>(prop->name));
	if (opt == "pack_null_compact" || opt == "pack_pad_record"
	    || opt == "pack_field_ordering") {
	    // ok
	} else {
	    INVARIANT(!prefixequal(opt, "pack_"),
		      format("Unrecognized global packing option %s") % opt);
	    INVARIANT(!prefixequal(opt, "opt_"),
		      format("Unrecognized global option %s") % opt);
	}
    }
    LintelLogDebug("ExtentType::XMLDecode", boost::format("ExtentType '%s'\n") % ret.name);

    string extentversion = strGetXMLProp(cur, "version");
    if (extentversion.empty()) {
	ret.major_version = 0;
	ret.minor_version = 0;
    } else {
	vector<string> bits;
	split(extentversion, ".", bits);
	INVARIANT(bits.size() == 2, 
		  boost::format("bad version '%s' should be #.#") 
		  % extentversion);
	ret.major_version = stringToInteger<int32_t>(bits[0]);
	ret.minor_version = stringToInteger<int32_t>(bits[1]);
    }

    ret.type_namespace = strGetXMLProp(cur, "namespace", true);

    cur = cur->xmlChildrenNode;
    unsigned bool_fields = 0, byte_fields = 0, int32_fields = 0, 
	eight_fields = 0, variable_fields = 0;
    while (true) {
	if (cur == NULL) 
	    break;
 	INVARIANT(xmlStrcmp(cur->name, (const xmlChar *)"field") == 0,
 		  boost::format("Error: ExtentType sub-element should be"
 				" field, not '%s'") % cur->name);

	for(xmlAttr *prop = cur->properties; prop != NULL; prop = prop->next) {
	    // if you add a new type or type option, you should update
	    // test.C:test_makecomplexfile() and the regression test.
	    if (xmlStrcmp(prop->name,(const xmlChar *)"pack_scale") == 0) {
		// ok
	    } else if (xmlStrcmp(prop->name,(const xmlChar *)"pack_relative") == 0) {
		// ok
	    } else if (xmlStrcmp(prop->name,(const xmlChar *)"pack_unique") == 0) {
		// ok
	    } else if (xmlStrcmp(prop->name,(const xmlChar *)"opt_doublebase") == 0) {
		// ok
	    } else if (xmlStrcmp(prop->name,(const xmlChar *)"opt_nullable") == 0) {
		// ok
	    } else {
		INVARIANT(xmlStrncmp(prop->name,(const xmlChar *)"pack_",5)!=0,
			  boost::format("Unrecognized local packing"
					" option %s") % prop->name);
		INVARIANT(xmlStrncmp(prop->name,(const xmlChar *)"opt_",4)!=0,
			  boost::format("Unrecognized local option %s")
			  % prop->name);
	    }
	}

	fieldInfo info;

	info.name = strGetXMLProp(cur, "name");
	INVARIANT(!info.name.empty(), "Error: ExtentType field missing name attribute");
	INVARIANT(info.name[0] != ' ',
		  boost::format("Error: Field name '%s' invalid, values starting with a space are reserved for the library.")
		  % info.name);

	info.xmldesc = cur;
	INVARIANT(info.name.size() <= 255,
		  boost::format("type name '%s' is too long.") % info.name);
	INVARIANT(getColumnNumber(ret, info.name) == -1,
		  boost::format("Error: ExtentType '%s', duplicate field '%s'")
		  % ret.name % info.name);
	
	string type_str = strGetXMLProp(cur, "type");
	INVARIANT(!type_str.empty(), "Error: ExtentType field missing type attribute");
	if (type_str == "bool") {
	    info.type = ft_bool;
	    ++bool_fields;
	} else if (type_str == "byte") {
	    info.type = ft_byte;
	    ++byte_fields;
	} else if (type_str == "int32") {
	    info.type = ft_int32;
	    ++int32_fields;
	} else if (type_str == "int64") {
	    info.type = ft_int64;
	    ++eight_fields;
	} else if (type_str == "double") {
	    info.type = ft_double;
	    ++eight_fields;
	} else if (type_str == "variable32") {
	    info.type = ft_variable32;
	    ++variable_fields;
	} else if (type_str == "fixedwidth") {
	    LintelLog::warn("Fixed width fields are experimental.\n");
	    info.type = ft_fixedwidth;
	    ++byte_fields;
	} else {
	    FATAL_ERROR(boost::format("Unknown field type '%s'") % type_str);
	}
        LintelLogDebug("ExtentType::XMLDecode", boost::format("  field type='%s', name='%s'\n") % type_str % info.name);

	string pack_unique = strGetXMLProp(cur, "pack_unique");
	if (info.type == ft_variable32) {
	    ret.variable32_field_columns.push_back(ret.field_info.size());
	    info.unique = parseYesNo(cur, "pack_unique", false);
	} else {
	    INVARIANT(pack_unique.empty(),
		      "pack_unique only allowed for variable32 fields");
	}
	
	bool nullable = parseYesNo(cur, "opt_nullable", false);
	// Real field will go into size, so null field into size+1.
	info.null_fieldnum 
	    = nullable ? static_cast<int>(ret.field_info.size()) + 1 : -1;

	string opt_doublebase = strGetXMLProp(cur, "opt_doublebase");
	if (!opt_doublebase.empty()) {
	    INVARIANT(info.type == ft_double,
		      "opt_doublebase only allowed for double fields");
	    info.doublebase = stringToDouble(opt_doublebase);
	}	    

	string size = strGetXMLProp(cur, "size");
        if (!size.empty()) {
            INVARIANT(info.type == ft_fixedwidth,
                      "size only allowed for fixed width fields");
            info.size = stringToInteger<int32_t>(size);
            INVARIANT(info.size > 0, "size must be positive");
        }

	// TODO: consider a variant of pack_scale where you can
	// specify Q#.#[b#] as an alternative specification following
	// http://en.wikipedia.org/wiki/Fixed-point_arithmetic#Nomenclature
	// Much further extention would be non-space preserving
	// transformations to make it take up less space.
	string pack_scale_v = strGetXMLProp(cur, "pack_scale");
	if (!pack_scale_v.empty()) {
	    INVARIANT(info.type == ft_double,
		      "pack_scale only valid for double fields");
	    double scale = stringToDouble(pack_scale_v);
	    INVARIANT(scale != 0, "pack_scale=0 invalid");
	    ret.pack_scale.push_back(pack_scaleT(ret.field_info.size(),scale));
            LintelLogDebug("ExtentType::XMLDecode", boost::format("pack_scaling field %d by %.10g (1/%.10g)\n")
                           % ret.field_info.size() % (1.0/scale) % scale);
	}
	string pack_relative = strGetXMLProp(cur, "pack_relative");
	if (!pack_relative.empty()) {
	    INVARIANT(info.type == ft_double ||
		      info.type == ft_int64 ||
		      info.type == ft_int32,
		      "Only double, int32, int64 fields currently supported for relative packing");
	    unsigned field_num = ret.field_info.size();
	    if (pack_relative == info.name) {
		ret.pack_self_relative.push_back(pack_self_relativeT(field_num));
		if (info.type == ft_double) {
		    INVARIANT(!pack_scale_v.empty(),
			      "for self-relative packing of a double, scaling is required -- otherwise errors in unpacking accumulate");
		    ret.pack_self_relative.back().scale 
			= ret.pack_scale.back().scale;
		    ret.pack_self_relative.back().multiplier 
			= ret.pack_scale.back().multiplier;
		}
                LintelLogDebug("ExtentType::XMLDecode", boost::format("pack_self_relative field %s\n") % field_num);
	    } else {
		int base_field_num = getColumnNumber(ret, pack_relative);
		INVARIANT(base_field_num != -1,
			  boost::format("Unrecognized field %s to use as base field for relative packing of %s")
			  % pack_relative % info.name);
		INVARIANT(info.type == ret.field_info[base_field_num].type,
			  boost::format("Both fields for relative packing must have same type type(%s) != type(%s)")
			  % info.name % pack_relative);
		ret.pack_other_relative.push_back(pack_other_relativeT(field_num, base_field_num));
                LintelLogDebug("ExtentType::XMLDecode", boost::format("pack_relative_other field %d based on field %d\n")
                               % field_num % base_field_num);
	    }
	}
	cur = cur->next;
	ret.field_info.push_back(info);
	ret.visible_fields.push_back(ret.field_info.size()-1);
	if (nullable) {
	    DEBUG_SINVARIANT(info.null_fieldnum == static_cast<int>(ret.field_info.size()));
			    
	    // auto-generate the boolean "null" field
	    info.name = nullableFieldname(info.name);
	    info.type = ft_bool;
	    info.size = 1;
	    info.offset = -1;
	    info.bitpos = -1;
	    info.unique = false;
	    info.null_fieldnum = -1;
	    info.null_compact_info = NULL;
	    info.doublebase = 0;
	    info.xmldesc = NULL;
	    ret.field_info.push_back(info);
	}
    }

    // need to put in the variable sized special fields here!

    int32 byte_pos = 0; // TODO: make this uint32_t

    if (ret.field_ordering == FieldOrderingSmallToBigSepVar32) {
	parsePackBitFields(ret, byte_pos);
	parsePackByteAlignedFields(ret, byte_pos);
	if (ret.pad_record == PadRecordOriginal || 
	    int32_fields > 0 || variable_fields > 0) {
	    unsigned zero_pad = (4 - (byte_pos % 4)) % 4;
            LintelLogDebug("ExtentType::Packing", boost::format("%s bytes of zero padding\n") % zero_pad);
	    byte_pos += zero_pad;
	}
	parsePackInt32Fields(ret, byte_pos);
	parsePackVar32Fields(ret, byte_pos);
	if (ret.pad_record == PadRecordOriginal || eight_fields > 0) {
	    unsigned zero_pad = (8 - (byte_pos % 8)) % 8;
            LintelLogDebug("ExtentType::Packing", boost::format("%s bytes of zero padding\n") % zero_pad);
	    byte_pos += zero_pad;
	}
	parsePackSize8Fields(ret, byte_pos);
    } else if (ret.field_ordering == FieldOrderingBigToSmallSepVar32) {
	parsePackSize8Fields(ret, byte_pos);
	parsePackInt32Fields(ret, byte_pos);
	parsePackVar32Fields(ret, byte_pos);
	parsePackByteAlignedFields(ret, byte_pos);
	parsePackBitFields(ret, byte_pos);
	uint32_t align_size = 1;
	if (int32_fields > 0 || variable_fields > 0) {
	    align_size = 4;
	}
	if (eight_fields > 0 || ret.pad_record == PadRecordOriginal) {
	    align_size = 8;
	}
	unsigned zero_pad = (align_size - (byte_pos % align_size)) 
	    % align_size;
	byte_pos += zero_pad;
	SINVARIANT((byte_pos % align_size) == 0);
    } else {
	FATAL_ERROR("?");
    }

    ret.fixed_record_size = byte_pos;

    ret.bool_bytes = 0;
    for(unsigned i = 0; i < ret.field_info.size(); ++i) {
	fieldInfo &field(ret.field_info[i]);
	if (field.type == ft_bool) {
	    field.null_compact_info = NULL;
	    if (field.offset + 1 > ret.bool_bytes) {
		ret.bool_bytes = field.offset + 1;
	    }
	    continue;
	} 
	nullCompactInfo n;
	n.type = field.type;
	n.field_num = i;
	n.size = field.size;
	n.offset = field.offset;

	if (field.null_fieldnum > 0) {
	    INVARIANT(static_cast<unsigned>(field.null_fieldnum) == i+1, 
		      format("? %d != %d") % field.null_fieldnum % (i+1));
	    fieldInfo &null_field(ret.field_info[field.null_fieldnum]);
	    INVARIANT(null_field.type == ft_bool, "?");
	    n.null_offset = null_field.offset;
	    n.null_bitmask = 1 << null_field.bitpos;
	}
	switch(n.type)
	    {
	    case ft_byte: case ft_fixedwidth:
		ret.nonbool_compact_info_size1.push_back(n);
		break;
	    case ft_int32: case ft_variable32:
		ret.nonbool_compact_info_size4.push_back(n);
		break;
	    case ft_int64: case ft_double:
		ret.nonbool_compact_info_size8.push_back(n);
		break;
	    default: FATAL_ERROR(boost::format("Unrecognized type #%s") % n.type);
	    }
    }

    // TODO: fix this check so that we are properly verifying we have
    // nullable fields, not just that we have boolean fields; or
    // decide to just allow compaction in all cases, even if we don't
    // have nulls, which could save a little bit of space, e.g. 7
    // bytes with 1 byte of bools|byte and then a double or int64.
    INVARIANT(ret.pack_null_compact == CompactNo || ret.bool_bytes > 0, 
	      "should not enable null compaction with no nullable fields");

    ret.sortAssignNCI(ret.nonbool_compact_info_size1);
    ret.sortAssignNCI(ret.nonbool_compact_info_size4);
    ret.sortAssignNCI(ret.nonbool_compact_info_size8);

    return ret;
}

ExtentType::ExtentType(const string &_xmldesc)
    : rep(parseXML(_xmldesc))
{ }

int ExtentType::getColumnNumber(const ParsedRepresentation &rep,
				const string &column,
				bool missing_ok) {
    for(unsigned int i=0; i<rep.field_info.size(); i++) {
	if (rep.field_info[i].name == column) {
            LintelLogDebug("ExtentType::GetColNum", boost::format("column %s -> %d\n") % column % i);
	    return i;
	}
    }
    LintelLogDebug("ExtentType::GetColNum", boost::format("column %s -> -1\n") % column);
    INVARIANT(missing_ok, boost::format("Unknown column '%s' in type '%s'") 
	      % column % rep.name);
    return -1;
}

ExtentType::int32 ExtentType::getSize(int column) const {
    INVARIANT(column >= 0 && column < (int)rep.field_info.size(),
	      boost::format("internal error, column %d out of range [0..%d]\n")
	      % column % (rep.field_info.size()-1));
    INVARIANT(rep.field_info[column].size > 0,
	      "internal error, getSize() on variable sized field doesn't make sense\n");
    return rep.field_info[column].size;
}

ExtentType::int32 ExtentType::getOffset(int column) const {
    INVARIANT(column >= 0 && column < (int)rep.field_info.size(),
	      boost::format("internal error, column %d out of range [0..%d]\n")
	      % column % (rep.field_info.size()-1));
    INVARIANT(rep.field_info[column].offset >= 0,
	      boost::format("internal error, getOffset() on variable sized field (%s, #%d) doesn't make sense\n")
	      % rep.field_info[column].name % column);
    return rep.field_info[column].offset;
}

int ExtentType::getBitPos(int column) const {
    INVARIANT(column >= 0 && column < (int)rep.field_info.size(),
	      boost::format("internal error, column %d out of range [0..%d]\n")
	      % column % (rep.field_info.size()-1));
    INVARIANT(rep.field_info[column].bitpos >= 0,
	      boost::format("internal error, getBitPos() on non-bool field (%s, #%d) doesn't make sense\n")
	      % rep.field_info[column].name % column);
    return rep.field_info[column].bitpos;
}


ExtentType::fieldType ExtentType::getFieldType(int column) const {
    INVARIANT(column >= 0 && column < (int)rep.field_info.size(),
	      boost::format("internal error, column %d out of range [0..%d]\n")
	      % column % (rep.field_info.size()-1));
    return rep.field_info[column].type;
}

bool ExtentType::getUnique(int column) const {
    INVARIANT(column >= 0 && column < (int)rep.field_info.size(),
	      boost::format("internal error, column %d out of range [0..%d]\n")
	      % column % (rep.field_info.size()-1));
    return rep.field_info[column].unique;
}

bool ExtentType::getNullable(int column) const {
    INVARIANT(column >= 0 && column < (int)rep.field_info.size(),
	      boost::format("internal error, column %d out of range [0..%d]\n")
	      % column % (rep.field_info.size()-1));
    return rep.field_info[column].null_fieldnum > 0;
}

double ExtentType::getDoubleBase(int column) const {
    INVARIANT(column >= 0 && column < (int)rep.field_info.size(),
	      boost::format("internal error, column %d out of range [0..%d]\n")
	      % column % (rep.field_info.size()-1));
    return rep.field_info[column].doublebase;
}

string ExtentType::nullableFieldname(const string &fieldname) {
    string ret(" ");

    ret += fieldname;
    return ret;
}

string ExtentType::xmlFieldDesc(int field_num) const {
    INVARIANT(field_num >= 0 && field_num < (int)rep.field_info.size(),
	      "bad field num");
    xmlBufferPtr buf = xmlBufferCreate();
    xmlBufferSetAllocationScheme(buf,XML_BUFFER_ALLOC_DOUBLEIT);
    xmlNodeDump(buf, rep.xml_description_doc, rep.field_info[field_num].xmldesc, 2, 1);
    string ret((char *)xmlBufferContent(buf));
    xmlBufferFree(buf);
    return ret;
}

xmlNodePtr ExtentType::xmlNodeFieldDesc(int field_num) const {
    INVARIANT(field_num >= 0 && field_num < (int)rep.field_info.size(),
	      "bad field num");
    return rep.field_info[field_num].xmldesc;
}

static const string fieldtypes[] = {
    "unknown",
    "bool",
    "byte",
    "int32",
    "int64",
    "double",
    "variable32",
    "fixedwidth"
};

static int Nfieldtypes = sizeof(fieldtypes)/sizeof(const string);

const string &ExtentType::fieldTypeString(fieldType ft) {
    INVARIANT(ft >= 0 && ft < Nfieldtypes,
	      boost::format("invalid fieldtype %d") % ft);
    return fieldtypes[ft];
}

const ExtentType &ExtentTypeLibrary::registerTypeR(const string &xmldesc) {
    const ExtentType &type(sharedExtentType(xmldesc));
    
    INVARIANT(name_to_type.find(type.getName()) == name_to_type.end(),
	      boost::format("Type %s already registered")
	      % type.getName());

    name_to_type[type.getName()] = &type;
    return type;
}    

void ExtentTypeLibrary::registerType(const ExtentType &type) {
    INVARIANT(name_to_type.find(type.getName()) == name_to_type.end(),
	      boost::format("Type %s already registered")
	      % type.getName());

    name_to_type[type.getName()] = &type;
}    

const ExtentType * ExtentTypeLibrary::getTypeByName(const string &name, bool null_ok) const {
    if (name == ExtentType::getDataSeriesXMLType().getName()) {
	return &ExtentType::getDataSeriesXMLType();
    } else if (name == ExtentType::getDataSeriesIndexTypeV0().getName()) {
	return &ExtentType::getDataSeriesIndexTypeV0();
    }
    std::map<const std::string, const ExtentType *>::const_iterator i 
	= name_to_type.find(name);
    if (i == name_to_type.end()) {
	if (null_ok) {
	    return NULL;
	}
	FATAL_ERROR(format("No type named %s registered") % name);
    }
    const ExtentType *f = i->second;
    SINVARIANT(f != NULL);
    return f;
}

const ExtentType * ExtentTypeLibrary::getTypeByPrefix(const string &prefix, bool null_ok) const {
    const ExtentType *f = NULL;
    for(map<const string, const ExtentType *>::const_iterator i = name_to_type.begin();
	i != name_to_type.end();++i) {
	if (prefixequal(i->first, prefix)) {
	    INVARIANT(f == NULL,
		      boost::format("Invalid getTypeByPrefix, two types match prefix '%s': %s and %s\n")
		      % prefix % f->getName() % i->first);
	    f = i->second;
	}
    }
    INVARIANT(null_ok || f != NULL,
	      boost::format("No type matching prefix %s found?!\n")
	      % prefix);
    return f;
}

const ExtentType * ExtentTypeLibrary::getTypeBySubstring(const string &substr, bool null_ok) const {
    const ExtentType *f = NULL;
    for(map<const string, const ExtentType *>::const_iterator i = name_to_type.begin();
	i != name_to_type.end();++i) {
	if (i->first.find(substr) != string::npos) {
	    INVARIANT(f == NULL,
		      boost::format("Invalid getTypeBySubstring, two types match substring %s: %s and %s\n")
		      % substr % f->getName() % i->first);
	    f = i->second;
	}
    }
    INVARIANT(null_ok || f != NULL,
	      boost::format("No type matching substring %s found?!\n")
	      % substr);
    return f;
}

const ExtentType * ExtentTypeLibrary::getTypeMatch(const std::string &match, 
						   bool null_ok, bool skip_info) {
    const ExtentType *t = NULL;

    static string str_DataSeries("DataSeries:");
    static string str_Info("Info:");
    if (match == "*") {
	for(map<const string, const ExtentType *>::iterator i = name_to_type.begin();
	    i != name_to_type.end();++i) {
	    if (prefixequal(i->first,str_DataSeries)) {
		continue;
	    }
	    if (skip_info && prefixequal(i->first,str_Info)) {
		continue;
	    }
	    INVARIANT(t == NULL, 
		      boost::format("Invalid getTypeMatch, '*' matches both '%s' and '%s'")
		      % t->getName() % i->first);
	    t = i->second;
	}
    } else {
	t = getTypeByName(match, true);
	if (t != NULL) return t;
	t = getTypeByPrefix(match, true);
	if (t != NULL) return t;
	t = getTypeBySubstring(match, true);
    }
    INVARIANT(null_ok || t != NULL,
	      boost::format("No type matching %s found; try '*' if you only have one type.\n")
	      % match);
    return t;
}

// This optimization to only have one extent type object for each xml
// description is here for when you are handling a whole lot of small
// files.  This is a memory usage optimization not a performance one
// although it happens to improve the performance.

namespace dataseries {
    bool in_tilde_xml_decode;

    struct xmlDecode {
	typedef HashMap<string, ExtentType *> HTType;

	HTType table;
	PThreadMutex mutex;
	~xmlDecode() { // called exactly once.
	    SINVARIANT(!in_tilde_xml_decode);
	    in_tilde_xml_decode = true;
	    for(HTType::iterator i = table.begin(); i != table.end(); ++i) {
		delete i->second;
	    }
	    table.clear();
	}
    };

    static xmlDecode &decodeInfo() {
	// C++ semantics say this will be initialized the first time we
	// pass through this call
	static xmlDecode decode_info;
	return decode_info;
    }
}

const ExtentType &ExtentTypeLibrary::sharedExtentType(const string &xmldesc) {
    using dataseries::decodeInfo;
    PThreadAutoLocker lock(decodeInfo().mutex);

    ExtentType **d = decodeInfo().table.lookup(xmldesc);
    if (d != NULL) { // should be common case, just return the value
	SINVARIANT(*d != NULL);
	return **d;
    }

    ExtentType *tmp = new ExtentType(xmldesc);
    decodeInfo().table[xmldesc] = tmp;
    return *tmp;
}

ExtentType::~ExtentType() { 
    SINVARIANT(dataseries::in_tilde_xml_decode);
}
