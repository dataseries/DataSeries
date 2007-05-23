// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    ExtentType class implementation
*/

#include <vector>

using namespace std;
#include <libxml/parser.h>

#include <Lintel/LintelAssert.H>
#include <Lintel/PThread.H>
#include <Lintel/HashTable.H>
#include <Lintel/StringUtil.H>

#include <DataSeries/ExtentType.H>

struct xmlDecodeInfo {
    string xmldesc;
    xmlDocPtr field_desc_doc;
    ExtentType *extenttype;
};

struct xmlDecodeInfoHash {
    unsigned operator()(const xmlDecodeInfo *k) {
	return HashTable_hashbytes(k->xmldesc.data(),k->xmldesc.size());
    }
};

struct xmlDecodeInfoEqual {
    bool operator()(const xmlDecodeInfo *a, const xmlDecodeInfo *b) {
	return a->xmldesc == b->xmldesc;
    }
};

static HashTable<xmlDecodeInfo *,xmlDecodeInfoHash,xmlDecodeInfoEqual> xmlDecodeTable;
PThreadMutex xmlDecodeTableMutex;

static const string &
getSharedDecodeString(const string &xmldesc)
{
    xmlDecodeInfo k;
    k.xmldesc = xmldesc;
    xmlDecodeTableMutex.lock();
    xmlDecodeInfo **d = xmlDecodeTable.lookup(&k);
    if (d == NULL) {
	xmlDecodeInfo *f = new xmlDecodeInfo;
	f->xmldesc = xmldesc;
	// can't create it here, ExtentType needs the shared xmldesc
	f->extenttype = NULL; 
	LIBXML_TEST_VERSION;
	xmlKeepBlanksDefault(0);
	f->field_desc_doc = xmlParseMemory((char *)xmldesc.data(),xmldesc.size());
	AssertAlways(f->field_desc_doc != NULL,
		     ("Error: parsing ExtentType description failed\n"));
	d = xmlDecodeTable.add(f);
    }
    xmlDecodeTableMutex.unlock();
    return (**d).xmldesc;
}

static const bool debug_getcolnum = false;
static const bool debug_xml_decode = false;
static const bool debug_packing = false;

ExtentType::ExtentType(const string &_xmldesc)
    : xmldesc(getSharedDecodeString(_xmldesc)) // assuming the STL string implementation shares the underlying string when possible, this is a huge memory win.
{
    AssertAlways(sizeof(byte) == 1 && sizeof(int32) == 4 &&
		 sizeof(uint32) == 4 && sizeof(int64) == 8,
		 ("sizeof check bad\n"));
    field_desc_doc = ExtentTypeLibrary::sharedDocPtr(xmldesc);
    xmlNodePtr cur = xmlDocGetRootElement(field_desc_doc);
    AssertAlways(cur != NULL,("Error: ExtentType description missing document\n"));
    AssertAlways(xmlStrcmp(cur->name, (const xmlChar *) "ExtentType") == 0,
		 ("Error: ExtentType description has wrong type, '%s' != '%s'\n",
		  cur->name, "ExtentType"));
    xmlChar *extentname = xmlGetProp(cur, (const xmlChar *)"name");
    AssertAlways(extentname != NULL,("Error, ExtentType missing name\n"));

    for(xmlAttr *prop = cur->properties; prop != NULL; prop = prop->next) {
	AssertAlways(xmlStrncmp(prop->name,(const xmlChar *)"pack_",5)!=0,
		     ("Unrecognized global packing option %s\n",prop->name));
	AssertAlways(xmlStrncmp(prop->name,(const xmlChar *)"opt_",5)!=0,
		     ("Unrecognized global option %s\n",prop->name));
    }
    name = (char *)extentname;
    AssertAlways(name.length() <= 255,
		 ("invalid extent type name, max of 255 characters allowed\n"));

    if (debug_xml_decode) printf("ExtentType '%s'\n",name.c_str());

    char *extentversion = reinterpret_cast<char *>(xmlGetProp(cur, (const xmlChar *)"version"));
    if (extentversion == NULL) {
        major_version = 0;
	minor_version = 0;
    } else {
	vector<string> bits;
	split(extentversion, ".", bits);
	INVARIANT(bits.size() == 2, boost::format("bad version '%s' should be #.#") % extentversion);
	major_version = static_cast<unsigned>(stringToLong(bits[0]));
	minor_version = static_cast<unsigned>(stringToLong(bits[1]));
    }

    char *namespace_charptr = reinterpret_cast<char *>(xmlGetProp(cur, (const xmlChar *)"namespace"));
    if (namespace_charptr != NULL) {
	type_namespace = namespace_charptr;
    }

    cur = cur->xmlChildrenNode;
    int bool_fields = 0, byte_fields = 0, int32_fields = 0, eight_fields = 0, 
	variable_fields = 0;
    while (true) {
	while (cur != NULL && xmlIsBlankNode(cur)) {
	    cur = cur->next;
	}
	if (cur == NULL) 
	    break;
	AssertAlways(xmlStrcmp(cur->name, (const xmlChar *)"field") == 0,
		     ("Error: ExtentType sub-element should be field, not '%s'\n",
		      cur->name));
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
		AssertAlways(xmlStrncmp(prop->name,(const xmlChar *)"pack_",5)!=0,
			     ("Unrecognized local packing option %s\n",prop->name));
		AssertAlways(xmlStrncmp(prop->name,(const xmlChar *)"opt_",4)!=0,
			     ("Unrecognized local option %s\n",prop->name));
	    }
	}

	xmlChar *fieldname = xmlGetProp(cur, (const xmlChar *)"name");
	AssertAlways(fieldname != NULL,("Error: ExtentType field missing name attribute\n"));
	AssertAlways(fieldname[0] != ' ',
		     ("Error: Field name '%s' invalid, values starting with a space are reserved for the library.\n",fieldname));
	fieldInfo info;
	info.xmldesc = cur;
	info.name.assign((char *)fieldname);
	AssertAlways(info.name.size() <= 255,("type name '%s' is too long.\n",info.name.c_str()));
	AssertAlways(getColumnNumber(info.name) == -1,
		     ("Error: ExtentType '%s', duplicate field '%s'\n",
		      extentname, fieldname));
	
	xmlChar *type_str = xmlGetProp(cur, (const xmlChar *)"type");
	AssertAlways(type_str != NULL,("Error: ExtentType field missing type attribute\n"));
	if (xmlStrcmp(type_str,(const xmlChar *)"bool") == 0) {
	    info.type = ft_bool;
	    ++bool_fields;
	} else if (xmlStrcmp(type_str,(const xmlChar *)"byte") == 0) {
	    info.type = ft_byte;
	    ++byte_fields;
	} else if (xmlStrcmp(type_str,(const xmlChar *)"int32") == 0) {
	    info.type = ft_int32;
	    ++int32_fields;
	} else if (xmlStrcmp(type_str,(const xmlChar *)"int64") == 0) {
	    info.type = ft_int64;
	    ++eight_fields;
	} else if (xmlStrcmp(type_str,(const xmlChar *)"double") == 0) {
	    info.type = ft_double;
	    ++eight_fields;
	} else if (xmlStrcmp(type_str,(const xmlChar *)"variable32") == 0) {
	    info.type = ft_variable32;
	    ++variable_fields;
	} else {
	    AssertFatal(("Unknown field type '%s'\n",type_str));
	}
	
	if (debug_xml_decode) printf("  field type='%s', name='%s'\n",type_str,fieldname);

	xmlChar *pack_unique = xmlGetProp(cur, (const xmlChar *)"pack_unique");
	if (info.type == ft_variable32) {
	    variable32_field_columns.push_back(field_info.size());
	    if (pack_unique != NULL) {
		AssertAlways(xmlStrcmp(pack_unique,(xmlChar *)"yes") == 0 ||
			     xmlStrcmp(pack_unique,(xmlChar *)"no") == 0,
			     ("pack_unique should be either 'yes' or 'no', not '%s'\n",pack_unique));
		info.unique = xmlStrcmp(pack_unique,(xmlChar *)"yes") == 0;
	    }
	} else {
	    AssertAlways(pack_unique == NULL,
			 ("pack_unique only allowed for variable32 fields\n"));
	}
	
	xmlChar *opt_nullable = xmlGetProp(cur, (const xmlChar *)"opt_nullable");
	if (opt_nullable != NULL) {
	    AssertAlways(xmlStrcmp(opt_nullable,(xmlChar *)"yes") == 0 ||
			 xmlStrcmp(opt_nullable,(xmlChar *)"no") == 0,
			 ("opt_nullable should be either 'yes' or 'no', not '%s'\n",opt_nullable));
	    info.nullable = xmlStrcmp(opt_nullable,(xmlChar *)"yes") == 0;
	}	    

	xmlChar *opt_doublebase = xmlGetProp(cur, (const xmlChar *)"opt_doublebase");
	if (opt_doublebase != NULL) {
	    AssertAlways(info.type == ft_double,
			 ("opt_doublebase only allowed for double fields\n"));
	    char *endptr;
	    info.doublebase = strtod((char *)opt_doublebase,&endptr);
	    AssertAlways(*opt_doublebase != '\0',
			 ("must have an value for opt_doublebase\n"));
	    AssertAlways(*endptr == '\0',
			 ("conversion problem on opt_doublebase '%s'\n",opt_doublebase));
	}	    


	xmlChar *pack_scale_v = xmlGetProp(cur, (const xmlChar *)"pack_scale");
	if (pack_scale_v != NULL) {
	    AssertAlways(info.type == ft_double,
			 ("pack_scale only valid for double fields\n"));
	    double scale = atof((char *)pack_scale_v);
	    AssertAlways(scale != 0,("pack_scale=0 invalid\n"));
	    pack_scale.push_back(pack_scaleT(field_info.size(),scale));
	    if (debug_xml_decode) printf("pack_scaling field %d by %.10g (1/%.10g)\n",
			      field_info.size(),1.0/scale,scale);
	}
	xmlChar *pack_relative = xmlGetProp(cur, (const xmlChar *)"pack_relative");
	if (pack_relative != NULL) {
	    AssertAlways(info.type == ft_double ||
			 info.type == ft_int64 ||
			 info.type == ft_int32,
			 ("Only double, int32, int64 fields currently supported for relative packing\n"));
	    int field_num = field_info.size();
	    if (xmlStrcmp(pack_relative,fieldname) == 0) {
		pack_self_relative.push_back(pack_self_relativeT(field_num));
		if (info.type == ft_double) {
		    AssertAlways(pack_scale_v != NULL,
				 ("for self-relative packing of a double, scaling is required -- otherwise errors in unpacking accumulate\n"));
		    pack_self_relative.back().scale = pack_scale.back().scale;
		    pack_self_relative.back().multiplier = pack_scale.back().multiplier;
		}
		if (debug_xml_decode) printf("pack_self_relative field %d\n",field_num);
	    } else {
		int base_field_num = getColumnNumber((char *)pack_relative);
		AssertAlways(base_field_num != -1,
			     ("Unrecognized field %s to use as base field for relative packing of %s\n",pack_relative,fieldname));
		AssertAlways(info.type == field_info[base_field_num].type,
			     ("Both fields for relative packing must have same type type(%s) != type(%s)\n",fieldname,pack_relative));
		pack_other_relative.push_back(pack_other_relativeT(field_num, base_field_num));
		if (debug_xml_decode) printf("pack_relative_other field %d based on field %d\n",
					     field_num, base_field_num);
	    }
	}
	cur = cur->next;
	field_info.push_back(info);
	visible_fields.push_back(field_info.size()-1);
	if (info.nullable) {
	    // auto-generate the boolean "null" field
	    info.name = nullableFieldname(info.name);
	    info.type = ft_bool;
	    info.size = 1;
	    info.offset = -1;
	    info.bitpos = -1;
	    info.unique = false;
	    info.nullable = false;
	    info.doublebase = 0;
	    field_info.push_back(info);
	}
    }

    // need to put in the variable sized special fields here!

    if (debug_packing) printf("packing bool fields...\n");
    int32 bit_pos = 0;
    int32 byte_pos = 0;
    for(unsigned int i=0;i<field_info.size();i++) {
	if (field_info[i].type == ft_bool) {
	    field_info[i].size = 1;
	    field_info[i].offset = byte_pos;
	    field_info[i].bitpos = bit_pos;
	    if (debug_packing) printf("  field %s at position %d:%d\n",field_info[i].name.c_str(),byte_pos,bit_pos);
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
    if (debug_packing) printf("packing byte fields...\n");
    for(unsigned int i=0;i<field_info.size();i++) {
	if (field_info[i].type == ft_byte) {
	    field_info[i].size = 1;
	    field_info[i].offset = byte_pos;
	    if (debug_packing) printf("  field %s at position %d\n",field_info[i].name.c_str(),byte_pos);
	    byte_pos += 1;
	}
    }
    int zero_pad = (4 - (byte_pos % 4)) % 4;
    if (debug_packing) printf("%d bytes of zero padding\n",zero_pad);
    byte_pos += zero_pad;
    if (debug_packing) printf("packing int32 fields...\n");
    for(unsigned int i=0;i<field_info.size();i++) {
	if (field_info[i].type == ft_int32) {
	    field_info[i].size = 4;
	    field_info[i].offset = byte_pos;
	    if (debug_packing) printf("  field %s (#%d) at position %d\n",field_info[i].name.c_str(),i,byte_pos);
	    byte_pos += 4;
	}
    }
    // these tend to have lots of different values making compression
    // worse, so we pack them after the other int32 fields, but to
    // avoid alignment glitches before the 8 byte fields
    if (debug_packing) printf("packing variable32 fields...\n");
    for(unsigned int i=0;i<field_info.size();i++) {
	if (field_info[i].type == ft_variable32) {
	    field_info[i].offset = byte_pos;
	    if (debug_packing) printf("  field %s (#%d) at position %d\n",field_info[i].name.c_str(),i,byte_pos);
	    byte_pos += 4;
	}
    }
    zero_pad = (8 - (byte_pos % 8)) % 8;
    if (debug_packing) printf("%d bytes of zero padding\n",zero_pad);
    byte_pos += zero_pad;
    if (debug_packing) printf("packing int64 and double fields...\n");
    for(unsigned int i=0;i<field_info.size();i++) {
	if (field_info[i].type == ft_int64 || field_info[i].type == ft_double) {
	    field_info[i].size = 8;
	    field_info[i].offset = byte_pos;
	    if (debug_packing) printf("  field %s at position %d\n",field_info[i].name.c_str(),byte_pos);
	    byte_pos += 8;
	}
    }
    
    fixed_record_size = byte_pos;
}

int 
ExtentType::getColumnNumber(const string &column) const
{
    for(unsigned int i=0;i<field_info.size();i++) {
	if (field_info[i].name == column) {
	    if (debug_getcolnum) printf("column %s -> %d\n",column.c_str(),i);
	    return i;
	}
    }
    return -1;
}

ExtentType::int32
ExtentType::getSize(int column) const
{
    AssertAlways(column >= 0 && column < (int)field_info.size(),
		 ("internal error, column %d out of range [0..%d]\n",
		  column,field_info.size()-1));
    AssertAlways(field_info[column].size > 0,
		 ("internal error, getSize() on variable sized field doesn't make sense\n"));
    return field_info[column].size;
}

ExtentType::int32
ExtentType::getOffset(int column) const
{
    AssertAlways(column >= 0 && column < (int)field_info.size(),
		 ("internal error, column %d out of range [0..%d]\n",
		  column,field_info.size()-1));
    AssertAlways(field_info[column].offset >= 0,
		 ("internal error, getOffset() on variable sized field (%s, #%d) doesn't make sense\n",field_info[column].name.c_str(),column));
    return field_info[column].offset;
}

int
ExtentType::getBitPos(int column) const
{
    AssertAlways(column >= 0 && column < (int)field_info.size(),
		 ("internal error, column %d out of range [0..%d]\n",
		  column,field_info.size()-1));
    AssertAlways(field_info[column].bitpos >= 0,
		 ("internal error, getBitPos() on non-bool field (%s, #%d) doesn't make sense\n",field_info[column].name.c_str(),column));
    return field_info[column].bitpos;
}


ExtentType::fieldType
ExtentType::getFieldType(int column) const
{
    AssertAlways(column >= 0 && column < (int)field_info.size(),
		 ("internal error, column %d out of range [0..%d]\n",
		  column,field_info.size()-1));
    return field_info[column].type;
}

bool
ExtentType::getUnique(int column) const
{
    AssertAlways(column >= 0 && column < (int)field_info.size(),
		 ("internal error, column %d out of range [0..%d]\n",
		  column,field_info.size()-1));
    return field_info[column].unique;
}

bool
ExtentType::getNullable(int column) const
{
    AssertAlways(column >= 0 && column < (int)field_info.size(),
		 ("internal error, column %d out of range [0..%d]\n",
		  column,field_info.size()-1));
    return field_info[column].nullable;
}

double
ExtentType::getDoubleBase(int column) const
{
    AssertAlways(column >= 0 && column < (int)field_info.size(),
		 ("internal error, column %d out of range [0..%d]\n",
		  column,field_info.size()-1));
    return field_info[column].doublebase;
}

string
ExtentType::nullableFieldname(const string &fieldname)
{
    string ret(" ");

    ret += fieldname;
    return ret;
}

string
ExtentType::xmlFieldDesc(int field_num) const
{
    AssertAlways(field_num >= 0 && field_num < (int)field_info.size(),
		 ("bad field num\n"));
    xmlBufferPtr buf = xmlBufferCreate();
    xmlBufferSetAllocationScheme(buf,XML_BUFFER_ALLOC_DOUBLEIT);
    xmlNodeDump(buf,field_desc_doc,field_info[field_num].xmldesc,2,1);
    string ret((char *)xmlBufferContent(buf));
    xmlBufferFree(buf);
    return ret;
}

xmlNodePtr
ExtentType::xmlNodeFieldDesc(int field_num) const
{
    AssertAlways(field_num >= 0 && field_num < (int)field_info.size(),
		 ("bad field num\n"));
    return field_info[field_num].xmldesc;
}

static const string fieldtypes[] = {
    "unknown",
    "bool",
    "byte",
    "int32",
    "int64",
    "double",
    "variable32",
};

static int Nfieldtypes = sizeof(fieldtypes)/sizeof(const string);

const string &
ExtentType::fieldTypeString(fieldType ft)
{
    AssertAlways(ft >= 0 && ft < Nfieldtypes,
		 ("invalid fieldtype %d\n",ft));
    return fieldtypes[ft];
}

bool
ExtentType::prefixmatch(const string &a, const string &prefix)
{
    if (a.size() < prefix.size())
	return false;
    return a.substr(0,prefix.size()) == prefix;
}

ExtentType *
ExtentTypeLibrary::registerType(const string &xmldesc)
{
    ExtentType *type = sharedExtentType(xmldesc);
    
    AssertAlways(name_to_type.find(type->name) == name_to_type.end(),
                 ("Type %s already registered\n", type->name.c_str()));

    name_to_type[type->name] = type;
    return type;
}    

ExtentType *
ExtentTypeLibrary::getTypeByName(const string &name, bool null_ok)
{
    if (null_ok) {
	if (name_to_type.find(name) == name_to_type.end()) {
	    return NULL;
	}
    }
    ExtentType *f = name_to_type[name];
    AssertAlways(f != NULL, ("No type named %s registered\n", name.c_str()));
    return f;
}

ExtentType *
ExtentTypeLibrary::getTypeByPrefix(const string &prefix, bool null_ok)
{
    ExtentType *f = NULL;
    for(map<const string, ExtentType *>::iterator i = name_to_type.begin();
	i != name_to_type.end();++i) {
	if (strncmp(i->first.c_str(),prefix.c_str(),prefix.size()) == 0) {
	    AssertAlways(f == NULL,
			 ("Invalid getTypeByPrefix, two types match prefix %s: %s and %s\n",
			  prefix.c_str(),f->name.c_str(),i->first.c_str()));
	    f = i->second;
	}
    }
    AssertAlways(null_ok || f != NULL,("No type matching prefix %s found?!\n",
				       prefix.c_str()));
    return f;
}

ExtentType *
ExtentTypeLibrary::sharedExtentType(const string &xmldesc)
{
    xmlDecodeInfo k;
    k.xmldesc = xmldesc;
    xmlDecodeTableMutex.lock();
    xmlDecodeInfo **d = xmlDecodeTable.lookup(&k);
    xmlDecodeTableMutex.unlock();
    if (d != NULL && (**d).extenttype != NULL) {
	// should be common case, just return the value
	return (**d).extenttype;
    }
    if (d == NULL) {
	getSharedDecodeString(xmldesc); // force pointer to exist.
	xmlDecodeTableMutex.lock();
	d = xmlDecodeTable.lookup(&k);
	xmlDecodeTableMutex.unlock();
    }
    AssertAlways(d != NULL,("internal error\n"));
    if ((**d).extenttype == NULL) {
	// now safe to create, as constructor will be able to look up string
	ExtentType *f = new ExtentType(xmldesc); 
	xmlDecodeTableMutex.lock();
	if ((**d).extenttype == NULL) {
	    // normal case
	    (**d).extenttype = f;
	} else {
	    // someone else must have created it in the mean time, delete it.
	    delete f;
	}
	xmlDecodeTableMutex.unlock();
    }
    return (**d).extenttype;
}

xmlDocPtr 
ExtentTypeLibrary::sharedDocPtr(const string &xmldesc)
{
    xmlDecodeInfo k;
    k.xmldesc = xmldesc;
    xmlDecodeTableMutex.lock();
    xmlDecodeInfo **d = xmlDecodeTable.lookup(&k);
    xmlDecodeTableMutex.unlock();
    AssertAlways(d != NULL,("internal error\n"));
    return (**d).field_desc_doc;
}

