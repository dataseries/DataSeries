// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    General field implementation
*/

#include <algorithm>

#include <boost/format.hpp>

#include <Lintel/Clock.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/GeneralField.hpp>

using namespace std;
using boost::format;

namespace {
    static const string s_true("true");
    static const string s_false("false");
    static const string s_on("on");
    static const string s_off("off");
    static const string s_yes("yes");
    static const string s_no("no");
    static const string s_null("null");
}

// TODO: performance time boost::format, and if possible, unify the
// write(ostream) and write(FILE *) code paths.

void GeneralValue::set(const GeneralValue &from) {
    if (gvtype != from.gvtype) {
        delete v_variable32;
        v_variable32 = NULL;
    }
    gvtype = from.gvtype;
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: break;
	case ExtentType::ft_bool: 
	    gvval.v_bool = from.gvval.v_bool; break;
	case ExtentType::ft_byte: 
	    gvval.v_byte = from.gvval.v_byte; break;
	case ExtentType::ft_int32: 
	    gvval.v_int32 = from.gvval.v_int32; break;
	case ExtentType::ft_int64: 
	    gvval.v_int64 = from.gvval.v_int64; break;
	case ExtentType::ft_double: 
	    gvval.v_double = from.gvval.v_double; break;
        case ExtentType::ft_variable32: case ExtentType::ft_fixedwidth: 
	    if (NULL == v_variable32) {
		v_variable32 = new string;
	    }
	    *v_variable32 = *from.v_variable32;
	    break;
	default: FATAL_ERROR("internal error, unexpected type"); break;
	}
}

void GeneralValue::set(const GeneralField &from) {
    if (gvtype != from.gftype) {
        delete v_variable32;
        v_variable32 = NULL;
    }
    if (from.typed_field.isNull()) {
        gvtype = ExtentType::ft_unknown;
        return;
    }
    gvtype = from.gftype;
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: break;
	case ExtentType::ft_bool: 
	    gvval.v_bool = ((GF_Bool *)&from)->val(); break;
	case ExtentType::ft_byte: 
	    gvval.v_byte = ((GF_Byte *)&from)->val(); break;
	case ExtentType::ft_int32: 
	    gvval.v_int32 = ((GF_Int32 *)&from)->val(); break;
	case ExtentType::ft_int64: 
	    gvval.v_int64 = ((GF_Int64 *)&from)->val(); break;
	case ExtentType::ft_double: 
	    gvval.v_double = ((GF_Double *)&from)->val(); break;
	case ExtentType::ft_variable32: {
	    if (NULL == v_variable32) {
		v_variable32 = new string;
	    }
	    const GF_Variable32 *tmp = reinterpret_cast<const GF_Variable32 *>(&from);
	    *v_variable32 = tmp->myfield.stringval(); 
	    break;
	}
	case ExtentType::ft_fixedwidth: {
	    const GF_FixedWidth *tmp = reinterpret_cast<const GF_FixedWidth *>(&from);
            if (v_variable32 != NULL && v_variable32->size() == static_cast<size_t>(tmp->size())) {
		memcpy(&((*v_variable32)[0]), tmp->myfield.val(), tmp->myfield.size());
            } else {
                delete v_variable32;
		v_variable32 = new string(reinterpret_cast<const char *>(tmp->val()), tmp->size());
	    }
	    break;
	}
	default: FATAL_ERROR("internal error, unexpected type"); break;
	}
}

void GeneralValue::set(const GeneralField &from, const Extent &e,
                       const dataseries::SEP_RowOffset &row_offset) {
    if (gvtype != from.gftype) {
        delete v_variable32;
        v_variable32 = NULL;
    }
    if (from.typed_field.isNull(e, row_offset)) {
        gvtype = ExtentType::ft_unknown;
        return;
    }
    gvtype = from.gftype;
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: break;
	case ExtentType::ft_bool: 
	    gvval.v_bool = reinterpret_cast<const GF_Bool *>(&from)->myfield.val(e, row_offset); 
            break;
	case ExtentType::ft_byte: 
	    gvval.v_byte = reinterpret_cast<const GF_Byte *>(&from)->myfield.val(e, row_offset); 
            break;
	case ExtentType::ft_int32: 
	    gvval.v_int32 = reinterpret_cast<const GF_Int32 *>(&from)->myfield.val(e, row_offset); 
            break;
	case ExtentType::ft_int64: 
	    gvval.v_int64 = reinterpret_cast<const GF_Int64 *>(&from)->myfield.val(e, row_offset); 
            break;
	case ExtentType::ft_double: 
            gvval.v_double = reinterpret_cast<const GF_Double *>(&from)
                ->myfield.val(e, row_offset); 
            break;
	case ExtentType::ft_variable32: {
	    if (NULL == v_variable32) {
		v_variable32 = new string;
	    }
	    const GF_Variable32 *tmp = reinterpret_cast<const GF_Variable32 *>(&from);
	    *v_variable32 = tmp->myfield.stringval(e, row_offset); 
	    break;
	}
	case ExtentType::ft_fixedwidth: {
	    const GF_FixedWidth *tmp = reinterpret_cast<const GF_FixedWidth *>(&from);
            if (v_variable32 != NULL && v_variable32->size() == static_cast<size_t>(tmp->size())) {
		memcpy(&((*v_variable32)[0]), tmp->myfield.val(e, row_offset), tmp->myfield.size());
            } else {
                delete v_variable32;
                const char *v = reinterpret_cast<const char *>(tmp->myfield.val(e, row_offset));
		v_variable32 = new string(v, tmp->size());
	    }
	    break;
	}
	default: FATAL_ERROR("internal error, unexpected type"); break;
	}
}

uint32_t GeneralValue::hash(uint32_t partial_hash) const {
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: return partial_hash;
	case ExtentType::ft_bool: 
	    return lintel::BobJenkinsHashMix3(gvval.v_bool, 1492, partial_hash);
	case ExtentType::ft_byte: 
	    return lintel::BobJenkinsHashMix3(gvval.v_byte, 1941, partial_hash);
	case ExtentType::ft_int32: 
	    return lintel::BobJenkinsHashMix3(gvval.v_int32, 1861, partial_hash);
	case ExtentType::ft_int64: 
	    return lintel::BobJenkinsHashMixULL(gvval.v_int64, partial_hash);
	case ExtentType::ft_double: 
	    BOOST_STATIC_ASSERT(sizeof(ExtentType::int64) == sizeof(double));
	    BOOST_STATIC_ASSERT(offsetof(gvvalT, v_double) 
				== offsetof(gvvalT, v_int64));
	    return lintel::BobJenkinsHashMixULL(static_cast<uint64_t>(gvval.v_int64),
						partial_hash);
	case ExtentType::ft_variable32: case ExtentType::ft_fixedwidth: 
	    return lintel::hashBytes(v_variable32->data(),
				     v_variable32->size(), partial_hash);
	default: FATAL_ERROR("internal error, unexpected type"); 
	    return 0; 
	}
}

void GeneralValue::setBool(bool val) {
    INVARIANT(gvtype == ExtentType::ft_unknown || 
	      gvtype == ExtentType::ft_bool,
	      "invalid to change type of generalvalue");
    gvtype = ExtentType::ft_bool;
    gvval.v_bool = val;
}

void GeneralValue::setByte(uint8_t val) {
    INVARIANT(gvtype == ExtentType::ft_unknown || 
	      gvtype == ExtentType::ft_byte,
	      "invalid to change type of generalvalue");
    gvtype = ExtentType::ft_byte;
    gvval.v_byte = val;
}

void GeneralValue::setInt32(int32_t val) {
    INVARIANT(gvtype == ExtentType::ft_unknown || 
	      gvtype == ExtentType::ft_int32,
	      "invalid to change type of generalvalue");
    gvtype = ExtentType::ft_int32;
    gvval.v_int32 = val;
}

void GeneralValue::setInt64(int64_t val) {
    INVARIANT(gvtype == ExtentType::ft_unknown || 
	      gvtype == ExtentType::ft_int64,
	      "invalid to change type of generalvalue");
    gvtype = ExtentType::ft_int64;
    gvval.v_int64 = val;
}

void GeneralValue::setDouble(double val) {
    INVARIANT(gvtype == ExtentType::ft_unknown || 
	      gvtype == ExtentType::ft_double,
	      "invalid to change type of generalvalue");
    gvtype = ExtentType::ft_double;
    gvval.v_double = val;
}

void GeneralValue::setVariable32(const string &val) {
    INVARIANT(gvtype == ExtentType::ft_unknown || gvtype == ExtentType::ft_variable32,
	      "invalid to change type of generalvalue");
    gvtype = ExtentType::ft_variable32;
    if (NULL == v_variable32) v_variable32 = new string;
    *v_variable32 = val;
}

void GeneralValue::setFixedWidth(const string &val) {
    INVARIANT(gvtype == ExtentType::ft_unknown || 
	      gvtype == ExtentType::ft_fixedwidth,
	      "invalid to change type of generalvalue");
    gvtype = ExtentType::ft_fixedwidth;
    if (NULL == v_variable32) v_variable32 = new string;
    *v_variable32 = val;
}

bool GeneralValue::strictlylessthan(const GeneralValue &gv) const {
    INVARIANT(gvtype == gv.gvtype,
	      format("currently invalid to compare general values of different types %s != %s")
              % ExtentType::fieldTypeToStr(gvtype) % ExtentType::fieldTypeToStr(gv.gvtype));
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: return false;
	case ExtentType::ft_bool:
	    return gvval.v_bool < gv.gvval.v_bool;
	case ExtentType::ft_byte:
	    return gvval.v_byte < gv.gvval.v_byte;
	case ExtentType::ft_int32:
	    return gvval.v_int32 < gv.gvval.v_int32;
	case ExtentType::ft_int64:
	    return gvval.v_int64 < gv.gvval.v_int64;
	case ExtentType::ft_double:
	    return gvval.v_double < gv.gvval.v_double;
	case ExtentType::ft_variable32: case ExtentType::ft_fixedwidth: 
	    return *v_variable32 < *gv.v_variable32;
	default:
	    FATAL_ERROR("internal error, unexpected type");
	    return false;
	}
    
}

bool GeneralValue::equal(const GeneralValue &gv) const {
    if (gvtype != gv.gvtype) {
        return false; // definitionally not equal, different types.
    }
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: return true; // other type is also null, null == null.
	case ExtentType::ft_bool:
	    return gvval.v_bool == gv.gvval.v_bool;
	case ExtentType::ft_byte:
	    return gvval.v_byte == gv.gvval.v_byte;
	case ExtentType::ft_int32:
	    return gvval.v_int32 == gv.gvval.v_int32;
	case ExtentType::ft_int64:
	    return gvval.v_int64 == gv.gvval.v_int64;
	case ExtentType::ft_double:
	    return gvval.v_double == gv.gvval.v_double;
        case ExtentType::ft_variable32: case ExtentType::ft_fixedwidth: 
	    return *v_variable32 == *gv.v_variable32;
	default:
	    FATAL_ERROR("internal error, unexpected type");
	    return false;
	}
    
}

namespace {
    char *long_int_format() {
	BOOST_STATIC_ASSERT(sizeof(long) == 4 || sizeof(long) == 8); 
	if (sizeof(long) == 8) {
	    return (char *)"%ld";
	} else if (sizeof(long) == 4) {
	    return (char *)"%lld";
	}
    }
}

void GeneralValue::write(FILE *to) {
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: 
	    fputs("*unknown-type*", to);
	    break;
	case ExtentType::ft_bool:
	    fputs((gvval.v_bool ? s_true.c_str() : s_false.c_str()), to);
	    break;
	case ExtentType::ft_byte:
	    fprintf(to,"%d", static_cast<uint32_t>(gvval.v_byte));
	    break;
	case ExtentType::ft_int32:
	    fprintf(to,"%d",gvval.v_int32);
	    break;
	case ExtentType::ft_int64:
	    fprintf(to, long_int_format(),gvval.v_int64);
	    break;
	case ExtentType::ft_double:
            // TODO: change all of the formats to be the same, unclear what the right choice
            // should be.  Too much precision leads to differing output; too little loses 
            // information.
	    fprintf(to,"%.12g",gvval.v_double);
	    break;
	case ExtentType::ft_variable32:	case ExtentType::ft_fixedwidth: 
	    fputs(maybehexstring(*v_variable32).c_str(), to);
	    break;
	default:
	    FATAL_ERROR("internal error, unexpected type");
	}
    
}

ostream &GeneralValue::write(ostream &to) const {
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: 
	    to << "*unknown-type*";
	    break;
	case ExtentType::ft_bool:
	    to << (gvval.v_bool ? s_true : s_false);
	    break;
	case ExtentType::ft_byte:
	    to << format("%d") % static_cast<uint32_t>(gvval.v_byte);
	    break;
	case ExtentType::ft_int32:
	    to << format("%d") % gvval.v_int32;
	    break;
	case ExtentType::ft_int64:
	    to << format("%lld") % gvval.v_int64;
	    break;
	case ExtentType::ft_double:
	    to << format("%.12g") % gvval.v_double;
	    break;
	case ExtentType::ft_variable32: case ExtentType::ft_fixedwidth: {
	    to << maybehexstring(*v_variable32);
	    break;
	}
	default:
	    FATAL_ERROR("internal error, unexpected type");
	}
    return to;
}

bool GeneralValue::valBool() const {
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: return false;
	case ExtentType::ft_bool:    return gvval.v_bool;
	case ExtentType::ft_byte:    return gvval.v_byte ? true : false;
	case ExtentType::ft_int32:   return gvval.v_int32 ? true : false;
	case ExtentType::ft_int64:   return gvval.v_int64 ? true : false;
	case ExtentType::ft_double:  return gvval.v_double ? true : false;
	case ExtentType::ft_variable32: {
	    SINVARIANT(v_variable32 != NULL);
	    if (*v_variable32 == s_true || *v_variable32 == s_on || *v_variable32 == s_yes) {
		return true;
	    } else if (*v_variable32 == s_false || *v_variable32 == s_off 
		       || *v_variable32 == s_no) {
		return false;
	    } else {
		FATAL_ERROR(format("Unable to convert string '%s' to boolean, expecting true, on,"
                                   " yes, false, off, or no") % *v_variable32);
	    }
	    break;
	}
	case ExtentType::ft_fixedwidth:
            FATAL_ERROR("haven't decided how to translate byte arrays to bools");
	default:
	    FATAL_ERROR("internal error, unexpected type"); 
	}
    return 0;
}

uint8_t GeneralValue::valByte() const {
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: return 0; 
	case ExtentType::ft_bool:    return gvval.v_bool ? 1 : 0;
	case ExtentType::ft_byte:    return gvval.v_byte;
	case ExtentType::ft_int32:   return gvval.v_int32;
	case ExtentType::ft_int64:   return gvval.v_int64;
	case ExtentType::ft_double:  return static_cast<uint8_t>(gvval.v_double);
	case ExtentType::ft_variable32: return stringToInteger<int32_t>(*v_variable32);
	case ExtentType::ft_fixedwidth:
	    FATAL_ERROR("haven't decided how to translate byte arrays to bytes");
	    break;
	default:
            FATAL_ERROR("internal error, unexpected type"); 
	}
    return 0;
}

int32_t GeneralValue::valInt32() const {
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: return 0; 
	case ExtentType::ft_bool:    return gvval.v_bool ? 1 : 0;
	case ExtentType::ft_byte:    return gvval.v_byte;
	case ExtentType::ft_int32:   return gvval.v_int32;
	case ExtentType::ft_int64:   return gvval.v_int64;
	case ExtentType::ft_double:  return static_cast<int32_t>(gvval.v_double);
	case ExtentType::ft_variable32: return stringToInteger<int32_t>(*v_variable32);
	case ExtentType::ft_fixedwidth:
	    FATAL_ERROR("haven't decided how to turn a byte array into an int");
	    break;
	default:
	    FATAL_ERROR("internal error, unexpected type"); 
	}
    return 0;
}

int64_t GeneralValue::valInt64() const {
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: return 0;
	case ExtentType::ft_bool:    return gvval.v_bool ? 1 : 0;
	case ExtentType::ft_byte:    return gvval.v_byte;
	case ExtentType::ft_int32:   return gvval.v_int32;
	case ExtentType::ft_int64:   return gvval.v_int64;
	case ExtentType::ft_double:  return static_cast<int64_t>(gvval.v_double);
	case ExtentType::ft_variable32: return stringToInteger<int64_t>(*v_variable32);
	case ExtentType::ft_fixedwidth:
            FATAL_ERROR("haven't decided how to translate byte arrays to integers");
	default:
	    FATAL_ERROR("internal error, unexpected type"); 
	}
    return 0;
}

double GeneralValue::valDouble() const {
    switch(gvtype) 
	{
            // If you have a nullable DoubleField, and do not specify a default, it will be 0.
            // We duplicate that behavior here, for better or worse.
	case ExtentType::ft_unknown: return 0;
	case ExtentType::ft_bool:    return gvval.v_bool ? 1 : 0;
	case ExtentType::ft_byte:    return gvval.v_byte;
	case ExtentType::ft_int32:   return gvval.v_int32;
	case ExtentType::ft_int64:   return gvval.v_int64;
	case ExtentType::ft_double:  return gvval.v_double;
	case ExtentType::ft_variable32: return stringToDouble(*v_variable32);
	case ExtentType::ft_fixedwidth:
            FATAL_ERROR("haven't decided how to translate byte arrays to doubles");
	default:
	    FATAL_ERROR("internal error, unexpected type"); 
	}
    return 0;
}

// TODO: can probably refactor this to use ostrstream
const std::string GeneralValue::valString() const {
    switch(gvtype) 
        {
        case ExtentType::ft_unknown: return std::string();
	case ExtentType::ft_bool: return gvval.v_bool ? s_true : s_false;
	case ExtentType::ft_byte: return str(format("%d") % static_cast<uint32_t>(gvval.v_byte));
	case ExtentType::ft_int32: return str(format("%d") % gvval.v_int32);
	case ExtentType::ft_int64: return str(format("%d") % gvval.v_int64);
	case ExtentType::ft_double: return str(format("%.20g") % gvval.v_double);
	case ExtentType::ft_fixedwidth:	case ExtentType::ft_variable32:
	    return *v_variable32;
	default:
	    FATAL_ERROR("internal error, unexpected type"); 
    }
    return 0;
}

void GeneralField::set(const string &from) {
    GeneralValue tmp;
    tmp.setVariable32(from);
    
    set(tmp);
}

void GeneralField::enableCSV(void) {
    csv_enabled = true;
}

void GeneralField::deleteFields(vector<GeneralField *> &fields) {
    for(vector<GeneralField *>::iterator i = fields.begin(); i != fields.end(); ++i) {
	delete *i;
	*i = NULL;
    }
    vector<GeneralField *> tmp;
    tmp.swap(fields);
}

static xmlChar *myXmlGetProp(xmlNodePtr xml, const xmlChar *prop) {
    if (xml == NULL) {
	return NULL;
    } else {
	return xmlGetProp(xml,prop);
    }
}

GF_Bool::GF_Bool(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column)
    : GeneralField(ExtentType::ft_bool, myfield), myfield(series, column, Field::flag_nullable)
{
    xmlChar *xmltrue = myXmlGetProp(fieldxml, (const xmlChar *)"print_true");
    if (xmltrue == NULL) {
	xmltrue = (xmlChar *)"T";
    }
    xmlChar *xmlfalse = myXmlGetProp(fieldxml, (const xmlChar *)"print_false");
    if (xmlfalse == NULL) {
	xmlfalse = (xmlChar *)"F";
    }
    s_true = (char *)xmltrue;
    s_false = (char *)xmlfalse;
}

GF_Bool::~GF_Bool() { }

void GF_Bool::write(FILE *to) {
    if (myfield.isNull()) {
	fputs("null", to);
    } else {
	if (myfield.val()) {
	    fputs(s_true.c_str(), to);
	} else {
	    fputs(s_false.c_str(), to);
	}
    }
}

void GF_Bool::write(std::ostream &to) {
    if (myfield.isNull()) {
	to << "null";
    } else {
	if (myfield.val()) {
	    to << s_true;
	} else {
	    to << s_false;
	}
    }
}

// TODO: see whether it's worth optimizing the set functions to special
// case the same-type copy; that's probably the common case.
// one experiment with this for GF_Bool did not show any benefit.
// fixing dsrepack to use type-specific fields for the copy did have a 
// huge benefit; it may be worth trying to figure out if there is a way 
// to get most of the benefit without having to make a special case like 
// that.

void GF_Bool::set(GeneralField *from) {
    if (from->isNull()) {
	myfield.setNull();
	return;
    }
    switch(from->getType())
	{
	case ExtentType::ft_bool: 
	    myfield.set(((GF_Bool *)from)->val());
	    break;
	case ExtentType::ft_byte:
	    myfield.set(((GF_Byte *)from)->val() == 0);
	    break;
	case ExtentType::ft_int32:
	    myfield.set(((GF_Int32 *)from)->val() == 0);
	    break;
	case ExtentType::ft_int64: 
	    myfield.set(((GF_Int64 *)from)->val() == 0);
	    break;
	case ExtentType::ft_double:
	    myfield.set(((GF_Double *)from)->val() == 0);
	    break;
	case ExtentType::ft_variable32:
	    FATAL_ERROR("variable32 -> bool not implemented yet");
	    break;
            // TODO: this is different behavior than if we passed from GeneralField to
            // GeneralValue, then passed to bool.  That is slightly wrong.  Further, we should have
            // a testcase that ensures that the different ways of passing back and forth are all
            // consistent.
	case ExtentType::ft_fixedwidth:
	    FATAL_ERROR("fixedwidth -> bool not implemented yet");
            break;
	default: 
	    FATAL_ERROR(format("internal error, unknown field type %d") % from->getType());
	}
}

void GF_Bool::set(const GeneralValue *from) {
    myfield.set(from->valBool());
}

double GF_Bool::valDouble() {
    return myfield.val() ? 1 : 0;
}

void GF_Bool::set(Extent &e, uint8_t *row_pos, const GeneralValue &from) {
    DEBUG_SINVARIANT(&e != NULL);
    if (from.getType() == ExtentType::ft_unknown) {
        myfield.setNull(e, row_pos, true);
    } else {
        typedef dataseries::detail::BoolFieldImpl ImplType;
        myfield.ImplType::set(e, row_pos, from.valBool());
    }
}



GF_Byte::GF_Byte(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column) 
    : GeneralField(ExtentType::ft_byte, myfield), myfield(series, column, Field::flag_nullable)
{
    xmlChar *xmlprintspec = myXmlGetProp(fieldxml, (const xmlChar *)"print_format");
    if (xmlprintspec == NULL) {
	xmlprintspec = (xmlChar *)"%d";
    }
    printspec = (char *)xmlprintspec;
    if (false) printf("should use printspec %s\n",printspec);
}

GF_Byte::~GF_Byte() { }

void GF_Byte::write(FILE *to) {
    if (myfield.isNull()) {
	fputs("null", to);
    } else {
	fprintf(to,printspec,myfield.val());
    }
}

void GF_Byte::write(std::ostream &to) {
    if (myfield.isNull()) {
	to << "null";
    } else {
	char buf[1024];
	int ok = snprintf(buf,1024,printspec,myfield.val());
	INVARIANT(ok > 0 && ok < 1000, format("bad printspec '%s'") % printspec);
	to << buf;
    }
}

void GF_Byte::set(GeneralField *from) {
    if (from->isNull()) {
	myfield.setNull();
	return;
    }
    ByteField::byte val = 0;
    switch(from->getType()) 
	{
	case ExtentType::ft_bool: 
	    val = ((GF_Bool *)from)->val() ? 1 : 0;
	    break;
	case ExtentType::ft_byte:
	    val = ((GF_Byte *)from)->val();
	    break;
	case ExtentType::ft_int32:
	    val = static_cast<ByteField::byte>(((GF_Int32 *)from)->val() & 0xFF);
	    break;
	case ExtentType::ft_int64: 
	    val = static_cast<ByteField::byte>(((GF_Int64 *)from)->val());
	    break;
	case ExtentType::ft_double:
	    val = static_cast<ByteField::byte>(round(((GF_Double *)from)->val()));
	    break;
	case ExtentType::ft_variable32:
	    FATAL_ERROR("unimplemented conversion from variable32 -> byte");
	    break;
	case ExtentType::ft_fixedwidth:
	    FATAL_ERROR("unimplemented conversion from fixed width -> byte");
	    break;
	default:
	    FATAL_ERROR(format("internal error, unknown field type %d") % from->getType());
    }
    myfield.set(val);
}

void GF_Byte::set(const GeneralValue *from) {
    myfield.set(from->valByte());
}

double GF_Byte::valDouble() {
    return static_cast<double>(myfield.val());
}

void GF_Byte::set(Extent &e, uint8_t *row_pos, const GeneralValue &from) {
    DEBUG_SINVARIANT(&e != NULL);
    if (from.getType() == ExtentType::ft_unknown) {
        myfield.setNull(e, row_pos, true);
    } else {
        typedef dataseries::detail::SimpleFixedFieldImpl<uint8_t> ImplType;
        myfield.ImplType::set(e, row_pos, from.valByte());
    }
}

GF_Int32::GF_Int32(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column) 
    : GeneralField(ExtentType::ft_int32, myfield), myfield(series, column, Field::flag_nullable)
{
    xmlChar *xmlprintspec = myXmlGetProp(fieldxml, (const xmlChar *)"print_format");
    if (xmlprintspec == NULL) {
	xmlprintspec = (xmlChar *)"%d";
    }
    printspec = (char *)xmlprintspec;
    if (false) printf("should use printspec %s\n",printspec);
    
    xmlChar *xml_divisor = myXmlGetProp(fieldxml, (const xmlChar *)"print_divisor");
    if (xml_divisor == NULL) {
	divisor = 1;
    } else {
	divisor = atoi((char *)xml_divisor);
    }
}

GF_Int32::~GF_Int32() { }

static const string ipv4addr("ipv4");

void GF_Int32::write(FILE *to) {
    if (myfield.isNull()) {
	fputs("null", to);
    } else if (printspec == ipv4addr) {
	SINVARIANT(divisor == 1);
	uint32_t v = static_cast<uint32_t>(myfield.val());
	fprintf(to, "%d.%d.%d.%d", v >> 24, (v >> 16) & 0xFF,
		(v >> 8) & 0xFF, v & 0xFF);
    } else {
	fprintf(to,printspec,myfield.val()/divisor);
    }
}

void GF_Int32::write(std::ostream &to) {
    FATAL_ERROR("inconsistent with write(FILE)");
    if (myfield.isNull()) {
	to << "null";
    } else {
	char buf[1024];
	int ok = snprintf(buf,1024,printspec,myfield.val()/divisor);
	INVARIANT(ok > 0 && ok < 1000, format("bad printspec '%s'") % printspec);
	to << buf;
    }
}

void GF_Int32::set(GeneralField *from) {
    if (from->isNull()) {
	myfield.setNull();
	return;
    }
    switch(from->getType()) 
	{
	case ExtentType::ft_bool: 
	    myfield.set(((GF_Bool *)from)->val() ? 1 : 0);
	    break;
	case ExtentType::ft_byte:
	    myfield.set(((GF_Byte *)from)->val());
	    break;
	case ExtentType::ft_int32:
	    myfield.set(((GF_Int32 *)from)->val());
	    break;
	case ExtentType::ft_int64: 
	    myfield.set((ExtentType::int32)((GF_Int64 *)from)->val());
	    break;
	case ExtentType::ft_double:
	    myfield.set((ExtentType::int32)round(((GF_Double *)from)->val()));
	    break;
	case ExtentType::ft_variable32:
	    FATAL_ERROR("unimplemented conversion from variable32 -> int32");
	    break;
	case ExtentType::ft_fixedwidth:
            FATAL_ERROR("unimplemented conversion from fixedwidth -> int32");
            break;
	default:
	    FATAL_ERROR(format("internal error, unknown field type %d") % from->getType());
	}
}

void GF_Int32::set(const GeneralValue *from) {
    myfield.set(from->valInt32());
}


double GF_Int32::valDouble() {
    return static_cast<double>(myfield.val());
}

void GF_Int32::set(Extent &e, uint8_t *row_pos, const GeneralValue &from) {
    DEBUG_SINVARIANT(&e != NULL);
    if (from.getType() == ExtentType::ft_unknown) {
        myfield.setNull(e, row_pos, true);
    } else {
        typedef dataseries::detail::SimpleFixedFieldImpl<int32_t> ImplType;
        myfield.ImplType::set(e, row_pos, from.valInt32());
    }
}

#ifdef __hpux
#include <inttypes.h>
static inline ExtentType::int64 
atoll(char *str)
{
    return __strtoll(str,NULL,0);
}
#endif

static string str_sec_nanosec = ("sec.nsec");

GF_Int64::GF_Int64(xmlNodePtr fieldxml, ExtentSeries &series, 
		   const std::string &column) 
    : GeneralField(ExtentType::ft_int64, myfield), myfield(series, column, Field::flag_nullable),
      relative_field(NULL), myfield_time(NULL), offset_first(false) 
{
    xmlChar *xmlprintspec = myXmlGetProp(fieldxml, (const xmlChar *)"print_format");
    if (xmlprintspec == NULL) {
	xmlprintspec = (xmlChar *)"%lld";
    }
    printspec = (char *)xmlprintspec;
    if (false) printf("should use printspec %s\n",printspec);
    
    if (printspec == str_sec_nanosec) {
	string units = strGetXMLProp(fieldxml, "units");
	string epoch = strGetXMLProp(fieldxml, "epoch");

	Int64TimeField::TimeType time_type
	    = Int64TimeField::convertUnitsEpoch(units, epoch, column);
	myfield_time = new Int64TimeField(series, column, Field::flag_nullable,
					  time_type);
    } 
	       
    xmlChar *xml_divisor = myXmlGetProp(fieldxml, (const xmlChar *)"print_divisor");
    if (xml_divisor == NULL) {
	divisor = 1;
    } else {
	divisor = stringToInteger<int64_t>(reinterpret_cast<char *>(xml_divisor));
    }

    xmlChar *xml_offset = myXmlGetProp(fieldxml, (const xmlChar *)"print_offset");
    if (xml_offset == NULL) {
	offset = 0;
    } else if (xmlStrcmp(xml_offset,(const xmlChar *)"first") == 0) {
	offset = 0;
	offset_first = true;
    } else if (xmlStrncmp(xml_offset,(const xmlChar *)"relativeto:",11) == 0) {
	std::string relname = (char *)(xml_offset + 11);
	relative_field = new Int64Field(series,relname);
    } else {
	offset = stringToInteger<int64_t>(reinterpret_cast<char *>(xml_offset));
    }

}

GF_Int64::~GF_Int64() {
    delete relative_field;
    delete myfield_time;
}

void GF_Int64::write(FILE *to) {
    if (myfield.isNull()) {
	fputs("null", to);
    } else {
	if (offset_first) {
	    offset = myfield.val();
	    offset_first = false;
	} else if (relative_field != NULL) {
	    offset = relative_field->val();
	}
	if (myfield_time != NULL) {
	    DEBUG_SINVARIANT(printspec == str_sec_nanosec);

	    int64_t v = myfield.val() - offset;

	    fputs(myfield_time->rawToStrSecNano(v).c_str(), to);
	} else {
	    fprintf(to,printspec,(myfield.val() - offset)/divisor);
	}
    }
}

void GF_Int64::write(std::ostream &to) {
    FATAL_ERROR("broken, inconsistent with FILE * version");
    if (myfield.isNull()) {
	to << "null";
    } else {
	char buf[1024];
	int ok = snprintf(buf,1024,printspec,myfield.val()/divisor);
	INVARIANT(ok > 0 && ok < 1000, format("bad printspec '%s'") % printspec);
	to << buf;
    }
}

void GF_Int64::set(GeneralField *from) {
    if (from->isNull()) {
	myfield.setNull();
	return;
    }
    switch(from->getType()) 
	{
	case ExtentType::ft_bool: 
	    myfield.set(((GF_Bool *)from)->val() ? 1 : 0);
	    break;
	case ExtentType::ft_byte:
	    myfield.set(((GF_Byte *)from)->val());
	    break;
	case ExtentType::ft_int32:
	    myfield.set(((GF_Int32 *)from)->val());
	    break;
	case ExtentType::ft_int64: 
	    myfield.set(((GF_Int64 *)from)->val());
	    break;
	case ExtentType::ft_double:
	    myfield.set((ExtentType::int64)round(((GF_Double *)from)->val()));
	    break;
	case ExtentType::ft_variable32:
	    FATAL_ERROR("unimplemented conversion from variable32 -> int64");
	    break;
	case ExtentType::ft_fixedwidth:
            FATAL_ERROR("unimplemented conversion from fixedwidth -> int64");
            break;
	default:
	    FATAL_ERROR(format("internal error, unknown field type %d") % from->getType());
	}
}

void GF_Int64::set(const GeneralValue *from) {
    myfield.set(from->valInt64());
}

double GF_Int64::valDouble() {
    return static_cast<double>(myfield.val());
}

// TODO: we have a lot of duplicated code in the various set and valFOO code.  Some of this may be
// reducable via template metaprogramming.  Specifically, there are three options:
//
// 1) Just give in that generalFOO is messy.
//
// 2) Use template metaprogramming.  E.g. make a GeneralValue::val<type T>(); which is abstract,
// and make GeneralField templated with a default type (void, say).  Then the GF_FOO classes could
// inheirit off of GeneralField<their type>, and use common code in GeneralValue.
// 
// 3) Use C preprocessor metaprogramming to #define the various set functions.
void GF_Int64::set(Extent &e, uint8_t *row_pos, const GeneralValue &from) {
    DEBUG_SINVARIANT(&e != NULL);
    if (from.getType() == ExtentType::ft_unknown) {
        myfield.setNull(e, row_pos, true);
    } else {
        typedef dataseries::detail::SimpleFixedFieldImpl<int64_t> ImplType;
        myfield.ImplType::set(e, row_pos, from.valInt64());
    }
}



GF_Double::GF_Double(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column) 
    : GeneralField(ExtentType::ft_double, myfield), 
      myfield(series, column, DoubleField::flag_nullable | DoubleField::flag_allownonzerobase)
{
    xmlChar *xmlprintspec = myXmlGetProp(fieldxml, (const xmlChar *)"print_format");
    if (xmlprintspec == NULL) {
	xmlprintspec = (xmlChar *)"%.9g";
    }
    printspec = (char *)xmlprintspec;
    if (false) printf("should use printspec %s\n",printspec);
    
    relative_field = NULL;
    xmlChar *xml_offset = myXmlGetProp(fieldxml, (const xmlChar *)"print_offset");
    if (xml_offset == NULL) {
	offset = 0;
    } else if (xmlStrcmp(xml_offset,(const xmlChar *)"first") == 0) {
	offset = Double::NaN;
    } else if (xmlStrncmp(xml_offset,(const xmlChar *)"relativeto:",11) == 0) {
	std::string relname = (char *)(xml_offset + 11);
	relative_field = new DoubleField(series,relname,DoubleField::flag_allownonzerobase);
    } else {
	offset = atof((char *)xml_offset);
    }
    xmlChar *xml_multiplier = myXmlGetProp(fieldxml, (const xmlChar *)"print_multiplier");
    if (xml_multiplier == NULL) {
	multiplier = 1;
    } else {
	multiplier = atof((char *)xml_multiplier);
    }
}

GF_Double::~GF_Double() {
    delete relative_field;
}

void GF_Double::write(FILE *to) {
    if (myfield.isNull()) {
	fputs("null", to);
    } else {
	if (isnan(offset)) {
	    offset = myfield.val();
	}
	if (relative_field != NULL) {
	    // offset needs to be calculated in absolute space, but things may be
	    // offset differently.
	    offset = relative_field->val() + (relative_field->base_val - myfield.base_val);
	}
	fprintf(to,printspec,multiplier * (myfield.val() - offset));
    }
}

void GF_Double::write(std::ostream &to) {
    if (myfield.isNull()) {
	to << "null";
    } else {
	if (offset != offset) {
	    offset = myfield.val();
	}
	if (relative_field != NULL) {
	    // offset needs to be calculated in absolute space, but things may be
	    // offset differently.
	    offset = relative_field->val() + (relative_field->base_val - myfield.base_val);
	}
	char buf[1024];
	int ok = snprintf(buf,1024,printspec,multiplier * (myfield.val() - offset));
	INVARIANT(ok > 0 && ok < 1000, format("bad printspec '%s'") % printspec);
		  
	to << buf;
    }
}

void GF_Double::set(GeneralField *from) {
    if (from->isNull()) {
	myfield.setNull();
	return;
    }
    switch(from->getType()) 
	{
	case ExtentType::ft_bool: 
	    myfield.set(((GF_Bool *)from)->val() ? 1 : 0);
	    break;
	case ExtentType::ft_byte:
	    myfield.set(((GF_Byte *)from)->val());
	    break;
	case ExtentType::ft_int32:
	    myfield.set(((GF_Int32 *)from)->val());
	    break;
	case ExtentType::ft_int64: 
	    myfield.set(((GF_Int64 *)from)->val());
	    break;
	case ExtentType::ft_double: {
	    GF_Double *dblfrom = (GF_Double *)from;
	    myfield.set(dblfrom->val() + (dblfrom->myfield.base_val - myfield.base_val));
	}
	    break;
	case ExtentType::ft_variable32:
	    FATAL_ERROR("unimplemented conversion from variable32 -> double");
	    break;
	case ExtentType::ft_fixedwidth:
            FATAL_ERROR("unimplemented conversion from fixedwidth -> double");
            break;
	default:
	    FATAL_ERROR(format("internal error, unknown field type %d") % from->getType());
	}
}

void GF_Double::set(const GeneralValue *from) {
    myfield.set(from->valDouble());
}

double GF_Double::valDouble() {
    return myfield.val();
}

void GF_Double::set(Extent &e, uint8_t *row_pos, const GeneralValue &from) {
    DEBUG_SINVARIANT(&e != NULL);
    if (from.getType() == ExtentType::ft_unknown) {
        myfield.setNull(e, row_pos, true);
    } else {
        typedef dataseries::detail::SimpleFixedFieldImpl<double> ImplType;
        myfield.ImplType::set(e, row_pos, from.valDouble());
    }
}

GF_Variable32::GF_Variable32(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column) 
    : GeneralField(ExtentType::ft_variable32, myfield), 
      myfield(series, column, Field::flag_nullable)
{
    xmlChar *xmlprintspec = myXmlGetProp(fieldxml, (const xmlChar *)"print_format");
    if (xmlprintspec == NULL) {
	xmlprintspec = (xmlChar *)"%s";
    }
    printspec = (char *)xmlprintspec;
    if (false) printf("should use printspec %s\n",printspec);
    
    // maybehex conversion will always be applied before csv conversion
    // there might be conflicts between csv and printspec, especially if printspec adds , and "
    xmlChar *xmlprintstyle = myXmlGetProp(fieldxml, (const xmlChar *)"print_style");
    if (xmlprintstyle == NULL) {
	// assume printstyle is printmaybehex if no style is present
	printstyle = printmaybehex;
    } else {
	// check to see if the style is a valid one:
	// for now: hex, maybehex, csv
	if (xmlStrcmp(xmlprintstyle,(xmlChar *)"hex")==0) {
	    printstyle = printhex;
	} else if (xmlStrcmp(xmlprintstyle, (xmlChar *)"maybehex")==0) {
	    printstyle = printmaybehex;
	} else if (xmlStrcmp(xmlprintstyle, (xmlChar *)"csv")==0) {
	    printstyle = printcsv;
	    csv_enabled = true;
	} else if (xmlStrcmp(xmlprintstyle, (xmlChar *)"text")==0) { 
	    printstyle = printtext;
	} else {
	    FATAL_ERROR(format("print_style should be hex, maybehex, csv, or text not '%s'")
			% reinterpret_cast<char *>(xmlprintstyle));
	}
    }
    
    // in case print_hex and print_maybehex are used print an obsolete abort message 
    // with the explanation to switch to printstyle
    // Stopped asserting because old DS files have the old style, so difficult to make
    // the changeover -- stupid need to support backwards compatibility.

    xmlChar *xml_printhex = myXmlGetProp(fieldxml, (const xmlChar *)"print_hex");
    if (xml_printhex != NULL) {
        // FATAL_ERROR("print_hex is obsolete, it was replaced by print_style=\"hex\"");
	printstyle = printhex;
    }

    xmlChar *xml_printmaybehex = myXmlGetProp(fieldxml, (const xmlChar *)"print_maybehex");
    if (xml_printmaybehex != NULL) {
        // FATAL_ERROR("print_maybehex is obsolete, it was replaced by print_style=\"maybehex\"");
	printstyle = printmaybehex;
    }

    if (false) printf("column %s: %d %d\n",column.c_str(),printhex,printmaybehex);

}

GF_Variable32::~GF_Variable32() { }

static const bool print_v32_spaces = false;

void GF_Variable32::write(FILE *to) {
    // TODO: unify with boths writes and val_formatted (but take care
    // as this write has extra behavior in that always applies a
    // printf format.
    if (myfield.isNull()) {
	fputs("null", to);
    } else {
	string v = valFormatted();
        fprintf(to, printspec, v.c_str());
    }
}

void GF_Variable32::write(std::ostream &to) {
    if (myfield.isNull()) {
	to << "null";
    } else if (printstyle == printhex) {
	to << hexstring(myfield.stringval());
    } else if (printstyle == printmaybehex) {
        to << maybehexstring(myfield.stringval());
    } else if ((printstyle == printcsv) || csv_enabled) {
        // no printspec for maybehex. For now, also do not apply printspec to csv and printmaybehex 
        to << toCSVform(maybehexstring(myfield.stringval()));
    } else {            
        const int bufsize = 1024;
        char buf[bufsize];
        snprintf(buf,bufsize-1,printspec,myfield.stringval().c_str());
        buf[bufsize-1] = '\0';
	to << buf;
    }
}

void GF_Variable32::set(GeneralField *from) {
    if (from->isNull()) {
	myfield.setNull();
	return;
    }
    switch(from->getType()) 
	{
	case ExtentType::ft_bool: 
	    if (((GF_Bool *)from)->val()) {
		myfield.set(s_true);
	    } else {
		myfield.set(s_false);
	    }
	    break;
	case ExtentType::ft_byte: { 
	    ExtentType::byte tmp = ((GF_Byte *)from)->val();
	    myfield.set(&tmp,1);
	}
	break;
	case ExtentType::ft_int32: {
	    ExtentType::int32 tmp = ((GF_Int32 *)from)->val();
	    myfield.set(&tmp,4);
	}
	break;
	case ExtentType::ft_int64: {
	    ExtentType::int64 tmp = ((GF_Int64 *)from)->val();
	    myfield.set(&tmp,8);
	}
	break;
	case ExtentType::ft_double: {
	    double tmp = ((GF_Double *)from)->val();
	    myfield.set(&tmp,8);
	}
	break;
	case ExtentType::ft_variable32: {
	    GF_Variable32 *tmp = (GF_Variable32 *)from;
	    myfield.set(tmp->myfield.val(),tmp->myfield.size());
	}
	break;
	case ExtentType::ft_fixedwidth:
            FATAL_ERROR("unimplemented conversion from fixedwidth -> variable32");
            break;
	default:
	    FATAL_ERROR(format("internal error, unknown field type %d") % from->getType());
	}
}

void GF_Variable32::set(const GeneralValue *from) {
    if (from->gvtype == ExtentType::ft_variable32) {
	myfield.set(*from->v_variable32);
    } else {
	myfield.set(from->valString());
    }
}

double GF_Variable32::valDouble() {
    return stringToDouble(myfield.stringval());
}

const std::string GF_Variable32::val() const {
    return myfield.stringval();
}

const std::string GF_Variable32::valFormatted() {
    if (myfield.isNull()) {
	return s_null;
    } else if (printstyle == printhex) {      
        return hexstring(myfield.stringval());
    } else if (printstyle == printmaybehex) {
	return maybehexstring(myfield.stringval());
    } else if ((printstyle == printcsv) || csv_enabled) {
	return toCSVform(maybehexstring(myfield.stringval()));
    } else if (printstyle == printtext) {
	return myfield.stringval();
    } else {
	return myfield.stringval();
    }
}

void GF_Variable32::set(Extent &e, uint8_t *row_pos, const GeneralValue &from) {
    DEBUG_SINVARIANT(&e != NULL);
    if (from.getType() == ExtentType::ft_unknown) {
        myfield.setNull(e, row_pos, true);
    } else {
        string v(from.valString());
        myfield.set(e, row_pos, v.data(), v.size());
    }
}

GF_FixedWidth::GF_FixedWidth(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column)
    : GeneralField(ExtentType::ft_fixedwidth, myfield),
      myfield(series, column, Field::flag_nullable)
{
    // TODO: do we want some of the fancy printspec stuff like in var32 fields?
}

GF_FixedWidth::~GF_FixedWidth() {
}

void GF_FixedWidth::write(FILE *to) {
    if (myfield.isNull()) {
        fputs("null", to);
    } else {
        string hex(maybehexstring(myfield.val(), myfield.size()));
        fputs(hex.c_str(), to);
    }
}

void GF_FixedWidth::write(std::ostream &to) {
    if (myfield.isNull()) {
        to << "null";
    } else {
        to << maybehexstring(myfield.val(), myfield.size());
    }
}

void GF_FixedWidth::set(GeneralField *from) {
    if (from->isNull()) {
        myfield.setNull();
        return;
    }
    switch(from->getType())
        {
        case ExtentType::ft_bool:
            FATAL_ERROR("unimplemented conversion from bool -> fixedwidth");
            break;
        case ExtentType::ft_byte:
            FATAL_ERROR("unimplemented conversion from byte -> fixedwidth");
            break;
        case ExtentType::ft_int32:
            FATAL_ERROR("unimplemented conversion from int32 -> fixedwidth");
            break;
        case ExtentType::ft_int64:
            FATAL_ERROR("unimplemented conversion from int64 -> fixedwidth");
            break;
        case ExtentType::ft_double:
            FATAL_ERROR("unimplemented conversion from double -> fixedwidth");
            break;
        case ExtentType::ft_variable32:
            FATAL_ERROR("unimplemented conversion from variable32 -> fixedwidth");
            break;
        case ExtentType::ft_fixedwidth:
            myfield.set((static_cast<GF_FixedWidth*>(from))->val());
            break;
        default:
            FATAL_ERROR(boost::format("internal error, unknown field type %d")
                        % from->getType());
        }
}

void GF_FixedWidth::set(const GeneralValue *from) {
    INVARIANT(from->gvtype == ExtentType::ft_fixedwidth,
              "can't set GF_FixedWidth from non-fixedwidth general value");
    SINVARIANT(from->v_variable32->size() == static_cast<size_t>(myfield.size()));
    myfield.set(from->v_variable32->data(), from->v_variable32->size());
}

double GF_FixedWidth::valDouble() {
    FATAL_ERROR("unimplemented conversion from fixedwidth -> double");
}

void GF_FixedWidth::set(Extent &e, uint8_t *row_pos, const GeneralValue &from) {
    DEBUG_SINVARIANT(&e != NULL);
    if (from.getType() == ExtentType::ft_unknown) {
        myfield.setNull(e, row_pos, true);
    } else {
        string val(from.valString());
        SINVARIANT(val.size() == static_cast<size_t>(myfield.size()));
        myfield.set(e, row_pos, val.data(), val.size());
    }
}

GeneralField::~GeneralField() { }

GeneralField *
GeneralField::create(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column) {
    INVARIANT(series.getType() != NULL,"need to set series type!");
    if (fieldxml == NULL) {
	fieldxml = series.getType()->xmlNodeFieldDesc(column);
    }
    switch (series.getType()->getFieldType(column)) 
	{
	case ExtentType::ft_bool:
	    return new GF_Bool(fieldxml,series,column);
	case ExtentType::ft_byte:
	    return new GF_Byte(fieldxml,series,column);
	case ExtentType::ft_int32:
	    return new GF_Int32(fieldxml,series,column);
	case ExtentType::ft_int64:
	    return new GF_Int64(fieldxml,series,column);
	case ExtentType::ft_double:
	    return new GF_Double(fieldxml,series,column);
	case ExtentType::ft_variable32:
	    return new GF_Variable32(fieldxml,series,column);
	case ExtentType::ft_fixedwidth:
	    return new GF_FixedWidth(fieldxml,series,column);
	default:
	    FATAL_ERROR("unimplemented");
	}    
    return NULL;
}

ExtentRecordCopy::ExtentRecordCopy(ExtentSeries &_source, ExtentSeries &_dest)
    : fixed_copy_size(-1), source(_source), dest(_dest)
{ }

void ExtentRecordCopy::prep(const ExtentType *copy_type) {
    SINVARIANT(fixed_copy_size == -1);
    SINVARIANT(source.type != NULL);
    SINVARIANT(dest.type != NULL);
    if (copy_type == NULL) {
        copy_type = dest.type;
    }
    if (source.getTypeCompat() == ExtentSeries::typeExact 
	&& dest.getTypeCompat() == ExtentSeries::typeExact
	&& source.getType() == dest.getType() && source.getType() == copy_type) {
	// Can do a bitwise copy.
	fixed_copy_size = source.getType()->fixedrecordsize();
	INVARIANT(fixed_copy_size > 0,"internal error");
	for(unsigned i=0;i<source.getType()->getNFields(); ++i) {
            const std::string &fieldname = source.getType()->getFieldName(i);
            if (source.getType()->getFieldType(fieldname) == ExtentType::ft_variable32) {
		sourcevarfields.push_back(new Variable32Field(source, fieldname, 
                                                              Field::flag_nullable));
		destvarfields.push_back(new Variable32Field(dest, fieldname, Field::flag_nullable));
	    }
	}
    } else {
	fixed_copy_size = 0;
	for(unsigned i=0; i < copy_type->getNFields(); ++i) {
	    const std::string &fieldname = copy_type->getFieldName(i);
	    sourcefields.push_back(GeneralField::create(NULL, source, fieldname));
	    INVARIANT(dest.getType()->hasColumn(fieldname),
		      format("Destination for copy is missing field %s") % fieldname);
	    destfields.push_back(GeneralField::create(NULL, dest, fieldname));
	}
    }
}

ExtentRecordCopy::~ExtentRecordCopy() {
    for(unsigned i=0;i < sourcefields.size();++i) {
	delete sourcefields[i];
	delete destfields[i];
    }
    
    for(unsigned i=0;i < sourcevarfields.size();++i) {
	delete sourcevarfields[i];
	delete destvarfields[i];
    }
}

void ExtentRecordCopy::copyRecord() {
    if (fixed_copy_size == -1) {
	prep();
    }
    INVARIANT(dest.morerecords(), "you forgot to create the destination record");
    INVARIANT(source.morerecords(), "you forgot to set the source record");
    if (fixed_copy_size > 0) {
	dest.checkOffset(fixed_copy_size-1);
	memcpy(dest.pos.record_start(),source.pos.record_start(),fixed_copy_size);
	// need to do things this way because in the process of doing
	// the memcpy we mangled the variable offsets that are stored
	// in the fixed fields.  If we don't pre-clear them, when we
	// call set it could try to overwrite non-existant bits.
	for(unsigned int i=0;i<sourcevarfields.size();++i) {
	    destvarfields[i]->clear();
            if (sourcevarfields[i]->isNull()) {
                destvarfields[i]->setNull();
            } else {
                destvarfields[i]->set(*sourcevarfields[i]);
            }
	}
    } else {
	SINVARIANT(destfields.size() == sourcefields.size());
	for(unsigned int i=0;i<sourcefields.size();++i) {
	    destfields[i]->set(sourcefields[i]);
	}
    }	
}

void ExtentRecordCopy::copyRecord(const Extent &extent, const dataseries::SEP_RowOffset &offset) {
    if (fixed_copy_size == -1) {
	prep();
    }
    INVARIANT(dest.morerecords(), "you forgot to create the destination record");

    if (fixed_copy_size > 0) {
        SINVARIANT(&extent.getType() == source.getType());
        const uint8_t *row_pos = offset.rowPos(extent);
	dest.checkOffset(fixed_copy_size-1);
	memcpy(dest.pos.record_start(), row_pos, fixed_copy_size);
	// need to do things this way because in the process of doing
	// the memcpy we mangled the variable offsets that are stored
	// in the fixed fields.  If we don't pre-clear them, when we
	// call set it could try to overwrite non-existant bits.
	for(unsigned int i=0;i<sourcevarfields.size();++i) {
	    destvarfields[i]->clear();
	    destvarfields[i]->set(sourcevarfields[i]->val(extent, offset),
                                  sourcevarfields[i]->size(extent, offset));
	}
    } else {
	SINVARIANT(destfields.size() == sourcefields.size());
	for(unsigned int i=0;i<sourcefields.size();++i) {
            FATAL_ERROR("unimplemented; will need a variant of set that takes extent, offset");
//	    destfields[i]->set(sourcefields[i]->val(extent, offset),
//                               sourcefields[i]-);
	}
    }	
}

