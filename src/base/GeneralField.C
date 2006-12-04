// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    General field implementation
*/

#include <algorithm>

#include <StringUtil.H>

#include <GeneralField.H>
#if defined(__HP_aCC) && __HP_aCC < 35000
#else
using namespace std;
#endif

GeneralValue::GeneralValue()
    : gvtype(ExtentType::ft_unknown)
{
}

GeneralValue::GeneralValue(const GeneralField &from)
    : gvtype(ExtentType::ft_unknown)
{
    set(from);
}

GeneralValue::GeneralValue(const GeneralField *from)
    : gvtype(ExtentType::ft_unknown)
{
    set(*from);
}

void
GeneralValue::set(const GeneralField &from)
{
    AssertAlways(gvtype == ExtentType::ft_unknown || 
		 gvtype == from.gftype,
		 ("invalid to change type of generalvalue\n"));
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
	case ExtentType::ft_variable32: 
	    v_variable32 = ((GF_Variable32 *)&from)->myfield.stringval(); 
	    break;
	default: AssertFatal(("internal error")); break;
	}
}

void
GeneralValue::set(const GeneralValue &from)
{
    AssertAlways(gvtype == ExtentType::ft_unknown || 
		 gvtype == from.gvtype,
		 ("invalid to change type of generalvalue\n"));
    gvtype = from.gvtype;
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: break;
	case ExtentType::ft_bool: 
	    gvval.v_bool = from.gvval.v_bool;
	case ExtentType::ft_byte: 
	    gvval.v_byte = from.gvval.v_byte;
	case ExtentType::ft_int32: 
	    gvval.v_int32 = from.gvval.v_int32;
	case ExtentType::ft_int64: 
	    gvval.v_int64 = from.gvval.v_int64;
	case ExtentType::ft_double: 
	    gvval.v_double = from.gvval.v_double;
	case ExtentType::ft_variable32: 
	    v_variable32 = from.v_variable32;
	    break;
	default: AssertFatal(("internal error")); break;
	}
}

void
GeneralValue::setInt32(ExtentType::int32 from)
{
    AssertAlways(gvtype == ExtentType::ft_unknown || 
		 gvtype == ExtentType::ft_int32,
		 ("invalid to change type of generalvalue\n"));
    gvtype = ExtentType::ft_int32;
    gvval.v_int32 = from;
}

void
GeneralValue::setVariable32(const string &from)
{
    AssertAlways(gvtype == ExtentType::ft_unknown || 
		 gvtype == ExtentType::ft_variable32,
		 ("invalid to change type of generalvalue\n"));
    gvtype = ExtentType::ft_variable32;
    v_variable32 = from;
}

bool 
GeneralValue::strictlylessthan(const GeneralValue &gv) const 
{
    AssertAlways(gvtype == gv.gvtype,
		 ("currently invalid to compare general values of different types\n"));
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
	case ExtentType::ft_variable32: {
	    int diff = memcmp(v_variable32.data(),gv.v_variable32.data(),
			      min(v_variable32.size(),gv.v_variable32.size()));
	    if (diff != 0) {
		return diff < 0;
	    } else {
		return v_variable32.size() < gv.v_variable32.size();
	    }
	}
	default:
	    AssertFatal(("internal error"));
	    return false;
	}
    
}

bool 
GeneralValue::equal(const GeneralValue &gv) const
{
    AssertAlways(gvtype == gv.gvtype,
		 ("currently invalid to compare general values of different types\n"));
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: return false;
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
	case ExtentType::ft_variable32: {
	    if (v_variable32.size() == gv.v_variable32.size()) {
		return memcmp(v_variable32.data(),gv.v_variable32.data(),
			      v_variable32.size()) == 0;
	    } else {
		return false;
	    }
	}
	default:
	    AssertFatal(("internal error"));
	    return false;
	}
    
}

void
GeneralValue::write(FILE *to)
{
    switch(gvtype) 
	{
	case ExtentType::ft_unknown: 
	    fprintf(to,"*unknown-type*");
	    break;
	case ExtentType::ft_bool:
	    fprintf(to,gvval.v_bool ? "true" : "false");
	    break;
	case ExtentType::ft_byte:
	    fprintf(to,"%d",(unsigned char)gvval.v_byte);
	    break;
	case ExtentType::ft_int32:
	    fprintf(to,"%d",gvval.v_int32);
	    break;
	case ExtentType::ft_int64:
	    fprintf(to,"%lld",gvval.v_int64);
	    break;
	case ExtentType::ft_double:
	    fprintf(to,"%.12g",gvval.v_double);
	    break;
	case ExtentType::ft_variable32: {
	    fprintf(to,"%s",maybehexstring(v_variable32).c_str());
	    break;
	}
	default:
	    AssertFatal(("internal error"));
	}
    
}


void GeneralField::enableCSV(void){
    csvEnabled = true;
}


static xmlChar *
myXmlGetProp(xmlNodePtr xml, const xmlChar *prop)
{
    if (xml == NULL) {
	return NULL;
    } else {
	return xmlGetProp(xml,prop);
    }
}

GF_Bool::GF_Bool(xmlNodePtr fieldxml, ExtentSeries &series, 
		 const std::string &column)
    : GeneralField(ExtentType::ft_bool),
      myfield(series,column,Field::flag_nullable)
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

GF_Bool::~GF_Bool()
{
}

void
GF_Bool::write(FILE *to)
{
    if (myfield.isNull()) {
	fprintf(to,"null");
    } else {
	if (myfield.val()) {
	    fprintf(to,"%s",s_true.c_str());
	} else {
	    fprintf(to,"%s",s_false.c_str());
	}
    }
}

void
GF_Bool::write(std::ostream &to)
{
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

bool
GF_Bool::isNull()
{
    return myfield.isNull();
}

void
GF_Bool::set(GeneralField *from)
{
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
	    AssertFatal(("variable32 -> bool not implemented yet"));
	    break;
	default:
	    AssertFatal(("internal error, unknown field type %d\n",from->getType()));
	}
}

void
GF_Bool::set(const GeneralValue *from)
{
    AssertAlways(from->gvtype == ExtentType::ft_bool,
		 ("can't set GF_Bool from non-bool general value"));
    myfield.set(from->gvval.v_bool);
}

GF_Byte::GF_Byte(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column) 
    : GeneralField(ExtentType::ft_byte), myfield(series,column,Field::flag_nullable)
{
    xmlChar *xmlprintspec = myXmlGetProp(fieldxml, (const xmlChar *)"print_format");
    if (xmlprintspec == NULL) {
	xmlprintspec = (xmlChar *)"%d";
    }
    printspec = (char *)xmlprintspec;
    if (false) printf("should use printspec %s\n",printspec);
}

GF_Byte::~GF_Byte() 
{
}

void 
GF_Byte::write(FILE *to) {
    if (myfield.isNull()) {
	fprintf(to,"null");
    } else {
	fprintf(to,printspec,myfield.val());
    }
}

void 
GF_Byte::write(std::ostream &to) {
    if (myfield.isNull()) {
	to << "null";
    } else {
	char buf[1024];
	int ok = snprintf(buf,1024,printspec,myfield.val());
	AssertAlways(ok > 0 && ok < 1000,("bad printspec '%s'\n",printspec));
	to << buf;
    }
}

bool
GF_Byte::isNull()
{
    return myfield.isNull();
}

void
GF_Byte::set(GeneralField *from)
{
    if (from->isNull()) {
	myfield.setNull();
	return;
    }
    AssertFatal(("unimplemented\n"));
}

void
GF_Byte::set(const GeneralValue *from)
{
    AssertAlways(from->gvtype == ExtentType::ft_byte,
		 ("can't set GF_Byte from non-byte general value"));
    myfield.set(from->gvval.v_byte);
}


GF_Int32::GF_Int32(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column) 
    : GeneralField(ExtentType::ft_int32), myfield(series,column,Field::flag_nullable)
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

GF_Int32::~GF_Int32() 
{
}

void 
GF_Int32::write(FILE *to) {
    if (myfield.isNull()) {
	fprintf(to,"null");
    } else {
	fprintf(to,printspec,myfield.val()/divisor);
    }
}

void 
GF_Int32::write(std::ostream &to) {
    if (myfield.isNull()) {
	to << "null";
    } else {
	char buf[1024];
	int ok = snprintf(buf,1024,printspec,myfield.val()/divisor);
	AssertAlways(ok > 0 && ok < 1000,("bad printspec '%s'\n",printspec));
	to << buf;
    }
}

bool
GF_Int32::isNull()
{
    return myfield.isNull();
}

void
GF_Int32::set(GeneralField *from)
{
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
	    AssertFatal(("unimplemented conversion from variable32 -> int32"));
	    break;
	default:
	    AssertFatal(("internal error, unknown field type %d\n",from->getType()));
	}
}

void
GF_Int32::set(const GeneralValue *from)
{
    AssertAlways(from->gvtype == ExtentType::ft_int32,
		 ("can't set GF_Int32 from non-int32 general value"));
    myfield.set(from->gvval.v_int32);
}


#ifdef __hpux
#include <inttypes.h>
static inline ExtentType::int64 
atoll(char *str)
{
    return __strtoll(str,NULL,0);
}
#endif

GF_Int64::GF_Int64(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column) 
    : GeneralField(ExtentType::ft_int64), myfield(series,column,Field::flag_nullable)
{
    xmlChar *xmlprintspec = myXmlGetProp(fieldxml, (const xmlChar *)"print_format");
    if (xmlprintspec == NULL) {
	xmlprintspec = (xmlChar *)"%lld";
    }
    printspec = (char *)xmlprintspec;
    if (false) printf("should use printspec %s\n",printspec);
    
    xmlChar *xml_divisor = myXmlGetProp(fieldxml, (const xmlChar *)"print_divisor");
    if (xml_divisor == NULL) {
	divisor = 1;
    } else {
	divisor = atoll((char *)xml_divisor);
    }
}

GF_Int64::~GF_Int64() 
{
}

void 
GF_Int64::write(FILE *to) 
{
    if (myfield.isNull()) {
	fprintf(to,"null");
    } else {
	fprintf(to,printspec,myfield.val()/divisor);
    }
}

void 
GF_Int64::write(std::ostream &to) 
{
    if (myfield.isNull()) {
	to << "null";
    } else {
	char buf[1024];
	int ok = snprintf(buf,1024,printspec,myfield.val()/divisor);
	AssertAlways(ok > 0 && ok < 1000,("bad printspec '%s'\n",printspec));
	to << buf;
    }
}

bool
GF_Int64::isNull()
{
    return myfield.isNull();
}

void
GF_Int64::set(GeneralField *from)
{
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
	    AssertFatal(("unimplemented conversion from variable32 -> int64"));
	    break;
	default:
	    AssertFatal(("internal error, unknown field type %d\n",from->getType()));
	}
}

void
GF_Int64::set(const GeneralValue *from)
{
    AssertAlways(from->gvtype == ExtentType::ft_int64,
		 ("can't set GF_Int64 from non-int64 general value"));
    myfield.set(from->gvval.v_int64);
}


GF_Double::GF_Double(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column) 
    : GeneralField(ExtentType::ft_double), myfield(series,column,DoubleField::flag_nullable | DoubleField::flag_allownonzerobase)
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

GF_Double::~GF_Double() 
{
}

void 
GF_Double::write(FILE *to) 
{
    if (myfield.isNull()) {
	fprintf(to,"null");
    } else {
	if (offset != offset) {
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

void 
GF_Double::write(std::ostream &to) 
{
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
	AssertAlways(ok > 0 && ok < 1000,("bad printspec '%s'\n",printspec));
	to << buf;
    }
}

bool
GF_Double::isNull()
{
    return myfield.isNull();
}

void
GF_Double::set(GeneralField *from)
{
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
	    AssertFatal(("unimplemented conversion from variable32 -> double"));
	    break;
	default:
	    AssertFatal(("internal error, unknown field type %d\n",from->getType()));
	}
}

void
GF_Double::set(const GeneralValue *from)
{
    AssertAlways(from->gvtype == ExtentType::ft_double,
		 ("can't set GF_Double from non-double general value"));
    myfield.set(from->gvval.v_double);
}

GF_Variable32::GF_Variable32(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column) 
    : GeneralField(ExtentType::ft_variable32), myfield(series,column,Field::flag_nullable)
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
	    csvEnabled = true;
	} else if (xmlStrcmp(xmlprintstyle, (xmlChar *)"text")==0) { 
	    printstyle = printtext;
	} else {
	    AssertFatal(("print_style should be hex, maybehex or csv not '%s'\n",
			 (char *)xmlprintstyle));
	}
    }
    
    // in case print_hex and print_maybehex are used print an obsolete abort message 
    // with the explanation to switch to printstyle
    // Stopped asserting because old DS files have the old style, so difficult to make
    // the changeover -- stupid need to support backwards compatibility.

    xmlChar *xml_printhex = myXmlGetProp(fieldxml, (const xmlChar *)"print_hex");
    if (xml_printhex != NULL) {
        // AssertFatal(("print_hex is obsolete, it was replaced by print_style=\"hex\"\n"));
	printstyle = printhex;
    }

    xmlChar *xml_printmaybehex = myXmlGetProp(fieldxml, (const xmlChar *)"print_maybehex");
    if (xml_printmaybehex != NULL) {
        // AssertFatal(("print_maybehex is obsolete, it was replaced by print_style=\"maybehex\"\n"));
	printstyle = printmaybehex;
    }

    if (false) printf("column %s: %d %d\n",column.c_str(),printhex,printmaybehex);

}

GF_Variable32::~GF_Variable32() 
{
}

static const bool print_v32_spaces = false;

void 
GF_Variable32::write(FILE *to) 
{
    if (myfield.isNull()) {
	fprintf(to,"null");
    } else if (printstyle == printhex) {      
        fprintf(to,printspec,hexstring(myfield.stringval()).c_str());
    } else if (printstyle == printmaybehex) {
	fprintf(to,printspec,maybehexstring(myfield.stringval()).c_str());
    } else if ((printstyle == printcsv) || csvEnabled) {
        fprintf(to,printspec,(toCSVform(maybehexstring(myfield.stringval()))).c_str());
    } else if (printstyle == printtext) {
	fprintf(to,printspec,myfield.stringval().c_str());
    } else {	      
	fprintf(to,printspec,myfield.stringval().c_str());
    }
}

void 
GF_Variable32::write(std::ostream &to) 
{
    if (myfield.isNull()) {
	to << "null";
    } else if (printstyle == printhex) {
	to << hexstring(myfield.stringval());
    } else if (printstyle == printmaybehex) {
        to << maybehexstring(myfield.stringval());
    } else if ((printstyle == printcsv) || csvEnabled) {
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

bool
GF_Variable32::isNull()
{
    return myfield.isNull();
}

static const string str_true("true");
static const string str_false("false");

void
GF_Variable32::set(GeneralField *from)
{
    if (from->isNull()) {
	myfield.setNull();
	return;
    }
    switch(from->getType()) 
	{
	case ExtentType::ft_bool: 
	    if (((GF_Bool *)from)->val()) {
		myfield.set(str_true);
	    } else {
		myfield.set(str_false);
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
	default:
	    AssertFatal(("internal error, unknown field type %d\n",from->getType()));
	}
}

void
GF_Variable32::set(const GeneralValue *from)
{
    AssertAlways(from->gvtype == ExtentType::ft_variable32,
		 ("can't set GF_Variable32 from non-variable32 general value"));
    myfield.set(from->v_variable32);
}

GeneralField::~GeneralField() 
{
}

GeneralField *
GeneralField::create(xmlNodePtr fieldxml, ExtentSeries &series, const std::string &column)
{
    AssertAlways(series.type != NULL,("need to set series type!\n"));
    if (fieldxml == NULL) {
	fieldxml = series.type->xmlNodeFieldDesc(column);
    }
    switch (series.type->getFieldType(column)) 
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
	default:
	    AssertFatal(("unimplemented\n"));
	}    
    return NULL;
}

ExtentRecordCopy::ExtentRecordCopy(ExtentSeries &_source, ExtentSeries &_dest)
    : source(_source), dest(_dest)
{
    if ((source.getTypeCompat() == ExtentSeries::typeExact ||
	 source.getTypeCompat() == ExtentSeries::typeXMLIdentical ||
	 dest.getTypeCompat() == ExtentSeries::typeExact ||
	 dest.getTypeCompat() == ExtentSeries::typeXMLIdentical) 
	&& source.type->xmldesc == dest.type->xmldesc) {
	fixed_copy_size = source.type->fixedrecordsize();
	AssertAlways(fixed_copy_size > 0,("internal error\n"));
	for(int i=0;i<source.type->getNFields(); ++i) {
	    const std::string &fieldname = source.type->getFieldName(i);
	    if (source.type->getFieldType(fieldname) == ExtentType::ft_variable32) {
		sourcevarfields.push_back(new GF_Variable32(NULL,source,fieldname));
		destvarfields.push_back(new GF_Variable32(NULL,dest,fieldname));
	    }
	}
    } else {
	fixed_copy_size = 0;
	for(int i=0;i<source.type->getNFields(); ++i) {
	    const std::string &fieldname = source.type->getFieldName(i);
	    sourcefields.push_back(GeneralField::create(NULL,source,fieldname));
	    destfields.push_back(GeneralField::create(NULL,dest,fieldname));
	}
    }
}

ExtentRecordCopy::~ExtentRecordCopy()
{
    for(unsigned i=0;i < sourcefields.size();++i) {
	delete sourcefields[i];
	delete destfields[i];
    }
    
    for(unsigned i=0;i < sourcevarfields.size();++i) {
	delete sourcevarfields[i];
	delete destvarfields[i];
    }
}

void
ExtentRecordCopy::copyrecord()
{
    if (fixed_copy_size > 0) {
	dest.pos.checkOffset(fixed_copy_size-1);
	memcpy(dest.pos.record_start(),source.pos.record_start(),fixed_copy_size);
	// need to do things this way because in the process of doing
	// the memcpy we mangled the variable offsets that are stored
	// in the fixed fields.  If we don't pre-clear them, when we
	// call set it could try to overwrite non-existant bits.
	for(unsigned int i=0;i<sourcevarfields.size();++i) {
	    destvarfields[i]->clear();
	    destvarfields[i]->set(sourcevarfields[i]);
	}
    } else {
	for(unsigned int i=0;i<sourcefields.size();++i) {
	    destfields[i]->set(sourcevarfields[i]);
	}
    }	
}

