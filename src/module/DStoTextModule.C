// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation
*/

#include <DStoTextModule.H>
#include <GeneralField.H>

#if defined(__HP_aCC) && __HP_aCC < 35000
#else
using namespace std;
#endif

DStoTextModule::DStoTextModule(DataSeriesModule &_source,
			       ostream &text_dest)
  : source(_source), stream_text_dest(&text_dest), text_dest(NULL), print_index(true), print_extent_type(true), print_extent_fieldnames(true), csvEnabled(false), separator(" ")
{
}

DStoTextModule::DStoTextModule(DataSeriesModule &_source,
			       FILE *_text_dest)
  : source(_source), stream_text_dest(NULL), text_dest(_text_dest), print_index(true), print_extent_type(true), print_extent_fieldnames(true), csvEnabled(false), separator(" ")
{
}

DStoTextModule::~DStoTextModule()
{
}

void
DStoTextModule::setPrintSpec(const char *xmlText)
{
    xmlNodePtr cur = parseXML(xmlText, "printSpec");
    xmlChar *extenttype = xmlGetProp(cur, (const xmlChar *)"type");
    AssertAlways(extenttype != NULL,("Error: printSpec missing type attribute\n"));
    xmlChar *fieldname = xmlGetProp(cur, (const xmlChar *)"name");
    AssertAlways(fieldname != NULL,("Error: printSpec missing field name attribute\n"));
    setPrintSpec((char *)extenttype,(char *)fieldname,cur);
}

void
DStoTextModule::setHeader(const char *xmlText)
{
    xmlNodePtr cur = parseXML(xmlText,"header");
    xmlChar *extenttype = xmlGetProp(cur, (const xmlChar *)"type");
    AssertAlways(extenttype != NULL,("Error: header missing type attribute\n"));
    xmlChar *header = xmlNodeListGetString(cur->doc,cur->xmlChildrenNode, 1);
    AssertAlways(header != NULL,("Error: header missing content?!\n"));
    setHeader((char *)extenttype,(char *)header);
}

void
DStoTextModule::setFields(const char *xmlText)
{
    xmlNodePtr cur = parseXML(xmlText,"fields");
    xmlChar *extenttype = xmlGetProp(cur, (const xmlChar *)"type");
    AssertAlways(extenttype != NULL,
		 ("error fields must have a type!\n"));
    std::string s_et = (char *)extenttype;
    std::vector<std::string> &fields = fieldLists[s_et];
    for(cur = cur->xmlChildrenNode; cur != NULL; cur = cur->next) {
	if (xmlIsBlankNode(cur)) {
	    cur = cur->next;
	    continue;
	}
	AssertAlways(xmlStrcmp(cur->name, (const xmlChar *)"field") == 0,
		     ("Error: fields sub-element should be field, not '%s\n",
		      cur->name));
	xmlChar *name = xmlGetProp(cur,(const xmlChar *)"name");
	AssertAlways(name != NULL,("error field must have a name\n"));
	std::string s_name = (char *)name;
	fields.push_back(s_name);
    }
}

void
DStoTextModule::setSeparator(const std::string &s)
{
    separator = s;
}

static const std::string str_star("*");

void
DStoTextModule::enableCSV(void)
{
    csvEnabled = true;
    print_extent_type = false;
}


void
DStoTextModule::getExtentPrintSpecs(std::map<std::string, xmlNodePtr> &printspecs,
				      ExtentSeries &es)
{
    printspecs = overridePrintSpecs[es.type->name];
    xmlDocPtr doc = ExtentTypeLibrary::sharedDocPtr(es.type->xmldesc);
    xmlNodePtr cur = xmlDocGetRootElement(doc);
    cur = cur->xmlChildrenNode;
    while (cur != NULL) {
	while(cur != NULL && xmlIsBlankNode(cur)) {
	    cur = cur->next;
	}
	if (cur == NULL)
	    break;
	xmlChar *fname = xmlGetProp(cur,(const xmlChar *)"name");
	AssertAlways(fname != NULL,("?!\n"));
	if (printspecs[(char *)fname] == NULL) {
	    printspecs[(char *)fname] = cur;
	}
	cur = cur->next;
    }
}

void
DStoTextModule::getExtentPrintHeaders(std::map<std::string, xmlNodePtr> &printspecs,
				      ExtentSeries &es, std::vector<GeneralField *> &fields)
{
    if (print_extent_type) {
	if (text_dest == NULL) {
	    *stream_text_dest << "# Extent, type='" << es.type->name << "'" << std::endl;
	} else {
	    fprintf(text_dest,"# Extent, type='%s'\n",es.type->name.c_str());
	}
    }

    bool print_default_fieldnames = print_extent_fieldnames;
    if (print_extent_fieldnames && headers[es.type->name].size() != 0) {
	if (text_dest == NULL) {
	    *stream_text_dest << headers[es.type->name] << std::endl;
	} else {
	    fprintf(text_dest,"%s\n",headers[es.type->name].c_str());
	}
	print_default_fieldnames = false;
    }
    std::vector<std::string> &field_names = fieldLists[es.type->name];
    if (field_names.empty() && fieldLists[str_star].empty() == false) {
	field_names = fieldLists[str_star];
    }
    if (field_names.empty()) {
	for(int i=0;i<es.type->getNFields();++i) {
	    field_names.push_back(es.type->getFieldName(i));
	}
    }
    bool printed_any = false;
    for(std::vector<std::string>::iterator i = field_names.begin();
	i != field_names.end(); ++i) {
	xmlNodePtr field_desc = printspecs[*i];
	fields.push_back(GeneralField::create(field_desc,es,*i));
	if (csvEnabled) {
	    fields[fields.size()-1]->enableCSV();
	}
	if (print_default_fieldnames) {
	    if (text_dest == NULL) {
		if (printed_any)
		    *stream_text_dest << separator;
		*stream_text_dest << *i;
	    } else {
		if (printed_any)
		    fprintf(text_dest,separator.c_str());
		fprintf(text_dest,"%s",i->c_str());
	    }
	    printed_any = true;
	}
    }

    if (print_default_fieldnames) {
	if (text_dest == NULL) {
	    *stream_text_dest << std::endl;
	} else {
	    fprintf(text_dest,"\n");
	}
    }
}

Extent *
DStoTextModule::getExtent()
{
    Extent *e = source.getExtent();
    if (e == NULL) 
	return NULL;
    if (e->type->name == "DataSeries: XmlType") {
	return e; // for now, never print these, that was previous behavior of ds2txt because the default source module skips the type extent at the beginning
    }

    if (print_index == false && e->type->name == "DataSeries: ExtentIndex") {
	return e;
    }
    ExtentSeries es(e);

    std::map<std::string, xmlNodePtr> printspecs;
    getExtentPrintSpecs(printspecs,es);
    std::vector<GeneralField *> fields;
    getExtentPrintHeaders(printspecs,es,fields);

    for (;es.pos.morerecords();++es.pos) {
	for(unsigned int i=0;i<fields.size();i++) {
	    if (text_dest == NULL) {
		fields[i]->write(*stream_text_dest);		
		if (i != (fields.size() - 1)){		  
		    *stream_text_dest << separator;
		}
	    } else {
		fields[i]->write(text_dest);
		if (i != (fields.size() - 1))
		    fprintf(text_dest,separator.c_str());
	    }
	}
	if (text_dest == NULL) {
	    *stream_text_dest << std::endl;
	} else {
	    fprintf(text_dest,"\n");
	}
    }
    for(std::vector<GeneralField *>::iterator i = fields.begin();
	i != fields.end();++i) {
	delete *i;
    }
    return e;
}

// this interface assumes you're just going to leak the document
xmlNodePtr
DStoTextModule::parseXML(std::string xml, const std::string &roottype)
{
    LIBXML_TEST_VERSION;
    xmlKeepBlanksDefault(0);
    xmlDocPtr doc = xmlParseMemory((char *)xml.data(),xml.size());
    AssertAlways(doc != NULL,("Error: parsing %s failed\n",roottype.c_str()));
    xmlNodePtr cur = xmlDocGetRootElement(doc);
    AssertAlways(cur != NULL,("Error: %s missing document\n",roottype.c_str()));
    AssertAlways(xmlStrcmp(cur->name, (const xmlChar *)roottype.c_str()) == 0,
		 ("Error: %s has wrong type\n",roottype.c_str()));
    return cur;
}
    
