// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation
*/

#include <Lintel/LintelAssert.hpp>

#include <DataSeries/DStoTextModule.hpp>
#include <DataSeries/GeneralField.hpp>

using namespace std;

static const string str_star("*");

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
    // TODO: delete all the general fields in PerTypeState.
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
DStoTextModule::setPrintSpec(const string &extenttype,
			     const string &fieldname,
			     xmlNodePtr printSpec)
{
    type_to_state[extenttype].override_print_specs[fieldname] = printSpec;
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
DStoTextModule::setHeader(const string &extenttype,
			  const string &header) {
    type_to_state[extenttype].header = header;
}

void
DStoTextModule::setFields(const char *xmlText)
{
    xmlNodePtr cur = parseXML(xmlText,"fields");
    xmlChar *extenttype = xmlGetProp(cur, (const xmlChar *)"type");
    AssertAlways(extenttype != NULL,
		 ("error fields must have a type!\n"));
    string s_et = reinterpret_cast<char *>(extenttype);
    vector<string> &fields = type_to_state[s_et].field_names;
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
	string s_name = (char *)name;
	fields.push_back(s_name);
    }
}

void 
DStoTextModule::addPrintField(const string &extenttype, 
			      const string &field)
{
    if (extenttype == str_star) {
	default_fields.push_back(field);
    } else {
	type_to_state[extenttype].field_names.push_back(field);
    }
}


void
DStoTextModule::setSeparator(const string &s)
{
    separator = s;
}

void
DStoTextModule::enableCSV(void)
{
    csvEnabled = true;
    print_extent_type = false;
}


void
DStoTextModule::getExtentPrintSpecs(PerTypeState &state)
{
    if (!state.print_specs.empty()) {
	return;
    }
    state.print_specs = state.override_print_specs;
    const xmlDocPtr doc = state.series.type->getXmlDescriptionDoc();
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
	string s_fname = reinterpret_cast<char *>(fname);
	if (state.print_specs[s_fname] == NULL) {
	    state.print_specs[s_fname] = cur;
	}
	cur = cur->next;
    }
}

DStoTextModule::PerTypeState::~PerTypeState()
{
    for(vector<GeneralField *>::iterator i = fields.begin();
	i != fields.end(); ++i) {
	delete *i;
	*i = NULL;
    }
    fields.clear();
    for(map<string, xmlNodePtr>::iterator i = override_print_specs.begin();
	i != override_print_specs.end(); ++i) {
	xmlFreeDoc(i->second->doc);
	i->second = NULL;
    }
    override_print_specs.clear();
}

void
DStoTextModule::getExtentPrintHeaders(PerTypeState &state) 
{
    const string &type_name = state.series.type->name;
    if (print_extent_type) {
	if (text_dest == NULL) {
	    *stream_text_dest << "# Extent, type='" << type_name << "'\n";
	} else {
	    fprintf(text_dest,"# Extent, type='%s'\n", type_name.c_str());
	}
    }

    bool print_default_fieldnames = print_extent_fieldnames;
    if (print_extent_fieldnames && !state.header.empty()) {
	if (text_dest == NULL) {
	    *stream_text_dest << state.header << "\n";
	} else {
	    fprintf(text_dest,"%s\n",state.header.c_str());
	}
	print_default_fieldnames = false;
    }
    if (state.field_names.empty() && !default_fields.empty()) {
	state.field_names = default_fields;
    }

    if (state.field_names.empty()) {
	for(unsigned i=0;i<state.series.type->getNFields();++i) {
	    state.field_names.push_back(state.series.type->getFieldName(i));
	}
    }
    if (state.fields.empty()) {
	for(vector<string>::iterator i = state.field_names.begin();
	    i != state.field_names.end(); ++i) {
	    xmlNodePtr field_desc = state.print_specs[*i];
	    state.fields.push_back(GeneralField::create(field_desc,
							state.series,*i));
	    if (csvEnabled) {
		state.fields.back()->enableCSV();
	    }
	}
    }
    if (print_default_fieldnames) {
	bool printed_any = false;
	for(vector<string>::iterator i = state.field_names.begin();
	    i != state.field_names.end(); ++i) {
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
	    *stream_text_dest << "\n";
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
    if (e->type.getName() == "DataSeries: XmlType") {
	return e; // for now, never print these, that was previous behavior of ds2txt because the default source module skips the type extent at the beginning
    }

    if (print_index == false &&
	e->type.getName() == "DataSeries: ExtentIndex") {
	return e;
    }

    PerTypeState &state = type_to_state[e->type.getName()];

    state.series.setExtent(e);
    getExtentPrintSpecs(state);
    getExtentPrintHeaders(state);

    for (;state.series.pos.morerecords();++state.series.pos) {
	for(unsigned int i=0;i<state.fields.size();i++) {
	    if (text_dest == NULL) {
		state.fields[i]->write(*stream_text_dest);		
		if (i != (state.fields.size() - 1)){		  
		    *stream_text_dest << separator;
		}
	    } else {
		state.fields[i]->write(text_dest);
		if (i != (state.fields.size() - 1))
		    fprintf(text_dest,separator.c_str());
	    }
	}
	if (text_dest == NULL) {
	    *stream_text_dest << "\n";
	} else {
	    fprintf(text_dest,"\n");
	}
    }
    return e;
}

// this interface assumes you're just going to leak the document
xmlNodePtr
DStoTextModule::parseXML(string xml, const string &roottype)
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
    
