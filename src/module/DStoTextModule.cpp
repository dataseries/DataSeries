// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation
*/

#include <DataSeries/DSExpr.hpp>
#include <DataSeries/DStoTextModule.hpp>
#include <DataSeries/GeneralField.hpp>

using namespace std;
using boost::format;

static const string str_star("*");

DStoTextModule::DStoTextModule(DataSeriesModule &_source,
			       ostream &text_dest)
    : processed_rows(), ignored_rows(),
      source(_source), stream_text_dest(&text_dest),
      text_dest(NULL), print_index(true),
      print_extent_type(true), print_extent_fieldnames(true), 
      csvEnabled(false), separator(" "), 
      header_only_once(false), header_printed(false)
{
}

DStoTextModule::DStoTextModule(DataSeriesModule &_source,
			       FILE *_text_dest)
    : processed_rows(), ignored_rows(),
      source(_source), stream_text_dest(NULL),
      text_dest(_text_dest), print_index(true),
      print_extent_type(true), print_extent_fieldnames(true),
      csvEnabled(false), separator(" "),
      header_only_once(false), header_printed(false)
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
    INVARIANT(extenttype != NULL, "Error: printSpec missing type attribute");
    xmlChar *fieldname = xmlGetProp(cur, (const xmlChar *)"name");
    INVARIANT(fieldname != NULL, 
	      "Error: printSpec missing field name attribute");
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
    INVARIANT(extenttype != NULL, "Error: header missing type attribute");
    xmlChar *header = xmlNodeListGetString(cur->doc,cur->xmlChildrenNode, 1);
    INVARIANT(header != NULL, "Error: header missing content?!");
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
    INVARIANT(extenttype != NULL, "error fields must have a type!");
    string s_et = reinterpret_cast<char *>(extenttype);
    vector<string> &fields = type_to_state[s_et].field_names;
    for(cur = cur->xmlChildrenNode; cur != NULL; cur = cur->next) {
	if (xmlIsBlankNode(cur)) {
	    cur = cur->next;
	    continue;
	}
	INVARIANT(xmlStrcmp(cur->name, (const xmlChar *)"field") == 0,
	    format("Error: fields sub-element should be field, not '%s")
		  % reinterpret_cast<const char *>(cur->name));
	xmlChar *name = xmlGetProp(cur,(const xmlChar *)"name");
	INVARIANT(name != NULL, "error field must have a name");
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
DStoTextModule::setWhereExpr(const string &extenttype,
			     const string &where_expr_str)
{
    INVARIANT(type_to_state[extenttype].where_expr_str.empty(),
	      format("Error: multiple where expr for extent type '%s'")
	      % extenttype);
    INVARIANT(!where_expr_str.empty(),
	      format("Error: empty where expression for extent type '%s'")
	      % extenttype);
    type_to_state[extenttype].where_expr_str = where_expr_str;
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
DStoTextModule::setHeaderOnlyOnce()
{
    header_only_once = true;
}

void
DStoTextModule::getExtentPrintSpecs(PerTypeState &state)
{
    if (!state.print_specs.empty()) {
	return;
    }
    state.print_specs = state.override_print_specs;
    const xmlDocPtr doc = state.series.getTypePtr()->getXmlDescriptionDoc();
    xmlNodePtr cur = xmlDocGetRootElement(doc);
    cur = cur->xmlChildrenNode;
    while (cur != NULL) {
	while(cur != NULL && xmlIsBlankNode(cur)) {
	    cur = cur->next;
	}
	if (cur == NULL)
	    break;
	xmlChar *fname = xmlGetProp(cur,(const xmlChar *)"name");
	SINVARIANT(fname != NULL);
	string s_fname = reinterpret_cast<char *>(fname);
	if (state.print_specs[s_fname] == NULL) {
	    state.print_specs[s_fname] = cur;
	}
	cur = cur->next;
    }
}

void
DStoTextModule::getExtentParseWhereExpr(PerTypeState &state)
{
    if ((state.where_expr == NULL) &&
	(!state.where_expr_str.empty())) {
	state.where_expr = DSExpr::make(state.series, state.where_expr_str);
    }

}


DStoTextModule::PerTypeState::PerTypeState()
    : where_expr(NULL)
{}

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
    delete where_expr;
    where_expr = NULL;
}

void
DStoTextModule::getExtentPrintHeaders(PerTypeState &state) 
{
    if (header_only_once && header_printed) return;
    header_printed = true;

    const string &type_name = state.series.getTypePtr()->getName();
    if (print_extent_type) {
	if (text_dest == NULL) {
	    *stream_text_dest << "# Extent, type='" << type_name << "'";
	    if (state.where_expr) {
		*stream_text_dest << ", where='" << state.where_expr_str << "'";
	    }
	    *stream_text_dest << "\n";
	} else {
	    fprintf(text_dest,"# Extent, type='%s'", type_name.c_str());
	    if (state.where_expr) {
		fprintf(text_dest, ", where='%s'", state.where_expr_str.c_str());
	    }
	    fputc('\n', text_dest);
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
	for(unsigned i=0;i<state.series.getTypePtr()->getNFields();++i) {
	    state.field_names.push_back(state.series.getTypePtr()->getFieldName(i));
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
		    fputs(separator.c_str(), text_dest);
		fputs(i->c_str(), text_dest);
	    }
	    printed_any = true;
	}
    }

    if (print_default_fieldnames) {
	if (text_dest == NULL) {
	    *stream_text_dest << "\n";
	} else {
	    fputc('\n', text_dest);
	}
    }
}



Extent::Ptr DStoTextModule::getSharedExtent() {
    Extent::Ptr e = source.getSharedExtent();
    if (e == NULL) {
	return e;
    }
    if (e->type->getName() == "DataSeries: XmlType") {
	return e; // for now, never print these, that was previous behavior of ds2txt because the default source module skips the type extent at the beginning
    }

    if (print_index == false && e->type->getName() == "DataSeries: ExtentIndex") {
	return e;
    }

    PerTypeState &state = type_to_state[e->type->getName()];

    state.series.setExtent(e);
    getExtentParseWhereExpr(state);
    getExtentPrintSpecs(state);
    getExtentPrintHeaders(state);

    for (;state.series.morerecords();++state.series) {
	if (state.where_expr && !state.where_expr->valBool()) {
	    ++ignored_rows;
	} else {
	    ++processed_rows;
	    for(unsigned int i=0;i<state.fields.size();i++) {
		if (text_dest == NULL) {
		    state.fields[i]->write(*stream_text_dest);		
		    if (i != (state.fields.size() - 1)){		  
			*stream_text_dest << separator;
		    }
		} else {
		    state.fields[i]->write(text_dest);
		    if (i != (state.fields.size() - 1))
			fputs(separator.c_str(), text_dest);
		}
	    }
	    if (text_dest == NULL) {
		*stream_text_dest << "\n";
	    } else {
		fputs("\n", text_dest);
	    }
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
    INVARIANT(doc != NULL,
	      format("Error: parsing %s failed") % roottype);
    xmlNodePtr cur = xmlDocGetRootElement(doc);
    INVARIANT(cur != NULL,
	      format("Error: %s missing document") % roottype.c_str());
    INVARIANT(xmlStrcmp(cur->name, (const xmlChar *)roottype.c_str()) == 0,
	      format("Error: %s has wrong type") % roottype);
    return cur;
}
    
