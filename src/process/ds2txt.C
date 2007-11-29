// -*-C++-*-
/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Convert DataSeries files to text
*/

#include <Lintel/StringUtil.H>

#include <DataSeries/DStoTextModule.H>
#include <DataSeries/TypeIndexModule.H>

#ifndef __HP_aCC
using namespace std;
#endif

static string str_DataSeries("DataSeries:");

int
main(int argc, char *argv[])
{
    TypeIndexModule source("");
    DStoTextModule toText(source);

    Extent::setReadChecksFromEnv(true); // ds2txt so slow, may as well check
    string select_extent_type, select_fields;

    bool skip_types = false;
    while (argc > 2) {
        if (strncmp(argv[1],"--csv",5)==0) {
	    toText.enableCSV();
	    toText.setSeparator(",");
	    skip_types = true;
	    toText.skipIndex(); 
	    toText.skipExtentType();
	} else if (strncmp(argv[1],"--separator=",12)==0) {
	    std::string s = (char *)(argv[1] + 12);
	    toText.setSeparator(s);
	} else if (strncmp(argv[1],"--printSpec=",12)==0) {
	    toText.setPrintSpec(argv[1] + 12);
	} else if (strncmp(argv[1],"--header=",9)==0) {
	    toText.setHeader(argv[1] + 9);
	} else if (strncmp(argv[1],"--fields=",9)==0) {
	    toText.setFields(argv[1] + 9);
	} else if (strcmp(argv[1],"--skip-index")==0) {
	    toText.skipIndex(); 
	} else if (strcmp(argv[1],"--skip-types")==0) {
	    skip_types = true;
	} else if (strcmp(argv[1],"--skip-extent-type")==0) {
	    toText.skipExtentType(); 
	} else if (strcmp(argv[1],"--skip-extent-fieldnames")==0) {
	    toText.skipExtentFieldnames(); 
	} else if (strcmp(argv[1],"--skip-all")==0) {
	    toText.skipIndex();
	    toText.skipExtentType();
	    toText.skipExtentFieldnames();
	    skip_types = true;
	} else if (strncmp(argv[1],"--type=",7)==0) {
	    source.setPrefix(argv[1]+7);
	} else if (strcmp(argv[1],"--select")==0) {
	    AssertAlways(argc > 4,("--select needs two arguments"));
	    select_extent_type = argv[2];
	    AssertAlways(select_extent_type != "",("--select type needs to be non-empty"));
	    select_fields = argv[3];
	    for(int i=3;i<argc;i++) {
		argv[i-2] = argv[i];
	    }
	    argc -= 2;
	} else if (strncmp(argv[1],"-",1)==0) {
	    AssertFatal(("Unknown argument %s\n",argv[1]));
	} else {
	    break;
	}
	for(int i=2;i<argc;i++) {
	    argv[i-1] = argv[i];
	}
	--argc;
    }
	    
    AssertAlways(argc >= 2 && strcmp(argv[1],"-h") != 0,
		 ("Usage: %s [--csv] [--separator=...] [--printSpec=...] [--header=...]\n"
		  "  [--select '*'|'extent-type' '*'|'field,field,field']\n"
		  "  [--fields=<fields type=\"...\"><field name=\"...\"/></fields>]\n"
		  "  [--skip-index] [--skip-types] [--skip-extent-type]\n"
		  "  [--skip-extent-fieldnames] <file...>\n",argv[0]));
    for(int i=1;i<argc;++i) {
	source.addSource(argv[i]);
    }

    DataSeriesSource *first_source = new DataSeriesSource(argv[1]);

    if (select_extent_type != "") {
	string match_extent_type;
	for(std::map<const std::string, ExtentType *>::iterator i = first_source->mylibrary.name_to_type.begin();
	    i != first_source->mylibrary.name_to_type.end(); ++i) {
	    if ((select_extent_type == "*" && // ignore DS types when selecting "*"
		 !ExtentType::prefixmatch(i->first,str_DataSeries)) ||
		ExtentType::prefixmatch(i->first,select_extent_type)) {
		AssertAlways(match_extent_type == "",
			     ("select type '%s' matches both '%s' and '%s'",
			      select_extent_type.c_str(), i->first.c_str(),
			      match_extent_type.c_str()));
		match_extent_type = i->first;
	    } else {
		// skip, doesn't match select type
	    }
	}
	AssertAlways(match_extent_type != "",
		     ("select type '%s' doesn't match anything, try '*'",
		      select_extent_type.c_str()));
	vector<string> fields;
	split(select_fields,",",fields);
	string xmlspec("<fields type=\"");
	xmlspec.append(match_extent_type);
	xmlspec.append("\">");
	if (select_fields == "*") {
	    ExtentType *t = 
		first_source->mylibrary.getTypeByPrefix(match_extent_type);
	    INVARIANT(t != NULL, "internal");
	    for(unsigned i = 0; i < t->getNFields(); ++i) {
		xmlspec.append((boost::format("<field name=\"%s\"/>")
				% t->getFieldName(i)).str());
	    }
	} else {
	    for(unsigned i=0;i<fields.size();++i) {
		xmlspec.append((boost::format("<field name=\"%s\"/>")
				% fields[i]).str());
	    }
	}
	xmlspec.append("</fields>");
	//	    printf("XXY %s\n",xmlspec.c_str());
	if (select_extent_type != "*") {
	    source.setPrefix(select_extent_type);
	}
	toText.setFields(xmlspec.c_str());
    }

    // Note that this doesn't completely do the right thing with
    // multiple files as we won't print out the extent types that only
    // occur in later files, of course, without a type of *, it's not
    // going to see those anyway.

    if (skip_types == false) {
	std::cout << "# Extent Types ...\n";
	for(std::map<const std::string, ExtentType *>::iterator i = first_source->mylibrary.name_to_type.begin();
	    i != first_source->mylibrary.name_to_type.end(); ++i) {
	    std::cout << i->second->xmldesc << "\n";
	}
    }

    // Same issue as above w.r.t. multiple files.

    if (toText.printIndex() && first_source->indexExtent != NULL) {
	ExtentSeries es(first_source->indexExtent);
	Int64Field offset(es,"offset");
	Variable32Field extenttype(es,"extenttype");
	std::cout << "extent offset  ExtentType" << std::endl;
	for(;es.pos.morerecords();++es.pos) {
	    char buf[30];
	    snprintf(buf,20,(char *)"%-13lld  ",offset.val());
	    buf[19] = '\0';
	    std::cout << buf << extenttype.stringval() << std::endl;
	}
    }
    
    while(true) {
        Extent *e = toText.getExtent();
	if (e == NULL)
	    break;
	delete e;
    }
    
    return 0;
}

