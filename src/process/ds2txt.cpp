// -*-C++-*-
/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Convert DataSeries files to text
*/

#include <Lintel/StringUtil.hpp>

#include <DataSeries/DStoTextModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/DSExpr.hpp>

using namespace std;
using boost::format;

static string str_DataSeries("DataSeries:");

int
main(int argc, char *argv[])
{
    TypeIndexModule source("");
    DStoTextModule toText(source);

    Extent::setReadChecksFromEnv(true); // ds2txt so slow, may as well check
    string select_extent_type, select_fields;
    string where_extent_type, where_expr_str;

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
	    if (*(argv[1]+12) == '<') {
		toText.setPrintSpec(argv[1] + 12);
	    } else if (strncmp(argv[1]+12, "type=", 5) == 0) {
		string tmp = string("<printSpec ") + (argv[1]+12) + "/>";
		toText.setPrintSpec(tmp.c_str()); // TODO: make it take a string
	    } else {
		FATAL_ERROR(format("Unexpected argument '%s' to --printSpec;\n expected it to start with '<' or 'type='") % (argv[1]+12));
	    }
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
	    source.setMatch(argv[1]+7);
	} else if (strcmp(argv[1],"--select")==0) {
	    INVARIANT(argc > 4, "--select needs two arguments");
	    INVARIANT(select_extent_type.empty(),
		      "multiple --select arguments specified");
	    select_extent_type = argv[2];
	    INVARIANT(select_extent_type != "",
		      "--select type needs to be non-empty");
	    select_fields = argv[3];
	    for(int i=3;i<argc;i++) {
		argv[i-2] = argv[i];
	    }
	    argc -= 2;
	} else if (strcmp(argv[1],"--where")==0) {
	    INVARIANT(argc > 4, "--where needs two arguments");
	    INVARIANT(where_extent_type.empty(),
		      "multiple where statements specified");
	    where_extent_type = argv[2];
	    INVARIANT(where_extent_type != "",
		      "--where type needs to be non-empty");
	    where_expr_str = argv[3];
	    for(int i=3;i<argc;i++) {
		argv[i-2] = argv[i];
	    }
	    argc -= 2;
	} else if (strncmp(argv[1],"-",1)==0) {
	    FATAL_ERROR(format("Unknown argument %s\n") % argv[1]);
	} else {
	    break;
	}
	for(int i=2;i<argc;i++) {
	    argv[i-1] = argv[i];
	}
	--argc;
    }
	    
    INVARIANT(argc >= 2 && strcmp(argv[1],"-h") != 0,
	      format("Usage: %s [--csv] [--separator=...] [--header=...]\n"
		     "  [--select '*'|'extent-type-match' '*'|'field,field,field']\n"
		     "  [--printSpec='type=\"...\" name=\"...\" print_format=\"...\" [units=\"...\" epoch=\"...\"]']\n"
// put in the <printSpec ... /> version into man page
// fields is mostly obsolete, move to man page eventually
//			    "  [--fields=<fields type=\"...\"><field name=\"...\"/></fields>]\n"
		     "  [--skip-index] [--skip-types] [--skip-extent-type]\n"
		     "  [--skip-extent-fieldnames] <file...>\n"
		     "  [--skip-all]\n"
		     "  [--where '*'|extent-type-match bool-expr]\n"
		     "\n%s\n")
	      % argv[0] % DSExpr::usage);
    for(int i=1;i<argc;++i) {
	source.addSource(argv[i]);
    }

    DataSeriesSource *first_source = new DataSeriesSource(argv[1]);

    if (select_extent_type != "") {
	string match_extent_type;
	const ExtentType *match_type 
	    = first_source->mylibrary.getTypeMatch(select_extent_type, 
						   false, true);

	match_extent_type = match_type->getName();
	vector<string> fields;
	split(select_fields,",",fields);
	string xmlspec("<fields type=\"");
	xmlspec.append(match_extent_type);
	xmlspec.append("\">");
	if (select_fields == "*") {
	    const ExtentType *t = 
		first_source->mylibrary.getTypeByPrefix(match_extent_type);
	    INVARIANT(t != NULL, "internal");
	    for(unsigned i = 0; i < t->getNFields(); ++i) {
		xmlspec.append((format("<field name=\"%s\"/>")
				% t->getFieldName(i)).str());
	    }
	} else {
	    for(unsigned i=0;i<fields.size();++i) {
		xmlspec.append((format("<field name=\"%s\"/>")
				% fields[i]).str());
	    }
	}
	xmlspec.append("</fields>");
	//	    printf("XXY %s\n",xmlspec.c_str());
	if (select_extent_type != "*") {
	    source.setMatch(select_extent_type);
	}
	toText.setFields(xmlspec.c_str());
    }

    if (where_extent_type != "") {
	const ExtentType *match_type 
	    = first_source->mylibrary.getTypeMatch(where_extent_type, 
						   false, true);
	toText.setWhereExpr(match_type->getName(), where_expr_str);
    }

    // Note that this doesn't completely do the right thing with
    // multiple files as we won't print out the extent types that only
    // occur in later files, of course, without a type of *, it's not
    // going to see those anyway.

    if (skip_types == false) {
	std::cout << "# Extent Types ...\n";
	for(std::map<const std::string, const ExtentType *>::iterator i = first_source->mylibrary.name_to_type.begin();
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

