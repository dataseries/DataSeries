// -*-C++-*-
/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Convert DataSeries files to text
*/

/*
=pod

=head1 NAME

ds2txt - convert a dataseries file into text (or comma separated value)

=head1 SYNOPSIS

 % ds2txt [OPTIONS] input.ds [input-2.ds] > output.txt

=head1 DESCRIPTION

ds2txt converts one or more input DataSeries files into text.  It has a variety of ways to select
sub-parts of a file to convert, and various options to control the output text format.  One of the
useful options is --csv to convert to a comma separated value outptu format.

=head1 OPTIONS

=over 4

=item --csv

Specify that the output should be in csv format.  Sets the separator to ',' and automatically skips
print the types index and extent type header.

=item --separator=I<string>

Specify that value that should be used.

=item --printSpec=I<xml specification|sub-part specification>

Specify the format that should be used for printing out a particular column.  Takes the options
supported by the DsToTextModule.  The format can either be <.../> to specify raw xml, or
type=I<type-name> ...; to specify the format more simply, in which case, ds2txt will automatically
wrap what you write in <printSpec I<your arguments> />.  Two arguments are required, type="..." and
name="..." in order to select the field that is being formatted.  Some standard optional formats
include print_format="I<a boost format like string>" units="units to use" epoch="epoch for a time
field".  TODO: document better.

=item --header=I<string>

Specify the header that should be printed at the start of each extent

=item --header-only-once

Specify that the header should be printed at most once.

=item --fields=I<field1,field2,field3>

Specify a list of fields that should be printed.

=item --skip-index

Skip printing the index extent.

=item --skip-types

Skip printing the types of all extents at the beginning of the output

=item --skip-extent-type

Skip printing the extent type header at the beginning of each extent.

=item --skip-extent-fieldnames

Skip printing the extent fieldnames in the header at the beginning of each extent.

=item --skip-all

Equivalent to specifying all of the --skip-* options.

=item --type=I<type-match>

Specify the type to print out, if it matches exactly that type is selected.  It it matches a unique
prefix, that type is printed, ortherwise if it matches a unique substring that type is selected.
Otherwise an error is generated.

=item --select I<extent-type-match|*> I<field,field,field>

Select a specific extent type and list of fields to print.  TODO: figure out if this is equivalent
to some of the other options.

=item --where I<expression>

Specify an expression to evaluate for each line.  If the expression returns true then print
out the matching row/record.

=back

=cut
*/

#include <Lintel/StringUtil.hpp>

#include <DataSeries/DStoTextModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/DSExpr.hpp>

using namespace std;
using boost::format;

static string str_DataSeries("DataSeries:");

static void eat_args(int n, int &argc, char *argv[])
{
    for(int i = n + 1; i < argc; i++) {
	argv[i - n] = argv[i];
    }
    argc -= n;
}

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
	    string s = (char *)(argv[1] + 12);
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
	} else if (strncmp(argv[1],"--header-only-once",18)==0) {
	    toText.setHeaderOnlyOnce();
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
	    eat_args(2, argc, argv);
	} else if (strcmp(argv[1],"--where")==0) {
	    INVARIANT(argc > 4, "--where needs two arguments");
	    INVARIANT(where_extent_type.empty(),
		      "multiple where statements specified");
	    where_extent_type = argv[2];
	    INVARIANT(where_extent_type != "",
		      "--where type needs to be non-empty");
	    where_expr_str = argv[3];
	    eat_args(2, argc, argv);
	} else if (strncmp(argv[1],"-",1)==0) {
	    FATAL_ERROR(format("Unknown argument %s\n") % argv[1]);
	} else {
	    break;
	}
	eat_args(1, argc, argv);
    }
	    
    INVARIANT(argc >= 2 && strcmp(argv[1],"-h") != 0 && strcmp(argv[1], "--help") != 0,
	      format("Usage: %s [--csv] [--separator=...]\n"
		     "  [--header=...] [--header-only-once]\n"
		     "  [--select '*'|'extent-type-match' '*'|'field,field,field']\n"
		     "  [--printSpec='type=\"...\" name=\"...\" print_format=\"...\" [units=\"...\" epoch=\"...\"]']\n"
// put in the <printSpec ... /> version into man page
// fields is mostly obsolete, move to man page eventually
//			    "  [--fields=<fields type=\"...\"><field name=\"...\"/></fields>]\n"
		     "  [--skip-index] [--skip-types] [--skip-extent-type]\n"
		     "  [--skip-extent-fieldnames] [--skip-all]\n"
		     "  [--where '*'|extent-type-match bool-expr]\n"
		     "  <file...>\n"
		     "\n%s\n")
	      % argv[0] % DSExpr::usage());
    for(int i=1;i<argc;++i) {
	source.addSource(argv[i]);
    }

    DataSeriesSource *first_source = new DataSeriesSource(argv[1]);

    if (select_extent_type != "") {
	string match_extent_type;
	const ExtentType *match_type 
	    = first_source->getLibrary().getTypeMatch(select_extent_type, 
					  	      false, true);

	match_extent_type = match_type->getName();
	vector<string> fields;
	split(select_fields,",",fields);
	string xmlspec("<fields type=\"");
	xmlspec.append(match_extent_type);
	xmlspec.append("\">");
	if (select_fields == "*") {
	    const ExtentType *t = 
		first_source->getLibrary().getTypeMatch(match_extent_type);
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
	    = first_source->getLibrary().getTypeMatch(where_extent_type, 
						      false, true);
	toText.setWhereExpr(match_type->getName(), where_expr_str);
    }

    // Note that this doesn't completely do the right thing with
    // multiple files as we won't print out the extent types that only
    // occur in later files, of course, without a type of *, it's not
    // going to see those anyway.

    if (skip_types == false) {
	cout << "# Extent Types ...\n";
	for(map<const string, const ExtentType *>::iterator i 
		= first_source->getLibrary().name_to_type.begin();
	    i != first_source->getLibrary().name_to_type.end(); ++i) {
	    cout << i->second->getXmlDescriptionString() << "\n";
	}
    }

    // Same issue as above w.r.t. multiple files.

    if (toText.printIndex() && first_source->indexExtent != NULL) {
	ExtentSeries es(first_source->indexExtent);
	Int64Field offset(es,"offset");
	Variable32Field extenttype(es,"extenttype");
	cout << "extent offset  ExtentType\n";
	for(;es.morerecords();++es) {
	    cout << format("%-13d  %s\n") % offset.val() % extenttype.stringval();
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

