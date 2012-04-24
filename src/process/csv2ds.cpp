// -*-C++-*-
/*
   (c) Copyright 2004-2006, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Convert csv files to DataSeries files
*/

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <Lintel/AssertBoost.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/ProgramOptions.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/commonargs.hpp>
#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/GeneralField.hpp>

/*
=pod

=head1 NAME

csv2ds - convert a comma separated value file into dataseries.

=head1 SYNOPSIS

 % csv2ds [options] --xml-desc-file=<path> <input.csv|-> <output.ds>

=head1 DESCRIPTION

Convert a comma separated value (CSV) file into dataseries.  Each line in a CSV
file is skipped if it starts with I<comment-prefix>.  Otherwise, it is divided
into separate fields using the following rules.  If it starts with
I<string-quote-character> then following characters are read, treating two
I<string-quote-characters> in a row as a single I<string-quote-character>.  A
field stops when we reach a I<field-separator-string> or the end of the
line, which can either be a newline or a a carriage return and a newline.

=head1 EXAMPLES

With the file test.xml containing:

	<ExtentType name="test-csv2ds" namespace="simpl.hpl.hp.com" version="1.0">
	  <field type="bool" name="bool" />
	  <field type="byte" name="byte" />
	  <field type="int32" name="int32" />
	  <field type="int64" name="int64" />
	  <field type="double" name="double" />
	  <field type="variable32" name="variable32" pack_unique="yes" />
	</ExtentType>

And the file test.csv containing:

	# bool,byte,int32,int64,double,var32
	true,33,1000,56,1,"Hello, World"
	false,88,-15331,1000000000000,3.14159,"I said ""hello there."""
	true,10,11,5,2.718281828,unquoted

You can run the command:

	% csv2ds --compress-lzf --xml-desc-file=test.xml test.csv test.ds

To produce a DataSeries file that contains the contents of the csv file.

=head1 OPTIONS

=over 4

=item --xml-desc-file=I<path>

Specifies the path of the file containing the xml description.  At the current time,
this option is required since csv2ds does not auto-infer field types.

=item --comment-prefix=I<string>

Specifies the string that may occur at the beginning of a line to introduce a comment.
Comment lines are skipped.

=item --field-separator=I<string>

Specifies the string that separates fields.  If the string starts with 0x; then it will be
decoded as a hexadecimal string, so you can, for example, use 0x00 to separate fields with
nulls.

=item --hex-encoded-variable32

Specifies that any variable32 fields (as indicated by the xml description) are hex encoded, and
so should be decoded before being added to the dataseries file.

=back

=head1 TODO

=over 4

=item *

Make csv2ds able to auto-infer field types

=back

=cut

*/

using namespace std;
using boost::format;

namespace {
    lintel::ProgramOption<string> po_xml_desc_file("xml-desc-file", "Specify the filename of the XML type specification that should be used for the output dataseries file");
    lintel::ProgramOption<string> po_comment_prefix("comment-prefix", "Specify a string that starts lines and causes them to be ignored; empty string for no comment prefix", "#");
    lintel::ProgramOption<string> po_field_separator("field-separator", "Specify the string that separates fields in the csv file", ",");
    lintel::ProgramOption<bool> po_hex_encoded_variable32("hex-encoded-variable32", "Specify that variable32 fields are hex encoded.");
    lintel::ProgramOption<string> po_null_string("null-string", "Specify the string that will be interpreted as a null field", "null");
}

const ExtentType::Ptr getXMLDescFromFile(const string &filename, ExtentTypeLibrary &lib) {
    ifstream xml_desc_input(filename.c_str());
    INVARIANT(xml_desc_input.good(), 
	      format("error opening %s: %s") % filename % strerror(errno));

    string xml_desc;
    while(!xml_desc_input.eof()) {
	char buf[8192];
	buf[0] = '\0';

	xml_desc_input.read(buf, 8192);

	INVARIANT(xml_desc_input.good() || xml_desc_input.eof(), 
		  format("error reading %s: %s") % filename % strerror(errno));

	streamsize read_bytes = xml_desc_input.gcount();

	INVARIANT(read_bytes > 0 || xml_desc_input.eof(), format("error reading %s: %s")
		  % filename % strerror(errno));
	
	xml_desc.append(string(buf, read_bytes));
    }

    LintelLogDebug("csv2ds::xml", format("XML description:\n%s") % xml_desc);

    return lib.registerTypePtr(xml_desc);
}

void makeFields(const ExtentType &type, ExtentSeries &series,
                vector<GeneralField *> &ret, vector<bool> &is_nullable) {
    ret.reserve(type.getNFields());
    is_nullable.reserve(type.getNFields());
    for(uint32_t i = 0; i < type.getNFields(); ++i) {
	ret.push_back(GeneralField::create(series, type.getFieldName(i)));
        is_nullable.push_back(type.getNullable(type.getFieldName(i)));
    }
}

const ExtentType::Ptr getType(ExtentTypeLibrary &lib) {
    if (po_xml_desc_file.used()) {
	return getXMLDescFromFile(po_xml_desc_file.get(), lib);
    } else {
	FATAL_ERROR("--xml-desc-file=path required right now");
    }
}

int main(int argc, char *argv[]) {
    LintelLog::parseEnv();
    lintel::programOptionsHelp("<csv-input-name> <ds-output-name>");
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);
    
    vector<string> remain = lintel::parseCommandLine(argc, argv, true);
    if (remain.size() != 2) {
	lintel::programOptionsUsage(argv[0]);
	exit(1);
    }
    
    string csv_input_filename(remain[0]);
    string ds_output_filename(remain[1]);

    {
	struct stat buf;
	INVARIANT(suffixequal(ds_output_filename, ".ds") 
		  || stat(ds_output_filename.c_str(),&buf) != 0,
		  boost::format("Refusing to overwrite existing file %s with non .ds extension.")
		  % ds_output_filename);
    }

    ExtentTypeLibrary lib;

    const ExtentType::Ptr type(getType(lib));

    DataSeriesSink outds(ds_output_filename, packing_args.compress_modes,
			 packing_args.compress_level);

    outds.writeExtentLibrary(lib);
    ExtentSeries series(type);
    OutputModule *outmodule = new OutputModule(outds, series, type, packing_args.extent_size);

    vector<bool> is_nullable;
    vector<GeneralField *> fields;
    makeFields(*type, series, fields, is_nullable);
    istream *csv_input;

    if (csv_input_filename == "-") {
        csv_input = &cin;
    } else {
        csv_input = new ifstream(csv_input_filename.c_str());
    }
    INVARIANT(csv_input->good(), 
	      format("error opening %s: %s") % csv_input_filename % strerror(errno));
    string comment_prefix(po_comment_prefix.get());
    char string_quote_character('"');
    string field_separator(po_field_separator.get());
    if (prefixequal(field_separator, "0x")) {
        INVARIANT(field_separator.size() >= 4, 
                  "--field-separator=0x.. needs to be at least 4 characters long");
        field_separator = hex2raw(field_separator.c_str() + 2, field_separator.size() - 2);
    }

    uint64_t line_num = 0;
    while(!csv_input->eof()) {
	++line_num;
	string line;
	
	getline(*csv_input, line);
	if (line.empty() || (line.size() == 1 && line[0] == '\r')) {
	    continue;
	}
	line.push_back('\n'); // pretend it was always there, could have been EOF :(

	INVARIANT(csv_input->good() || csv_input->eof(),
		  format("error reading %s: %s") % csv_input_filename % strerror(errno));
	
	LintelLogDebug("csv2ds::parse", format("line %d:") % line_num);
	if (!comment_prefix.empty() && prefixequal(line, comment_prefix)) {
            LintelLogDebug("csv2ds::parse", "  ... comment ...");
	    continue;
	}
	vector<string> str_fields;

	size_t pos = 0;

	while (true) {
	    INVARIANT(pos < line.size(), format("csv line %d does not end in newline") % line_num);

	    string field;
	    if (line[pos] == string_quote_character) {
		for(++pos; true; ++pos) {
		    INVARIANT(pos < line.size(), format("csv line %d ends in middle of string")
			      % line_num);
		    if (line[pos] == string_quote_character) {
			INVARIANT(pos + 1 < line.size(), 
				  format("csv line %d ends in middle of string") % line_num);
			if (line[pos + 1] == string_quote_character) {
			    ++pos;
			} else {
			    ++pos; // skip terminating string quote char
			    break;
			}
		    }
		    field.push_back(line[pos]);
		}
	    } else {
		for(; true; ++pos) {
		    INVARIANT(pos < line.size(), format("csv line %d ends in middle of field")
			      % line_num);
		    if (line[pos] == '\r' || line[pos] == '\n' 
			|| line.compare(pos, field_separator.size(), field_separator) == 0) {
			break;
		    }
		    field.push_back(line[pos]);
		}
	    }

	    LintelLogDebug("csv2ds::parse", format("  field %d: %s") % str_fields.size() % field);
	    str_fields.push_back(field);
	    
	    INVARIANT(pos < line.size(), format("csv line %d does not terminate properly")
		      % line_num);
	    if (line[pos] == '\r') {
		INVARIANT(pos + 1 < line.size() && line[pos + 1] == '\n',
			  format("csv line %d has a carriage return not followed by a newline")
			  % line_num);
		SINVARIANT(pos + 2 == line.size());
		break;
	    }
	    if (line[pos] == '\n') {
		INVARIANT(pos + 1 == line.size(), format("csv line %d has stuff after newline")
			  % line_num);
		break;
	    }

	    INVARIANT(pos + field_separator.size() <= line.size() 
                      && line.compare(pos, field_separator.size(), field_separator) == 0,
		      format("csv line %d at pos %d is '%c', not a field separator")
		      % line_num % pos % line[pos]);
	    pos += field_separator.size();
	}

	INVARIANT(fields.size() == str_fields.size(),
		  format("csv line %d has %d fields, not %d as in type definition")
		  % line_num % str_fields.size() % fields.size());
	outmodule->newRecord();
	for(size_t i = 0; i < str_fields.size(); ++i) {
            SINVARIANT(fields[i] != NULL);
            if (is_nullable[i] && str_fields[i] == po_null_string.get()) {
                fields[i]->setNull();
            } else {
                if (fields[i]->getType() == ExtentType::ft_variable32 
                    && po_hex_encoded_variable32.get()) {
                    str_fields[i] = hex2raw(str_fields[i]);
                }
                fields[i]->set(str_fields[i]);
            }
	}
    }
    
    delete outmodule;
    GeneralField::deleteFields(fields);
    return 0;
}
 
    
