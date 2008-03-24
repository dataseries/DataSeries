// -*-C++-*-
/*
   (c) Copyright 2008 Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file 
    Convert the World Cup custom format into DataSeries
    http://ita.ee.lbl.gov/html/contrib/WorldCup.html
*/

/*
=pod
=head1 NAME

wcweb2ds - convert the 1998 world cup traces to dataseries

=head1 SYNOPSIS

  % wcweb2ds [ds-common-args] <input-path or -> <output-ds-path>

=head1 DESCRIPTION

wcweb2ds converts the 1998 world cup traces to dataseries.  The world
cup traces are a very large, publically available set of web traces.
They have been published in a special binary record format that was
designed to reduce space usage.  This converter converts to a
dataseries version of that record format.

=head1 EXAMPLES

% gunzip -c < wc_day46_3.gz | wcweb2ds --compress-bz2 - wc_day46_3.ds
% wcweb2ds --compress-gz --extent-size=1000000 wc_day80_1 wc_day80_1.ds

=head1 SEE ALSO

ds2wcweb(1), DataSeries(5), DSCommonArgs(1)

=head1 AUTHOR/CONTACT

Eric Anderson <software@cello.hpl.hp.com>
=cut
*/

#include <arpa/inet.h>

#include <DataSeries/DataSeriesModule.H>
#include <DataSeries/commonargs.H>

using namespace std;
using boost::format;

// This directory contains several tools that can be used to read the
// binary logs that represent the access logs from the 1998 World Cup Web
// site (www.france98.com).  

const string web_wc_custom_xml(
  "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Log::Web::WorldCup::Custom\" version=\"1.0\" comment=\"field descriptions taken from the tools README; http://ita.ee.lbl.gov/html/contrib/WorldCup.html\" >\n"
  "  <field type=\"int32\" name=\"timestamp\" epoch=\"unix\" units=\"seconds\" pack_relative=\"timestamp\" comment=\"the time of the request, stored as the number of seconds since the Epoch.  The timestamp has been converted to GMT to allow for portability.  During the World Cup the local time was 2 hours ahead of GMT (+0200).  In order to determine the local time, each timestamp must be adjusted by this amount.\" />\n"
  "  <field type=\"int32\" name=\"clientID\" comment=\"a unique integer identifier for the client that issued the request (this may be a proxy); due to privacy concerns these mappings cannot be released; note that each clientID maps to exactly one IP address, and the mappings are preserved across the entire data set - that is if IP address 0.0.0.0 mapped to clientID X on day Y then any request in any of the data sets containing clientID X also came from IP address 0.0.0.0\" />\n"
  "  <field type=\"int32\" name=\"objectID\" comment=\"a unique integer identifier for the requested URL; these mappings are also 1-to-1 and are preserved across the entire data set.\" />\n"
  "  <field type=\"int32\" name=\"size\" comment=\"the number of bytes in the response\" />\n"
  "  <field type=\"byte\" name=\"method\" comment=\"the method contained in the client's request (e.g., GET). enum {get=0, head, post, put, delete, trace, options, connect, other_methods}\" />\n"
  "  <field type=\"byte\" name=\"http_version_and_status\" comment=\" this field contains two pieces of information; the 2 highest order bits contain the HTTP version indicated in the client's request (e.g., HTTP/1.0); the remaining 6 bits indicate the response status code (e.g., 200 OK); high bits enum {http_09 = 0, http_10, http_11, http_xx } low bits enum {sc_100 = 0, sc_101, sc_200..206, sc_300=9 .. sc_305, sc400=15 .. sc_415, sc_500=31 .. sc505 = 36, other = 37}\" />\n"
  "  <field type=\"byte\" name=\"type\" comment=\"the type of file requested (e.g., HTML, IMAGE, etc), generally based on the file extension (.html), or the presence of a parameter list (e.g., '?' indicates a DYNAMIC request).  If the url ends with '/', it is considered a DIRECTORY.  Mappings from the integer ID to the generic file type are contained in definitions.h.  If more specific mappings are required this information can be obtained from analyzing the object mappings file (state/object_mappings.sort); enum {html=0, image, audio, video, java, formatted, dynamic, text, compressed, programs, directory, icl, other_types}\" />\n"
  "  <field type=\"byte\" name=\"server\" comment=\"indicates which server handled the request.  The upper 3 bits indicate which region the server was at (e.g., SANTA CLARA=0, PLANO=1, HERNDON=2, PARIS=3); the remaining bits indicate which server at the site handled the request.  All 8 bits can also be used to determine a unique server.\" />\n"
  "</ExtentType>\n"
  );

ExtentSeries series;
OutputModule *outmodule;
Int32Field timestamp(series, "timestamp");
Int32Field clientID(series, "clientID");
Int32Field objectID(series, "objectID");
Int32Field size(series, "size");
ByteField method(series, "method");
ByteField status(series, "http_version_and_status");
ByteField type(series, "type");
ByteField server(series, "server");

struct record {
    uint32_t timestamp, clientID, objectID, size;
    uint8_t method, status, type, server;
};

unsigned nrecords;

int
main(int argc,char *argv[])
{
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    INVARIANT(argc == 3,
	      format("Usage: %s [ds-common-args] inname outdsname; - valid for inname")
	      % argv[0]);
    FILE *infile;
    if (strcmp(argv[1],"-")==0) {
	infile = stdin;
    } else {
	infile = fopen(argv[1],"r");
	INVARIANT(infile != NULL, format("Unable to open file %s: %s")
		  % argv[1] % strerror(errno));
    }

    DataSeriesSink outds(argv[2],
			 packing_args.compress_modes,
			 packing_args.compress_level);
    ExtentTypeLibrary library;
    const ExtentType *extent_type = library.registerType(web_wc_custom_xml);
    series.setType(*extent_type);
    outmodule = new OutputModule(outds, series, extent_type, 
				 packing_args.extent_size);
    outds.writeExtentLibrary(library);

    record buf;
    while(true) {
	size_t amt = fread(&buf, 1, sizeof(record), infile);
	if (amt == 0) {
	    SINVARIANT(feof(infile) && ! ferror(infile));
	    break;
	}
	SINVARIANT(amt == sizeof(record));
	++nrecords;
	outmodule->newRecord();
	timestamp.set(ntohl(buf.timestamp));
	clientID.set(ntohl(buf.clientID));
	objectID.set(ntohl(buf.objectID));
	size.set(ntohl(buf.size));
	method.set(buf.method);
	status.set(buf.status);
	type.set(buf.type);
	server.set(buf.server);
    }
    cout << format("Processed %d records\n") % nrecords;

    delete outmodule;
    outds.close();

    return 0;
}

