/*
  (c) Copyright 2007, Hewlett-Packard Development Company, LP

  See the file named COPYING for license details
*/

#include <errno.h>
#include <stdio.h>

#include <iostream>

#include <Lintel/AssertBoost.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/commonargs.hpp>
#include <DataSeries/cryptutil.hpp>

using namespace std;
using namespace boost;

/*
  =pod

  =head1 NAME

  iphost2ds - Convert a collection host -> IP address mappings into DataSeries

  =head1 SYNOPSIS

  % iphost2ds [common-args] input-file.txt output-file.ds

  =head1 DESCRIPTION

  This program converts the output of running:

  (date +%s; for i in `sort ip-addr-list-file`; do
  echo "host $i"
  host $i
  done) > output-file

  into a dataseries file with short, full, domain, ip address and mapping time columns.  It is useful
  for being able to combine the nfs and the lsf traces without having to guess at the hostname <-> ip
  mapping.

  =head1 BUGS

  This program should be a trivial perl script for format conversion and then a call to csv2ds.

  =head1 SEE ALSO

  dataseries-utils(7)

  =cut
*/

// TODO: figure out what we want to put in the output file that we can
// parse that will give us some idea as to the valid dates for the
// hostname mappings.  One possibility is to add this extent type to
// nettrace2ds, at which point the question of relevant times becomes
// mostly moot as the assumption would be the correct mapping was
// picked up during conversion of the network trace files.

const string ip_hostname_xml
(
    "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Network::IP-to-Hostname\" version=\"1.1\"\n"
    " comment=\"May get multiple lines with the same ipv4_address if the reverse mapping goes to multiple hostnames, recognized, entries that were looked up but were not found get null for all three names\" >\n"
    "  <field type=\"variable32\" name=\"shortname\" pack_unique=\"yes\" opt_nullable=\"yes\" comment=\"first part of the name; may be an encrypted string, so use Lintel/StringUtil::maybeHex to print\" />\n"
    "  <field type=\"variable32\" name=\"fullname\" pack_unique=\"yes\" opt_nullable=\"yes\" comment=\"full dns name, excluding trailing .; may be an encrypted string, so use Lintel/StringUtil::maybeHex to print\" />\n"
    "  <field type=\"variable32\" name=\"domainname\" pack_unique=\"yes\" opt_nullable=\"yes\" comment=\"domain name of host, excluding trailing .; may be an encrypted string, so use Lintel/StringUtil::maybeHex to print\" />\n"
    "  <field type=\"int32\" name=\"ipv4_address\" pack_relative=\"ipv4_address\" print_format=\"%08x\" />\n"
    "  <field type=\"int32\" name=\"mapping_time\" pack_relative=\"mapping_time\" print_format=\"%u\" comment=\"time in seconds since 1970 at which the ip to hostname mapping was calculated\" />\n"
    "</ExtentType>\n"
 );

extern bool enable_encrypt_memoize;

int
main(int argc, char *argv[])
{
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    INVARIANT(argc == 3,
              format("Usage: %s\n%s    in-file out-file\n")
              % argv[0] % packingOptions());

    ifstream infile(argv[1]);

    INVARIANT(infile.good(), format("unable to open %s") % argv[1]);

    prepareEncryptEnvOrRandom();
    DataSeriesSink ip_hostname_out(argv[2], packing_args.compress_modes,
                                   packing_args.compress_level);

    ExtentTypeLibrary library;
    const ExtentType::Ptr ip_hostname_type(library.registerTypePtr(ip_hostname_xml));

    ExtentSeries ip_hostname_series(ip_hostname_type);

    ip_hostname_out.writeExtentLibrary(library);

    Variable32Field shortname_field(ip_hostname_series, "shortname", Field::flag_nullable);
    Variable32Field fullname_field(ip_hostname_series, "fullname", Field::flag_nullable);
    Variable32Field domainname_field(ip_hostname_series, "domainname", Field::flag_nullable);
    Int32Field ipv4_addr_field(ip_hostname_series, "ipv4_address");
    Int32Field mapping_time_field(ip_hostname_series, "mapping_time");

    OutputModule *outmodule = new OutputModule(ip_hostname_out, 
                                               ip_hostname_series,
                                               ip_hostname_type, 
                                               packing_args.extent_size);

    uint32_t linenum = 0;
    string mapping_time_line;
    getline(infile, mapping_time_line); ++linenum;
    uint32_t mapping_time = stringToInteger<uint32_t>(mapping_time_line);

    string hostline;
    getline(infile, hostline); ++linenum;
    // Turns out you can get a reply from an inaddr query that
    // something is an alias.  For now we don't handle that case, you
    // need to edit it out in the input file.
    while (!hostline.empty()) {
        INVARIANT(hostline.substr(0,5) == "host ",
                  format("Expected line %d to start with 'host ' not '%s'")
                  % linenum % hostline.substr(0,5));
        
        vector<string> ipaddr_bits;
        split(hostline.substr(5), ".", ipaddr_bits);
        INVARIANT(ipaddr_bits.size() == 4, "bad");

        uint32_t ipv4_addr = stringtoipv4(hostline.substr(5));
        vector<string> next_line_bits = ipaddr_bits;
        reverse(next_line_bits.begin(), next_line_bits.end());
        next_line_bits.push_back("in-addr.arpa");
        string rev_addr = join(".", next_line_bits);
        string not_found = (format("Host %s not found: 3(NXDOMAIN)") % rev_addr).str();
        string found = (format("%s domain name pointer ") % rev_addr).str();

        string answerline;
        getline(infile, answerline); ++linenum;
        while (true) { // Can get multiple replies.
            if (answerline.substr(0, found.size()) == found) {
                INVARIANT(answerline.size() > found.size(), "huh");
                string fullname = answerline.substr(found.size(), answerline.size() - found.size());
                INVARIANT(fullname[fullname.size()-1] == '.', "huh");
                fullname = fullname.substr(0, fullname.size() - 1);
                vector<string> hostname_bits;
                split(fullname, ".", hostname_bits);
                INVARIANT(hostname_bits.size() > 1, "huh, no domain?");
                string shortname = hostname_bits[0];
                hostname_bits.erase(hostname_bits.begin());
                string domainname = join(".", hostname_bits);

                outmodule->newRecord();
                shortname_field.set(encryptString(shortname));
                fullname_field.set(encryptString(fullname));
                domainname_field.set(encryptString(domainname));
                ipv4_addr_field.set(ipv4_addr);
                mapping_time_field.set(mapping_time);
            } else if (answerline.substr(0, not_found.size()) == not_found) {
                outmodule->newRecord();
                ipv4_addr_field.set(ipv4_addr);
                mapping_time_field.set(mapping_time);
                shortname_field.setNull();
                fullname_field.setNull();
                domainname_field.setNull();
            } else {
                FATAL_ERROR(format("at line %d, expected line starting with either '%s' or '%s', not '%s'")
                            % linenum % found % not_found % answerline);
            }
            getline(infile, answerline); ++linenum;
            if (answerline.empty() 
                || answerline.substr(0, 5) == "host ") {
                // next entry
                hostline = answerline;
                break;
            }
        }
    }
    delete outmodule;
    return 0;
}
    
                                   
