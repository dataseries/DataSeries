// -*-C++-*-
/*
   (c) Copyright 2010, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/*
=pod

=head1 NAME

dsrecover - Recover any data that was successfully written to the specified DataSeries file.

=head1 SYNOPSIS

 % dsrecover [--fail-limit=10] [common-args] input output

=head1 DESCRIPTION

DataSeries files include a trailer in order to make reading the files more efficient.  However, it
the writer of a file crashed, they may not have written the trailer, and they may have partially
written an extent.  dsrecover attempts to read as much of a dataseries file as possible.  If an
individual extent is corrupt it will skip that extent and try to tread the next one.  It will keep
trying until it either reaches the end of the file or it has up to fail-limit consecutive failures.

=head1 SEE ALSO

dataseries-utils(7)

=cut
*/

#include <boost/format.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/ProgramOptions.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/commonargs.hpp>

using namespace std;
using boost::format;

lintel::ProgramOption<int32_t> po_fail_limit
("fail-limit", "limit of # of consecutive failures before bailing on the rest of input", 10);

int main(int argc, char *argv[]) {
    LintelLog::parseEnv();
    commonPackingArgs packing_args;
    getPackingArgs(&argc, argv, &packing_args);
    
    vector<string> unparsed = lintel::parseCommandLine(argc, argv, true);
    
    // confirm that we have valid arguments remaining
    if (unparsed.size() != 2) {
        cerr << format("Usage: %s dsrecover <input file> <output file>\n") % argv[0];
        return -1;
    }

    // make sure that we perform the CRC checks
    Extent::setReadChecksFromEnv(true);

    // have DataSeries throw exceptions from here forward
    AssertBoostFnBefore(AssertBoostThrowExceptionFn);

    DataSeriesSource * source_ptr = 0;
    try {
	source_ptr = new DataSeriesSource(unparsed[0], false, false);
    } catch (AssertBoostException &) {
	LintelLog::error("File too corrupt to even open; bailing");
	return -1;
    }	
    DataSeriesSource &source = *source_ptr;

    const ExtentTypeLibrary &library = source.getLibrary();    
    ExtentTypeLibrary new_library;
    for (ExtentTypeLibrary::NameToType::const_iterator
	     i = library.name_to_type.begin(); i != library.name_to_type.end(); i++) {
	if (!prefixequal(i->second->getName(), "DataSeries: ")) {
	    new_library.registerType(i->second);
	}
    }
	
    DataSeriesSink sink(unparsed[1], packing_args.compress_modes, packing_args.compress_level);
    sink.writeExtentLibrary(new_library);

    int32_t fails = 0;

    Extent *e;
    while (true) {
	try {
	    e = source.readExtent();
	    if (e==NULL) {
		break;
	    }
	    if (!prefixequal(e->getTypePtr()->getName(), "DataSeries: ")) {
                sink.writeExtent(*e, NULL);
            }
	    fails = 0;
	} catch (AssertBoostException &) {
	    LintelLog::error("Caught exception recovering file");
	    ++fails;
	    if (fails > po_fail_limit.get()) {
		LintelLog::error("Too many consecutive failures; finishing up");
		break;
	    }
	}	
    }
    sink.close();
    return 0;
}
