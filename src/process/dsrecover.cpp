// -*-C++-*-
/*
   (c) Copyright 2010, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Recover any data that was successfully written to the specified DataSeries file.
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
    if (argc < 2) {
        LintelLog::error("Must specify both an input to recover and an output to recover to.");
        std::cout << "dsrecover <input file> <output file>" << std::endl;
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
    ExtentTypeLibrary newLibrary;
    for (std::map<const std::string, const ExtentType *>::const_iterator
	     i = library.name_to_type.begin(); i != library.name_to_type.end(); i++) {
	if (!prefixequal(i->second->getName(), "DataSeries: ")) {
	    newLibrary.registerType(*i->second);
	}
    }
	
    DataSeriesSink sink(unparsed[1], packing_args.compress_modes, packing_args.compress_level);
    sink.writeExtentLibrary(newLibrary);

    int32_t fails = 0;

    Extent *e;
    while (true) {
	try {
	    e = source.readExtent();
	    if (e==NULL) {
		break;
	    }
	    if (!prefixequal(e->getType().getName(), "DataSeries: ")) {
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
