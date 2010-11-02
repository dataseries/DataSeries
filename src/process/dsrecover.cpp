// -*-C++-*-
/*
   (c) Copyright 2010, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Recover any data that was successfully written to the specified DataSeries file.
*/

#include <Lintel/LintelLog.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/commonargs.hpp>

int main(int argc, char *argv[]) {
    commonPackingArgs packing_args;
    getPackingArgs(&argc, argv, &packing_args);

    // confirm that we have valid arguments remaining
    if (argc < 3) {
        LintelLog::error("Must specify both an input to recover and an output to recover to.");
        std::cout << "dsrecover <input file> <output file>" << std::endl;
        return -1;
    }

    // make sure that we perform the CRC checks
    Extent::setReadChecksFromEnv(true);

    DataSeriesSource source(argv[1], false, false);
    const ExtentTypeLibrary &library = source.getLibrary();

    ExtentTypeLibrary newLibrary;
    for (std::map<const std::string, const ExtentType *>::const_iterator
             i = library.name_to_type.begin(); i != library.name_to_type.end(); i++) {
        if (!prefixequal(i->second->getName(), "DataSeries: ")) {
            newLibrary.registerType(*i->second);
        }
    }

    DataSeriesSink sink(argv[2], packing_args.compress_modes, packing_args.compress_level);
    sink.writeExtentLibrary(newLibrary);

    // have DataSeries throw exceptions from here forward
    AssertBoostFnBefore(AssertBoostThrowExceptionFn);

    try {
        while (Extent *e = source.readExtent()) {
            if (!prefixequal(e->getType().getName(), "DataSeries: ")) {
                sink.writeExtent(*e, NULL);
            }
        }
    } catch (AssertBoostException &) {
        // we got as far as we could...
    }

    sink.close();
    return 0;
}
