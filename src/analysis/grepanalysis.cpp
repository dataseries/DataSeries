/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

// TODO-tomer: rename file to dsfgrep.cpp

#include <string>
#include <algorithm>
#include <vector>
#include <iostream>

#include <boost/foreach.hpp>
#include <boost/format.hpp>

#include <Lintel/BoyerMooreHorspool.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/ProgramOptions.hpp>

#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/ParallelGrepModule.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/SimpleSourceModule.hpp>

using namespace std;
using lintel::BoyerMooreHorspool;

class StringFieldMatcher {
public:
    StringFieldMatcher(const string &needle)
        : matcher(new BoyerMooreHorspool(needle.c_str(), needle.size())) {
    }

    bool operator()(const Variable32Field &field) {
        return matcher->matches((const char*)field.val(), field.size());
    }

private:
    boost::shared_ptr<BoyerMooreHorspool> matcher;
};

// TODO-tomer: program options unix convention is - separated, e.g. count-only
lintel::ProgramOption<bool> countOnly("countOnly", "print match count but do not create output file");
// TODO-tomer: add a -DBUILD_PAPER to options.
// replace-string '\bnoCopy\b' 'no_copy'
#if 1 || BUILD_PAPER
lintel::ProgramOption<bool> noCopy("noCopy", "use a specialized input module for non-compressed"
                                             " data that does not memcpy, and does not work on general .ds files");
#endif
lintel::ProgramOption<string> extentTypeName("extentTypeName", "the extent type match, as defined by TypeIndexModule", string("Text"));
lintel::ProgramOption<string> fieldName("fieldName", "the name of the field in which"
                                                     " to search for the needle", string("line"));
// TODO-tomer: call this pattern to match with grep
lintel::ProgramOption<string> needle("needle", "the substring to look for in each record");
lintel::ProgramOption<string> inputFile("inputFile", "the path of the input file");
lintel::ProgramOption<string> outputFile("outputFile", "the path of the output file", string());

lintel::ProgramOption<bool> help("help", "get help");

int main(int argc, char *argv[]) {
    // TODO-tomer: use commonargs
    LintelLog::parseEnv();
    vector<string> args = lintel::parseCommandLine(argc, argv, false);

    LintelLogDebug("grepanalysis", "Starting grep analysis");

    INVARIANT(countOnly.get() == (outputFile.get().empty()),
              "Precisely one of outputFile and countOnly must be specified.");
    // TODO-tomer: make these positional arguments as with grep
    INVARIANT(!inputFile.get().empty(), "inputFile must be specified.");
    INVARIANT(!needle.get().empty(), "needle must be specified.");

    auto_ptr<DataSeriesModule> inputModule;
    if (noCopy.get()) {
        inputModule.reset(new SimpleSourceModule(inputFile.get()));
    } else {
        TypeIndexModule *typeInputModule = new TypeIndexModule(extentTypeName.get());
        typeInputModule->addSource(inputFile.get());
        inputModule.reset(typeInputModule);
    }


    StringFieldMatcher fieldMatcher(needle.get());
    ParallelGrepModule<Variable32Field, StringFieldMatcher>
        grepModule(*inputModule, fieldName.get(), fieldMatcher);

    size_t matches = 0;
    if (countOnly.get()) {
        Extent *extent = grepModule.getExtent();
        while (extent != NULL) {
            matches += extent->getRecordCount();
            delete extent;
            extent = grepModule.getExtent();
        }
    } else {
        Extent *extent = grepModule.getExtent();
	// TODO-tomer: generate the empty output file with the type
	// from the type index module.  lintel::PointerUtil for the downcast.
	// safeDownCast<TypeIndexModule>(inputModule.get())->...

        if (extent != NULL) {
            cout << extent->getType().getName() << endl;
            cout.flush();
	    // TODO-tomer: set output options based on commonargs
            DataSeriesSink sink(outputFile.get(), Extent::compress_none, 0);
            ExtentTypeLibrary library;
            library.registerType(extent->getType());
            sink.writeExtentLibrary(library);

            while (extent != NULL) {
                sink.writeExtent(*extent, NULL);
                matches += extent->getRecordCount();

                delete extent;
                extent = grepModule.getExtent();
            }
            sink.close();
        }
    }
    cerr << boost::format("Found %s occurrence(s) of the string '%s'") % matches % needle.get() << endl;
}
