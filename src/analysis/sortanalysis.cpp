/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <string>
#include <algorithm>
#include <memory>

#include <Lintel/LintelLog.hpp>
#include <Lintel/ProgramOptions.hpp>

#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentWriter.hpp>
#include <DataSeries/MemorySortModule.hpp>
#include <DataSeries/SortModule.hpp>
#include <DataSeries/ParallelRadixSortModule.hpp>
#include <DataSeries/MemoryRadixSortModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

using namespace std;

class Variable32FieldComparator {
public:
    // simply returning field0.stringval() < field1.stringval() would also work
    // but increases the running time by 4x!
    bool operator()(const Variable32Field &fieldLhs, const Variable32Field &fieldRhs) {
        int result = memcmp(fieldLhs.val(), fieldRhs.val(), std::min(fieldLhs.size(), fieldRhs.size()));
        return result == 0 ? (fieldLhs.size() < fieldRhs.size()) : (result < 0);
    }
};

class FixedWidthFieldComparator {
public:
    bool operator()(const FixedWidthField &fieldLhs, const FixedWidthField &fieldRhs) {
        DEBUG_SINVARIANT(fieldLhs.size() == fieldRhs.size());
        return memcmp(fieldLhs.val(), fieldRhs.val(), fieldLhs.size()) <= 0;
    }
};


lintel::ProgramOption<string>
    extentTypeNameOption("extentTypeName", "the extent type", string());

lintel::ProgramOption<string>
    fieldNameOption("fieldName", "the name of the field in which to search for the needle",
                    string());

lintel::ProgramOption<bool>
    fixedWidthOption("fixedWidth", "sort on a FixedWidthField field rather than Variable32Field");

lintel::ProgramOption<int>
    extentLimitOption("extentLimit", "maximum size of an extent for the sort module (mostly "
                      "affects external sort)", 1000000); // 1MB

lintel::ProgramOption<int>
    memoryLimitOption("memoryLimit", "maximum amount of memory to use for buffers (if input file "
                      "has more data, an external sort is used)", 2000 * 1000000); // 2GB

lintel::ProgramOption<bool>
    compressTempOption("compressTemp", "compress temporary files used for external (two-phase) "
                       "sort using LZF");

lintel::ProgramOption<string>
    tempFilePrefixOption("tempFilePrefix", string()); // random dir under /tmp by default

lintel::ProgramOption<bool>
    memOnlyOption("memOnly", "sort without writing the actual output file");

lintel::ProgramOption<bool>
    compressOutputOption("compressOutput", "compress the resulting data using LZF");

lintel::ProgramOption<string>
    inputFileOption("inputFile", "the path of the input file");

lintel::ProgramOption<string>
    outputFileOption("outputFile", "the path of the output file", string());

lintel::ProgramOption<bool> helpOption("help", "get help");


typedef SortModule<Variable32Field, Variable32FieldComparator> Variable32SortModule;
typedef SortModule<FixedWidthField, FixedWidthFieldComparator> FixedWidthSortModule;

int main(int argc, char *argv[]) {
    LintelLog::parseEnv();

    vector<string> args(lintel::parseCommandLine(argc, argv, false));

    INVARIANT(!inputFileOption.get().empty(), "an input file is required");
    INVARIANT(!outputFileOption.get().empty() || memOnlyOption.get(), "an output file is required if we are not doing a memory-only execution");

    string extentTypeName(extentTypeNameOption.get());
    if (extentTypeName.empty()) {
        extentTypeName = fixedWidthOption.get() ? "Gensort" : "Text";
    }
    TypeIndexModule inputModule(extentTypeName);
    inputModule.addSource(inputFileOption.get());


    auto_ptr<DataSeriesModule> sortModule;
    if (fixedWidthOption.get()) {
        LintelLogDebug("sortanalysis", "Creating sort module for gensort data");
        string fieldName(fieldNameOption.get().empty() ? "key" : fieldNameOption.get());
        /*sortModule.reset(new FixedWidthSortModule(inputModule,
                                                  fieldName,
                                                  FixedWidthFieldComparator(),
                                                  extentLimitOption.get(),
                                                  memoryLimitOption.get(),
                                                  compressTempOption.get(),
                                                  tempFilePrefixOption.get()));*/
        sortModule.reset(new ParallelRadixSortModule(inputModule, fieldName, 1 << 20));
    } else {
        LintelLogDebug("sortanalysis", "Creating sort module for txt2ds data");
        string fieldName(fieldNameOption.get().empty() ? "line" : fieldNameOption.get());
        sortModule.reset(new Variable32SortModule(inputModule,
                                                  fieldName,
                                                  Variable32FieldComparator(),
                                                  extentLimitOption.get(),
                                                  memoryLimitOption.get(),
                                                  compressTempOption.get(),
                                                  tempFilePrefixOption.get()));
    }

    if (!memOnlyOption.get()) {
        DataSeriesSink sink(outputFileOption.get(),
                            compressOutputOption.get() ?
                                8 /* Extent::compress_lzf */ :
                                0 /* Extent::compress_none */);
        bool wroteLibrary = false;
        Extent *extent = NULL;
        while ((extent = sortModule->getExtent()) != NULL) {
            LintelLogDebug("sortanalysis", "Read an extent");
            if (!wroteLibrary) {
                LintelLogDebug("sortanalysis", "Writing extent type library to output file");
                ExtentTypeLibrary library;
                library.registerType(extent->getType());
                sink.writeExtentLibrary(library);
                wroteLibrary = true;
            }
            sink.writeExtent(*extent, NULL);
            delete extent;
        }
        LintelLogDebug("sortanalysis", "Closing the sink");
        sink.close();
    } else {
        Extent *extent = NULL;
        while ((extent = sortModule->getExtent()) != NULL) {
            delete extent;
        }
    }
}
