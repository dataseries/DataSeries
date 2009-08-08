/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <math.h>

#include <string>
#include <algorithm>
#include <memory>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/format.hpp>

#include <Lintel/LintelLog.hpp>
#include <Lintel/ProgramOptions.hpp>

#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentWriter.hpp>
#include <DataSeries/ParallelMemorySortModule.hpp>
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

class Variable32FieldPartitioner {
public:
    void initialize(uint32_t partitionCount) {}

    uint32_t getPartition(const Variable32Field &field) {
        return 0;
    }
};

class FixedWidthFieldComparator {
public:
    bool operator()(const FixedWidthField &fieldLhs, const FixedWidthField &fieldRhs) {
        DEBUG_SINVARIANT(fieldLhs.size() == fieldRhs.size());
        return memcmp(fieldLhs.val(), fieldRhs.val(), fieldLhs.size()) <= 0;
    }
};

class FixedWidthFieldPartitioner {
public:
    void initialize(uint32_t partitionCount) {
        DEBUG_INVARIANT(partitionCount <= 256, "This partitioner partitions based on the first byte.");
        float increment = 256.0 / partitionCount;
        LintelLogDebug("parallelsortanalysis",
                       boost::format("Determining %s partitioning limits with an increment of %s.") %
                       partitionCount % increment);
        float currentLimit = increment;
        for (uint32_t i = 0; i < partitionCount - 1; ++i) {
            limits.push_back(static_cast<uint8_t>(round(currentLimit)));
            LintelLogDebug("parallelsortanalysis", boost::format("Partition limit: %s") % (uint32_t)limits.back());
            currentLimit += increment;
        }
        SINVARIANT(limits.size() == partitionCount - 1);
    }

    uint32_t getPartition(const FixedWidthField &field) {
        // Return a value between 0 and partitionCount - 1. A value in partition i
        // must be less than a value in partition j if i < j.
        uint8_t firstByte = static_cast<uint8_t*>(field.val())[0];
        uint32_t partition = 0;
        BOOST_FOREACH(uint8_t limit, limits) {
            if (firstByte <= limit) {
                return partition;
            }
            ++partition;
        }
        return limits.size();
    }
private:
    vector<uint8_t> limits;
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


typedef ParallelMemorySortModule<Variable32Field,
                                 Variable32FieldComparator,
                                 Variable32FieldPartitioner> Variable32SortModule;

typedef ParallelMemorySortModule<FixedWidthField,
                                 FixedWidthFieldComparator,
                                 FixedWidthFieldPartitioner> FixedWidthSortModule;


int main(int argc, char *argv[]) {
    LintelLog::parseEnv();

    vector<string> args(lintel::parseCommandLine(argc, argv, false));

    INVARIANT(!inputFileOption.get().empty(), "An input file is required.");
    INVARIANT(!outputFileOption.get().empty() || memOnlyOption.get(), "An output file is required if we are not doing a memory-only execution.");

    string extentTypeName(extentTypeNameOption.get());
    if (extentTypeName.empty()) {
        extentTypeName = fixedWidthOption.get() ? "Gensort" : "Text";
    }
    TypeIndexModule inputModule(extentTypeName);
    inputModule.addSource(inputFileOption.get());

    auto_ptr<DataSeriesModule> sortModule;
    if (fixedWidthOption.get()) {
        LintelLogDebug("parallelsortanalysis", "Creating sort module for gensort data.");
        string fieldName(fieldNameOption.get().empty() ? "key" : fieldNameOption.get());
        sortModule.reset(new FixedWidthSortModule(inputModule,
                                                  fieldName,
                                                  FixedWidthFieldComparator(),
                                                  FixedWidthFieldPartitioner()));
    } else {
        LintelLogDebug("parallelsortanalysis", "Creating sort module for txt2ds data.");
        string fieldName(fieldNameOption.get().empty() ? "line" : fieldNameOption.get());
        sortModule.reset(new Variable32SortModule(inputModule,
                                                  fieldName,
                                                  Variable32FieldComparator(),
                                                  Variable32FieldPartitioner()));
    }

    if (!memOnlyOption.get()) {
        DataSeriesSink sink(outputFileOption.get(),
                            compressOutputOption.get() ?
                                8 /* Extent::compress_lzf */ :
                                0 /* Extent::compress_none */);
        bool wroteLibrary = false;
        Extent *extent = NULL;
        while ((extent = sortModule->getExtent()) != NULL) {
            LintelLogDebug("parallelsortanalysis", "Read an extent.");
            if (!wroteLibrary) {
                LintelLogDebug("parallelsortanalysis", "Writing extent type library to output file.");
                ExtentTypeLibrary library;
                library.registerType(extent->getType());
                sink.writeExtentLibrary(library);
                wroteLibrary = true;
            }
            sink.writeExtent(*extent, NULL);
            delete extent;
        }
        LintelLogDebug("parallelsortanalysis", "Closing the sink.");
        sink.close();
    } else {
        Extent *extent = NULL;
        while ((extent = sortModule->getExtent()) != NULL) {
            delete extent;
        }
    }
}
