#include <vector>
#include <string>
#include <iostream>

#include <boost/foreach.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>

#include <Lintel/ProgramOptions.hpp>
#include <Lintel/Clock.hpp>
#include <Lintel/BoyerMooreHorspool.hpp>

#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>
#include <DataSeries/NetworkModules.hpp>
#include <DataSeries/ParallelGrepModule.hpp>
#include <DataSeries/SortModule.hpp>

using namespace std;
using namespace dataseries;
using lintel::BoyerMooreHorspool;

class Variable32FieldMatcher {
public:
    Variable32FieldMatcher(const string &needle)
        : matcher(new BoyerMooreHorspool(needle.c_str(), needle.size())) {
    }

    bool operator()(const Variable32Field &field) {
        return matcher->matches((const char*)field.val(), field.size());
    }

private:
    boost::shared_ptr<BoyerMooreHorspool> matcher;
};

class Variable32FieldComparator {
public:
    bool operator()(const Variable32Field &fieldLhs, const Variable32Field &fieldRhs) {
        int result = memcmp(fieldLhs.val(), fieldRhs.val(), std::min(fieldLhs.size(), fieldRhs.size()));
        return result == 0 ? (fieldLhs.size() < fieldRhs.size()) : (result < 0);
    }
};

class Variable32FieldPartitioner {
public:
    void initialize(uint32_t partitionCount) {
        DEBUG_INVARIANT(partitionCount <= 256, "This partitioner partitions based on the first byte.");
        float increment = 256.0 / partitionCount;
        LintelLogDebug("parallelnetworkgrep",
                       boost::format("Determining %s partitioning limits with an increment of %s.") %
                       partitionCount % increment);
        float currentLimit = increment;
        for (uint32_t i = 0; i < partitionCount - 1; ++i) {
            limits.push_back(static_cast<uint8_t>(round(currentLimit)));
            LintelLogDebug("parallelnetworkgrep", boost::format("Partition limit: %s") % (uint32_t)limits.back());
            currentLimit += increment;
        }
        SINVARIANT(limits.size() == partitionCount - 1);
    }

    uint32_t getPartition(const Variable32Field &field) {
        // Return a value between 0 and partitionCount - 1. A value in partition i
        // must be less than a value in partition j if i < j.
        if (field.size() == 0) {
            return 0;
        }
        uint8_t firstByte = static_cast<const uint8_t*>(field.val())[0];
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

const ExtentType *getExtentType(const string &file_name, const string &type_match) {
    TypeIndexModule input_module(type_match);
    input_module.addSource(file_name);
    Extent *first_extent = input_module.getExtent();
    if (first_extent == NULL) {
        return NULL;
    }
    return &first_extent->getType();
}

lintel::ProgramOption<string>
    input_file_name_option("input-file-name", "the input file name");

lintel::ProgramOption<string>
    output_file_name_option("output-file-name", "the output file name");

lintel::ProgramOption<string>
    extent_type_match_option("extent-type-match", "the extent type", string("Text"));

lintel::ProgramOption<int>
    node_index_option("node-index", "index of this node in the nodeNames list", -1);

lintel::ProgramOption<string>
    node_name_prefix_option("node-name-prefix", "the name of this node (a prefix of a single item in nodeNames)");

lintel::ProgramOption<string>
    node_names_option("node-names", "list of nodes");

lintel::ProgramOption<string>
    field_name_option("field-name", "name of the key field", string("line"));

lintel::ProgramOption<string>
    needle("needle", "substring to search for", string("abc"));

int main(int argc, char *argv[]) {
    LintelLog::parseEnv();
    vector<string> args(lintel::parseCommandLine(argc, argv, false));

    vector<string> node_names;
    string tmp(node_names_option.get());
    boost::split(node_names, tmp, boost::is_any_of(","));

    SINVARIANT(!node_names_option.get().empty());
    SINVARIANT(!input_file_name_option.get().empty());
    SINVARIANT(!output_file_name_option.get().empty());
    SINVARIANT(!needle.get().empty());
    int32_t i = 0;
    int32_t node_index = node_index_option.get();
    if (node_index_option.get() == -1) {
        INVARIANT(!node_name_prefix_option.get().empty(),
                  "You must provide --node-name-prefix if you don't provide --node-index.");
        BOOST_FOREACH(string &node_name, node_names) {
            if (boost::starts_with(node_name, node_name_prefix_option.get())) {
                INVARIANT(node_index == -1,
                          boost::format("There are at least two nodes that start with '%s': '%s', '%s'") %
                          node_name_prefix_option.get() % node_names[node_index] % node_names[i]);
                node_index = i;
            }
            ++i;
        }
    }
    LintelLogDebug("networkgrep", boost::format("Node index: %s") % node_index);

    Variable32FieldMatcher field_matcher(needle.get());

    // NetworkInputModule needs the extent type since our current networking protocol doesn't
    // convey type information.
    const ExtentType *extent_type = getExtentType(input_file_name_option.get(), extent_type_match_option.get());

    TypeIndexModule input_module(extent_type_match_option.get());
    input_module.addSource(input_file_name_option.get());

    ParallelGrepModule<Variable32Field, Variable32FieldMatcher>
           grep_module(input_module, field_name_option.get(), field_matcher);

    NetworkSink<Variable32Field, Variable32FieldPartitioner>
            network_sink(&grep_module, node_names, node_index, field_name_option.get(), Variable32FieldPartitioner());

    NetworkInputModule network_input_module(node_names, node_index, extent_type);
    network_input_module.start();

    SortModule<Variable32Field, Variable32FieldComparator>
            sort_module(network_input_module, field_name_option.get(), Variable32FieldComparator());


    network_sink.start(); // This starts sending extents over the network.

    DataSeriesSink sink(output_file_name_option.get(), Extent::compress_none);
    ExtentTypeLibrary library;
    library.registerType(*extent_type);
    sink.writeExtentLibrary(library);

    Extent *extent = NULL;
    while ((extent = sort_module.getExtent()) != NULL) {
        SINVARIANT(extent_type == &extent->getType());
        // Write the extent to the output file.
        sink.writeExtent(*extent, NULL);
        delete extent;
    }

    return 0;
}

