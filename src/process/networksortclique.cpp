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
#include <DataSeries/NetworkClique.hpp>
#include <DataSeries/ParallelSortModule.hpp>
#include <DataSeries/PrefetchBufferModule.hpp>

using namespace std;
using namespace dataseries;


class FixedWidthFieldComparator {
public:
    bool operator()(const FixedWidthField &fieldLhs, const FixedWidthField &fieldRhs) {
        DEBUG_SINVARIANT(fieldLhs.size() == fieldRhs.size());
        return memcmp(fieldLhs.val(), fieldRhs.val(), fieldLhs.size()) <= 0;
    }
};

class FixedWidthFieldPartitioner {
public:
    void initialize(uint32_t partition_count) {
        DEBUG_INVARIANT(partition_count <= 256, "This partitioner partitions based on the first byte.");
        float increment = 256.0 / partition_count;
        float current_limit = increment;

        for (uint32_t i = 0; i < partition_count - 1; ++i) {
            uint8_t top = static_cast<uint8_t>(round(current_limit));
            while (partitions.size() < top) {
                LintelLogDebug("networksortclique", boost::format("partitions[%s] == %s") % partitions.size() % i);
                partitions.push_back(i);
            }

            current_limit += increment;
        }

        while (partitions.size() < 256) {
            LintelLogDebug("networksortclique", boost::format("partitions[%s] == %s") % partitions.size() % (partition_count - 1));
            partitions.push_back(partition_count - 1);
        }
    }

    uint32_t getPartition(const FixedWidthField &field) {
        uint8_t first_byte = static_cast<const uint8_t*>(field.val())[0];
        return partitions[first_byte];
    }
private:
    vector<uint32_t> partitions;
};

lintel::ProgramOption<string>
    input_file_name_option("input-file-name", "the input file name");

lintel::ProgramOption<string>
    output_file_name_option("output-file-name", "the output file name");

lintel::ProgramOption<string>
    extent_type_match_option("extent-type-match", "the extent type", string("Gensort"));

lintel::ProgramOption<int>
    node_index_option("node-index", "index of this node in the nodeNames list", -1);

lintel::ProgramOption<string>
    node_name_prefix_option("node-name-prefix", "the name of this node (a prefix of a single item in nodeNames)");

lintel::ProgramOption<string>
    node_names_option("node-names", "list of nodes");

lintel::ProgramOption<string>
    field_name_option("field-name", "name of the key field", string("key"));

int main(int argc, char *argv[]) {
    LintelLog::parseEnv();
    vector<string> args(lintel::parseCommandLine(argc, argv, false));

    vector<string> node_names;
    string tmp(node_names_option.get());
    boost::split(node_names, tmp, boost::is_any_of(","));

    SINVARIANT(!node_names_option.get().empty());
    SINVARIANT(!input_file_name_option.get().empty());
    SINVARIANT(!output_file_name_option.get().empty());
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
    if (node_index == -1 || (uint32_t)node_index >= node_names.size()) {
    	cout << "This node is not participating in the computation." << endl;
    	return 0;
    }
    LintelLogDebug("networksortclique", boost::format("Node index: %s") % node_index);

    TypeIndexModule input_module(extent_type_match_option.get());
    input_module.addSource(input_file_name_option.get());

    NetworkClique<FixedWidthField, FixedWidthFieldPartitioner>
            network_clique(&input_module, node_names, node_index, field_name_option.get(), FixedWidthFieldPartitioner(), 1 << 20);
    network_clique.start();

    uint64_t memory_limit = 2500; memory_limit *= 1000000;
    ParallelSortModule
            sort_module(network_clique, field_name_option.get(), 1000000, -1, memory_limit, false, "/opt/array/tmp/sort");

    ExtentWriter writer(output_file_name_option.get(), false, false);
    Extent *extent = NULL;
    while ((extent = sort_module.getExtent()) != NULL) {
        writer.writeExtent(extent);
        delete extent;
    }
    /*
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
    */
    network_clique.join();
    return 0;
}

