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
#include <DataSeries/ParallelRadixSortModule.hpp>
#include <DataSeries/ParallelSortModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/NetworkClique.hpp>

using namespace std;
using namespace dataseries;

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

int main(int argc, char *argv[]) {
    LintelLog::parseEnv();

    while (true) {
		TypeIndexModule input_module("Gensort");
		input_module.addSource("/opt/array/gensort1gb.ds");

		//ParallelSortModule sort_module(input_module, "key", 1 << 20, -1, 600 * 1000 * 1000, false, "/opt/array/tmp/sort");

		//ParallelRadixSortModule sort_module(input_module, "key");
		vector<string> node_names;
		node_names.push_back("10.10.10.10");
		node_names.push_back("10.10.10.11");

		NetworkClique<FixedWidthField, FixedWidthFieldPartitioner>
		            network_clique(&input_module, node_names, atoi(argv[1]), "key", FixedWidthFieldPartitioner(), 1 << 20);
		    network_clique.start();


		Extent *extent = NULL;
		while ((extent = network_clique.getExtent()) != NULL) {
			delete extent;
		}
    }
    return 0;
}

