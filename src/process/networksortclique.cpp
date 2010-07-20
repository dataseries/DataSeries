#include <vector>
#include <string>
#include <iostream>
#include <stdlib.h>
#include <sys/time.h>
#include <arpa/inet.h>

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

class AccuratePartitioner {
public:
    void initialize(uint32_t partition_count) {
	DEBUG_INVARIANT(partition_count <= 256, "This partitioner only supports 256 partitions.");
	float increment = 65536.0 / partition_count;
	float current_limit = 0;

	partitions = new uint16_t[partition_count+1];
	count = partition_count;

	for (uint32_t i = 0; i < count; ++i) {
	    partitions[i] = static_cast<uint16_t>(round(current_limit));
	    current_limit += increment;
	}
	partitions[partition_count] = 0xFFFF; //max

	uint32_t j=0;
	increment = 256.0 / partition_count;
	current_limit = increment;
	for (uint32_t i = 0; i < partition_count - 1; ++i) {
	    uint8_t top = static_cast<uint8_t>(round(current_limit));

	    while (j < top) {
		LintelLogDebug("networksortclique", boost::format("partitions[%s] == %s") % j % i);
		quick_par[j] = i;
		++j;
	    }

	    // Delimit border with a hint
	    quick_par[j-1]=-(i+1);
	    quick_par[j]=-(i+1);
	    ++j;
	    current_limit += increment;
	}

	while (j < 256) {
	    LintelLogDebug("networksortclique", boost::format("partitions[%s] == %s") % j % (partition_count - 1));
	    quick_par[j] = partition_count-1;
	    ++j;
	}
    }

    uint32_t getPartition(const FixedWidthField &field) {
	uint8_t first_byte = static_cast<const uint8_t*>(field.val())[0];
	if (quick_par[first_byte]>=0) {
	    return quick_par[first_byte];
	}

	uint16_t word = htons(reinterpret_cast<const uint16_t*>(field.val())[0]);
	int16_t l;
	l = (-quick_par[first_byte]); // decode hint.  We also add one
				      // to skip first comparison
				      // below.
	while (partitions[l] < word) {
	    ++l;
	}
	return l-1;
    }
private:
    int16_t quick_par[256];
    uint16_t * partitions;
    uint8_t count;
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
    // Timing info
    struct timeval *Tps, *Tpf, *Tsyncs, *Tsyncf, *Tjoins, *Tjoinf;
    Tps = (struct timeval*) malloc(sizeof(struct timeval));
    Tpf = (struct timeval*) malloc(sizeof(struct timeval));
    Tsyncs = (struct timeval*) malloc(sizeof(struct timeval));
    Tsyncf = (struct timeval*) malloc(sizeof(struct timeval));
    Tjoins = (struct timeval*) malloc(sizeof(struct timeval));
    Tjoinf = (struct timeval*) malloc(sizeof(struct timeval));

    gettimeofday (Tps, NULL);

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

    NetworkClique<FixedWidthField, AccuratePartitioner>
            network_clique(&input_module, node_names, node_index, field_name_option.get(), AccuratePartitioner(), 1 << 20);
    network_clique.start();

    uint64_t memory_limit = 4400; memory_limit *= 1000000;
    ParallelSortModule
            sort_module(network_clique, field_name_option.get(), 1000000, -1, memory_limit, false, "/tmp/sort");

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

    // Force sync for durability
    gettimeofday (Tsyncs, NULL);
    system("sudo sync");
    gettimeofday (Tsyncf, NULL);
    
    gettimeofday (Tjoins, NULL);
    network_clique.join();
    gettimeofday (Tjoinf, NULL);

    // Get total time statistics
    gettimeofday (Tpf, NULL);

    long tsynctim = (Tsyncf->tv_sec-Tsyncs->tv_sec)*1000000 + Tsyncf->tv_usec-Tsyncs->tv_usec;
    LintelLogDebug("NetworkClique", boost::format("Sync call took %ld us") % tsynctim);
    long tjointim = (Tjoinf->tv_sec-Tjoins->tv_sec)*1000000 + Tjoinf->tv_usec-Tjoins->tv_usec;
    LintelLogDebug("NetworkClique", boost::format("Join call took %ld us") % tjointim);
    long ttim = (Tpf->tv_sec-Tps->tv_sec)*1000000 + Tpf->tv_usec-Tps->tv_usec;
    LintelLogDebug("NetworkClique", boost::format("Total Time: %ld us") % ttim);

    return 0;
}
