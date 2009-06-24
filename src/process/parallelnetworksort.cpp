/*
   (c) Copyright 2008-2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <vector>
#include <string>
#include <iostream>

#include <boost/foreach.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>

#include <Lintel/ProgramOptions.hpp>

#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>
#include <DataSeries/ParallelHierarchicalMemorySortModule.hpp>
#include <DataSeries/ParallelNetworkProgram.hpp>
#include <DataSeries/ParallelSortModule.hpp>
#include <DataSeries/PushModule.hpp>

using namespace std;

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
        LintelLogDebug("parallelnetworksort",
                       boost::format("Determining %s partitioning limits with an increment of %s.") %
                       partitionCount % increment);
        float currentLimit = increment;
        for (uint32_t i = 0; i < partitionCount - 1; ++i) {
            limits.push_back(static_cast<uint8_t>(round(currentLimit)));
            LintelLogDebug("parallelnetworksort", boost::format("Partition limit: %s") % (uint32_t)limits.back());
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

class ParallelNetworkSortProgram;
class OutputThread : public PThread {
public:
    OutputThread(ParallelNetworkSortProgram *program)
        : program(program) {}

    virtual ~OutputThread() {}

    virtual void* run();

private:
    ParallelNetworkSortProgram *program;
};


class ParallelNetworkSortProgram : public dataseries::ParallelNetworkProgram<FixedWidthField,
    FixedWidthFieldPartitioner> {
public:
    ParallelNetworkSortProgram(std::vector<std::string> node_names,
                               uint32_t node_index,
                               const std::string &input_file_prefix,
                               const std::string &extent_type_match,
                               const std::string &output_file_prefix,
                               const std::string &field_name)
        : dataseries::ParallelNetworkProgram<FixedWidthField, FixedWidthFieldPartitioner>(
              node_names, node_index, input_file_prefix, extent_type_match,
              field_name, FixedWidthFieldPartitioner()),
          sort_module(push_module, field_name),
          received_from_network(false),
          output_file((boost::format("%s.%s") % output_file_prefix % node_index).str()),
          sink(output_file, Extent::compress_none) {
        LintelLogDebug("ParallelNetworkSortProgram",
                       boost::format("Starting parallel sort on %s nodes (extent type match: %s).") %
                       node_names.size() % extent_type_match);
        uint32_t i = 0;
        BOOST_FOREACH(string &node_name, node_names) {
            LintelLogDebug("ParallelNetworkSortProgram",
                           boost::format("%sNode: %s") %
                           ((i == node_index) ? "* " : "") %
                           node_name);
            ++i;
        }
    }

    virtual ~ParallelNetworkSortProgram() {
        if (output_thread.get() != NULL) {
            output_thread->join();
        }
    }

    virtual Extent* processExtentFromFile(Extent *extent) {
        return extent;
    }

    virtual void processExtentFromNetwork(Extent *extent) {
        PThreadScopedLock lock(mutex);
        if (!received_from_network) {
            ExtentTypeLibrary library;
            LintelLogDebug("ParallelNetworkSortProgram",
                           boost::format("Registering extent type '%s' with sink.") % extent->getType().getName());
            library.registerType(extent->getType());
            sink.writeExtentLibrary(library);

            output_thread.reset(new OutputThread(this));
            output_thread->start();
            received_from_network = true;
        }
        push_module.addExtent(extent);
    }

    virtual void finishedFile() {
        LintelLogDebug("ParallelNetworkSortProgram",
                       boost::format("Finished reading %s extents from file.") % getFileExtentCount());
    }

    virtual void finishedNetwork() {
        LintelLogDebug("ParallelNetworkSortProgram",
                       "Finished reading from network. Closing push module.");

        push_module.close();
    }

    void writeOutput() {
        Extent *extent = NULL;
        LintelLogDebug("ParallelNetworkSortProgram", "Starting to write output.");
        while ((extent = sort_module.getExtent()) != NULL) {
            LintelLogDebug("ParallelNetworkSortProgram", "Writing an extent to the sink.");
            sink.writeExtent(*extent, NULL);
            delete extent;
        }
        LintelLogDebug("ParallelNetworkSortProgram", "Closing the sink.");
        sink.close();
    }

private:
    PushModule push_module;
    ParallelSortModule sort_module;
    boost::shared_ptr<OutputThread> output_thread;
    bool received_from_network;
    string output_file;
    DataSeriesSink sink;
    PThreadMutex mutex;
};

void* OutputThread::run() {
    program->writeOutput();
    return NULL;
}

lintel::ProgramOption<string>
    input_file_prefix_option("input-file-prefix", "the input file prefix (.node-index will be appended)");

lintel::ProgramOption<string>
    output_file_prefix_option("output-file-prefix", "the output file prefix (.node-index will be appended)");

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
    cout << input_file_prefix_option.get() << endl;

    vector<string> node_names;
    string tmp(node_names_option.get());
    boost::split(node_names, tmp, boost::is_any_of(","));

    SINVARIANT(!node_names_option.get().empty());
    SINVARIANT(!input_file_prefix_option.get().empty());
    SINVARIANT(!output_file_prefix_option.get().empty());

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
    LintelLogDebug("ParallelNetworkSortProgram", boost::format("Node index: %s") % node_index);

    ParallelNetworkSortProgram program(node_names,
                                       node_index,
                                       input_file_prefix_option.get(),
                                       extent_type_match_option.get(),
                                       output_file_prefix_option.get(),
                                       field_name_option.get());
    program.start();
    return 0;
}
