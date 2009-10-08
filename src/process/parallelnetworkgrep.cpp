/*
   (c) Copyright 2008-2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

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
#include <DataSeries/ParallelHierarchicalMemorySortModule.hpp>
#include <DataSeries/ParallelNetworkProgram.hpp>
#include <DataSeries/SortModule.hpp>
#include <DataSeries/PushModule.hpp>

using namespace std;
using lintel::BoyerMooreHorspool;

class Variable32FieldComparator {
public:
    bool operator()(const Variable32Field &fieldLhs, const Variable32Field &fieldRhs) {
        int result = memcmp(fieldLhs.val(), fieldRhs.val(), min(fieldLhs.size(), fieldRhs.size()));
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

class ParallelNetworkGrepProgram;
class OutputThread : public PThread {
public:
    OutputThread(ParallelNetworkGrepProgram *program)
        : program(program) {}

    virtual ~OutputThread() {}

    virtual void* run();

private:
    ParallelNetworkGrepProgram *program;
};


class ParallelNetworkGrepProgram : public dataseries::ParallelNetworkProgram<Variable32Field,
    Variable32FieldPartitioner> {
public:
    ParallelNetworkGrepProgram(vector<string> node_names,
                               uint32_t node_index,
                               const string &input_file_prefix,
                               const string &extent_type_match,
                               const string &output_file_prefix,
                               const string &field_name,
                               const string &needle)
        : dataseries::ParallelNetworkProgram<Variable32Field, Variable32FieldPartitioner>(
              node_names, node_index, input_file_prefix, extent_type_match,
              field_name, Variable32FieldPartitioner()),
          sort_module(push_module, field_name, Variable32FieldComparator()),
          received_from_network(false), output_file_prefix(output_file_prefix),
          field(source_series, field_name), copier(source_series, destination_series),
          matcher(needle.c_str(), needle.size()) {
        LintelLogDebug("ParallelNetworkGrepProgram",
                       boost::format("Starting parallel sort on %s nodes (extent type match: %s).") %
                       node_names.size() % extent_type_match);
        uint32_t i = 0;
        BOOST_FOREACH(string &node_name, node_names) {
            LintelLogDebug("ParallelNetworkGrepProgram",
                           boost::format("%sNode: %s") %
                           ((i == node_index) ? "* " : "") %
                           node_name);
            ++i;
        }

        if (!output_file_prefix.empty()) {
            output_file = (boost::format("%s.%s") % output_file_prefix % node_index).str();
            sink.reset(new DataSeriesSink(output_file, Extent::compress_none));
        }
    }

    virtual ~ParallelNetworkGrepProgram() {
        if (output_thread.get() != NULL) {
            output_thread->join();
        }
    }

    virtual Extent* processExtentFromFile(Extent *extent) {
        Extent *destination_extent = new Extent(extent->getType());
        destination_series.setExtent(destination_extent);

        for (source_series.start(extent); source_series.more(); source_series.next()) {
            if (matcher.matches(field.val(), field.size())) {
                destination_series.newRecord();
                copier.copyRecord();
            }
        }

        delete extent;
        return destination_extent;
    }

    virtual void processExtentFromNetwork(Extent *extent) {
        if (extent == NULL) {
            return;
        }
        PThreadScopedLock lock(mutex);
        if (!received_from_network) {
            if (!output_file_prefix.empty()) {
                ExtentTypeLibrary library;
                LintelLogDebug("ParallelNetworkGrepProgram",
                               boost::format("Registering extent type '%s' with sink.") % extent->getType().getName());
                library.registerType(extent->getType());
                sink->writeExtentLibrary(library);
            }
            output_thread.reset(new OutputThread(this));
            output_thread->start();
            received_from_network = true;
        }
        push_module.addExtent(extent);
    }

    virtual void finishedFile() {
        LintelLogDebug("ParallelNetworkGrepProgram",
                       boost::format("Finished reading %s extents from file.") % getFileExtentCount());
    }

    virtual void finishedNetwork() {
        LintelLogDebug("ParallelNetworkGrepProgram",
                       boost::format("Finished reading %s extents from network. Closing push module.") % getNetworkExtentCount());

        push_module.close();
    }

    void writeOutput() {
        Clock::Tfrac start_clock = Clock::todTfrac();

        if (output_file_prefix.empty()) {
            Extent *extent = NULL;
            while ((extent = sort_module.getExtent()) != NULL) {
                delete extent;
            }
        } else {
            Extent *extent = NULL;
            LintelLogDebug("ParallelNetworkGrepProgram", "Starting to write output.");
            while ((extent = sort_module.getExtent()) != NULL) {
                LintelLogDebug("ParallelNetworkGrepProgram", "Writing an extent to the sink.");
                sink->writeExtent(*extent, NULL);
                delete extent;
            }
            LintelLogDebug("ParallelNetworkGrepProgram", "Closing the sink.");
            sink->close();
        }

        Clock::Tfrac stop_clock = Clock::todTfrac();
        LintelLogDebug("ParallelNetworkProgram", boost::format("======== writeOutput (from receiving first network extent to completion): %s") % Clock::TfracToDouble(stop_clock - start_clock));
    }

private:
    PushModule push_module;
    SortModule<Variable32Field, Variable32FieldComparator> sort_module;
    boost::scoped_ptr<OutputThread> output_thread;
    bool received_from_network;
    string output_file_prefix;
    string output_file;
    boost::scoped_ptr<DataSeriesSink> sink;
    PThreadMutex mutex;

    ExtentSeries source_series, destination_series;
    Variable32Field field;
    ExtentRecordCopy copier;

    BoyerMooreHorspool matcher;
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
    needle("needle", "substring to search for", string("seven"));

int main(int argc, char *argv[]) {
    LintelLog::parseEnv();
    vector<string> args(lintel::parseCommandLine(argc, argv, false));
    cout << input_file_prefix_option.get() << endl;

    vector<string> node_names;
    string tmp(node_names_option.get());
    boost::split(node_names, tmp, boost::is_any_of(","));

    SINVARIANT(!node_names_option.get().empty());
    SINVARIANT(!input_file_prefix_option.get().empty());
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
    LintelLogDebug("ParallelNetworkGrepProgram", boost::format("Node index: %s") % node_index);

    ParallelNetworkGrepProgram program(node_names,
                                       node_index,
                                       input_file_prefix_option.get(),
                                       extent_type_match_option.get(),
                                       output_file_prefix_option.get(),
                                       field_name_option.get(),
                                       needle.get());
    program.start();
    return 0;
}
