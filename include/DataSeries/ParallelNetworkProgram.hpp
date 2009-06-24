// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A class for creating DataSeries-based programs that run across multiple
    machines in parallel.
*/

#ifndef __DATASERIES_PARALLELNETWORKPROGRAM_H
#define __DATASERIES_PARALLELNETWORKPROGRAM_H

#include <vector>

#include <DataSeries/Extent.hpp>
#include <DataSeries/NetworkTcp.hpp>
#include <DataSeries/TypeIndexModule.hpp>

namespace dataseries {
/** An abstract class for implementing a DataSeries-based program that runs in parallel on
    multiple machines. In the first phase, the subclass' processExtentFromFile is called
    for each extent in the local DataSeries file. That method must return an extent, which
    is then partitioned (according to the user-supplied partitioner). Each partition is buffered
    and sent to a different node.
    In the second phase, the program receives records/extents over the network from all the
    other nodes, and processExtentFromNetwork is called. This function decides what to do with
    the data, but it does not return anything.
    Note that the subclass can also implement finishedFile and/or finishedNetwork to receive
    notifications when all of the local data has been read and all of the data has been received
    from the other nodes, respectively.
    Also note that a node must have local data in order to participate in the parallel
    computation. The justification for this limitation is that the network protocol currently
    does not convey extent type information, so the nodes are expected to learn about types
    from their local DataSeries files. */
template <typename F, typename P>
class ParallelNetworkProgram : public ParallelNetworkServerHandler {
public:
    ParallelNetworkProgram(const std::vector<std::string> &node_names,
                           uint32_t node_index,
                           const std::string &input_file_prefix,
                           const std::string &extent_type_match,
                           const std::string &field_name,
                           const P &record_partitioner,
                           uint32_t extent_limit = 1 << 20)
        : node_names(node_names), node_index(node_index),
          input_file_prefix(input_file_prefix), extent_type_match(extent_type_match),
          field_name(field_name), record_partitioner(record_partitioner), extent_limit(extent_limit),
          file_extent_count(0), network_extent_count(0),
          client(node_names, node_index), server(node_names, node_index, *this),
          connect_thread(this) {
        this->record_partitioner.initialize(node_names.size());
    }
    virtual ~ParallelNetworkProgram();
    void start();

    virtual Extent* processExtentFromFile(Extent *extent) = 0;
    virtual void processExtentFromNetwork(Extent *extent) = 0;

    virtual void finishedFile() {}
    virtual void finishedNetwork() {}

    virtual void handleExtent(Extent *extent);

private:
    std::vector<std::string> node_names;
    uint32_t node_index;
    std::string input_file_prefix;
    std::string extent_type_match;
    std::string field_name;
    P record_partitioner;
    uint32_t extent_limit;

    uint64_t file_extent_count;
    uint64_t network_extent_count;
    std::vector<Extent*> outgoing_extents;

    ParallelNetworkTcpClient client;
    ParallelNetworkTcpServer server;

    void startFile();
    void startNetwork();
    void stopNetwork();

    void createOutgoingExtents(const ExtentType &type);
    void sendExtent(uint32_t partition);

    class ConnectThread : public PThread {
    public:
        ConnectThread(ParallelNetworkProgram<F, P> *program)
            : program(program) {}

        virtual ~ConnectThread() {}

        virtual void* run() {
            program->client.connectToAllServers();
            return NULL;
        }

    private:
        ParallelNetworkProgram<F, P> *program;
    };
    friend class ConnectThread;

    ConnectThread connect_thread;

protected:
    uint64_t getFileExtentCount() { return file_extent_count; }
    uint64_t getNetworkExtentCount() { return network_extent_count; }
};

template <typename F, typename P>
ParallelNetworkProgram<F, P>::~ParallelNetworkProgram() {
    LintelLogDebug("ParallelNetworkProgram", "Making sure that connect thread has terminated.");
    connect_thread.join();
    LintelLogDebug("ParallelNetworkProgram", "The connect thread has terminated.");
}

template <typename F, typename P>
void ParallelNetworkProgram<F, P>::start() {
    startNetwork();
    startFile();
    stopNetwork();
}

template <typename F, typename P>
void ParallelNetworkProgram<F, P>::startNetwork() {
    LintelLogDebug("ParallelNetworkProgram", "Connecting to all servers.");
    connect_thread.start();

    LintelLogDebug("ParallelNetworkProgram", "Waiting for all client connections to establish.");
    server.waitForAllConnect(); // Wait for all clients to connect.
}

template <typename F, typename P>
void ParallelNetworkProgram<F, P>::stopNetwork() {
    LintelLogDebug("ParallelNetworkProgram", "************ Stopping network.");
    client.close();

    LintelLogDebug("ParallelNetworkProgram", "Waiting for all client connections to terminate.");
    server.waitForAllClose(); // Wait for all connections to close (the clients close the connections).
    finishedNetwork();
    LintelLogDebug("ParallelNetworkProgram", "Finished network.");
}

template <typename F, typename P>
void ParallelNetworkProgram<F, P>::createOutgoingExtents(const ExtentType &type) {
    outgoing_extents.reserve(node_names.size());
    for (uint32_t i = 0; i < node_names.size(); ++i) {
        Extent *extent = new Extent(type);
        extent->fixeddata.reserve(extent_limit);
        outgoing_extents.push_back(extent);
    }
}

template <typename F, typename P>
void ParallelNetworkProgram<F, P>::sendExtent(uint32_t partition) {
    Extent *extent = outgoing_extents[partition];
    outgoing_extents[partition] = NULL;
    client.sendExtent(extent, partition);
    delete extent;
}

template <typename F, typename P>
void ParallelNetworkProgram<F, P>::startFile() {
    TypeIndexModule input_module(extent_type_match);
    std::string input_file_name((boost::format("%s.%s") % input_file_prefix % node_index).str());
    input_module.addSource(input_file_name);

    ExtentSeries source_series;
    F field(source_series, field_name);

    ExtentSeries destination_series;
    ExtentRecordCopy copier(source_series, destination_series);

    LintelLogDebug("ParallelNetworkProgram",
                   boost::format("Reading from input file '%s'.") %
                   input_file_name);


    bool first_extent = true;
    Extent *source_extent = NULL;
    while ((source_extent = input_module.getExtent()) != NULL) {
        Extent *processed_extent = processExtentFromFile(source_extent);

        if (first_extent) {
            server.receiveExtents(processed_extent->getType()); // Asynchronously receive extents from all clients.
            createOutgoingExtents(processed_extent->getType());
            first_extent = false;
        }

        for (source_series.start(processed_extent); source_series.more(); source_series.next()) {
            uint32_t partition = record_partitioner.getPartition(field);
            Extent *destination_extent = outgoing_extents[partition];

            destination_series.setExtent(destination_extent);
            destination_series.newRecord(false);
            copier.copyRecord();

            // We have filled up an entire extent for this partition, so send it.
            if (destination_extent->size() + processed_extent->getType().fixedrecordsize() >
                extent_limit) {
                sendExtent(partition);
                Extent *extent = new Extent(processed_extent->getType());
                extent->fixeddata.reserve(extent_limit);
                outgoing_extents[partition] = extent;
            }
        }

        delete processed_extent;

        ++file_extent_count;
    }

    // Send all the left-over extents to the other nodes.
    for (uint32_t i = 0; i < node_names.size(); ++i) {
        sendExtent(i);
    }

    finishedFile();
    LintelLogDebug("ParallelNetworkProgram", "Finished file.");
}

template <typename F, typename P>
void ParallelNetworkProgram<F, P>::handleExtent(Extent *extent) {
    processExtentFromNetwork(extent);
}

}

#endif
