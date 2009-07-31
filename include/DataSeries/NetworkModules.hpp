// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A class for creating DataSeries-based programs that run across multiple
    machines in parallel.
*/

#ifndef DATASERIES_NETWORKMODULES_HPP
#define DATASERIES_NETWORKMODULES_HPP

#include <deque>
#include <vector>

#include <Lintel/Clock.hpp>
#include <Lintel/PThread.hpp>

#include <DataSeries/Extent.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/NetworkTcp.hpp>
#include <DataSeries/TypeIndexModule.hpp>

namespace dataseries {
class NetworkInputModule : public DataSeriesModule, public ParallelNetworkServerHandler {
public:
    NetworkInputModule(const std::vector<std::string> &node_names,
                       uint32_t node_index,
                       const ExtentType *extent_type)
        : node_names(node_names), node_index(node_index), extent_type(extent_type),
          server(node_names, node_index, *this), done(false) {
    }

    virtual ~NetworkInputModule() {}

    void start(); // Wait for all clients to connect.

    Extent *getExtent(); // This should only be called after start and before stop.

private:
    std::vector<std::string> node_names;
    uint32_t node_index;
    const ExtentType *extent_type;

    ParallelNetworkTcpServer server;

    PThreadMutex buffer_mutex;
    PThreadCond buffer_not_full_cond;
    PThreadCond buffer_not_empty_cond;
    std::deque<Extent*> buffer;

    bool done;

public:
    virtual void handleExtent(Extent *extent);
};

void NetworkInputModule::start() {
    LintelLogDebug("NetworkInputModule", "Waiting for all connections.");
    server.waitForAllConnect(); // Wait for all clients to connect.
    server.receiveExtents(*extent_type);
    LintelLogDebug("NetworkInputModule", "Waiting for incoming extents.");
}

void NetworkInputModule::handleExtent(Extent *extent) {
    PThreadScopedLock lock(buffer_mutex);
    while (buffer.size() == 10) {
        buffer_not_full_cond.wait(buffer_mutex);
    }

    buffer.push_back(extent);
    if (extent == NULL) {
        SINVARIANT(!done);
        done = true;
    }
    buffer_not_empty_cond.signal();
}

Extent* NetworkInputModule::getExtent() {
    PThreadScopedLock lock(buffer_mutex);
    while (buffer.empty() && !done) {
        buffer_not_empty_cond.wait(buffer_mutex);
    }

    Extent *extent = buffer.front();
    buffer.pop_front();
    buffer_not_full_cond.signal();

    return extent;
}

template <typename F, typename P>
class NetworkSink {
public:
    NetworkSink(DataSeriesModule *upstream_module,
                        const std::vector<std::string> &node_names,
                        uint32_t node_index,
                        const std::string &field_name,
                        const P &record_partitioner,
                        uint32_t extent_limit = 10 << 20)
        : upstream_module(upstream_module), node_names(node_names), node_index(node_index),
          field_name(field_name), record_partitioner(record_partitioner),
          extent_limit(extent_limit), client(node_names, node_index),
          connect_thread(this), data_thread(this) {
        this->record_partitioner.initialize(node_names.size());
        connect_thread.start(); // Create the thread that connects to the servers.
    }

    virtual ~NetworkSink() {}

    void start(); // Wait until we connect to all the servers. The server MUST be started first!

private:
    DataSeriesModule *upstream_module;
    std::vector<std::string> node_names;
    uint32_t node_index;
    std::string field_name;
    P record_partitioner;
    uint32_t extent_limit;

    std::vector<Extent*> outgoing_extents;
    ParallelNetworkTcpClient client;

    void createOutgoingExtents(const ExtentType &type);
    void sendExtent(uint32_t partition);
    void startInternal();

    class ConnectThread : public PThread {
    public:
        ConnectThread(NetworkSink<F, P> *module)
            : module(module) {}

        virtual ~ConnectThread() {}

        virtual void* run() {
            module->client.connectToAllServers();
            return NULL;
        }

    private:
        NetworkSink<F, P> *module;
    };
    friend class ConnectThread;
    ConnectThread connect_thread;

    class DataThread : public PThread {
    public:
        DataThread(NetworkSink<F, P> *module)
            : module(module) {}

        virtual ~DataThread() {}

        virtual void* run() {
            module->startInternal();
            return NULL;
        }

    private:
        NetworkSink<F, P> *module;
    };
    friend class DataThread;
    DataThread data_thread;
};

template <typename F, typename P>
void NetworkSink<F, P>::start() {
    data_thread.start();
}

template <typename F, typename P>
void NetworkSink<F, P>::startInternal() {
    LintelLogDebug("NetworkSink", "startInternal called.");
    ExtentSeries source_series;
    F field(source_series, field_name);

    ExtentSeries destination_series;
    ExtentRecordCopy copier(source_series, destination_series);


    LintelLogDebug("NetworkSink", "Waiting until connected to all servers.");
    connect_thread.join(); // Wait until we successfully connect to all the servers.
    LintelLogDebug("NetworkSink", "Connected to all servers.");


    LintelLogDebug("NetworkSink", "Retrieving first extent from upstream module.");
    Extent *source_extent = upstream_module->getExtent();
    if (source_extent == NULL) {
        LintelLogDebug("NetworkSink", "Upstream module doesn't have even one extent!");
        client.close();
        return;
    }

    LintelLogDebug("NetworkSink", "Retrieved first extent from upstream module.");
    createOutgoingExtents(source_extent->getType());

    LintelLogDebug("NetworkSink", "Pulling extents and partitioning records.");
    do {
        // Partition the extent's records and send full extents to the other side.
        for (source_series.start(source_extent); source_series.more(); source_series.next()) {
            uint32_t partition = record_partitioner.getPartition(field);
            Extent *destination_extent = outgoing_extents[partition];

            destination_series.setExtent(destination_extent);
            destination_series.newRecord(false);
            copier.copyRecord();

            // We have filled up an entire extent for this partition, so send it.
            if (destination_extent->size() + source_extent->getType().fixedrecordsize() >
                    extent_limit) {
                sendExtent(partition);
                Extent *extent = new Extent(source_extent->getType());
                extent->fixeddata.reserve(extent_limit);
                outgoing_extents[partition] = extent;
            }
        }

        delete source_extent;

        // Fetch the next extent from the upstream module.
        source_extent = upstream_module->getExtent();

        // TODO shirant: Get rid of this
        LintelLogDebug("NetworkSink", "NetworkSink called getExtent()");

    } while (source_extent != NULL);

    // Send all the left-over extents to the other nodes.
    for (uint32_t i = 0; i < node_names.size(); ++i) {
        sendExtent(i);
    }

    LintelLogDebug("NetworkSink", "Closing client.");
    client.close();
}

template <typename F, typename P>
void NetworkSink<F, P>::createOutgoingExtents(const ExtentType &type) {
    outgoing_extents.reserve(node_names.size());
    for (uint32_t i = 0; i < node_names.size(); ++i) {
        Extent *extent = new Extent(type);
        extent->fixeddata.reserve(extent_limit);
        outgoing_extents.push_back(extent);
    }
}

template <typename F, typename P>
void NetworkSink<F, P>::sendExtent(uint32_t partition) {
    LintelLogDebug("NetworkSink", boost::format("Sending an extent to partition %s.") % partition);
    Extent *extent = outgoing_extents[partition];
    outgoing_extents[partition] = NULL;
    client.sendExtent(extent, partition);
    delete extent;
}
}

#endif
