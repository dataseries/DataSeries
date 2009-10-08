// -*-C++-*-
/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#ifndef DATASERIES_NETWORKTCP_H
#define DATASERIES_NETWORKTCP_H

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <Lintel/LintelLog.hpp>
#include <Lintel/PThread.hpp>

#include <DataSeries/ExtentReader.hpp>
#include <DataSeries/ExtentWriter.hpp>
#include <DataSeries/Network.hpp>

class NetworkEndpoint {
public:
	NetworkEndpoint() : writer(false, true) {}
    struct sockaddr_in address;
    int socket_fd;
    std::string name;
    ExtentWriter writer;
};

typedef boost::shared_ptr<NetworkEndpoint> NetworkEndpointPtr;

class ParallelNetworkTcpClient : public ParallelNetworkClient {
public:
    ParallelNetworkTcpClient(std::vector<std::string> node_names,
                             uint32_t client_node_index,
                             std::string log_file_name = "",
                             uint16_t port = 13131);

    virtual ~ParallelNetworkTcpClient() { }

    virtual void sendExtent(Extent *extent, uint32_t server_node_index);

    void connectToAllServers();
    void close();

private:
    std::vector<NetworkEndpointPtr> servers;
    std::string log_file_name;
    uint16_t port;

    boost::scoped_ptr<DataSeriesSink> log_sink;
    bool first_extent;
};

class ParallelNetworkTcpServer : public ParallelNetworkServer {
public:
    ParallelNetworkTcpServer(std::vector<std::string> node_names,
                             uint32_t server_node_index,
                             ParallelNetworkServerHandler &handler,
                             uint16_t port = 13131);

    virtual ~ParallelNetworkTcpServer();

    // Wait for all incoming connections before returning.
    void waitForAllConnect();

    // Accept extents on the incoming connections and call handler for each one. This function
    // is asynchronous so it returns immediately.
    void receiveExtents(const ExtentType &extent_type);

    // Wait for all client connections to terminate.
    void waitForAllClose();

private:
    uint16_t port;

    std::vector<NetworkEndpointPtr> clients;
    int socket_fd;
    struct sockaddr_in address;
    uint32_t active_connections;

    PThreadMutex active_connections_lock;
    PThreadCond active_connections_cond;

    void startReceiveExtents(uint32_t node_index, const ExtentType &extent_type);

    class ReceiveThread : public PThread {
    public:
        ReceiveThread(ParallelNetworkTcpServer *server, uint32_t node_index, const ExtentType &extent_type)
            : server(server), node_index(node_index), extent_type(extent_type) {}

        virtual ~ReceiveThread() {}

        virtual void* run() {
            server->startReceiveExtents(node_index, extent_type);
            return NULL;
        }

    private:
        ParallelNetworkTcpServer *server;
        uint32_t node_index;
        const ExtentType &extent_type;
    };
    friend class ReceiveThread;
    typedef boost::shared_ptr<PThread> PThreadPtr;

    std::vector<PThreadPtr> receive_threads;
};

#endif
