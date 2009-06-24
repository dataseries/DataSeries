// -*-C++-*-
/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#ifndef DATASERIES_NETWORK_H
#define DATASERIES_NETWORK_H

#include <string>
#include <vector>

#include <DataSeries/Extent.hpp>

class ParallelNetworkClient {
public:
    ParallelNetworkClient(std::vector<std::string> node_names, uint32_t client_node_index)
        : node_names(node_names), client_node_index(client_node_index)
    { }
    virtual ~ParallelNetworkClient() { }

    virtual void sendExtent(Extent *extent, uint32_t server_node_index) = 0;

protected:
    std::vector<std::string> node_names;
    uint32_t client_node_index;
};


class ParallelNetworkServerHandler {
public:
    virtual void handleExtent(Extent *extent) = 0;
    virtual ~ParallelNetworkServerHandler() { }
};


class ParallelNetworkServer {
public:
    ParallelNetworkServer(std::vector<std::string> node_names,
                          uint32_t server_node_index,
                          ParallelNetworkServerHandler &handler)
        : node_names(node_names), server_node_index(server_node_index), handler(handler)
    { }

    virtual ~ParallelNetworkServer() { }

protected:
    std::vector<std::string> node_names;
    uint32_t server_node_index;
    ParallelNetworkServerHandler &handler;
};

#endif
