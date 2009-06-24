#include <DataSeries/NetworkTcp.hpp>

#include <arpa/inet.h>

#include <list>

#include <boost/foreach.hpp>

#include <Lintel/AssertBoost.hpp>

#include <DataSeries/Extent.hpp>

using namespace std;

ParallelNetworkTcpClient::ParallelNetworkTcpClient(vector<std::string> node_names,
                                                   uint32_t client_node_index,
                                                   uint16_t port)
    : ParallelNetworkClient(node_names, client_node_index), port(port) {
}

void ParallelNetworkTcpClient::sendExtent(Extent *extent, uint32_t server_node_index) {
    servers[server_node_index]->writer.writeExtent(extent);
}

void ParallelNetworkTcpClient::connectToAllServers() {
    list<NetworkEndpointPtr> inactive_servers;

    BOOST_FOREACH(string &node_name, node_names) {
        NetworkEndpointPtr server(new NetworkEndpoint);

        server->name = node_name;

        // Resolve the server's name to an IP address.
        struct hostent *he = gethostbyname(node_name.c_str());
        struct in_addr **address_list =(struct in_addr **)he->h_addr_list;
        char buf[INET_ADDRSTRLEN];
        const char *result = inet_ntop(AF_INET, address_list[0], buf, INET_ADDRSTRLEN);
        SINVARIANT(result != NULL);
        LintelLogDebug("NetworkTcp",
                       boost::format("Resolved hostname '%s' to '%s'/%s.") %
                       node_name % he->h_name % buf);

        memset(&server->address, 0, sizeof(server->address));
        server->address.sin_family = AF_INET;
        memcpy(&(server->address.sin_addr.s_addr), he->h_addr, he->h_length);
        server->address.sin_port = htons(port);

        server->socket_fd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
        INVARIANT(server->socket_fd != -1, "Unable to create a client socket.");

        server->writer.setFileDescriptor(server->socket_fd);

        servers.push_back(server);
        inactive_servers.push_back(server);
    }

    while (!inactive_servers.empty()) {
        for (list<NetworkEndpointPtr>::iterator it = inactive_servers.begin(); it != inactive_servers.end(); ++it) {
            NetworkEndpointPtr &server = *it;
            int result = ::connect(server->socket_fd, (struct sockaddr*)&server->address, sizeof(server->address));
            if (result != -1) {
                LintelLogDebug("NetworkTcp", boost::format("Connected to server '%s'.") % server->name);
                inactive_servers.erase(it);
                break;
            }
        }
    }

    LintelLogDebug("NetworkTcp", "Connected to all servers. Terminating thread.");
}

void ParallelNetworkTcpClient::close() {
    BOOST_FOREACH(NetworkEndpointPtr &server, servers) {
        //::close(server->socket_fd);
        server->writer.close();
    }
}

ParallelNetworkTcpServer::ParallelNetworkTcpServer(std::vector<std::string> node_names,
                                                   uint32_t server_node_index,
                                                   ParallelNetworkServerHandler &handler,
                                                   uint16_t port)
    : ParallelNetworkServer(node_names, server_node_index, handler), port(port),
      active_connections(0) {
    socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    INVARIANT(socket_fd != -1, "Unable to create the server socket.");
}

void ParallelNetworkTcpServer::waitForAllConnect() {
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = INADDR_ANY;
    memset(&(address.sin_zero), 0, 8);

    int result = ::bind(socket_fd, (struct sockaddr *)&address, sizeof(struct sockaddr));
    INVARIANT(result != -1, "Unable to bind the server socket.");

    result = ::listen(socket_fd, node_names.size());
    INVARIANT(result != -1, "Unable to listen on the server socket.");

    // Synchronously wait for the incoming TCP connections from all the nodes (including self).
    for (uint32_t i = 0; i < node_names.size(); ++i) {
        NetworkEndpointPtr client(new NetworkEndpoint);
        socklen_t length = sizeof(client->address);
        client->socket_fd = ::accept(socket_fd, (struct sockaddr *)&client->address, &length);
        INVARIANT(client->socket_fd != -1, "Unable to accept incoming connection.");

        ++active_connections;
        clients.push_back(client);
    }
}

void ParallelNetworkTcpServer::startReceiveExtents(uint32_t node_index, const ExtentType &extent_type) {
    ExtentReader reader(clients[node_index]->socket_fd, extent_type);
    Extent *extent = NULL;
    while ((extent = reader.getExtent()) != NULL) {
        handler.handleExtent(extent);
    }
    reader.close();

    PThreadScopedLock lock(active_connections_lock);
    --active_connections;
    if (active_connections == 0) {
        active_connections_cond.signal();
    }

    LintelLogDebug("NetworkTcp",
                   boost::format("Server now has %s active connection(s).") %
                   active_connections);
}

void ParallelNetworkTcpServer::receiveExtents(const ExtentType &extent_type) {
    for (uint32_t i = 0; i < node_names.size(); ++i) {
        PThreadPtr receive_thread(new ReceiveThread(this, i, extent_type));
        receive_threads.push_back(receive_thread);
    }

    BOOST_FOREACH(PThreadPtr &receive_thread, receive_threads) {
        receive_thread->start();
    }
}

void ParallelNetworkTcpServer::waitForAllClose() {
    PThreadScopedLock lock(active_connections_lock);
    while (active_connections > 0) {
        active_connections_cond.wait(active_connections_lock);
    }
    LintelLogDebug("NetworkTcp", "Server no longer has any active connections.");
}

ParallelNetworkTcpServer::~ParallelNetworkTcpServer() {
    ::close(socket_fd);
    LintelLogDebug("NetworkTcp", "Making sure that all receive threads have terminated.");
    BOOST_FOREACH(PThreadPtr &receive_thread, receive_threads) {
        receive_thread->join();
    }
    LintelLogDebug("NetworkTcp", "All receive threads have terminated.");
}
