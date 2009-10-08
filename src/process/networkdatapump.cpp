#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>

#include <iostream>
#include <vector>
#include <string>

#include <boost/format.hpp>

#include <Lintel/Clock.hpp>
#include <Lintel/PThread.hpp>

using namespace std;

void blockWrite(int fd, void *buffer, uint32_t size) {
    uint8_t *byte_buffer = static_cast<uint8_t*>(buffer);
    while (size > 0) {
        ssize_t ret = ::send(fd, byte_buffer, size, 0);
        INVARIANT(ret > 0, "Unable to write buffer to socket.");
        size -= ret;
        byte_buffer += ret;
    }
}

void blockRead(int fd, void *buffer, uint32_t size) {
    uint8_t *byte_buffer = reinterpret_cast<uint8_t*>(buffer);
    while (size > 0) {
        ssize_t ret = ::recv(fd, byte_buffer, size, 0);
        size -= ret;
        byte_buffer += ret;
    }
}

class ReadThread : public PThread {
public:
    ReadThread(uint32_t local, uint32_t remote, int fd, uint32_t chunk_in_mb) : local(local), remote(remote), fd(fd), chunk_in_mb(chunk_in_mb) {}
    virtual ~ReadThread() {}

    virtual void* run() {
        char *buffer = new char[1000000]; // 1 MB buffer
        bzero(buffer, 1000000);

        while (true) {
            Clock::Tfrac start_clock = Clock::todTfrac();
            for (uint32_t i = 0; i < chunk_in_mb; ++i) {
                blockRead(fd, buffer, 1000000);
            }
            Clock::Tfrac stop_clock = Clock::todTfrac();
            double s = Clock::TfracToDouble(stop_clock - start_clock);
            double throughput = chunk_in_mb/s;
            cerr << boost::format("R*** %s<-%s ***\tT = %s MB/s ||| data = %s MB ||| t = %s s") % local % remote % throughput % chunk_in_mb % s << endl;
        }
        return NULL;
    }

private:
    uint32_t local, remote;
    int fd;
    uint32_t chunk_in_mb;
};

class WriteThread : public PThread {
public:
    WriteThread(uint32_t local, uint32_t remote, int fd, uint32_t chunk_in_mb) : local(local), remote(remote), fd(fd), chunk_in_mb(chunk_in_mb) {}
    virtual ~WriteThread() {}

    virtual void* run() {
        char *buffer = new char[1000000]; // 1 MB buffer
        bzero(buffer, 1000000);

        while (true) {
            Clock::Tfrac start_clock = Clock::todTfrac();
            for (uint32_t i = 0; i < chunk_in_mb; ++i) {
                blockWrite(fd, buffer, 1000000);
            }
            Clock::Tfrac stop_clock = Clock::todTfrac();
            double s = Clock::TfracToDouble(stop_clock - start_clock);
            double throughput = chunk_in_mb/s;
            cerr << boost::format("W*** %s->%s ***\tT = %s MB/s ||| data = %s MB ||| t = %s s") % local % remote % throughput % chunk_in_mb % s << endl;
        }
        return NULL;
    }

private:
    uint32_t local, remote;
    int fd;
    uint32_t chunk_in_mb;
};

class Pump {
public:
    Pump(uint32_t local, uint32_t remote, int fd, uint32_t chunk_in_mb) :
        local(local), remote(remote), chunk_in_mb(chunk_in_mb), fd(fd), rt(NULL), wt(NULL) {}

    void start() {
        rt = new ReadThread(local, remote, fd, chunk_in_mb);
        wt = new WriteThread(local, remote, fd, chunk_in_mb);
        rt->start();
        wt->start();
    }

    void join() {
        rt->join();
        wt->join();
    }

private:
    uint32_t local, remote;
    uint32_t chunk_in_mb;
    int fd;
    ReadThread *rt;
    WriteThread *wt;
};

int createListener(uint16_t port) {
    struct sockaddr_in listen_sin;
    listen_sin.sin_family = AF_INET;
    listen_sin.sin_port = htons(port);
    listen_sin.sin_addr.s_addr = INADDR_ANY;
    memset(&(listen_sin.sin_zero), 0, 8);

    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    INVARIANT(listen_fd != -1, "Unable to create the server socket.");

    uint32_t yes = 1;
    int result = ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    INVARIANT(result == 0, "Unable to set SO_REUSEADDR on socket.");

    result = ::bind(listen_fd, (struct sockaddr *)&listen_sin, sizeof(struct sockaddr));
    INVARIANT(result != -1, "Unable to bind the server socket.");

    result = ::listen(listen_fd, 1);
    INVARIANT(result != -1, "Unable to listen on the server socket.");

    return listen_fd;
}

int acceptConnection(int listen_fd) {
    struct sockaddr_in remote_node_sin;
    socklen_t length = sizeof(remote_node_sin);
    int accept_fd = ::accept(listen_fd, (struct sockaddr *)&remote_node_sin, &length);
    INVARIANT(accept_fd != -1, "Unable to accept incoming connection.");

    //::close(listen_fd);
    return accept_fd;
}

int createConnection(const string &hostname, uint16_t port) {
    struct hostent *he = gethostbyname(hostname.c_str());
    struct in_addr **address_list =(struct in_addr **)he->h_addr_list;
    char buf[INET_ADDRSTRLEN];
    const char *result = inet_ntop(AF_INET, address_list[0], buf, INET_ADDRSTRLEN);
    SINVARIANT(result != NULL);

    struct sockaddr_in node_sin;
    memset(&node_sin, 0, sizeof(node_sin));
    node_sin.sin_family = AF_INET;
    memcpy(&(node_sin.sin_addr.s_addr), he->h_addr, he->h_length);
    node_sin.sin_port = htons(port); // We are assuming that all nodes listen on the same port.

    int node_fd = ::socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    INVARIANT(node_fd != -1, "Unable to create a client socket.");

    // Keep trying to connect until we succeed. There are smarter ways, but this is good enough.
    while (::connect(node_fd, (struct sockaddr*)&node_sin, sizeof(node_sin)) == -1);

    return node_fd;
}

int main(int argc, char *argv[]) {
    uint32_t experiment_count = atol(argv[1]);
    uint32_t node_index = atol(argv[2]);
    uint32_t chunk_in_mb = 1000;
    if (node_index >= experiment_count) {
        cerr << "Not participating in this experiment." << endl;
        return 0;
    }

    vector<uint16_t> ports;
    vector<string> nodes;
    vector<int> fds;
    vector<Pump*> pumps;

    nodes.push_back("10.10.10.10");
    nodes.push_back("10.10.10.11");
    nodes.push_back("10.10.10.13");
    nodes.push_back("10.10.10.14");
    nodes.push_back("10.10.10.15");
    nodes.push_back("10.10.10.16");
    nodes.push_back("10.10.10.17");
    nodes.push_back("10.10.10.18");
    nodes.push_back("10.10.10.19");
    nodes.push_back("10.10.10.20");

    for (uint32_t i = 0; i < nodes.size(); ++i) {
        ports.push_back(13131 + i);
    }

    fds.resize(nodes.size());

    int listen_fd = createListener(13130);

    // Accept connections from higher node indices.
    for (int32_t i = node_index - 1; i >= 0; --i) {
        // Listen on ports[i]. Accept single connection and assign socket to fds[i].
        fds[i] = acceptConnection(listen_fd); // ports[i]
    }

    // Initiate connections to lower node indices.
    for (uint32_t i = node_index + 1; i < experiment_count; ++i) {
        // Connect to nodes[i] on ports[node_index]. Assign socket to fds[i].
        fds[i] = createConnection(nodes[i], 13130); // ports[node_index]
    }

    for (uint32_t i = 0; i < experiment_count; ++i) {
        if (i == node_index) {
            pumps.push_back(NULL);
            continue;
        }
        Pump *pump = new Pump(node_index, i, fds[i], chunk_in_mb);
        SINVARIANT(pump != NULL);
        pumps.push_back(pump);
        pump->start();
        cout << "Pump created." << endl;
    }

    for (uint32_t i = 0; i < experiment_count; ++i) {
        if (i == node_index) continue;
        pumps[i]->join();
    }

    return 0;}
