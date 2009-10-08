#ifndef DATASERIES_NETWORKCLIQUE_HPP
#define DATASERIES_NETWORKCLIQUE_HPP

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>

#include <deque>
#include <string>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/shared_ptr.hpp>

#include <Lintel/LintelLog.hpp>
#include <Lintel/PThread.hpp>

#include <DataSeries/ExtentReader.hpp>
#include <DataSeries/ExtentWriter.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/ThreadSafeBuffer.hpp>

template <typename F, typename P>
class NetworkClique : public DataSeriesModule {
public:
    NetworkClique(DataSeriesModule *upstream_module,
                  const std::vector<std::string> &nodes,
                  uint32_t node_index,
                  const std::string &field_name,
                  const P &record_partitioner,
                  uint32_t extent_limit = 1 << 20,
                  uint16_t port = 13131);
    virtual ~NetworkClique();

    void start(); // Establish all-to-all connectivity and start reading from upstream module.
    void join(); // Or let the destructor call this automatically.
    virtual Extent* getExtent();

private:
    class Node;
    typedef boost::shared_ptr<Node> NodePtr;
    typedef std::vector<NodePtr> NodeVector;
    typedef boost::shared_ptr<ExtentReader> ExtentReaderPtr;
    typedef boost::shared_ptr<ExtentWriter> ExtentWriterPtr;
    typedef boost::shared_ptr<PThread> PThreadPtr;

    struct Node {
        std::string name;
        uint32_t index;
        int fd;
        PThreadPtr receive_thread;
        PThreadPtr send_thread;
        ExtentReaderPtr reader;
        ExtentWriterPtr writer;
        bool reading;
        bool writing;
        Extent *outgoing_extent;
        ThreadSafeBuffer<Extent*> outgoing_buffer;
    };


    class ListenThread : public PThread {
    public:
        ListenThread(NetworkClique *clique, uint32_t connection_count) :
                clique(clique), connection_count(connection_count) {}
        virtual ~ListenThread() {}

        virtual void* run() {
            clique->startListenThread(connection_count);
            return NULL;
        }

    private:
        NetworkClique *clique;
        uint32_t connection_count;
    };
    friend class ListenThread;

    class ReceiveThread : public PThread {
    public:
        ReceiveThread(NetworkClique *clique, uint32_t remote_node_index) :
                clique(clique), remote_node_index(remote_node_index) {}
        virtual ~ReceiveThread() {}

        virtual void* run() {
            clique->startReceiveThread(remote_node_index);
            return NULL;
        }

    private:
        NetworkClique *clique;
        uint32_t remote_node_index;
    };
    friend class ReceiveThread;

    class PartitionThread : public PThread {
    public:
        PartitionThread(NetworkClique *clique, Extent *first_extent) :
                clique(clique), first_extent(first_extent) {}
        virtual ~PartitionThread() {}

        virtual void* run() {
            clique->startPartitionThread(first_extent);
            return NULL;
        }

    private:
        NetworkClique *clique;
        Extent *first_extent;
    };
    friend class PartitionThread;

    class SendThread : public PThread {
    public:
        SendThread(NetworkClique *clique, uint32_t remote_node_index) :
            clique(clique), remote_node_index(remote_node_index) {}
        virtual ~SendThread() {}

        virtual void* run() {
            clique->startSendThread(remote_node_index);
            return NULL;
        }

    private:
        NetworkClique *clique;
        uint32_t remote_node_index;
    };
    friend class SendThread;

    void startListenThread(uint32_t connection_count); // x 1
    void startReceiveThread(uint32_t remote_node_index); // x n
    void startPartitionThread(Extent *first_extent); // x 1
    void startSendThread(uint32_t remote_node_index); // x n
    void connectToNode(const NodePtr &node);
    void sendExtentToNode(const NodePtr &node);
    void incrementOwnedExtents();
    void decrementOwnedExtents();

    DataSeriesModule *upstream_module;
    NodeVector nodes;
    uint32_t node_index;
    std::string field_name;
    P record_partitioner;
    uint32_t extent_limit;
    uint16_t port;

    PThreadMutex mutex;
    uint32_t count_active_readers;
    bool joined;

    ThreadSafeBuffer<Extent*> incoming_buffer;
    ThreadSafeBuffer<Extent*> direct_transfer_buffer;

    PThreadPtr partition_thread;
    uint32_t owned_extent_count, highest_owned_extent_count;
};

template <typename F, typename P>
NetworkClique<F, P>::NetworkClique(DataSeriesModule *upstream_module,
                                   const std::vector<std::string> &node_names,
                                   uint32_t node_index,
                                   const std::string &field_name,
                                   const P &record_partitioner,
                                   uint32_t extent_limit,
                                   uint16_t port) :
        upstream_module(upstream_module), node_index(node_index), field_name(field_name),
        record_partitioner(record_partitioner), extent_limit(extent_limit), port(port),
        count_active_readers(node_names.size()), joined(false),
        incoming_buffer(node_names.size() * 2), direct_transfer_buffer(node_names.size() * 2),
        owned_extent_count(0), highest_owned_extent_count(0) {
    BOOST_FOREACH(const std::string &name, node_names) {
        NodePtr node(new Node);
        node->name = name;
        node->index = nodes.size();
        node->fd = -1;
        node->reading = true;
        node->writing = true;
        node->outgoing_extent = NULL;
        nodes.push_back(node);
    }
    this->record_partitioner.initialize(nodes.size());
}

template <typename F, typename P>
NetworkClique<F, P>::~NetworkClique() {
    join();
}

template <typename F, typename P>
void NetworkClique<F, P>::incrementOwnedExtents() {
	++owned_extent_count; // No locking. We're not looking for 100% accuracy.
	if (owned_extent_count > highest_owned_extent_count) {
		highest_owned_extent_count = owned_extent_count;
		LintelLogDebug("NetworkClique", boost::format("Owned extents: %s") % owned_extent_count);
	}
}

template <typename F, typename P>
void NetworkClique<F, P>::decrementOwnedExtents() {
	--owned_extent_count; // No locking. We're not looking for 100% accuracy.
}

template <typename F, typename P>
Extent* NetworkClique<F, P>::getExtent() {
    Extent *extent = NULL;
    //LintelLogDebug("NetworkClique", "Waiting (or not) for a new extent.");
    bool done = !incoming_buffer.remove(&extent);
    if (done) {
        return NULL;
    }
    SINVARIANT(extent != NULL);
    //LintelLogDebug("NetworkClique", "Returned an extent.");
    decrementOwnedExtents();
    return extent;
}

template <typename F, typename P>
void NetworkClique<F, P>::connectToNode(const NodePtr &node) {
    // Resolve the server's name to an IP address.
    struct hostent *he = gethostbyname(node->name.c_str());
    struct in_addr **address_list =(struct in_addr **)he->h_addr_list;
    char buf[INET_ADDRSTRLEN];
    const char *result = inet_ntop(AF_INET, address_list[0], buf, INET_ADDRSTRLEN);
    SINVARIANT(result != NULL);
    LintelLogDebug("NetworkClique",
                   boost::format("Resolved hostname '%s' to '%s'/%s.") %
                   node->name % he->h_name % buf);

    struct sockaddr_in node_sin;
    memset(&node_sin, 0, sizeof(node_sin));
    node_sin.sin_family = AF_INET;
    memcpy(&(node_sin.sin_addr.s_addr), he->h_addr, he->h_length);
    node_sin.sin_port = htons(port); // We are assuming that all nodes listen on the same port.

    int node_fd = ::socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    INVARIANT(node_fd != -1, "Unable to create a client socket.");

    // Keep trying to connect until we succeed. There are smarter ways, but this is good enough.
    while (::connect(node_fd, (struct sockaddr*)&node_sin, sizeof(node_sin)) == -1);
    uint32_t bytes_sent = ::send(node_fd, &node_index, sizeof(node_index), 0);
    INVARIANT(bytes_sent == sizeof(node_index), "Unable to send node index on socket.");

    node->fd = node_fd;
}

template <typename F, typename P>
void NetworkClique<F, P>::join() {
    if (!joined) {
        partition_thread->join();
        BOOST_FOREACH(const NodePtr &node, nodes) {
            node->receive_thread->join();
            node->send_thread->join();
        }
        joined = true;
    }
}

template <typename F, typename P>
void NetworkClique<F, P>::start() {
    // Create a thread that will listen on listener_fd and accept incoming connections.
    ListenThread listen_thread(this, node_index);
    listen_thread.start();

    // Connect to all the nodes that have a higher index and tell them the node's index (4 bytes).
    for (uint32_t i = node_index + 1; i < nodes.size(); ++i) {
        connectToNode(nodes[i]);
    }

    // Wait for the listen thread to finish.
    listen_thread.join();
    LintelLogDebug("NetworkClique", "Connectivity established. Terminated listen thread.");

    Extent *first_extent = upstream_module->getExtent();
    incrementOwnedExtents();
    INVARIANT(first_extent != NULL, "We need at least one extent in the local file. (This can be fixed.)");
    const ExtentType &extent_type = first_extent->getType();

    // Start the single partition thread.
    partition_thread.reset(new PartitionThread(this, first_extent));
    partition_thread->start();

    // Start the send/receive threads for each node.
    for (uint32_t i = 0; i < nodes.size(); ++i) {
        const NodePtr &node = nodes[i];
        if (i != node_index) {
            node->reader.reset(new ExtentReader(node->fd, extent_type));
            node->writer.reset(new ExtentWriter(node->fd, false, true));
        }
        node->receive_thread.reset(new ReceiveThread(this, i));
        node->receive_thread->start();
        node->send_thread.reset(new SendThread(this, i));
        node->send_thread->start();
    }
}

template <typename F, typename P>
void NetworkClique<F, P>::startSendThread(uint32_t remote_node_index) {
    LintelLogDebug("NetworkClique", "startSendThread() was called.");
    Extent *outgoing_extent = NULL;
    NodePtr &node = nodes[remote_node_index];
    while (node->outgoing_buffer.remove(&outgoing_extent)) {
        //LintelLogDebug("NetworkClique", boost::format("SendThread: Waiting (or not) to write an extent (node %s).") % remote_node_index);
        if (remote_node_index == node_index) {
            // Sending to ourselves. Transfer directly to incoming buffer.
            direct_transfer_buffer.add(outgoing_extent);
        } else {
            node->writer->writeExtent(outgoing_extent);
            delete outgoing_extent;
            decrementOwnedExtents();
        }
        //LintelLogDebug("NetworkClique", boost::format("SendThread: Wrote an extent (node %s).") % remote_node_index);
    }


    PThreadScopedLock lock(mutex);
    node->writing = false;
    if (!node->reading && node->index != node_index) {
        node->reader->close(); // Only the reader or the writer should be closed, in order to avoid double-close.
        LintelLogDebug("NetworkClique", boost::format("SendThread [%s]: Closed socket. (R: %s B, %s s, %s MB/s; W: %s B, %s s, %s MB/s)") %
                remote_node_index %
                node->reader->getTotalSize() % node->reader->getTotalTime() % node->reader->getThroughput() %
                node->writer->getTotalSize() % node->writer->getTotalTime() % node->writer->getThroughput());
    }
    LintelLogDebug("NetworkClique", boost::format("SendThread [%s]: Terminated.") % remote_node_index);
}

template <typename F, typename P>
void NetworkClique<F, P>::startPartitionThread(Extent *first_extent) {
    LintelLogDebug("NetworkClique", "startPartitionThread() was called.");

    if (nodes.size() > 1) {
        BOOST_FOREACH(const NodePtr &node, nodes) {
            node->outgoing_extent = new Extent(first_extent->getType());
            incrementOwnedExtents();
        }

        Extent *source_extent = first_extent;
        first_extent = NULL;

        ExtentSeries source_series, destination_series;
        F field(source_series, field_name);
        ExtentRecordCopy copier(source_series, destination_series);

        do {
            //LintelLogDebug("NetworkClique", "PartitionThread: Partitioning an extent's records.");
            // Partition the extent's records and send full extents to the other side.
            for (source_series.start(source_extent); source_series.more(); source_series.next()) {
                uint32_t partition = record_partitioner.getPartition(field);
                NodePtr &node = nodes[partition];
                destination_series.setExtent(node.get()->outgoing_extent);
                destination_series.newRecord(false);
                copier.copyRecord();

                // We have filled up an entire extent for this partition, so send it.
                if (node->outgoing_extent->size() > extent_limit) {
                    //LintelLogDebug("NetworkClique", "PartitionThread: Waiting (or not) to add an outgoing extent.");
                    const ExtentType &extent_type = node->outgoing_extent->getType();
                    node->outgoing_buffer.add(node->outgoing_extent); // Will delete node->outgoing_extent!!
                    //LintelLogDebug("NetworkClique", boost::format("PartitionThread: Added an outgoing extent for node %s.") % partition);
                    node->outgoing_extent = new Extent(extent_type);
                    incrementOwnedExtents();
                    node->outgoing_extent->fixeddata.reserve(extent_limit);
                }
            }

            delete source_extent;
            decrementOwnedExtents();
            source_extent = upstream_module->getExtent();
            incrementOwnedExtents();
        } while (source_extent != NULL);

        BOOST_FOREACH(const NodePtr &node, nodes) {
            node->outgoing_buffer.add(node->outgoing_extent);
            node->outgoing_extent = NULL;
            node->outgoing_buffer.add(NULL); // Send an empty extent to indicate that we're done.
            node->outgoing_buffer.signalDone();
        }
    } else {
        // Special handling for single-node case. Not entirely correct because we're not obeying extent_limit.
        // But we do get to avoid copying, partitioning, etc.
        Extent *source_extent = first_extent;
        do {
            nodes[0]->outgoing_buffer.add(source_extent);
            source_extent = upstream_module->getExtent();
            incrementOwnedExtents();
        } while (source_extent != NULL);
        nodes[0]->outgoing_buffer.add(NULL);
        nodes[0]->outgoing_buffer.signalDone();
    }
}

template <typename F, typename P>
void NetworkClique<F, P>::startReceiveThread(uint32_t remote_node_index) {
    LintelLogDebug("NetworkClique", "startReceiveThread() was called.");
    const NodePtr &node = nodes[remote_node_index];
    while (true) {
        //LintelLogDebug("NetworkClique", boost::format("ReceiveThread [%s]: Waiting (or not) to read an extent.") % remote_node_index);
        Extent *extent = NULL;
        if (remote_node_index == node_index) {
            direct_transfer_buffer.remove(&extent);
        } else {
            extent = node->reader->getExtent();
            incrementOwnedExtents();
        }
        //LintelLogDebug("NetworkClique", boost::format("ReceiveThread [%s]: Read an extent.") % remote_node_index);
        bool last_thread = false;

        if (extent == NULL) {
            LintelLogDebug("NetworkClique", boost::format("ReceiveThread [%s]: Received last extent.") % remote_node_index);
            mutex.lock();
            --count_active_readers; // We're doing this under the lock.
            if (count_active_readers == 0) {
                last_thread = true;
            }
            node->reading = false;
            if (!node->writing && remote_node_index != node_index) {
                node->reader->close(); // Only the reader or the writer should be closed, in order to avoid double-close.
                LintelLogDebug("NetworkClique", boost::format("ReceiveThread [%s]: Closed socket. (R: %s B, %s s, %s MB/s; W: %s B, %s s, %s MB/s)") %
                        remote_node_index %
                        node->reader->getTotalSize() % node->reader->getTotalTime() % node->reader->getThroughput() %
                        node->writer->getTotalSize() % node->writer->getTotalTime() % node->writer->getThroughput());
            }
            mutex.unlock();
            if (last_thread) {
                incoming_buffer.signalDone();
            }
            return;
        }

        //LintelLogDebug("NetworkClique", boost::format("ReceiveThread [%s]: Waiting (or not) to add an incoming extent.") % remote_node_index);
        incoming_buffer.add(extent);
        //LintelLogDebug("NetworkClique", boost::format("ReceiveThread [%s]: Added an incoming extent.") % remote_node_index);
    }
    LintelLogDebug("NetworkClique", boost::format("ReceiveThread [%s]: Terminated.") % remote_node_index);
}

template <typename F, typename P>
void NetworkClique<F, P>::startListenThread(uint32_t connection_count) {
    LintelLogDebug("NetworkClique", "startListenThread() was called.");
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

    result = ::listen(listen_fd, connection_count);
    INVARIANT(result != -1, "Unable to listen on the server socket.");

    LintelLogDebug("NetworkClique", boost::format("Waiting for %s connections.") % connection_count);

    // Synchronously wait for the incoming TCP connections from all the nodes (including self).
    while (connection_count > 0) {
        struct sockaddr_in remote_node_sin;
        socklen_t length = sizeof(remote_node_sin);
        int remote_node_fd = ::accept(listen_fd, (struct sockaddr *)&remote_node_sin, &length);
        INVARIANT(remote_node_fd != -1, "Unable to accept incoming connection.");

        uint32_t remote_node_index = 0;
        ssize_t bytes_received = ::recv(remote_node_fd, (void*)&remote_node_index, sizeof(remote_node_index), 0);
        INVARIANT(bytes_received == sizeof(remote_node_index), "Unable to read remote node index on socket.");
        LintelLogDebug("NetworkClique", boost::format("Received a connection from node %s.") % remote_node_index);

        nodes[remote_node_index]->fd = remote_node_fd;

        --connection_count;
    }

    LintelLogDebug("NetworkClique", "Closing listen socket. It's no longer needed.");
    ::close(listen_fd);
}

#endif
