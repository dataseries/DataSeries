#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <pthread.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/format.hpp>

#include <Lintel/ProgramOptions.hpp>
#include <Lintel/PThread.hpp>

using lintel::ProgramOption;
using namespace std;

#define BUF_SIZE 65536
#define MAX_READS 1000000  //Must be (sometimes a lot) greater than TOT_SIZE / BUF_SIZE
#define NETBAR "/home/krevate/projects/DataSeries/experiments/neta2a/net_call_bar pds-10"
#define NETBARSERVERS "/home/krevate/projects/DataSeries/experiments/neta2a/net_call_bar pds-11"
#define PORT_BASE 6000

typedef boost::shared_ptr<PThread> PThreadPtr;

ProgramOption<int> po_nodeIndex("node-index", "Node index in the node list", -1);
ProgramOption<long> po_dataAmount("data-amount", "Total amount of data to read from all hosts", 4000000000);
ProgramOption<string> po_nodeNames("node-names", "List of nodes");
static const string LOG_DIR("/home/krevate/projects/DataSeries/experiments/neta2a/logs/");
struct timeval *startTime, *jobEndTime;
vector<string> nodeNames;
int nodeIndex;

long tval2long(struct timeval *tval) {
    return ((tval->tv_sec*1000000) + tval->tv_usec);
}

long tval2longdiff(struct timeval *tvalstart, struct timeval *tvalend) {
    return (((tvalend->tv_sec-tvalstart->tv_sec)*1000000) + (tvalend->tv_usec-tvalstart->tv_usec));
}

class ReadThread : public PThread {
public:
    ReadThread(long dataAmount, int sockid, FILE *outlog)
	: dataAmount(dataAmount), sockid(sockid), outlog(outlog) {
	readDoneTime = (struct timeval*) malloc(sizeof(struct timeval));
	localEndTime = (struct timeval*) malloc(sizeof(struct timeval));	
    }

    virtual ~ReadThread() {
	free(readDoneTime);
	free(localEndTime);
    }

    virtual void *run() {
	long *retSizePtr;
	retSizePtr = (long *)malloc(sizeof(long));
	*retSizePtr = 42; //runReader();
	printReadTimes();
	fprintf(outlog, "Local work finished: %ld us\n", tval2longdiff(startTime, localEndTime) );
	return retSizePtr;
    }

    void printReadTimes() {
	int i;
	long totReadTime = 0;
	long totReadAmnt = 0;
	fprintf(outlog, "\nReadTime ReadAmnt\n");
	for (i = 0; i < totReads; i++) {
	    fprintf(outlog, "%ld %ld\n", readTimes[i], readAmounts[i]);
	    totReadAmnt += readAmounts[i];
	    totReadTime += readTimes[i];
	}
	fprintf(outlog, "Total number of reads: %ld\n", totReads);
	fprintf(outlog, "Total amount read: %ld\n", totReadAmnt);
	fprintf(outlog, "Total time in reads: %ld us\n", totReadTime);
    }
        
    long runReader() {
	int ret;
	long tot = 0;
	long thisReadTime = 0;
	long lastReadTime = 0;
	totReads = 0;

	for (ret = 0, totReads = 0, lastReadTime = tval2long(startTime); tot < dataAmount; ++totReads, tot += ret) {
	    // Note: always reading buf_size is ok because we control exact amount sent
	    ret = recv(sockid, buf, BUF_SIZE, 0);
	    gettimeofday(readDoneTime, NULL);
	    readAmounts[totReads] = ret;
	    thisReadTime = tval2long(readDoneTime);
	    readTimes[totReads] = thisReadTime - lastReadTime;
	    lastReadTime = thisReadTime;
	    if (ret == 0) {
		break;
	    }
	}
	
	++totReads;
	gettimeofday(localEndTime, NULL);
	
	return tot;
    }

    struct timeval *readDoneTime, *localEndTime;
    char buf[BUF_SIZE];
    long readTimes[MAX_READS]; //time between successful reads
    long readAmounts[MAX_READS]; //amount of data actually read each time
    long totReads; //total reads taken to read all data
    long dataAmount; //amount of data to read from socket
    int sockid; //id of socket to read from
    FILE *outlog; //output file id
};

class SetupAndTransferThread : public PThread {

public:

    SetupAndTransferThread(long dataAmount, unsigned short int serverPort, int isServer, int otherNodeIndex)
	: dataAmount(dataAmount), serverPort(serverPort), isServer(isServer), otherNodeIndex(otherNodeIndex) {}

    ~SetupAndTransferThread() {}

    virtual void *run() {
	long *retSizePtr = 0;
	int listenSocket, connectSocket;
	PThreadPtr readThread;
	FILE *outlog;
	socklen_t clientAddressLength;
	struct sockaddr_in clientAddress, serverAddress;
	struct hostent *hostInfo, *tempHostInfo;
	int ret;
	int totWrites;
	int totLeft;

	bzero(buf, BUF_SIZE);

	// Open output file for log
	string outlogfile = (boost::format("%s%d.%d.%s.test") % LOG_DIR %
			     (isServer == 0 ? otherNodeIndex : nodeIndex) % 
			     (isServer == 0 ? nodeIndex : otherNodeIndex) % 
			     (isServer == 0 ? "c" : "s")).str();
	cout << outlogfile;
	printf("%d: isServer? %d otherNodeIndex: %d\n", nodeIndex, isServer, otherNodeIndex);
	outlog = fopen(outlogfile.c_str(), "w+");
	if (outlog == NULL) {
	    fprintf(stderr, "ERROR opening outlog");
	}

	// Connect or listen as per the args, getting a sockid
	if (isServer == 0) {
	    // This node is the client, so connect to the right server
	    hostInfo = new struct hostent();
	    tempHostInfo = gethostbyname(nodeNames[otherNodeIndex].c_str());
	    if (tempHostInfo == NULL) {
		cout << "SERVER[i]: " << nodeNames[otherNodeIndex] << endl;
		cout << "ERROR: " << h_errno << endl;
		exit(1);
	    } else {
		memcpy(hostInfo, tempHostInfo, sizeof(struct hostent));
		bcopy(tempHostInfo->h_addr, (struct in_addr *)&(serverAddress.sin_addr), tempHostInfo->h_length);
		
		cout << "Host: " << nodeNames[otherNodeIndex] << endl;
		if (hostInfo == NULL) {
		    cout << "problem interpreting host: " << nodeNames[otherNodeIndex] << "\n";
		    exit(1);
		}
	    }
	    
	    connectSocket = socket(AF_INET, SOCK_STREAM, 0);

	    if (connectSocket < 0) {
		cerr << "cannot create socket\n";
		exit(1);
	    }

	    // set socket buffer sizes
	    // increasing buffer sizes for high bandwidth low latency link
	    int sndsize = 512000;
	    setsockopt(connectSocket, SOL_SOCKET, SO_RCVBUF, (char*)&sndsize, (int)sizeof(sndsize));
	    setsockopt(connectSocket, SOL_SOCKET, SO_SNDBUF, (char*)&sndsize, (int)sizeof(sndsize));
  
	    int sockbufsize = 0;
	    socklen_t sizeb = sizeof(int);
	    int err = getsockopt(connectSocket, SOL_SOCKET, SO_RCVBUF, (char*)&sockbufsize, &sizeb);
	    printf("Recv socket buffer size: %d\n", sockbufsize);
	    
	    err = getsockopt(connectSocket, SOL_SOCKET, SO_SNDBUF, (char*)&sockbufsize, &sizeb);
	    printf("Send socket buffer size: %d\n", sockbufsize);
  
	    // TCP NO DELAY
	    int flag = 1;
	    int result = setsockopt(connectSocket,
				    IPPROTO_TCP,
				    TCP_NODELAY,
				    (char *) &flag,
				    sizeof(int));

	    if (result < 0) {
		perror("Could not set TCP_NODELAY sock opt\n");
	    }

	    serverAddress.sin_family = hostInfo->h_addrtype;
	    serverAddress.sin_port = htons(serverPort);
	    
	    if (connect(connectSocket, (struct sockaddr *) &serverAddress,
			sizeof(serverAddress)) < 0) {
		cerr << "cannot connect\n";
		exit(1);
	    }	   
 
	} else {
	    // Setup the server and listen for connections
	    //Create socket for listening for client connection requests
	    listenSocket = socket(AF_INET, SOCK_STREAM, 0);
	    if (listenSocket < 0) {
		cerr << "cannot create listen socket";
		exit(1);
	    }
	    
	    // Bind listen socket to listen port.  
	    serverAddress.sin_family = AF_INET;
	    serverAddress.sin_addr.s_addr = htonl(INADDR_ANY);
	    serverAddress.sin_port = htons(serverPort);


	    // set socket buffer sizes
	    // increasing buffer sizes for high bandwidth low latency link
	    int sndsize = 512000;
	    setsockopt(listenSocket, SOL_SOCKET, SO_RCVBUF, (char*)&sndsize, (int)sizeof(sndsize));
	    setsockopt(listenSocket, SOL_SOCKET, SO_SNDBUF, (char*)&sndsize, (int)sizeof(sndsize));
  
	    int sockbufsize = 0;
	    socklen_t sizeb = sizeof(int);
	    int err = getsockopt(listenSocket, SOL_SOCKET, SO_RCVBUF, (char*)&sockbufsize, &sizeb);
	    printf("Recv socket buffer size: %d\n", sockbufsize);
	    
	    err = getsockopt(listenSocket, SOL_SOCKET, SO_SNDBUF, (char*)&sockbufsize, &sizeb);
	    printf("Send socket buffer size: %d\n", sockbufsize);
  
	    // TCP NO DELAY
	    int flag = 1;
	    int result = setsockopt(listenSocket,
				    IPPROTO_TCP,
				    TCP_NODELAY,
				    (char *) &flag,
				    sizeof(int));

	    if (result < 0) {
		perror("Could not set TCP_NODELAY sock opt\n");
	    }
	    
	    // Bind socket and listen for connections
	    if (bind(listenSocket,
		     (struct sockaddr *) &serverAddress,
		     sizeof(serverAddress)) < 0) {
		cerr << "cannot bind socket";
		exit(1);
	    }
	    
	    // Wait for connections from clients.
	    listen(listenSocket, 5);
	
	    cout << "Waiting for TCP connection on port " << serverPort << " ...\n";
    
	    // Accept a connection with a client that is requesting one.
	    clientAddressLength = sizeof(clientAddress);
	    connectSocket = accept(listenSocket,
				   (struct sockaddr *) &clientAddress,
				   &clientAddressLength);
	    if (connectSocket < 0) {
		cerr << "cannot accept connection ";
		exit(1);
	    }
	    
	    // Show the IP address of the client.
	    cout << "  connected to " << inet_ntoa(clientAddress.sin_addr);

	    // Show the client's port number.
	    cout << ":" << ntohs(clientAddress.sin_port) << "\n";
	}
	
	// Start up the data reader over this socket
	readThread.reset(new ReadThread(dataAmount, connectSocket, outlog));
	readThread->start();
	
	// Generate and send data over the connection
	for (ret = 0, totWrites = 0, totLeft = dataAmount; totLeft > 0; ++totWrites, totLeft -= ret) {
	    if ((ret = send(connectSocket, buf, (totLeft > BUF_SIZE ? BUF_SIZE : totLeft), 0)) < 0) {
		cerr << "Error: cannot send data";
	    }
	}
	++totWrites;
	fprintf(outlog, "Total number of writes: %d\n", totWrites);
	
	// Join on read thread
	retSizePtr = (long *)readThread->join();    

	// Cleanup and return
	close(connectSocket);
	fclose(outlog);
	return retSizePtr;
    }

    long dataAmount;
    unsigned short int serverPort;
    int isServer;
    int otherNodeIndex;
    char buf[BUF_SIZE];
};

int main(int argc, char **argv)
{
    lintel::parseCommandLine(argc,argv);
    long dataAmount = 0;
    long dataAmountPerNode = 0;
    string tmp;
    int numNodes = 0;
    int isServer = 0;
    long *retSizePtr = 0;
    long totReturned = 0;
    PThreadPtr *setupAndTransferThreads; //will hold array of pthread pointers, one for each nodeindex

    startTime = (struct timeval*) malloc(sizeof(struct timeval));
    jobEndTime = (struct timeval*) malloc(sizeof(struct timeval));        
    dataAmount = po_dataAmount.get();
    nodeIndex = po_nodeIndex.get();
    tmp = po_nodeNames.get();
    boost::split(nodeNames, tmp, boost::is_any_of(","));
    numNodes = nodeNames.size();
    dataAmountPerNode = dataAmount / numNodes;
    SINVARIANT(numNodes >= 0);
    setupAndTransferThreads = (PThreadPtr *) malloc(numNodes*sizeof(PThreadPtr));
    printf("\ngenread called with %d nodes and %ld Bytes\n",numNodes,dataAmount);

    system(NETBARSERVERS);
    gettimeofday(startTime, NULL);

    // Specify which connections apply to this node, as a server or client
    for (int i = (numNodes - 1); i >= 0; --i) {
	if (i == nodeIndex) {
	    // We finished setting up servers, wait for all other servers to go up
	    printf("\n%d: finished setting up servers\n", nodeIndex);
	    system(NETBARSERVERS);
	} else {
	    // Determine if this node acts as server or client
	    if (i > nodeIndex) {
		isServer = 1;
	    } else {
		isServer = 0;
	    }
	    setupAndTransferThreads[i].reset(new SetupAndTransferThread(dataAmountPerNode, PORT_BASE+i, isServer, i));
	    setupAndTransferThreads[i]->start();
	}
    }

    // Join all threads
    for (int i = 0; i < numNodes; i++) {
	if (i == nodeIndex) {
	    ; // Do nothing
	} else {
	    retSizePtr = (long *)setupAndTransferThreads[i]->join();
	    printf("\nData received from nodeindex %d: %ld\n", i, *retSizePtr);
	    totReturned += *retSizePtr;
	    free(retSizePtr);
	}
    }

    printf("\nTotal received: %ld\n", totReturned);
    system(NETBARSERVERS);
    
    gettimeofday(jobEndTime, NULL);    
    printf("Full job finished:   %ld us\n", tval2longdiff(startTime, jobEndTime) );

    free(startTime);
    free(jobEndTime);
}
