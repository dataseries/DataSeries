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

#define max(A,B) ( (A) > (B) ? (A):(B))
#define min(A,B) ( (A) > (B) ? (B):(A))

#define BUF_SIZE 65536
#define MAX_BUFSIZE (65536*20)  //Keep at most 20 full bufs outstanding
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
int numNodes;
PThreadCond sendCond;
PThreadMutex sendMutex;
long *bytesReadyPerNode;
long bytesPartitionedPerNode; //Same for every node

long tval2long(struct timeval *tval) {
    return ((tval->tv_sec*1000000) + tval->tv_usec);
}

long tval2longdiff(struct timeval *tvalstart, struct timeval *tvalend) {
    return (((tvalend->tv_sec-tvalstart->tv_sec)*1000000) + (tvalend->tv_usec-tvalstart->tv_usec));
}

// Optimize later
long findMax(long *array, int size) {
    long curMax = 0;
    for (int i = 0; i < size; i++) {
	if (i == nodeIndex) {
	    continue;
	}
	if (array[i] > curMax) {
	    curMax = array[i];
	}
	if (curMax == MAX_BUFSIZE) {
	    break;
	}
    }
    return curMax;
}

void printArray(long *array, int size) {
    for (int i = 0; i < size; i++) {
	if (i == nodeIndex) {
	    printf("[me]");
	    continue;
	}
	printf("[%ld]",array[i]);
    }
    printf("\n");
}

void incrementAll(long *array, int size, long amount) {
    for (int i = 0; i < size; i++) {
	if (i == nodeIndex) {
	    continue;
	}
	array[i] += amount;
    }
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
	*retSizePtr = runReader();
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
	    ret = read(sockid, buf, BUF_SIZE);
	    gettimeofday(readDoneTime, NULL);
	    readAmounts[totReads] = ret;
	    thisReadTime = tval2long(readDoneTime);
	    readTimes[totReads] = thisReadTime - lastReadTime;
	    //printf("read %d in time %ld\n", ret, readTimes[totReads]);
	    //fflush(stdout);
	    lastReadTime = thisReadTime;
	    //if (ret == 0) {
	    //break;
	    //}
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
	: dataAmount(dataAmount), serverPort(serverPort), isServer(isServer), otherNodeIndex(otherNodeIndex) {
	printf("\nSetupAndTransferThread called for %ld B, serverPort %d, isServer %d, otherNodeIndex %d\n", dataAmount, serverPort, isServer, otherNodeIndex);
	
	// Default as if we are client connecting to server, 
	// but switch this out later if another node connects to us
	connectIndex = otherNodeIndex; 

	if (isServer == 1) {
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
	    //printf("Recv socket buffer size: %d\n", sockbufsize);
	    
	    err = getsockopt(listenSocket, SOL_SOCKET, SO_SNDBUF, (char*)&sockbufsize, &sizeb);
	    //printf("Send socket buffer size: %d\n", sockbufsize);
  
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
	    
	    flag = 1;
	    if (setsockopt(listenSocket, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag)) < 0) {
		perror("setsockopt(SO_REUSEADDR) failed");
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
	}
    }

    ~SetupAndTransferThread() {}

    virtual void *run() {
	long *retSizePtr = 0;
	FILE *outlog;
	socklen_t clientAddressLength;
	struct hostent *hostInfo;
	int ret;
	long totWrites;
	long totLeft;
	long currentMax;
	long diffMax;
	long amountAllowed;

	bzero(buf, BUF_SIZE);

	// Open output file for log
	string outlogfile = (boost::format("%s%d.%d.%s") % LOG_DIR %
			     (isServer == 0 ? otherNodeIndex : nodeIndex) % 
			     (isServer == 0 ? nodeIndex : otherNodeIndex) % 
			     (isServer == 0 ? "c" : "s")).str();
	cout << outlogfile;
	//printf("\n%d: isServer? %d otherNodeIndex: %d\n", nodeIndex, isServer, otherNodeIndex);
	outlog = fopen(outlogfile.c_str(), "w+");
	if (outlog == NULL) {
	    fprintf(stderr, "ERROR opening outlog");
	}

	// Connect or listen as per the args, getting a sockid
	if (isServer == 0) {
	    // This node is the client, so connect to the right server
	    hostInfo = new struct hostent();
	    hostInfo = gethostbyname(nodeNames[otherNodeIndex].c_str());
	    printf("\ngethostbyname: %s\n", nodeNames[otherNodeIndex].c_str());
	    if (hostInfo == NULL) {
		cout << "SERVER[i]: " << nodeNames[otherNodeIndex] << endl;
		cout << "ERROR: " << h_errno << endl;
		exit(1);
	    } else {
		bzero((char *) &serverAddress, sizeof(serverAddress));
		serverAddress.sin_family = AF_INET;
		bcopy((char *)hostInfo->h_addr, (char *)&serverAddress.sin_addr.s_addr, hostInfo->h_length);
		//(struct in_addr *)&(serverAddress.sin_addr), tempHostInfo->h_length);
		
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
	    //printf("Recv socket buffer size: %d\n", sockbufsize);
	    
	    err = getsockopt(connectSocket, SOL_SOCKET, SO_SNDBUF, (char*)&sockbufsize, &sizeb);
	    //printf("Send socket buffer size: %d\n", sockbufsize);
  
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

	    cout << " ...... " << inet_ntoa(*((struct in_addr*)hostInfo->h_addr)) << endl;

	    serverAddress.sin_family = hostInfo->h_addrtype;
	    serverAddress.sin_port = htons(serverPort);
	    
	    if (connect(connectSocket, (struct sockaddr *) &serverAddress,
			sizeof(serverAddress)) < 0) {
		cerr << "cannot connect\n";
		exit(1);
	    } else {
		cout << "connected\n";
	    }
 
	} else {
	    // Accept a connection with a client that is requesting one.
	    // (server already called listen in constructor)
	    // connectindex warning: % numNodes is safer, but we create contiguous port nums so this will work
	    connectIndex = serverPort % PORT_BASE; 
	    clientAddressLength = sizeof(clientAddress);
	    connectSocket = accept(listenSocket,
				   (struct sockaddr *) &clientAddress,
				   &clientAddressLength);
	    if (connectSocket < 0) {
		cerr << "cannot accept connection\n";
		exit(1);
	    } else {
		cerr << "accepted connection\n";
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
	currentMax = 0;
	for (totWrites = 0, totLeft = dataAmount; totLeft > 0; totLeft -= ret) {
	    ret = 0;
	    //printf("%d: top bytesReady: %ld, totLeft: %ld\n",connectIndex,bytesReadyPerNode[connectIndex],totLeft);
	    //fflush(stdout);
	    sendMutex.lock();
	    //printf("%d: mutex locked\n",connectIndex);
	    if (bytesReadyPerNode[connectIndex] < BUF_SIZE && bytesReadyPerNode[connectIndex] < totLeft) {
		currentMax = findMax(bytesReadyPerNode, numNodes);
		//printArray(bytesReadyPerNode, numNodes);
		// assumes dataAmount is the same for each node, as it is currently
		diffMax = min((MAX_BUFSIZE - currentMax),(dataAmount - bytesPartitionedPerNode));
		//printf("%d: bytesPartitionedPerNode: %ld, currentMax: %ld, diffMax: %ld\n",connectIndex,bytesPartitionedPerNode,currentMax,diffMax);
		if (diffMax > 0) {
		    incrementAll(bytesReadyPerNode, numNodes, diffMax);
		    bytesPartitionedPerNode += diffMax;
		    sendCond.broadcast();
		} else if (bytesReadyPerNode[connectIndex] == 0) {
		    //printf("%d: can't send, waiting on cond\n", connectIndex);
		    //fflush(stdout);
		    sendCond.wait(sendMutex);
		    //printf("%d: wakeup from cond\n", connectIndex);
		}
	    }
	    amountAllowed = bytesReadyPerNode[connectIndex] > BUF_SIZE ? BUF_SIZE : bytesReadyPerNode[connectIndex];
	    //printf("%d: amountAllowed to send: %ld\n", connectIndex, amountAllowed);
	    //printf("%d: mutex unlocked\n", connectIndex);
	    sendMutex.unlock();

	    //printf("%d: write bytesReady: %ld\n",connectIndex,bytesReadyPerNode[connectIndex]);
	    if (amountAllowed > 0) {
		if ((ret = write(connectSocket, buf, amountAllowed)) < 0) {
		    cerr << "Error: cannot send data  ";
		}
		if (ret > 0) {
		    ++totWrites;
		    //fflush(stdout);
		    sendMutex.lock();
		    //printf("%d: sendMutex locked after write\n",connectIndex);
		    bytesReadyPerNode[connectIndex] -= ret;
		    //optimize later to avoid unnecessary broadcasts
		    //but right now this is necessary to avoid corner blocking case
		    sendCond.broadcast(); 
		    //printf("%d: sendMutex unlocked after broadcast and write\n",connectIndex);
		    sendMutex.unlock();
		}
	    }
	}
	
	// Join on read thread
	//printf("\nAfter %ld writes to %d, waiting on join for read\n", totWrites, otherNodeIndex);
	//fflush(stdout);
	retSizePtr = (long *)readThread->join();    

	fprintf(outlog, "Total number of writes: %ld\n", totWrites);

	// Cleanup and return
	close(connectSocket);
	fclose(outlog);
	return retSizePtr;
    }

    long dataAmount;
    unsigned short int serverPort;
    int isServer;
    int otherNodeIndex;
    int connectIndex;
    char buf[BUF_SIZE];
    int listenSocket, connectSocket;
    PThreadPtr readThread;
    struct sockaddr_in clientAddress, serverAddress;
};

int main(int argc, char **argv)
{
    lintel::parseCommandLine(argc,argv);
    long dataAmount = 0;
    long dataAmountPerNode = 0;
    string tmp;
    int isServer = 0;
    long *retSizePtr = 0;
    long totReturned = 0;
    FILE *globaljoblog;
    PThreadPtr *setupAndTransferThreads; //will hold array of pthread pointers, one for each nodeindex
	
    startTime = (struct timeval*) malloc(sizeof(struct timeval));
    jobEndTime = (struct timeval*) malloc(sizeof(struct timeval));        
    dataAmount = po_dataAmount.get();
    nodeIndex = po_nodeIndex.get();
    tmp = po_nodeNames.get();
    boost::split(nodeNames, tmp, boost::is_any_of(","));
    numNodes = nodeNames.size();
    dataAmountPerNode = dataAmount / (long)numNodes;
    SINVARIANT(numNodes >= 0);
    setupAndTransferThreads = new PThreadPtr[numNodes];
    printf("\ngenread called with %d nodes and %ld Bytes (%ld B per node)\n",numNodes,dataAmount,dataAmountPerNode);
    // Set up simulated partitioning and buffering
    bytesPartitionedPerNode = 0;
    bytesReadyPerNode = (long *)malloc(numNodes * sizeof(long));
    bzero(bytesReadyPerNode, (numNodes * sizeof(long)));

    system(NETBARSERVERS);
    gettimeofday(startTime, NULL);

    // Specify which connections apply to this node, as a server or client
    isServer = 1;
    for (int i = (numNodes - 1); i >= 0; --i) {
	if (i == nodeIndex) {
	    // We finished setting up servers, wait for all other servers to go up
	    system(NETBARSERVERS);
	    //printf("\n%d: finished setting up servers\n", nodeIndex);
	    isServer = 0;
	} else {
	    //printf("Calling setupAndTransfer for i=%d\n",i);
	    setupAndTransferThreads[i].reset(new SetupAndTransferThread(dataAmountPerNode, PORT_BASE+(isServer == 0 ? nodeIndex : i), isServer, i));
	    setupAndTransferThreads[i]->start();
	}
    }

    // Join all threads
    for (int i = 0; i < numNodes; i++) {
	if (i == nodeIndex) {
	    ; // Do nothing
	} else {
	    printf("\nWaiting on join for setupAndTransferThread");
	    retSizePtr = (long *)setupAndTransferThreads[i]->join();
	    printf("\nData received from nodeindex %d: %ld\n", i, *retSizePtr);
	    totReturned += *retSizePtr;
	    free(retSizePtr);
	}
    }

    printf("\nTotal received: %ld\n", totReturned);
    system(NETBARSERVERS);
    
    gettimeofday(jobEndTime, NULL);    
    printf("\nFull job finished:   %ld us\n", tval2longdiff(startTime, jobEndTime) );

    // Write out global job stats if we are the first nodeindex
    if (nodeIndex == 0) {
	// Open output file for global stats
	string globaljobfile = (boost::format("%s%d.out") % LOG_DIR % numNodes).str();
	cout << globaljobfile;
	globaljoblog = fopen(globaljobfile.c_str(), "w+");
	if (globaljoblog == NULL) {
	    fprintf(stderr, "ERROR opening globaljoblog");
	}
	
	fprintf(globaljoblog, "genread called with %d nodes and %ld Bytes (%ld B per node)\n",numNodes,dataAmount,dataAmountPerNode);
	fprintf(globaljoblog, "Total received at nodeIndex 0: %ld\n", totReturned);
	fprintf(globaljoblog, "Full job finished:   %ld us\n", tval2longdiff(startTime, jobEndTime) );
    }

    free(bytesReadyPerNode);
    delete [] setupAndTransferThreads;
    free(startTime);
    free(jobEndTime);
}
