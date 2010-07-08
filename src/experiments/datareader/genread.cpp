#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <fcntl.h>
#include <sys/uio.h>

#include <Lintel/ProgramOptions.hpp>

using lintel::ProgramOption;
using namespace std;

#define BUF_SIZE 65536
#define MAX_READS 1000000  //Must be (sometimes a lot) greater than TOT_SIZE / BUF_SIZE
#define NETBAR "/home/krevate/projects/DataSeries/experiments/neta2a/net_call_bar pds-10"

char buf[BUF_SIZE];
long readTimes[MAX_READS]; //time between successful reads
long readAmounts[MAX_READS]; //amount of data actually read each time
long totReads = 0;
struct timeval *startTime, *readDoneTime, *localEndTime, *jobEndTime;

ProgramOption<long> po_dataAmount("data-amount", "Total amount of data to read from all hosts", 4000000000);

long tval2long(struct timeval *tval) {
    return ((tval->tv_sec*1000000) + tval->tv_usec);
}

long tval2longdiff(struct timeval *tvalstart, struct timeval *tvalend) {
    return (((tvalend->tv_sec-tvalstart->tv_sec)*1000000) + (tvalend->tv_usec-tvalstart->tv_usec));
}

void printReadTimes() {
    int i;
    long totReadTime = 0;
    long totReadAmnt = 0;
    printf("\nReadTime ReadAmnt\n");
    for (i = 0; i < totReads; i++) {
	printf("%ld %ld\n", readTimes[i], readAmounts[i]);
	totReadTime += readTimes[i];
	totReadAmnt += readAmounts[i];
    }
    printf("Total number of reads: %ld\n", totReads);
    printf("Total amount read: %ld\n", totReadAmnt);
    printf("Total time in reads: %ld us\n", totReadTime);
}

long runReader(long dataAmount, int sockid) {
    int ret;
    long tot = 0;
    long thisReadTime = 0;
    long lastReadTime = 0;
    long numReads = 0;

    for (ret = 0, numReads = 0, lastReadTime = tval2long(startTime); tot < dataAmount; ++numReads, tot += ret) {
	// Note: always reading buf_size is ok because we control exact amount sent
	ret = read(sockid, buf, BUF_SIZE);
	gettimeofday(readDoneTime, NULL);
	//printf("read num: %ld\n",numReads);
	//printf("read amnt: %ld\n",ret);
	readAmounts[numReads] = ret;
	thisReadTime = tval2long(readDoneTime);
	readTimes[numReads] = thisReadTime - lastReadTime;
	lastReadTime = thisReadTime;
	if (ret == 0) {
	    break;
	}
    }

    totReads = ++numReads;
    gettimeofday(localEndTime, NULL);

    return tot;
}

int main(int argc, char **argv)
{
    lintel::parseCommandLine(argc,argv);
    long totSize = 0;
    long retSize = 0;
    int sockid = 0;
    //struct sockaddr_in serverAddress;
    //struct hostent *hostInfo;

    // Parse args
    //if (argc != 2) {
    //printf("Usage: ./genread dataAmount\n");
    //exit(0);
    //}
    //totSize = atol(argv[1]);
    totSize = po_dataAmount.get();

    startTime = (struct timeval*) malloc(sizeof(struct timeval));
    readDoneTime = (struct timeval*) malloc(sizeof(struct timeval));
    localEndTime = (struct timeval*) malloc(sizeof(struct timeval));
    jobEndTime = (struct timeval*) malloc(sizeof(struct timeval));

    system(NETBAR);
    
    gettimeofday(startTime, NULL);

    // Handle server setup

    system(NETBAR);
    
    // Either read from or write to socket id
    retSize = runReader(totSize, sockid);
    
    printf("\nTotal received: %ld\n",retSize);
    printf("Local work finished: %ld us\n", tval2longdiff(startTime, localEndTime) );
    
    system(NETBAR);
    
    gettimeofday(jobEndTime, NULL);
    
    printReadTimes();
    printf("Full job finished:   %ld us\n", tval2longdiff(startTime, jobEndTime) );
}
