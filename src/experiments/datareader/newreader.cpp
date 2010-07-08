#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <fcntl.h>
#include <sys/uio.h>

#define BUF_SIZE 65536
//#define TOT_SIZE 4000000000
#define MAX_READS 1000000  //Must be (sometimes a lot) greater than TOT_SIZE / BUF_SIZE
#define NETBAR "/home/krevate/projects/DataSeries/experiments/neta2a/net_call_bar pds-10"

char buf[BUF_SIZE];
long readTimes[MAX_READS]; //time between successful reads
int readAmounts[MAX_READS]; //amount of data actually read each time
long totReads = 0;
//char *buf;

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

int main(int argc, char **argv)
{
    int ret;
    long tot = 0;
    long totSize = 0;
    long thisReadTime = 0;
    long lastReadTime = 0;
    long readDoneTimeLong = 0;
    struct timeval *startTime, *readDoneTime, *localEndTime, *jobEndTime;
    long totTime;
    long numReads = 0;
    //char * used_buf;

    if (argc != 2) {
	printf("Usage: ./reader dataAmountToRead\n");
	exit(0);
    }
    totSize = atol(argv[1]);

    startTime = (struct timeval*) malloc(sizeof(struct timeval));
    readDoneTime = (struct timeval*) malloc(sizeof(struct timeval));
    localEndTime = (struct timeval*) malloc(sizeof(struct timeval));
    jobEndTime = (struct timeval*) malloc(sizeof(struct timeval));

    //buf = (char *)malloc(BUF_SIZE + 4096);
    //used_buf = (char *)((unsigned long long)(buf + 4096) & ((((unsigned long long)(0))-1) ^ 0x00FFFll));
    //printf("%X to %X\n", buf, used_buf);

    //struct iovec foo;
    //foo.iov_len = BUF_SIZE;
    //foo.iov_base = used_buf;    
    
    system(NETBAR);
    
    gettimeofday(startTime, NULL);

    for (ret = 0, numReads = 0, lastReadTime = tval2long(startTime); tot < totSize; ++numReads, tot += ret) {
    //while (1) {
	//ret = vmsplice(0, &foo, BUF_SIZE/4096, SPLICE_F_GIFT);
	// Note: always reading buf_size is ok because we control exact amount sent
	ret = read(0, buf, BUF_SIZE);
	gettimeofday(readDoneTime, NULL);
	//printf("read num: %ld\n",numReads);
	//printf("read amnt: %ld\n",ret);
	//if (numReads < MAX_READS) {
	    readAmounts[numReads] = ret;
	    thisReadTime = tval2long(readDoneTime);
	    readTimes[numReads] = thisReadTime - lastReadTime;
	    lastReadTime = thisReadTime;
	//}
	if (ret == 0) {
	    break;
	}
    }

    totReads = ++numReads;
    gettimeofday(localEndTime, NULL);
    printf("\nTotal received: %ld\n",tot);
    printf("Local work finished: %ld us\n", tval2longdiff(startTime, localEndTime) );

    system(NETBAR);
  
    gettimeofday(jobEndTime, NULL);

    printReadTimes();
    printf("Full job finished:   %ld us\n", tval2longdiff(startTime, jobEndTime) );

    return 0;
}
