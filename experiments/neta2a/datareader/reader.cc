#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <fcntl.h>
#include <sys/uio.h>

#define BUF_SIZE 524288
#define TOT_SIZE 4000000000
#define NETBAR "/home/krevate/projects/DataSeries/experiments/neta2a/net_call_bar pds-10"

long tval2long(struct timeval *tvalstart, struct timeval *tvalend) {
    return (((tvalend->tv_sec-tvalstart->tv_sec)*1000000) + (tvalend->tv_usec-tvalstart->tv_usec));
}

char buf[BUF_SIZE];
//char * buf;

int main(int argc, char **argv)
{
    int ret;
    long tot = 0;
    struct timeval *startTime, *localEndTime, *jobEndTime;
    long totTime;
    //char * used_buf;

    startTime = (struct timeval*) malloc(sizeof(struct timeval));
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

    while (tot < TOT_SIZE) {
    //while (1) {
	//ret = vmsplice(0, &foo, BUF_SIZE/4096, SPLICE_F_GIFT);
	ret = read(0, buf, BUF_SIZE);
	if (ret == 0) {
	    break;
	}
	tot += ret;
    }

    gettimeofday(localEndTime, NULL);
    printf("\nTotal received: %ld\n",tot);
    printf("Local work finished: %ld us\n", tval2long(startTime, localEndTime) );

    system(NETBAR);
		
    gettimeofday(jobEndTime, NULL);
    printf("\nFull job finished: %ld us\n", tval2long(startTime, jobEndTime) );

    return 0;
}
