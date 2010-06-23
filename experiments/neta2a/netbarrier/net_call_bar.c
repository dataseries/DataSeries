#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netdb.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main(int argc, char ** argv) {

    int port = 14341;

    if (argc == 2) {
	//Default
    } else if (argc == 3) {
	port = atoi(argv[2]);
    } else {
	//Error
	fprintf(stderr, "Got argc==%d, expecting 1, 2, or 3.\n Usage: net_call_bar host [port]", argc);
	exit(1);
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd<0) {
	perror("FAILED Can't open socket");
	exit(1);
    }
    struct hostent * hent;
    hent = gethostbyname(argv[1]);
    if (hent==0) {
	perror("FAILED resolving argv[1] as a host");
	exit(1);
    }
    struct sockaddr_in saddr;
    memset(&saddr, 0, sizeof(saddr));
    saddr.sin_family = AF_INET;
    memcpy(&saddr.sin_addr.s_addr, hent->h_addr, hent->h_length);
    saddr.sin_port = htons(port);
    
    int res;
    res=connect(fd, (const sockaddr *)&saddr, sizeof(saddr));
    if (res < 0) {
	perror("FAILED in connect");
	exit(1);
    }

    struct timeval r, t;
    do {
	res = read(fd, &r, sizeof(r));
    } while (res < 0 && errno==EINTR);
    gettimeofday(&t, 0);
    if (res==sizeof(r)) {
	printf("Barrier passed at %d.%06d local (%d.%06d remote)",
	       t.tv_sec, t.tv_usec, r.tv_sec, r.tv_usec);
    } else if (res < 0) {
	printf("FAILED at %d.%06d local", t.tv_sec, t.tv_usec);
    } else {
	printf("Barrier flaked at %d.%06d local", t.tv_sec, t.tv_usec);
    }

}
