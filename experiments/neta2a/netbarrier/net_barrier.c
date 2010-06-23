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

    int num_to_wait = 5;
    int port = 14341;

    if (argc == 1) {
	//Defaults
    } else if (argc == 2) {
	num_to_wait = atoi(argv[1]);
    } else if (argc == 3) {
	num_to_wait = atoi(argv[1]);
	port = atoi(argv[2]);
    } else {
	//Error
	fprintf(stderr, "Got argc==%d, expecting 1, 2, or 3.\n Usage: net_barrier [num_to_wait] [port]", argc);
	exit(1);
    }

    int * fds = (int *)malloc(num_to_wait * sizeof(int));
    int sock_fd;
    int on=1;
    sock_fd = socket(AF_INET,SOCK_STREAM,0); // fam, type, proto;
    if (sock_fd < 0) {
	perror("FAILED at socket()");
	exit(1);
    }
    
    if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
	perror("setsockopt(SO_REUSEADDR) failed");
    }

    struct sockaddr_in saddr;
    memset(&saddr, 0, sizeof(saddr));
    saddr.sin_family = AF_INET;
    saddr.sin_addr.s_addr = INADDR_ANY;
    saddr.sin_port = htons(port);

    if (bind(sock_fd, (struct sockaddr *) &saddr, sizeof(saddr)) < 0) {
	perror("FAILED at bind()");
	exit(1);
    }

    listen(sock_fd, 25); // above 5 doesn't work on some old OSes...


    while (1) {
	for(int i = 0; i<num_to_wait; ++i) {
	    struct sockaddr c_saddr;
	    int c_len = sizeof(c_saddr);
	    fds[i] = accept(sock_fd, (struct sockaddr *)&c_saddr, (socklen_t *)&c_len);
	    if (fds[i] < 0) {
		perror("FAILED on accept");
		exit(1);
	    }
	}
	struct timeval t;
	gettimeofday(&t, 0);
	printf("Got %d connections at %d.%06d\n", 
	       num_to_wait, t.tv_sec, t.tv_usec);
	int failed = 0;
	int flaked = 0;
	for(int i = 0; i<num_to_wait; ++i) {
	    int res = write(fds[i], &t, sizeof(t));
	    if (res!=sizeof(t)) {
		flaked++;
	    }
	    if (res < 0) {
		failed++;
	    }
	}
	gettimeofday(&t, 0);
	printf("Barrier complete at %d.%06d, %d flaked\n", 
	       t.tv_sec, t.tv_usec, flaked);
	if (failed>0 ) {
	    printf("%d FAILED\n", failed);
	}
	for(int i = 0; i<num_to_wait; ++i) {
	    close(fds[i]);
	}		
	fflush(stdout);
    }

}
