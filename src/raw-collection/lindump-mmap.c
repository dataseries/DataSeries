static int no_file_rotation = 0;

/* Copyright (c) 2002 Gianni Tedesco Released under the terms of the
 * GNU GPL version 2; Extensions to original to do multiple files,
 * mmap, etc (C) per the DataSeries/COPYING file */

#ifndef __linux__
#error "Are you loco? This is Linux only!"
#endif
#ifndef __GNUC__
#error "Need __builtin_memcpy, which is probably GNUC only"
#endif

#if __GNUC__ >= 4
#define __always_inline		inline __attribute__((always_inline))
#endif

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

#define __USE_XOPEN
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <features.h>    /* for the glibc version number */
#if __GLIBC__ >= 2 && __GLIBC_MINOR >= 1
#include <netpacket/packet.h>
#include <net/ethernet.h>     /* the L2 protocols */
#else
#include <asm/types.h>
#include <linux/if_packet.h>
#include <linux/if_ether.h>   /* The L2 protocols */
#endif
#if defined(__x86_64__)
#define mb() asm volatile("mfence":::"memory")
#elif defined(__i386__) || defined(__i486__) || defined(__i686__)
#define mb() asm volatile("lock; addl $0,0(%%esp)":::"memory")
#else
#error "?? mb()"
#endif
#include <string.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <linux/if.h>
#include <pcap.h>
#include <errno.h>
#include <strings.h>
#include <sys/mman.h>

char *names[]={
	"<", /* incoming */
	"B", /* broadcast */
	"M", /* multicast */
	"P", /* promisc */
	">", /* outgoing */
};

int fd=-1;
char *map;
struct tpacket_req req;
struct iovec *ring;

void sigproc(int sig)
{
	struct tpacket_stats st;
	socklen_t len=sizeof(st);

	if (!getsockopt(fd,SOL_PACKET,PACKET_STATISTICS,(unsigned char *)&st,&len)) {
		fprintf(stderr, "received %u packets, dropped %u\n",
			st.tp_packets, st.tp_drops);
	}
	
	if ( map ) munmap(map, req.tp_block_size * req.tp_block_nr);
	if ( fd>=0 ) close(fd);
	if ( ring ) free(ring);

	exit(0);
}

/*
 *  pcap-linux.c: Packet capture interface to the Linux kernel
 *
 *  Copyright (c) 2000 Torsten Landschoff <torsten@debian.org>
 *  		       Sebastian Krahmer  <krahmer@cs.uni-potsdam.de>
 *  
 *  License: BSD
 *  
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *  
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *  3. The names of the authors may not be used to endorse or promote
 *     products derived from this software without specific prior
 *     written permission.
 *  
 *  THIS SOFTWARE IS PROVIDED ``AS IS'' AND WITHOUT ANY EXPRESS OR
 *  IMPLIED WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
 */

/*  from libpcap-0.7.2
 *  Return the index of the given device name. Fill ebuf and return 
 *  -1 on failure.
 */
static int
iface_get_id(int fd, const char *device)
{
	struct ifreq	ifr;

	memset(&ifr, 0, sizeof(ifr));
	strncpy(ifr.ifr_name, device, sizeof(ifr.ifr_name));

	if (ioctl(fd, SIOCGIFINDEX, &ifr) == -1) {
	  perror("ioctl");
	  abort();
	}

	return ifr.ifr_ifindex;
}

// /*
//  *  Bind the socket associated with FD to the given device. 
//  */
// static int
// iface_bind(int fd, int ifindex)
// {
// 	struct sockaddr_ll	sll;
// 	int			err;
// 	socklen_t		errlen = sizeof(err);
// 
// 	memset(&sll, 0, sizeof(sll));
// 	sll.sll_family		= AF_PACKET;
// 	sll.sll_ifindex		= ifindex;
// 	sll.sll_protocol	= htons(ETH_P_ALL);
// 
// 	if (bind(fd, (struct sockaddr *) &sll, sizeof(sll)) == -1) {
// 	  fprintf(stderr,"error on bind: %s\n",strerror(errno));
// 	  abort();
// 	}
// 
// 	/* Any pending errors, e.g., network is down? */
// 
// 	if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &errlen) == -1) {
// 	  perror("getsockopt");
// 	  abort();
// 	}
// 
// 	if (err > 0) { 
// 	  fprintf(stderr,"Error reported by getsockopt(SO_ERROR): %s\n",strerror(err));
// 	  abort();
// 	}
// 
// 	return 0;
// }

static const int snapshot_size = 2048; // must divide block_size in main()
#define TCPDUMP_MAGIC 0xa1b2c3d4
FILE *
linpcap_dump_open(const char *filename)
{
  FILE *ret;
  struct pcap_file_header hdr;
  int bytes;

  ret = fopen(filename,"w+");
  if (ret == NULL) {
    fprintf(stderr,"noopen %s: %s\n",filename,strerror(errno));
    abort();
  }

  // lifted from libpcap
  hdr.magic = TCPDUMP_MAGIC;
  hdr.version_major = 2; // PCAP_VERSION_MAJOR;
  hdr.version_minor = 4; // PCAP_VERSION_MINOR;
  
  hdr.thiszone = 0; // thiszone; -- nothing seems to use it and it's not set in pcap
  hdr.snaplen = snapshot_size;
  hdr.sigfigs = 0;
  hdr.linktype = 1;// LINKTYPE_ETHERNET, or DLT_EN10
  bytes = fwrite(&hdr, 1, sizeof(struct pcap_file_header), ret);
  if (bytes != sizeof(struct pcap_file_header)) {
      fprintf(stderr,"Write failed %d != %d\n",bytes,(int)sizeof(struct pcap_file_header));
      exit(1);
  }
  fflush(ret);
  return ret;
}

char *filename_base;
int cur_file_num;
FILE *outfile;
void *output_map_base = 0;
static const int packet_bytes_interval = 200*1000*1000;
static int mmap_length;
unsigned char *write_buffer, *cur_write_buffer_pos;

void 
new_output_file()
{
    char buf[200];
    if (no_file_rotation && output_map_base > 0) {
	write_buffer = (unsigned char *)output_map_base;
	cur_write_buffer_pos = write_buffer + sizeof(struct pcap_file_header);
	printf("didn't rotate to file #%d\n",cur_file_num);
	++cur_file_num;
	return;
    } 
    snprintf(buf,200,"%s.%05d",filename_base,cur_file_num);
    if (outfile != NULL) {
	int ret = munmap(output_map_base,mmap_length);
	if (ret != 0) {
	    fprintf(stderr,"Error unmapping outfile: %s\n",strerror(errno));
	    abort();
	}
	ftruncate(fileno(outfile),cur_write_buffer_pos - write_buffer);
	ret = fclose(outfile);
	if (ret != 0) {
	    fprintf(stderr,"Error closing outfile: %s\n",strerror(errno));
	    abort();
	}
    }
    outfile = linpcap_dump_open(buf);
    if (outfile == NULL) {
	fprintf(stderr,"open %s failed\n",buf);
	abort();
    }
    ++cur_file_num;
    ftruncate(fileno(outfile),packet_bytes_interval+4096*1000);
    output_map_base = mmap(NULL, mmap_length, PROT_READ | PROT_WRITE,
			   MAP_SHARED, fileno(outfile), 0);
    if (output_map_base == NULL || output_map_base == MAP_FAILED) {
	fprintf(stderr,"mmap failed(%d): %s\n",errno, strerror(errno));
	abort();
    }
    write_buffer = (unsigned char *)output_map_base;
    cur_write_buffer_pos = write_buffer + sizeof(struct pcap_file_header);
    printf("now writing to output file %s\n",buf);
}

struct pcap_timeval {
    bpf_int32 tv_sec;		/* seconds */
    bpf_int32 tv_usec;		/* microseconds */
};

struct pcap_sf_pkthdr {
    struct pcap_timeval ts;	/* time stamp */
    bpf_u_int32 caplen;		/* length of portion present */
    bpf_u_int32 len;		/* length this packet (off wire) */
};

void *xmalloc(size_t size)
{
  void *ret = malloc(size);
  if (ret == NULL) {
    fprintf(stderr,"malloc failed\n");
    abort();
  }
  memset(ret,0,size);
  return ret;
}

double getcurtime()
{
  struct timeval tv;

  int ret = gettimeofday(&tv,NULL);
  if (ret != 0) {
    perror("gettimeofday");
    abort();
  }
  return tv.tv_sec + tv.tv_usec * 1.0e-6;
}

//static inline void * linux_memcpy(void * to, const void * from, size_t n)
//{
//int d0, d1, d2;
//__asm__ __volatile__(
//	"rep ; movsl\n\t"
//	"testb $2,%b4\n\t"
//	"je 1f\n\t"
//	"movsw\n"
//	"1:\ttestb $1,%b4\n\t"
//	"je 2f\n\t"
//	"movsb\n"
//	"2:"
//	: "=&c" (d0), "=&D" (d1), "=&S" (d2)
//	:"0" (n/4), "q" (n),"1" ((long) to),"2" ((long) from)
//	: "memory");
//return (to);
//}

void do_tracing(int argc, char *argv[], int device_ids[])
{
    struct pollfd pfd;
    int cur_ring_pos;
    long long packetcount = 0;
    int warn1 = 0,warn2 = 0;
    const int warn1_step = 2048;
    const int warn2_step = 16384;
    long long total_packets = 0;
    long long total_bytes = 0;
    long long total_writes = 0;
    long long total_drops = 0;
    long long last_switch_files_bytes = 0, last_switch_files_packets = 0, last_switch_files_writes = 0, last_total_drops = 0;
    double last_switch_files_time, curtime, starttime;
    double last_message;

    new_output_file();
    last_switch_files_time = getcurtime();
    starttime = getcurtime();
    printf("starting tracing, writing to %s\n",argv[argc-1]);
    last_message = getcurtime();
    fflush(stdout);
    for(cur_ring_pos=0;;) {
	while(*(volatile unsigned long*)ring[cur_ring_pos].iov_base) {
	    struct tpacket_hdr *h=ring[cur_ring_pos].iov_base;
	    struct sockaddr_ll *sll=(void *)h + TPACKET_ALIGN(sizeof(*h));
	    unsigned char *bp=(unsigned char *)h + h->tp_mac;

	    if (sll->sll_ifindex == device_ids[0] ||
		sll->sll_ifindex == device_ids[1]) {
		struct pcap_sf_pkthdr sf_hdr;
		++packetcount;
		sf_hdr.ts.tv_sec = h->tp_sec;
		sf_hdr.ts.tv_usec = h->tp_usec;
		sf_hdr.caplen = h->tp_snaplen;
		sf_hdr.len = h->tp_len;
		__builtin_memcpy(cur_write_buffer_pos,&sf_hdr,sizeof(struct pcap_sf_pkthdr));
		cur_write_buffer_pos += sizeof(struct pcap_sf_pkthdr);
		memcpy(cur_write_buffer_pos,bp,sf_hdr.caplen);
		cur_write_buffer_pos += sf_hdr.caplen;

		if (0) { 
		    printf("%u.%.6u: if%u %s %u bytes\n",
			   h->tp_sec, h->tp_usec,
			   sll->sll_ifindex,
			   names[sll->sll_pkttype],
			   h->tp_len);
		}
		//	      printf("pending mark %d %p\n",npackets_so_far,&h->tp_status);
		++total_packets;
		total_bytes += sizeof(struct pcap_sf_pkthdr) + sf_hdr.caplen;
	    } 
	    /* tell the kernel this packet is done with */
	    h->tp_status=0;
	    mb(); /* memory barrier */ // ERIC: could we safely move to the end of the loop + a check that says if we've skipped 32 mb()'s that we force one?

	    cur_ring_pos=(cur_ring_pos==req.tp_frame_nr-1) ? 0 : cur_ring_pos+1;
	    if (*(volatile unsigned long*)ring[(cur_ring_pos + warn1_step)%req.tp_frame_nr].iov_base && warn1 == 0) {
		//	      printf("warning 1, buffer at + %d full\n",warn1_step);
		warn1 = 1;
	    }
	    if (*(volatile unsigned long*)ring[(cur_ring_pos + warn2_step)%req.tp_frame_nr].iov_base && warn2 == 0) {
		printf("warning 2, buffer at + %d full\n",warn2_step);
		warn2 = 1;
	    }
	      
	    if ((total_bytes - last_switch_files_bytes) > packet_bytes_interval) {
		struct tpacket_stats st;
		socklen_t len = sizeof(st);
		warn1 = warn2 = 0;
		if (getsockopt(fd,SOL_PACKET,PACKET_STATISTICS,(char *)&st,&len)!= 0) {
		    perror("getsockopt");
		    abort();
		    fprintf(stderr, "received %u packets, dropped %u\n",
			    st.tp_packets, st.tp_drops);
		}
		total_drops += st.tp_drops;
		curtime = getcurtime();
		printf("%.6f: %.3f total GB, %.4f total million packets, %lld total writes\n",
		       curtime,(double)total_bytes/(1024.0*1024.0*1024.0), 
		       (double)total_packets/(1000.0*1000.0), total_writes);
		printf("  total: %.2f MB/s, %.2f packets/s, %.2f writes/s; %lld drops -- %.2f%%\n",
		       (double)(total_bytes)/(1024*1024.0*(curtime - starttime)),
		       (double)(total_packets)/(curtime - starttime),
		       (double)(total_writes)/(curtime - starttime),
		       total_drops, 100.0*total_drops/((double)total_packets + total_drops));
		printf("  interval: %.2f MB/s, %.2f packets/s, %.2f writes/s; %lld drops -- %.2f%%\n",
		       (double)(total_bytes - last_switch_files_bytes)/(1024*1024.0*(curtime - last_switch_files_time)),
		       (double)(total_packets - last_switch_files_packets)/(curtime - last_switch_files_time),
		       (double)(total_writes - last_switch_files_writes)/(curtime - last_switch_files_time),
		       total_drops - last_total_drops, 100.0*(total_drops - last_total_drops)/(double)(total_packets - last_switch_files_packets + total_drops - last_total_drops));
		last_switch_files_time = curtime;
		last_switch_files_bytes = total_bytes;
		last_switch_files_packets = total_packets;
		last_switch_files_writes = total_writes;
		last_total_drops = total_drops;
		fflush(stdout);
		new_output_file();
		last_message = getcurtime();
	    } 
	}
	//	  usleep(5000);
	if (1) {
	    if ((getcurtime() - last_message) >= 2) {
		printf("slow network @%.2f; %lld packets, %.2f MB since last file switch\n",
		       getcurtime(),
		       total_packets - last_switch_files_packets,
		       (double)(total_bytes - last_switch_files_bytes)/(1024.0*1024));
		fflush(stdout);
		last_message = getcurtime();
	    }
	    /* Sleep when nothings happening */
	    pfd.fd=fd;
	    pfd.events=POLLIN|POLLERR;
	    pfd.revents=0;
	    poll(&pfd, 1, 100);
	}
    }
}

void
pt_repeat_copy(void *a, void *b, int buffer_size, const int copy_size, const double test_len) 
{
    double start, end;
    int reps;

    buffer_size = copy_size * (buffer_size/copy_size);
    printf("  repeat (%d->large) copy: ",copy_size); fflush(stdout);
    reps = 0;
    for(start = end = getcurtime();end - start < test_len; end = getcurtime()) {
	int pos = 0;
	for(pos = 0;pos+copy_size<buffer_size;pos += copy_size) {
	    memcpy(a+pos,b,copy_size);
	}
	++reps;
    }
    printf("%d reps in %.3f seconds, %.2f MiB/s\n",
	   reps,end - start, (double)buffer_size*reps/(1024.0*1024.0*(end-start)));

}

#define test_memcpy __builtin_memcpy

static void
pt_shift_copy(void *a, void *b, int buffer_size, const int copy_size, const double test_len) 
{
    double start, end;
    int reps;

    buffer_size = copy_size * (buffer_size/copy_size);
    printf("  shifting (%d->large) copy: ",copy_size); fflush(stdout);
    reps = 0;
    for(start = end = getcurtime();end - start < test_len; end = getcurtime()) {
	int pos = 0;
	for(pos = 0;pos+copy_size<buffer_size;pos += copy_size) {
	    memcpy(a+pos,b+pos,copy_size);
	}
	++reps;
    }
    printf("%d reps in %.3f seconds, %.2f MiB/s\n",
	   reps,end - start, (double)buffer_size*reps/(1024.0*1024.0*(end-start)));

    printf("  shifting (%d->large) test-copy: ",copy_size); fflush(stdout);
    reps = 0;
    for(start = end = getcurtime();end - start < test_len; end = getcurtime()) {
	int pos = 0;
	for(pos = 0;pos+copy_size<buffer_size;pos += copy_size) {
	    test_memcpy(a+pos,b+pos,copy_size);
	}
	++reps;
    }
    printf("%d reps in %.3f seconds, %.2f MiB/s\n",
	   reps,end - start, (double)buffer_size*reps/(1024.0*1024.0*(end-start)));

}

void
performance_test()
{
    double start, end;
    const int buffer_size = 200000000;
    void *a = malloc(buffer_size);
    void *b = malloc(buffer_size);
    int reps;
    const double test_len = 2.5;
    //    const int small_size = 800;
    //    const int small_odd_size = 633;

    printf("running performance tests...\n");
    if (a == NULL || b == NULL) {
	printf("bad\n");
	exit(1);
    }
    printf("  long copy: "); fflush(stdout);
    reps = 0;
    for(start = end = getcurtime();end - start < test_len; end = getcurtime()) {
	memcpy(a,b,buffer_size);
	++reps;
    }
    printf("%d reps in %.3f seconds, %.2f MiB/s\n",
	   reps,end - start, (double)buffer_size*reps/(1024.0*1024.0*(end-start)));

    printf("  long test-copy: "); fflush(stdout);
    reps = 0;
    for(start = end = getcurtime();end - start < test_len; end = getcurtime()) {
	test_memcpy(a,b,buffer_size);
	++reps;
    }
    printf("%d reps in %.3f seconds, %.2f MiB/s\n",
	   reps,end - start, (double)buffer_size*reps/(1024.0*1024.0*(end-start)));

//    pt_repeat_copy(a,b,buffer_size,128,test_len);
//    pt_repeat_copy(a,b,buffer_size,small_size,test_len);
//    pt_repeat_copy(a,b,buffer_size,small_odd_size,test_len);

    // sizes here were chosen as they turn out to be ones that show up
    // a lot in NFS traces.
    pt_shift_copy(a,b,buffer_size,66,test_len);
    pt_shift_copy(a,b,buffer_size,182,test_len);
    pt_shift_copy(a,b,buffer_size,198,test_len);
    pt_shift_copy(a,b,buffer_size,1514,test_len);

    free(a);
    free(b);
}

int main ( int argc, char **argv ) 
{
    struct sockaddr_ll addr;
    struct packet_mreq	mr;
    int j;
    int device_ids[2];
    device_ids[0] = -1;
    device_ids[1] = -1;

    performance_test();

    mmap_length = 8192*((packet_bytes_interval + 4000000)/8192);
    signal(SIGINT, sigproc);

    if (argc < 3) {
	fprintf(stderr,"Usage: %s <device...> <output-basename>\n",argv[0]);
	exit(1);
    }
    filename_base = argv[argc-1];

    /* Open the packet socket */
    if ( (fd=socket(PF_PACKET, SOCK_RAW, 0))<0 ) { // was SOCK_DGRAM
	perror("socket()");
	return 1;
    }

    /* Setup the fd for mmap() ring buffer */
    req.tp_block_size=4096;
    req.tp_frame_size=snapshot_size; // max snapshot size
    if ((req.tp_block_size / req.tp_frame_size) * req.tp_frame_size != req.tp_block_size) {
	abort();
    }
    req.tp_block_nr=16384;
    req.tp_frame_nr=(req.tp_block_size / req.tp_frame_size)*req.tp_block_nr;
    printf("using %d bytes for ring buffer of %d entries\n",req.tp_block_nr * req.tp_block_size,req.tp_frame_nr);
    ring=malloc(req.tp_frame_nr * sizeof(struct iovec));
    if (ring == NULL) {
	fprintf(stderr,"bad malloc\n");
	abort();
    }
	  
    if ( (setsockopt(fd,
		     SOL_PACKET,
		     PACKET_RX_RING,
		     (char *)&req,
		     sizeof(req))) != 0 ) {
	perror("setsockopt()");
	close(fd);
	return 1;
    };

    /* mmap() the sucker */
    map=mmap(NULL,
	     req.tp_block_size * req.tp_block_nr,
	     PROT_READ|PROT_WRITE|PROT_EXEC, MAP_SHARED, fd, 0);
    if ( map==MAP_FAILED ) {
	perror("mmap()");
	close(fd);
	return 1;
    }

    /* Setup our ringbuffer */
    for(j=0; j<req.tp_frame_nr; j++) {
	ring[j].iov_base=(void *)((long)map)+(j*req.tp_frame_size);
	ring[j].iov_len=req.tp_frame_size;
    }
	
    /* bind the packet socket */
    memset(&addr, 0, sizeof(addr));
    addr.sll_family=AF_PACKET;
    addr.sll_protocol=htons(0x03);
    addr.sll_ifindex=0;
    addr.sll_hatype=0;
    addr.sll_pkttype=0;
    addr.sll_halen=0;
    if ( bind(fd, (struct sockaddr *)&addr, sizeof(addr)) ) {
	munmap(map, req.tp_block_size * req.tp_block_nr);
	perror("bind()");
	close(fd);
	return 1;
    }
	
    for(j=1;j<(argc-1);j++) {
	int device_id;
	printf("promiscuous on %s\n",argv[j]);
	device_id = iface_get_id(fd,argv[j]);
	//	  iface_bind(fd,device_id);
	if ((j-1) >= 2) {
	    fprintf(stderr,"too many devices\n");
	    abort();
	}
	device_ids[j-1] = device_id;

	// from libpcap-0.7.2: pcap-linux.c
	memset(&mr, 0, sizeof(mr));
	mr.mr_ifindex = device_id;
	mr.mr_type    = PACKET_MR_PROMISC;
	if (setsockopt(fd, SOL_PACKET, 
		       PACKET_ADD_MEMBERSHIP, &mr, sizeof(mr)) == -1)
	    {
		perror("setsockopt1");
		abort();
	    }
    }
	
    do_tracing(argc,argv,device_ids);
    return 0;
}
