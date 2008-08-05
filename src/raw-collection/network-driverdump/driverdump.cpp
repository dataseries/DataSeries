#include <stdio.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <linux/sockios.h>
#include <linux/if.h>
#include <pcap.h>
#include <pthread.h>
#include <signal.h>
#include <getopt.h>
#include <sys/statvfs.h>
#include <netpacket/packet.h>
#include <net/ethernet.h>     /* the L2 protocols */
#include <boost/format.hpp>

// TODO: on shutdown with 3 threads in the kernel, sending a signal to
// the parent thread isn't enough to get all of them to exit.  Need to
// think about the right way to handle this problem, trivial fix right
// now is to make stop-tracing send a signal to all of the threads.

#include <string>
using namespace std;

#include <Lintel/LintelAssert.hpp>
#include <Lintel/PThread.hpp>
#include <linux/wait.h>
#include "kernel-driver-add.h"

PThreadMutex mutex;
PThreadCond cond;
bool everyone_ok = false, stop_stats = false;
int last_output_filenum = -1;
long long total_output_bytes = 0;
int threads_running = 0;
int unique_id_count = 0;
int page_size;
int socket_fd;
int global_max_file_count = 2000000000;

char *output_basename;
char *interface_name;
int buffer_size;
int nthreads = 1;
bool getstats = false;
bool do_driverdump_test = false;
int quick_stats_interval = 1;

static const bool debug_dpd_dofile = false;

int sigcount = 0;
static void 
signal_handler(int sig) {
    ++sigcount;
    fprintf(stderr,"signal %d recieved %d count, abort after 20\n",
	    sig,sigcount);
    fflush(stderr);
    everyone_ok = false;
    if (sigcount > 20)
	abort();
}

// true on sucess
bool
check_enough_freespace(FILE *fp)
{
    struct statvfs fs_stats;
    AssertAlways(fstatvfs(fileno(fp),&fs_stats) == 0,
		 ("fstatfs failed: %s\n",strerror(errno)));

    if (false) {
	cout << boost::format("freespace %ld available blocks %ld free blocks %ld blocksize\n")
	    % fs_stats.f_bavail % fs_stats.f_bfree % fs_stats.f_bsize;
    }
    long long freebytes = (long long)fs_stats.f_bsize * (long long)fs_stats.f_bavail;
    long long needbytes = (long long)buffer_size * (long long)nthreads;
    if (freebytes < needbytes) {
	printf("Not enough free space on output filesystem, %lld < %lld\n",
	       freebytes,needbytes);
	fflush(stdout);
	return false;
    } else {
	return true;
    }
}

// true on success
bool
dpd_dofile(struct ifreq *ifreq,
	   struct driver_packet_dump_ioctl *dpd,
	   int my_unique_id)
{
    struct pcap_file_header hdr;

    sprintf(dpd->filename,"%s.drvdump.pending.%d",
	    output_basename,my_unique_id);
    FILE *pending_file = fopen(dpd->filename,"w");
    AssertAlways(pending_file != NULL, ("can't open file"));

    if (!check_enough_freespace(pending_file)) {
	fclose(pending_file);
	return false;
    }
    dpd->npages = buffer_size/page_size;
    dpd->bytes_written = sizeof(struct pcap_file_header);

    hdr.magic = 0xa1b2c3d4;
    hdr.version_major = 2;
    hdr.version_minor = 4;
    hdr.thiszone = 0;
    hdr.sigfigs = 0;
    hdr.snaplen = 32768;
    hdr.linktype = 1; // LINKTYPE_ETHERNET

    AssertAlways(fwrite(&hdr,sizeof(hdr),1,pending_file) == 1,
		 ("write failed"));
    AssertAlways(fseek(pending_file,dpd->npages * page_size,SEEK_SET) == 0,
		 ("fseek failed"));
    AssertAlways(fwrite(&dpd->npages,sizeof(int),1,pending_file) == 1,
		 ("write failed"));
    AssertAlways(fflush(pending_file) == 0,("fflush failed"));
    if (!check_enough_freespace(pending_file)) {
	fclose(pending_file);
	return false;
    }
    dpd->subcmd = driver_packet_dump_file;
    if (debug_dpd_dofile)
	printf("%d make ioctl %s\n",my_unique_id, dpd->filename);
    int err = ioctl(socket_fd, SIOCDRIVERPACKETDUMP, ifreq);
    if (err == 0) {
	char *foo = new char[strlen(output_basename) + 64];
	sprintf(foo,"%s.%05d",output_basename,dpd->file_index);
	mutex.lock();
	if (dpd->file_index > last_output_filenum) {
	    last_output_filenum = dpd->file_index;
	}
	total_output_bytes += dpd->bytes_written;
	long long tmp_bytes = total_output_bytes;
	mutex.unlock();
	if (debug_dpd_dofile)
	    printf("dofile %d bytes, idx %d -> %s\n",
		   dpd->bytes_written, dpd->file_index, foo);
	AssertAlways(ftruncate(fileno(pending_file), dpd->bytes_written) == 0,
		     ("truncate failed: %s\n",strerror(errno)));
	AssertAlways(fclose(pending_file) == 0,
		     ("close failed"));
    
	AssertAlways(rename(dpd->filename,foo) == 0,
		     ("rename failed"));
	printf("finished with file %s, %.2f GiB total output\n",
	       foo,(double)tmp_bytes/(1024.0*1024.0*1024.0));
	fflush(stdout);
	delete foo;
	if ((dpd->file_index+1) >= global_max_file_count) {
	    printf("Finished with all files\n");
	    return false;
	}
	return true;
    } else {
	printf("failure on processing file: %s (%d)\n", strerror(errno),err);
	fflush(stdout);
	AssertAlways(fclose(pending_file) == 0,
		     ("close failed"));
    
	return false;
    }
}

void *
dofile_pthread_start(void *arg)
{
    struct ifreq ifreq;

    memset(&ifreq, 0, sizeof(ifreq));
    strcpy(ifreq.ifr_name,interface_name);
    struct driver_packet_dump_ioctl dpd;
    memset(&dpd, 0, sizeof(dpd));
    ifreq.ifr_data = (char *)&dpd;

    mutex.lock();
    int my_id = unique_id_count;
    ++unique_id_count;
    ++threads_running;
    cond.signal();
    mutex.unlock();
    while(dpd_dofile(&ifreq,&dpd,my_id)) {
	mutex.lock();
	if (!everyone_ok) {
	    printf("everyone not ok, worker %d exiting\n", my_id);
	    fflush(stdout);
	    mutex.unlock();
	    break;
	}
	mutex.unlock();
    }
    printf("finishing dumper %d\n",my_id);
    fflush(stdout);
    mutex.lock();
    everyone_ok = false;
    --threads_running;
    cond.signal();
    mutex.unlock();
    printf("dumper returning\n");
    fflush(stdout);
    return NULL;
}

double
gettime()
{
    struct timeval tv;
    AssertAlways(gettimeofday(&tv,NULL) == 0,("gettimeofday failed"));
    return tv.tv_sec + tv.tv_usec/1.0e6;
}

void
print_stats(struct driver_packet_dump_ioctl &cur, double curtime,
	    struct driver_packet_dump_ioctl &prev, double prevtime,
	    const string &prefix)
{
    double elapsed_time = curtime - prevtime;
    if (elapsed_time < 0.01) {
	printf("whoa, instant return\n");
	fflush(stdout);
	return;
    }
    double pps = (cur.copy_packets - prev.copy_packets) / elapsed_time;
    double bps = (cur.copy_bytes - prev.copy_bytes) / elapsed_time;
    double dpps = (cur.drop_packets - prev.drop_packets) / elapsed_time;
    double dbps = (cur.drop_bytes - prev.drop_bytes) / elapsed_time;

    time_t now = (time_t)curtime;

    struct tm *date = localtime(&now);

    printf("%02d-%02d %02d:%02d:%02d.%02d - %s: %.3f kpps, %.6f MiB/s; drops: %.0f pps %.2f KiB/s\n",
	   date->tm_mon+1,date->tm_mday, date->tm_hour, date->tm_min, date->tm_sec,
	   (int)floor(100*(curtime - (double)now)),
	   prefix.c_str(), pps/1024.0, bps/(1024.0*1024.0), 
	   dpps, dbps/1024.0);
    fflush(stdout);
}

void *
dostats_pthread_start(void *arg)
{
    struct ifreq ifreq;
    struct timeval tv;

    memset(&ifreq, 0, sizeof(ifreq));
    strcpy(ifreq.ifr_name,interface_name);
    struct driver_packet_dump_ioctl dpd;
    memset(&dpd, 0, sizeof(dpd));
    ifreq.ifr_data = (char *)&dpd;

    mutex.lock();
    ++threads_running;
    cond.signal();
    mutex.unlock();

    printf("starting stats thread...\n");

    struct driver_packet_dump_ioctl start; double start_time, last_long_stats;
    struct driver_packet_dump_ioctl dpd_prev; double dpd_prev_time;
    struct driver_packet_dump_ioctl dpd_prev_60; double dpd_prev_60_time;

    dpd.subcmd = driver_packet_dump_getstats;

    AssertAlways(ioctl(socket_fd, SIOCDRIVERPACKETDUMP, &ifreq) == 0,
		 ("error in test: %s\n",strerror(errno)));
    start = dpd;
    dpd_prev = dpd;
    dpd_prev_60 = dpd;

    dpd_prev_60_time = dpd_prev_time = last_long_stats = start_time = gettime();
    tv.tv_sec = quick_stats_interval;
    tv.tv_usec = 0;
    select(0,NULL,NULL,NULL,&tv);

    double quick_stats_interval_min = (double)quick_stats_interval - 0.05;

    char quick_header[30];
    sprintf(quick_header,"%ds int",quick_stats_interval);

    while(true) {
	AssertAlways(ioctl(socket_fd, SIOCDRIVERPACKETDUMP, &ifreq) == 0,
		     ("error in test: %s\n",strerror(errno)));
	double curtime = gettime();
	if ((curtime - dpd_prev_time) >= quick_stats_interval_min) {
	    print_stats(dpd,curtime,dpd_prev,dpd_prev_time,quick_header);
	    dpd_prev = dpd; dpd_prev_time = curtime;
	    if ((curtime - dpd_prev_60_time) >= 60.0) {
		print_stats(dpd,curtime,dpd_prev_60,dpd_prev_60_time,"60s int");
		mutex.lock();
		printf("  last output index %d; %.2f GiB total output\n",
		       last_output_filenum, (double)total_output_bytes/(1024.0*1024.0*1024.0));
		fflush(stdout);
		mutex.unlock();
		dpd_prev_60 = dpd;
		dpd_prev_60_time = curtime;
	    }
	    if ((curtime - last_long_stats) >= 600.0) {
		print_stats(dpd,curtime,start,start_time,"since start");
		last_long_stats = curtime;
	    }
	}

	mutex.lock();
	if (stop_stats) {
	    printf("finishing stats threads.\n");
	    fflush(stdout);
	    print_stats(dpd,curtime,start,start_time,"overall");
	    --threads_running;
	    cond.signal();
	    mutex.unlock();
	    break;
	}
	mutex.unlock();
	
	tv.tv_sec = quick_stats_interval;
	tv.tv_usec = 0;
	select(0,NULL,NULL,NULL,&tv);
    }
    printf("stats thread returning\n");
    fflush(stdout);
    return NULL;
}

void
usage(char *progname)
{
    printf("Usage: %s [-b <buffer_mb>] [-h (help)]\n",
	   progname);
    printf("     [-i <interface-name>] [-o <output-basename>] [-t <nthreads>]\n");
    printf("     [-s (getstats)] [-q <quick-stats-interval-secs>]\n");
    printf("     default basename: %s\n", output_basename);
    printf("     interface name: %s\n", interface_name);
    printf("     buffer_size: %dMB\n",buffer_size/(1024*1024));
    printf("     threads: %d\n",nthreads);
}

void
get_driverdump_options(int argc, char *argv[])
{
    while(1) {
	int opt = getopt(argc, argv, "b:hi:n:o:q:st:z");
	if (opt == -1) break;
	switch(opt) 
	    {
	    case 'b': 
		buffer_size = atoi(optarg) * 1024 * 1024;
		if (buffer_size == 0) {
		    buffer_size = 262144;
		    printf("using special case buffer size of %d bytes\n", buffer_size);
		}
		AssertAlways(buffer_size > 0,("invalid buffer size\n"));
		break;
	    case 'h':
		usage(argv[0]);
		exit(1);
		break;
	    case 'i': 
		interface_name = new char[strlen(optarg)+1];
		strcpy(interface_name,optarg);
		break;
	    case 'n':
		global_max_file_count = atoi(optarg);
		AssertAlways(global_max_file_count > 0, ("no"));
		break;
	    case 'o':
		strcpy(output_basename,optarg);
		break;
	    case 'q':
		quick_stats_interval = atoi(optarg);
		AssertAlways(quick_stats_interval > 0,("invalid quick stats interval"));
		break;
	    case 's': 
		getstats = true;
		break;
	    case 't':
		nthreads = atoi(optarg);
		AssertAlways(nthreads > 0,("invalid nthreads\n"));
		break;
	    case 'z':
		do_driverdump_test = true;
		break;
	    case '?':
		usage(argv[0]);
		AssertFatal(("invalid option %c",optopt));
		break;
	    default:
		AssertFatal(("internal"));
	    }
    }
    if (optind != argc) {
	usage(argv[0]);
	AssertFatal(("unexpected option %s",argv[optind]));
    }
}

void
promiscuous_modify(int fd, char *interface_name, bool enable)
{
    struct ifreq	ifr;

    memset(&ifr, 0, sizeof(ifr));
    strncpy(ifr.ifr_name, interface_name, sizeof(ifr.ifr_name));

    if (ioctl(fd, SIOCGIFINDEX, &ifr) == -1) {
	perror("ioctl");
	abort();
    }

    int device_id = ifr.ifr_ifindex;

    // from libpcap-0.7.2: pcap-linux.c
    struct packet_mreq	mr;
    memset(&mr, 0, sizeof(mr));
    mr.mr_ifindex = device_id;
    mr.mr_type    = PACKET_MR_PROMISC;
    if (setsockopt(fd, SOL_PACKET, 
		   enable ? PACKET_ADD_MEMBERSHIP : PACKET_DROP_MEMBERSHIP,
		   &mr, sizeof(mr)) == -1)
	{
	    perror("setsockopt1");
	    abort();
	}
}

static char output_basename_default[] = "/mnt/tmpfs/network";
static char interface_name_default[] = "eth0";

int
main(int argc, char *argv[])
{
    output_basename = output_basename_default;
    interface_name = interface_name_default;
    buffer_size = 200*1024*1024;

    get_driverdump_options(argc, argv);

    struct ifreq ifreq;

    memset(&ifreq, 0, sizeof(ifreq));
    strcpy(ifreq.ifr_name,interface_name);
    struct driver_packet_dump_ioctl dpd;
    memset(&dpd, 0, sizeof(dpd));

    socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
    AssertAlways(socket_fd > 0,("can't get socket: %s",strerror(errno)));

    dpd.subcmd = driver_packet_dump_finish;
    ifreq.ifr_data = (char *)&dpd;

    if (getstats) {
	dpd.subcmd = driver_packet_dump_getstats;
	AssertAlways(ioctl(socket_fd, SIOCDRIVERPACKETDUMP, &ifreq) == 0,
		     ("error in test: %s\n",strerror(errno)));
	cout << boost::format("copy: packets %d; bytes %d\n")
	    % dpd.copy_packets % dpd.copy_bytes;
	cout << boost::format("drop: packets %d; bytes %d\n")
	    % dpd.drop_packets % dpd.drop_bytes;
	exit(0);
    }

    int err = ioctl(socket_fd, SIOCDRIVERPACKETDUMP, &ifreq);
    if (err == 0 || (err == -1 && errno == ENOENT)) {
	// ok
    } else {
	AssertFatal(("prepare 'finish' ioctl failed: (%d,%d) %s",
		     err,errno,strerror(errno)));
    }

    if (do_driverdump_test) {
	dpd.subcmd = driver_packet_dump_test;
	sprintf(dpd.filename,"/mnt/tmpfs/drvdump.1731.pending");
	FILE *pending_file = fopen(dpd.filename,"w");
	AssertAlways(fseek(pending_file,1024*1024,SEEK_SET) == 0,
		     ("fseek failed"));
	AssertAlways(fwrite(&dpd.npages,sizeof(int),1,pending_file) == 1,
		     ("write failed"));
	AssertAlways(fflush(pending_file) == 0,("fflush failed"));
	AssertAlways(fclose(pending_file) == 0,
		     ("close failed"));
	AssertAlways(ioctl(socket_fd, SIOCDRIVERPACKETDUMP, &ifreq) == 0,
		     ("error in test: %s\n",strerror(errno)));
	printf("**** TEST SUCCESSFULL\n");
	exit(0);
    }

    everyone_ok = true;
    struct sigaction sa;
    sa.sa_handler = signal_handler;
    sigemptyset(&(sa.sa_mask));
    sa.sa_flags = 0;
    AssertAlways(sigaction(SIGINT,&sa,0) == 0 && sigaction(SIGHUP,&sa,0) == 0 &&
		 sigaction(SIGTERM,&sa,0) == 0,
		 ("sigaction failed"));

    dpd.subcmd = driver_packet_dump_setup;
    page_size = ioctl(socket_fd, SIOCDRIVERPACKETDUMP, &ifreq);
    AssertAlways(page_size > 0,
		 ("bad pagesize %d %s\n",page_size,strerror(errno)));

    for(int i=1;i<nthreads;i++) {
	pthread_t tid;
	printf("starting file thread\n");
	AssertAlways(pthread_create(&tid,NULL,dofile_pthread_start,NULL) == 0,
		     ("pthread_create failed"));
    }

    bool do_stats_thread = true || buffer_size > 64*1024;
    if (do_stats_thread) {
	pthread_t tid;
	AssertAlways(pthread_create(&tid,NULL,dostats_pthread_start,NULL) == 0,
		     ("pthread_create failed"));
    } else {
	printf("skipping stats thread\n");
	mutex.lock();
	++threads_running;
	mutex.unlock();
    }

    mutex.lock();
    while(threads_running != nthreads) { // n-1 file threads, 1 stats thread
	cond.wait(mutex);
    }
    mutex.unlock();

    int promisc_fd=socket(PF_PACKET, SOCK_RAW, 0);
    AssertAlways(promisc_fd > 0,("error on socket\n"));

    // enable promiscuous....
    promiscuous_modify(promisc_fd, interface_name,true);

    dofile_pthread_start(NULL);
    mutex.lock();
    while(threads_running > 1) {
	cond.wait(mutex);
    }
    stop_stats = true;
    if (do_stats_thread == false) {
	threads_running--;
    }
    while (threads_running > 0) {
	printf("waiting %d\n",threads_running);
	fflush(stdout);
	cond.wait(mutex);
    }
    mutex.unlock();
    printf("closing down packet dumping...\n");
    fflush(stdout);
    promiscuous_modify(promisc_fd, interface_name,false);

    dpd.subcmd = driver_packet_dump_finish;
    AssertAlways(ioctl(socket_fd, SIOCDRIVERPACKETDUMP, &ifreq) == 0,
		 ("'finish' ioctl failed: (%d,%s)\n",errno,strerror(errno)));

    printf("exiting...\n");
    fflush(stdout);
    exit(0);
}
