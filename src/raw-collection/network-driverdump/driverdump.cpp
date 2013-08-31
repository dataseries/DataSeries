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

/*
  =pod

  =head1 NAME

  network-driverdump - faster packet capture than lindump-mmap and tcpdump on linux

  =head1 SYNOPSIS

  % network-driverdump [-i I<interface> ] [-o I<output-basename> ] [other options]

  =head1 DESCRIPTION

  When capturing full packet traces, occasionally lindump-mmap is still not sufficiently fast.  In
  that situation, if the nic driver has been patched to support driverdump, then the
  network-driverdump program can be used to control the drivers dumping of data directly to a file
  entirely bypassing the networking stack.  For small packets, driverdump can capture packets faster
  than the linux network stack can drop them (tcpdump host <no-such-host>) because it can avoid
  packet allocation code.  However, this requires dedicating an interface to packet capture, and
  patching the kernel driver.

  =head1 OPTIONS

  =over 4 

  =item -b I<buffer_mb>

  Specify the size of the buffer, and the size of each resulting file.

  =item -h 

  Get help

  =item -i I<interface-name>

  Specify the interface for packet capture.

  =item -o I<output-basename>

  Specify the base output name for the files. Usually this would be /dev/shm

  =item -t I<nthreads>

  Specify the number of threads that should be used with driverdump.  3 is usually sufficient to keep
  the kernel from ever failing to have a buffer to use for capture because the threads are doing
  relatively little work.

  =item -s

  Get driverdump statistics from the interface and then exit.

  =item -q I<quick-stats-interval-secs>

  Print interim statistics every I<quick-stats-interval-secs> seconds.  By default statistics are
  printed when the files are rotated.  This option helps verify that capture is working even when the
  network is lightly loaded and so is not generating enough traffic to rotate files.

  =back

  =head1 SEE ALSO

  tcpdump(1), /usr/share/doc/DataSeries/fast2009-nfs-analysis.pdf

  =cut
*/

// TODO: on shutdown with 3 threads in the kernel, sending a signal to
// the parent thread isn't enough to get all of them to exit.  Need to
// think about the right way to handle this problem, trivial fix right
// now is to make stop-tracing send a signal to all of the threads.

#include <string>

#include <Lintel/AssertBoost.hpp>
#include <Lintel/PThread.hpp>
#include <linux/wait.h>
#include "kernel-driver-add.h"

using namespace std;
using boost::format;

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
    cerr << format("signal %d received %d count, abort after 20") % sig % sigcount << endl;
    everyone_ok = false;
    if (sigcount > 20)
        abort();
}

// true on sucess
bool
check_enough_freespace(FILE *fp)
{
    struct statvfs fs_stats;
    CHECKED(fstatvfs(fileno(fp),&fs_stats) == 0, format("fstatfs failed: %s") % strerror(errno));

    if (false) {
        cout << boost::format("freespace %ld available blocks %ld free blocks %ld blocksize\n")
                % fs_stats.f_bavail % fs_stats.f_bfree % fs_stats.f_bsize;
    }
    long long freebytes = (long long)fs_stats.f_bsize * (long long)fs_stats.f_bavail;
    long long needbytes = (long long)buffer_size * (long long)nthreads;
    if (freebytes < needbytes) {
        cout << format("Not enough free space on output filesystem, %d < %d")
                % freebytes % needbytes << endl;
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
    INVARIANT(pending_file != NULL, "can't open file");

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

    CHECKED(fwrite(&hdr,sizeof(hdr),1,pending_file) == 1, "write failed");
    CHECKED(fseek(pending_file,dpd->npages * page_size,SEEK_SET) == 0, "fseek failed");
    CHECKED(fwrite(&dpd->npages,sizeof(int),1,pending_file) == 1, "write failed");
    CHECKED(fflush(pending_file) == 0, "fflush failed");
    if (!check_enough_freespace(pending_file)) {
        fclose(pending_file);
        return false;
    }
    dpd->subcmd = driver_packet_dump_file;
    if (debug_dpd_dofile) {
        cout << format("%d make ioctl %s\n") % my_unique_id % dpd->filename;
    }
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
        if (debug_dpd_dofile) {
            cout << format("dofile %d bytes, idx %d -> %s\n")
                    % dpd->bytes_written % dpd->file_index % foo;
        }
        CHECKED(ftruncate(fileno(pending_file), dpd->bytes_written) == 0,
                format("truncate failed: %s") % strerror(errno));
        CHECKED(fclose(pending_file) == 0, "close failed");
    
        CHECKED(rename(dpd->filename,foo) == 0, "rename failed");
        cout << format("finished with file %s, %.2f GiB total output\n")
                % foo % ((double)tmp_bytes/(1024.0*1024.0*1024.0));
        fflush(stdout);
        delete foo;
        if ((dpd->file_index+1) >= global_max_file_count) {
            cout << "Finished with all files\n";
            return false;
        }
        return true;
    } else {
        cout << format("failure on processing file: %s (%d)") % strerror(errno) % err << endl;
        CHECKED(fclose(pending_file) == 0, "close failed");
    
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
    while (dpd_dofile(&ifreq,&dpd,my_id)) {
        mutex.lock();
        if (!everyone_ok) {
            cout << format("everyone not ok, worker %d exiting") % my_id << endl;
            mutex.unlock();
            break;
        }
        mutex.unlock();
    }
    cout << format("finishing dumper %d") % my_id << endl;
    mutex.lock();
    everyone_ok = false;
    --threads_running;
    cond.signal();
    mutex.unlock();
    cout << "dumper returning" << endl;
    return NULL;
}

double
gettime()
{
    struct timeval tv;
    CHECKED(gettimeofday(&tv,NULL) == 0, "gettimeofday failed");
    return tv.tv_sec + tv.tv_usec/1.0e6;
}

void
print_stats(struct driver_packet_dump_ioctl &cur, double curtime,
            struct driver_packet_dump_ioctl &prev, double prevtime,
            const string &prefix)
{
    double elapsed_time = curtime - prevtime;
    if (elapsed_time < 0.01) {
        cout << "whoa, instant return" << endl;
        return;
    }
    double pps = (cur.copy_packets - prev.copy_packets) / elapsed_time;
    double bps = (cur.copy_bytes - prev.copy_bytes) / elapsed_time;
    double dpps = (cur.drop_packets - prev.drop_packets) / elapsed_time;
    double dbps = (cur.drop_bytes - prev.drop_bytes) / elapsed_time;

    time_t now = (time_t)curtime;

    struct tm *date = localtime(&now);

    cout << format("%02d-%02d %02d:%02d:%02d.%02d - %s: %.3f kpps, %.6f MiB/s; drops: %.0f pps %.2f KiB/s\n")
            % (date->tm_mon+1) % date->tm_mday % date->tm_hour % date->tm_min % date->tm_sec
            % ((int)floor(100*(curtime - (double)now))) % prefix % (pps/1024.0)
            % (bps/(1024.0*1024.0)) % dpps % (dbps/1024.0);
    cout.flush();
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

    cout << "starting stats thread...\n";

    struct driver_packet_dump_ioctl start; double start_time, last_long_stats;
    struct driver_packet_dump_ioctl dpd_prev; double dpd_prev_time;
    struct driver_packet_dump_ioctl dpd_prev_60; double dpd_prev_60_time;

    dpd.subcmd = driver_packet_dump_getstats;

    CHECKED(ioctl(socket_fd, SIOCDRIVERPACKETDUMP, &ifreq) == 0,
            format("error in test: %s") % strerror(errno));
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

    while (true) {
        CHECKED(ioctl(socket_fd, SIOCDRIVERPACKETDUMP, &ifreq) == 0,
                format("error in test: %s") % strerror(errno));
        double curtime = gettime();
        if ((curtime - dpd_prev_time) >= quick_stats_interval_min) {
            print_stats(dpd,curtime,dpd_prev,dpd_prev_time,quick_header);
            dpd_prev = dpd; dpd_prev_time = curtime;
            if ((curtime - dpd_prev_60_time) >= 60.0) {
                print_stats(dpd,curtime,dpd_prev_60,dpd_prev_60_time,"60s int");
                mutex.lock();
                cout << format("  last output index %d; %.2f GiB total output\n")
                        % last_output_filenum % ((double)total_output_bytes/(1024.0*1024.0*1024.0));
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
            cout << "finishing stats threads.\n";
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
    cout << "stats thread returning\n";
    fflush(stdout);
    return NULL;
}

void
usage(char *progname)
{
    cout << format("Usage: %s [-b <buffer_mb>] [-h (help)]\n") % progname;
    cout << format("     [-i <interface-name>] [-o <output-basename>] [-t <nthreads>]\n");
    cout << format("     [-s (getstats)] [-q <quick-stats-interval-secs>]\n");
    cout << format("     default basename: %s\n") % output_basename;
    cout << format("     interface name: %s\n") % interface_name;
    cout << format("     buffer_size: %dMB\n") % (buffer_size/(1024*1024));
    cout << format("     threads: %d\n") % nthreads;
}

void
get_driverdump_options(int argc, char *argv[])
{
    while (1) {
        int opt = getopt(argc, argv, "b:hi:n:o:q:st:z");
        if (opt == -1) break;
        switch(opt) 
        {
            case 'b': 
                buffer_size = atoi(optarg) * 1024 * 1024;
                if (buffer_size == 0) {
                    buffer_size = 262144;
                    cout << format("using special case buffer size of %d bytes\n") % buffer_size;
                }
                INVARIANT(buffer_size > 0, "invalid buffer size");
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
                SINVARIANT(global_max_file_count > 0);
                break;
            case 'o':
                strcpy(output_basename,optarg);
                break;
            case 'q':
                quick_stats_interval = atoi(optarg);
                INVARIANT(quick_stats_interval > 0, "invalid quick stats interval");
                break;
            case 's': 
                getstats = true;
                break;
            case 't':
                nthreads = atoi(optarg);
                INVARIANT(nthreads > 0, "invalid nthreads");
                break;
            case 'z':
                do_driverdump_test = true;
                break;
            case '?':
                usage(argv[0]);
                FATAL_ERROR(format("invalid option %c") % optopt);
                break;
            default:
                FATAL_ERROR("internal");
        }
    }
    if (optind != argc) {
        usage(argv[0]);
        FATAL_ERROR(format("unexpected option %s") % argv[optind]);
    }
}

void
promiscuous_modify(int fd, char *interface_name, bool enable)
{
    struct ifreq        ifr;

    memset(&ifr, 0, sizeof(ifr));
    strncpy(ifr.ifr_name, interface_name, sizeof(ifr.ifr_name));

    if (ioctl(fd, SIOCGIFINDEX, &ifr) == -1) {
        perror("ioctl");
        abort();
    }

    int device_id = ifr.ifr_ifindex;

    // from libpcap-0.7.2: pcap-linux.c
    struct packet_mreq  mr;
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
    INVARIANT(socket_fd > 0, format("can't get socket: %s") % strerror(errno));

    dpd.subcmd = driver_packet_dump_finish;
    ifreq.ifr_data = (char *)&dpd;

    if (getstats) {
        dpd.subcmd = driver_packet_dump_getstats;
        CHECKED(ioctl(socket_fd, SIOCDRIVERPACKETDUMP, &ifreq) == 0,
                format("error in test: %s") % strerror(errno));
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
        FATAL_ERROR(format("prepare 'finish' ioctl failed: (%d,%d) %s")
                    % err % errno % strerror(errno));
    }

    if (do_driverdump_test) {
        dpd.subcmd = driver_packet_dump_test;
        sprintf(dpd.filename,"/mnt/tmpfs/drvdump.1731.pending");
        FILE *pending_file = fopen(dpd.filename,"w");
        CHECKED(fseek(pending_file,1024*1024,SEEK_SET) == 0, "fseek failed");
        CHECKED(fwrite(&dpd.npages,sizeof(int),1,pending_file) == 1, "write failed");
        CHECKED(fflush(pending_file) == 0, "fflush failed");
        CHECKED(fclose(pending_file) == 0, "close failed");
        CHECKED(ioctl(socket_fd, SIOCDRIVERPACKETDUMP, &ifreq) == 0,
                format("error in test: %s") % strerror(errno));
        cout << "**** TEST SUCCESSFULL\n";
        exit(0);
    }

    everyone_ok = true;
    struct sigaction sa;
    sa.sa_handler = signal_handler;
    sigemptyset(&(sa.sa_mask));
    sa.sa_flags = 0;
    CHECKED(sigaction(SIGINT,&sa,0) == 0 && sigaction(SIGHUP,&sa,0) == 0 &&
            sigaction(SIGTERM,&sa,0) == 0, "sigaction failed");
            

    dpd.subcmd = driver_packet_dump_setup;
    page_size = ioctl(socket_fd, SIOCDRIVERPACKETDUMP, &ifreq);
    INVARIANT(page_size > 0, format("bad pagesize %d %s\n") % page_size % strerror(errno));

    for (int i=1;i<nthreads;i++) {
        pthread_t tid;
        cout << "starting file thread\n";
        CHECKED(pthread_create(&tid,NULL,dofile_pthread_start,NULL) == 0,
                "pthread_create failed");
    }

    bool do_stats_thread = true || buffer_size > 64*1024;
    if (do_stats_thread) {
        pthread_t tid;
        CHECKED(pthread_create(&tid,NULL,dostats_pthread_start,NULL) == 0,
                "pthread_create failed");
    } else {
        cout << "skipping stats thread\n";
        mutex.lock();
        ++threads_running;
        mutex.unlock();
    }

    mutex.lock();
    while (threads_running != nthreads) { // n-1 file threads, 1 stats thread
        cond.wait(mutex);
    }
    mutex.unlock();

    int promisc_fd=socket(PF_PACKET, SOCK_RAW, 0);
    INVARIANT(promisc_fd > 0, "error on socket");

    // enable promiscuous....
    promiscuous_modify(promisc_fd, interface_name,true);

    dofile_pthread_start(NULL);
    mutex.lock();
    while (threads_running > 1) {
        cond.wait(mutex);
    }
    stop_stats = true;
    if (do_stats_thread == false) {
        threads_running--;
    }
    while (threads_running > 0) {
        cout << format("waiting %d") % threads_running << endl;
        cond.wait(mutex);
    }
    mutex.unlock();
    cout << "closing down packet dumping...\n";
    fflush(stdout);
    promiscuous_modify(promisc_fd, interface_name,false);

    dpd.subcmd = driver_packet_dump_finish;
    CHECKED(ioctl(socket_fd, SIOCDRIVERPACKETDUMP, &ifreq) == 0,
            format("'finish' ioctl failed: (%d,%s)") % errno % strerror(errno));

    cout << "exiting..." << endl;
    exit(0);
}
