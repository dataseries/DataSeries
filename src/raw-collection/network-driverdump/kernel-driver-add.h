#define SIOCDRIVERPACKETDUMP (0x89F0 + 13)
typedef enum { driver_packet_dump_test = 0, driver_packet_dump_setup,
	       driver_packet_dump_finish, driver_packet_dump_file,
               driver_packet_dump_getstats, driver_packet_dump_abort } 
    dpd_command;

#define DRIVER_PACKET_DUMP_FILENAMELEN 64
struct driver_packet_dump_ioctl {
    dpd_command subcmd;
    /* could union arguments to different commands, but who cares */
    int immediate_skb_free;
    char filename[DRIVER_PACKET_DUMP_FILENAMELEN];
    int npages;
    int bytes_written;
    int file_index;
    uint64_t copy_packets, copy_bytes, drop_packets, drop_bytes;
};

#ifdef __KERNEL__

#define DPD_PCAP_FILE_HEADER_LEN 24

// same as in pcap.h, but with kernel-style typedefs
struct drvdump_pcap_pkthdr {
    uint32_t tv_sec;
    uint32_t tv_fractional; // pcap.h thinks this should be usec, we may want to make it nsec or 1/2^32 sec
    uint32_t caplen;
    uint32_t len;
};
    
#define DRIVERDUMP_MAGIC 0x1972FEED

struct driver_dump_data {
    int magic, npages, outputidxnum, cur_write_offset, max_write_offset, done_fill;
    struct file *fp;
    struct page **pages;
    wait_queue_head_t thread_waiting;
};

// must call init_waitqueue_head
struct driver_dump_privdata {
    struct semaphore sem;
    int dumping, immediate_skb_free, cur_idx_num, test_mode;
    // ioctl fills the empty pointer, later will be woken to clear the
    // filled pointer, interrupt handler moves pointer from empty to
    // filling and filling to filled.
    struct driver_dump_data *empty, *filling, *filled;
    uint64_t copy_packets, copy_bytes, drop_packets, drop_bytes;
    wait_queue_head_t wait_empty;
};

#endif
