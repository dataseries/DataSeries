/* 
   To add this to a driver:
   0) for 2.4.*, add:
        EXPORT_SYMBOL(dev_probe_lock);
        EXPORT_SYMBOL(dev_probe_unlock);
      to kernel/kmod.c near the bottom above EXPORT_SYMBOL(exec_usermodehelper);
      this is needed to undo the locking in net/core/dev.c below SIOCDEVPRIVATE
      which otherwise serializes all of the device-private ioctls
   1) add #include ".../driverdump/kern-drvdump.h" to includes
   2) add struct driver_dump_privdata driverdump to the driver private data
   3) add #include ".../driverdump/kernel-driver-add.c" to file after structure
      definition/include of header file with structure def, but near top
   4) add do_driver_packet_dump_init(&lp->driverdump); to the probe function,
      say near the SET_MODULE_OWNER(dev) call, but after any zeroing of the
      private driver structure.
   5) add:
               if (lp->driverdump.dumping) {
                   do_driver_packet_dump_rx_skb(&lp->driverdump,skb,0);
		   ... something to reset the rx buffer status so it ...
		   ... is reused. code for this usually have ...
		   ... skb_reserve(skb, NET_IP_ALIGN) and dev_alloc_skb( ...
		   ... in it. ...
               } else {
	           ... standard handling of eth_type_trans netif_rx 142...
               }
      to the interrupt or rx handler, above any call to eth_type_trans or
      netif_rx/netif_receive_skb
   6) add do_driver_packet_dump_finish(&lp->driverdump);
      to the close function (inside or outside of any locking doesn't matter)
   7) add:
               if (cmd == SIOCDEVPRIVATE + 13) {
                    return do_driver_packet_dump(&lp->driverdump,rq);
               }
      to the ioctl function

KNOWN BUGS: 

  1) if you call the file ioctl with not enough space on the
     filesystem to allocate the file, then the file that is so
     "created" will not be deletable; we bug out at shmem.c:450 it is
     not clear why this is happening, we free as many pages as we
     successfully allocated, and it happens regardless of whether the
     file is open or closed at user level.  We work around this in the
     user level driverdump code that makes sure we have enough free
     space :)

*/

static const int dpd_debug_memcpy = 0; // levels 0,1,2
static const int dpd_debug_ioctl_locking = 0;
static const int dpd_debug_packet_dump_file = 0;
static const int dpd_debug_file_open_close = 0;
static const int dpd_debug_promote_buffer = 0;
static const int dpd_debug_kmap = 0;
static const int dpd_debug_rx_skb = 0;
static const int dpd_debug_sleep_call = 0;

static const int dpd_debug_test_alloc_free = 0; // turning this on will make dumping impossible.
static const int dpd_debug_test_only_test_mode_rx = 0; // turning this on will make dumping impossible.

#include <linux/fs.h>
#include <linux/pagemap.h>
#include <linux/time.h>
#include <linux/rtnetlink.h>
#include <asm/uaccess.h>

void
do_driverdump_skb_reset(struct sk_buff *skb, int expected_size)
{
    int align_expected_size = SKB_DATA_ALIGN(expected_size);
    if (unlikely(skb->len != skb->tail - skb->data)) {  // internal sanity check
	printk("size mismatch %d != %d\n",
	       skb->len, skb->tail - skb->data);
	BUG();
    }

    if (unlikely((skb->truesize != (skb->end - skb->head + sizeof(struct sk_buff)))
		 || (align_expected_size != skb->truesize - sizeof(struct sk_buff)))) {
	printk("size mismatch %d != %d || %d -> %d != %d\n",skb->truesize,
	       skb->end - skb->head + sizeof(struct sk_buff),
	       expected_size,align_expected_size,skb->truesize - sizeof(struct sk_buff));
	BUG();
    }
    // following initialization order from skbuff.c: alloc_skb:
    //   * truesize, head, end - checked above 
    skb->data = skb->head;
    skb->tail = skb->head;
    skb->len = 0;
    //   * truesize, head, data, tail, end, len now set
    if (unlikely(skb->cloned != 0 || skb->data_len != 0 || atomic_read(&skb->users) != 1 ||
		 atomic_read(&(skb_shinfo(skb)->dataref)) != 1 || 
		 skb_shinfo(skb)->nr_frags != 0 || skb_shinfo(skb)->frag_list != NULL)) {
	printk("skb flag errors\n");
	BUG();
    }
    //   * truesize-len, cloned, data_len, users, shinfo->dataref, shinfo->nr_frags, shinfo->frag_list checked as ok.


    // and now the best part; in include/linux/skbuff.h in
    // __dev_alloc_skb, we reserve 16 bytes at the beginning of the
    // skbuff for "optimizations"; if we don't re-do this, some of the
    // sanity checks in the resetting code fail.  This is tolerable as
    // this code hasn't changed between 2.4.24 and 2.6.8, but is really
    // ugly

    skb_reserve(skb,16);

}

inline int
do_driver_packet_dump_can_fill(struct driver_dump_privdata *dd_priv,
			       struct sk_buff *skb)
{
    return dd_priv->filling == NULL || 
	(dd_priv->filling->cur_write_offset + skb->len + 16) > dd_priv->filling->max_write_offset;
}

void
do_driver_packet_dump_memcpy(struct driver_dump_privdata *dd_priv,
			     void *src, int len)
{
    struct driver_dump_data *filling = dd_priv->filling;
    if (dpd_debug_memcpy) {
	printk("dpdmemcpy(cwo=0x%x,%d,%d): ",filling->cur_write_offset,
	       filling->cur_write_offset,len);
    }
    while(1) {
	int page = filling->cur_write_offset >> PAGE_SHIFT;
	int offset = filling->cur_write_offset & (PAGE_SIZE - 1);
	int copy_amt = offset + len  <= PAGE_SIZE ? len : PAGE_SIZE - offset;
	void *pageaddr = NULL;
	if (dpd_debug_memcpy) printk(" (%d,%d,%d) ",page,offset,copy_amt);
	if (unlikely(page >= filling->npages))
	    BUG();
	{
	    enum fixed_addresses idx;
	    unsigned long vaddr;
	    idx = KM_SOFTIRQ1 + KM_TYPE_NR*smp_processor_id();
	    vaddr = __fix_to_virt(FIX_KMAP_BEGIN + idx);
	    if (dpd_debug_memcpy > 1) {
		printk("pre-kmap_atomic %d %p %d\n",
		       page,filling->pages[page],
		       pte_none(*(kmap_pte-idx)));
	    }
	}
	pageaddr = kmap_atomic(filling->pages[page], KM_SOFTIRQ1);
	if (dpd_debug_memcpy > 1) printk("post-kmap_atomic\n");
	if (pageaddr == NULL)
	    BUG();
	if (dpd_debug_memcpy > 1) printk("pre-memcpy\n");
	memcpy(pageaddr + offset, src, copy_amt);
	if (dpd_debug_memcpy > 1) printk("post-memcpy\n");
	kunmap_atomic(pageaddr, KM_SOFTIRQ1);
	if (dpd_debug_memcpy > 1) printk("post-kunmap_atomic\n");
	SetPageDirty(filling->pages[page]);
	if (dpd_debug_memcpy > 1) printk("post-dirty\n");
	len -= copy_amt;
	filling->cur_write_offset += copy_amt;

	if (unlikely(len > 0)) {
	    src = (void *)((char *)src + copy_amt);
	} else {
	    break;
	}
    }
    if (dpd_debug_memcpy) printk("\n");
}

static int last_rx_jiffies;

static void
do_driver_packet_dump_rx_skb(struct driver_dump_privdata *dd_priv,
			     struct sk_buff *skb,
			     int special_case_maclen)
{
    if (dd_priv->test_mode > 0) {
	--dd_priv->test_mode;
	printk("DPD test mode %d\n",dd_priv->test_mode);
	if (dd_priv->test_mode == 0) {
	    wake_up_all(&dd_priv->wait_empty);
	}
    }
    
    if (dpd_debug_rx_skb && (jiffies - last_rx_jiffies) >= HZ) {
	printk("DPD: in dump_rx_skb\n");
	last_rx_jiffies = jiffies;
    }

    if (dpd_debug_test_only_test_mode_rx) {
	return;
    }

    if (unlikely(do_driver_packet_dump_can_fill(dd_priv,skb))) {
	if (dd_priv->filling != NULL) {
	    if (dd_priv->filled == NULL) {
		if (dd_priv->filling->done_fill)
		    BUG();
		if (dpd_debug_promote_buffer)
		    printk("DPD promote buffer %d to filled\n",
			   dd_priv->filling->outputidxnum);
				/* promote buffer to filled */
		dd_priv->filling->done_fill = 1;
		dd_priv->filled = dd_priv->filling;
		dd_priv->filling = NULL;
		mb();
				/* theoretical race condition of job waking up
				   between when we move it to filled and when
				   we call wake up in that the dump_data could
				   be freed if the other thread somehow woke 
				   up.  Seems really unlikely, so ignore */
		wake_up_all(&dd_priv->filled->thread_waiting);
	    } else {
				/* can't promote, filled is still there */
	    }
	}
	if (dd_priv->filling == NULL && dd_priv->empty != NULL) {
	    /* promote empty buffer to filling */
	    if (dd_priv->empty->done_fill)
		BUG();
	    if (dpd_debug_promote_buffer)
		printk("DPD promote buffer %d to filling\n",
		       dd_priv->empty->outputidxnum);
	    dd_priv->filling = dd_priv->empty;
	    dd_priv->empty = NULL;
	    mb();
	    wake_up(&dd_priv->wait_empty);
	}
    }

    if (unlikely(do_driver_packet_dump_can_fill(dd_priv,skb))) {
	++dd_priv->drop_packets;
	dd_priv->drop_bytes += skb->len;
    } else {
	struct drvdump_pcap_pkthdr pkthdr;

	do_gettimeofday(&pkthdr.ts);
	pkthdr.caplen = skb->len;
	pkthdr.len = skb->len;
	
	if (unlikely(special_case_maclen)) {
	    if (special_case_maclen != 14) {
		printk("dpd bug %d\n",special_case_maclen);
		BUG();
	    }
	    pkthdr.len = (1 << 30) | special_case_maclen;
	}

	do_driver_packet_dump_memcpy(dd_priv,&pkthdr,16);
	do_driver_packet_dump_memcpy(dd_priv,skb->data,skb->len);

	++dd_priv->copy_packets;
	dd_priv->copy_bytes += skb->len + 16;
    }
			
}

static int 
do_driver_packet_dump_test(struct driver_packet_dump_ioctl *dpd_ioctl,
			   struct driver_dump_privdata *dd_privdata)
{
	struct file *fp;
	struct inode *inode;
	struct address_space *mapping;
	struct page *filepage;
	int ret;
	int err = 0;

	printk("dpd cmd = test\n");
	dd_privdata->dumping = 1;
	dd_privdata->test_mode = 8;
	ret = wait_event_interruptible(dd_privdata->wait_empty,
				       dd_privdata->test_mode == 0);
	if (ret == 0) {
	    printk("DPD wakeup!\n");
	} else {
	    printk("DPD interrupt!\n");
	}

	fp = filp_open(dpd_ioctl->filename, O_RDWR, 0660);
	if (IS_ERR(fp)) {
	    printk("Failed opening file %s (%ld)\n", dpd_ioctl->filename, -PTR_ERR(fp));
	    return PTR_ERR(fp);
	}
	printk("fiddle with file %s\n",dpd_ioctl->filename);

	inode = fp->f_dentry->d_inode;
	mapping = inode->i_mapping;
	filepage = read_cache_page(mapping, 0, 
				   (filler_t *)mapping->a_ops->readpage, NULL);
	printk("filepage %p %d %d\n",filepage,page_count(filepage),PageUptodate(filepage));
	if (IS_ERR(filepage)) {
	    printk("Error reading page: %ld\n",-PTR_ERR(filepage));
	    err = -EIO;
	} else if (filepage) {
	    void *addr = kmap(filepage);
	    if (addr == NULL) {
		printk("kmap failed?!\n");
	    } else {
		printk("kmap %p\n",addr);
		memcpy(addr,"hello world\nasdflakjsdflajsdflkjdsfl\n",32);
		kunmap(filepage);
	    }
	    page_cache_release(filepage);
	    err = 0;
	} else {
	    printk("huh");
	    err = -EINVAL;
	}
	filp_close(fp, 0);
	dd_privdata->dumping = 0;
	printk("test result %d\n",err);
	return err;
}

// probably ought to have this thing go to sleep indefinitely 
// and have that own the "dumping" state

static int do_driver_packet_dump_setup(struct driver_packet_dump_ioctl *dpd_ioctl,
				       struct driver_dump_privdata *dd_privdata)
{
    if (down_interruptible(&dd_privdata->sem))
	return -EINTR;
    if (dd_privdata->dumping) {
	up(&dd_privdata->sem);
	return -EBUSY;
    }
    dd_privdata->dumping = 1;
    dd_privdata->immediate_skb_free = dpd_ioctl->immediate_skb_free;
    dd_privdata->cur_idx_num = 0;
    dd_privdata->copy_packets = 0;
    dd_privdata->copy_bytes = 0;
    dd_privdata->drop_packets = 0;
    dd_privdata->drop_bytes = 0;
    if (dd_privdata->empty != NULL ||
	dd_privdata->filling != NULL ||
	dd_privdata->filled != NULL) {
	printk("dpd buffers not NULL\n");
	BUG();
    }
	   
    up(&dd_privdata->sem);
    return PAGE_SIZE;
}

static void do_driver_packet_dump_sleep(int wait_jiffies)
{
    wait_queue_head_t tmp_wq;
    int start_wait;
    if (dpd_debug_sleep_call)
	printk("DPD sleep call %d\n",wait_jiffies);
    init_waitqueue_head(&tmp_wq);
    start_wait = jiffies;
    while((jiffies - start_wait) < wait_jiffies) {
	int sleep_jiffies = wait_jiffies - (jiffies - start_wait);
	if (sleep_jiffies < 1) {
	    sleep_jiffies = 1;
	}
	/* don't want interruptible, once the interrupt happens we will spin loop */
	if (dpd_debug_sleep_call)
	    printk("DPD sleeping %d\n",sleep_jiffies);
	sleep_on_timeout(&tmp_wq,sleep_jiffies);
    }
    if (dpd_debug_sleep_call)
	printk("DPD done sleeping %d\n",wait_jiffies);
}

void
do_driver_dump_data_cleanup(struct driver_dump_data *new_ddd)
{
    int freed_pages,i;

    if (new_ddd->magic != DRIVERDUMP_MAGIC)
	BUG();
    new_ddd->magic = 0;
    freed_pages = -1;
    if (new_ddd->pages) {
	freed_pages = 0;
	for(i=0;i<new_ddd->npages;++i) {
	    if (new_ddd->pages[i] != NULL && !IS_ERR(new_ddd->pages[i])) {
		++freed_pages;
		page_cache_release(new_ddd->pages[i]);
		new_ddd->pages[i] = NULL;
	    }
	}
    }
    if (new_ddd->pages != NULL) {
	vfree(new_ddd->pages);
    }
    if (IS_ERR(new_ddd->fp) || new_ddd->fp == 0) {
	// ignore
    } else {
	if (dpd_debug_file_open_close) 
	    printk("DPD: close file %p; %d freed pages\n",new_ddd->fp,freed_pages);
	    
	filp_close(new_ddd->fp, 0);
    }
    // it would be nice to check no one is waiting on this
    kfree(new_ddd);
}

static int do_driver_packet_dump_finish(struct driver_dump_privdata *dd_privdata)
{
    if (down_interruptible(&dd_privdata->sem))
	return -EINTR;
    if (!dd_privdata->dumping) {
	up(&dd_privdata->sem);
	return -ENOENT;
    }
    printk("dpd finish\n");
    dd_privdata->dumping = 0;
    mb(); /* force out dumping change */

    if (dd_privdata->empty) {
	wake_up_all(&dd_privdata->empty->thread_waiting);
    }

    if (dd_privdata->filling) {
	wake_up_all(&dd_privdata->filling->thread_waiting);
    }

    if (dd_privdata->filled) {
	wake_up_all(&dd_privdata->filled->thread_waiting);
    }
	
    // can't find an easy better way to do this, we need to make sure
    // that the interrupt handler isn't going off and still using the
    // pointers; could disable interrupts on the driver, make sure
    // that the interrupt handler isn't running and do stuff, for
    // softirq/NAPI based things, we can schedule a task that does
    // this.  However, sleeping for 2 seconds after setting dumping to
    // 0 ought to be good enough :)

    do_driver_packet_dump_sleep(2*HZ); 

    if (dd_privdata->empty) {
	struct driver_dump_data *tmp = dd_privdata->empty;
	dd_privdata->empty = NULL;
	do_driver_dump_data_cleanup(tmp);
    }

    if (dd_privdata->filling) {
	struct driver_dump_data *tmp = dd_privdata->filling;
	dd_privdata->filling = NULL;
	do_driver_dump_data_cleanup(tmp);
    }

    if (dd_privdata->filled) {
	struct driver_dump_data *tmp = dd_privdata->filled;
	dd_privdata->filled = NULL;
	do_driver_dump_data_cleanup(tmp);
    }

    if (dd_privdata->empty != NULL ||
	dd_privdata->filling != NULL ||
	dd_privdata->filled != NULL) {
	printk("internal error, dd_privdata pointers not NULL\n");
	BUG();
    }
    up(&dd_privdata->sem);
    return 0;
}

static int
do_driver_packet_dump_locked_init_empty(struct driver_packet_dump_ioctl *dpd_ioctl,
					struct driver_dump_privdata *dd_privdata)
{
    struct driver_dump_data *new_ddd = NULL;
    int i, ret;

    if (dpd_ioctl->npages < 65536/PAGE_SIZE || 
	dpd_ioctl->npages > 256*1024*1024/PAGE_SIZE) {
	printk("DPD: allocation failure: npages out of bounds\n");
	goto alloc_failure;
    }
    new_ddd = kmalloc(sizeof(*new_ddd), GFP_KERNEL);
    if (new_ddd == NULL) {
	printk("DPD: allocation failure: on structure\n");
	goto alloc_failure;
    }
    memset(new_ddd,0,sizeof(*new_ddd));
    new_ddd->magic = DRIVERDUMP_MAGIC;
    new_ddd->npages = dpd_ioctl->npages;
    dpd_ioctl->filename[DRIVER_PACKET_DUMP_FILENAMELEN-1] = '\0';
    new_ddd->pages = vmalloc(new_ddd->npages * sizeof(struct page *));
    if (new_ddd->pages == NULL) {
	printk("DPD: allocation failure: on vectors %d, %d bytes %p\n",
	       new_ddd->npages * sizeof(struct page *),
	       new_ddd->npages * sizeof(void *),
	       new_ddd->pages);
	goto alloc_failure;
    }
    memset(new_ddd->pages,0,new_ddd->npages * sizeof(struct page *));
    if (dpd_debug_packet_dump_file)
	printk("DPD: opening file (%s) ...\n",dpd_ioctl->filename);
    new_ddd->fp = filp_open(dpd_ioctl->filename, O_WRONLY, 0660);
    if (dpd_debug_file_open_close)
	printk("DPD: open file %s -> %p\n",dpd_ioctl->filename,new_ddd->fp);
    if (IS_ERR(new_ddd->fp) || new_ddd->fp == NULL) {
	printk("dpd_init: failed opening file (%ld)\n", -PTR_ERR(new_ddd->fp));
	goto alloc_failure;
    }
    
    new_ddd->mapping = new_ddd->fp->f_dentry->d_inode->i_mapping;

    if (dpd_debug_packet_dump_file)
	printk("DPD: reading and mapping pages...\n");
    for(i = 0;i<new_ddd->npages;++i) {
	new_ddd->pages[i] 
	    = read_cache_page(new_ddd->mapping, i,
			      (filler_t *)new_ddd->mapping->a_ops->readpage, 
			      NULL);
	if (IS_ERR(new_ddd->pages[i]) || !PageUptodate(new_ddd->pages[i])) {
	    printk("dpd: unable to read page %d from %s (%ld)\n",
		   i,dpd_ioctl->filename, -PTR_ERR(new_ddd->pages[i]));
	    goto alloc_failure;
	}
    }
    if (dpd_debug_packet_dump_file)
	printk("DPD: read and mapped pages\n");
    if (dd_privdata->empty != NULL)
	BUG();
    new_ddd->nkmapped = new_ddd->npages;
    new_ddd->outputidxnum = dd_privdata->cur_idx_num;
    ++dd_privdata->cur_idx_num;
    new_ddd->cur_write_offset = DPD_PCAP_FILE_HEADER_LEN; 
    new_ddd->max_write_offset = new_ddd->npages * PAGE_SIZE;
    new_ddd->done_fill = 0;
    init_waitqueue_head(&new_ddd->thread_waiting);
    if (dpd_debug_test_alloc_free) {
	goto alloc_failure;
    }
    dd_privdata->empty = new_ddd;
    up(&dd_privdata->sem);

    if (dpd_debug_packet_dump_file)
	printk("DPD: waiting to fill buffer(%p) #%d\n",
	       new_ddd,dd_privdata->cur_idx_num);
    ret = wait_event_interruptible(new_ddd->thread_waiting,
				   new_ddd->done_fill ||
				   dd_privdata->dumping == 0);
    if (dpd_debug_packet_dump_file)
	printk("DPD: returned from fill buffer #%d %d\n",
	       new_ddd->outputidxnum, ret);
    if (ret != 0 || dd_privdata->dumping == 0) {
	return -EINTR;
    } 
    if (!new_ddd->done_fill)
	BUG();
    if (down_interruptible(&dd_privdata->sem)) {
	return -EINTR;
    }
    if (dpd_debug_packet_dump_file)
	printk("DPD: cleanup on buffer #%d, %d bytes\n",
	       new_ddd->outputidxnum, new_ddd->cur_write_offset);
    if (dd_privdata->dumping == 0 || dd_privdata->filled != new_ddd) {
	printk("DPD: abort cleanup %d %p %p\n",
	       dd_privdata->dumping, dd_privdata->filled, new_ddd);
	up(&dd_privdata->sem);
	return -EINTR;
    }
    /* we get to do cleanup */
    dd_privdata->filled = NULL;
    mb(); /* force out write so that driver can swap buffers if needed */
    
    dpd_ioctl->bytes_written = new_ddd->cur_write_offset;
    dpd_ioctl->file_index = new_ddd->outputidxnum;
    do_driver_dump_data_cleanup(new_ddd);
    /* do cleanup before releasing lock as this limits the total
       kmapped data to 3*buffer_size, without this assuming
       inconvinient scheduling it could be unlimited. */
    up(&dd_privdata->sem);
    return 0;
 alloc_failure:
    up(&dd_privdata->sem);
    printk("DPD: allocation failure\n");
    if (new_ddd) {
	do_driver_dump_data_cleanup(new_ddd);
    }
    return -ENOMEM;

}

static int 
do_driver_packet_dump_file(struct driver_packet_dump_ioctl *dpd_ioctl,
			   struct driver_dump_privdata *dd_privdata)
{
    int ret;
    if (dpd_debug_packet_dump_file)
	printk("dpd file %p %s\n",dd_privdata,dpd_ioctl->filename);
    if (dpd_ioctl->bytes_written != DPD_PCAP_FILE_HEADER_LEN) {
	return -EINVAL;
    }
    if (down_interruptible(&dd_privdata->sem)) {
	return -EINTR;
    }
    if (dpd_debug_packet_dump_file)
	printk("dpd: check dumping\n");
    if (!dd_privdata->dumping) {
	up(&dd_privdata->sem);
	printk("dpd file: not dumping\n");
	return -ENOENT;
    }
    while (dd_privdata->empty) {
	up(&dd_privdata->sem);
	if (dpd_debug_packet_dump_file)
	    printk("dpd: wait for empty\n");
	ret = wait_event_interruptible(dd_privdata->wait_empty,
				       dd_privdata->empty == NULL);
	if (dpd_debug_packet_dump_file)
	    printk("dpd: return wait for empty\n");
	if (ret != 0) {
	    return -EINTR;
	}
	if (down_interruptible(&dd_privdata->sem)) {
	    return -EINTR;
	}
    }
    return do_driver_packet_dump_locked_init_empty(dpd_ioctl,dd_privdata);
}

static void do_driver_packet_dump_init(struct driver_dump_privdata *dd_privdata)
{
    sema_init(&dd_privdata->sem,1);
    init_waitqueue_head(&dd_privdata->wait_empty);
}
				       
static int do_driver_packet_dump(struct driver_dump_privdata *dd_privdata, struct ifreq *rq)
{
    struct driver_packet_dump_ioctl dpd;
    int ret = 0;

    if (!capable(CAP_NET_RAW)) {
	printk("DPD: no net raw\n");
	return -EPERM;
    }

    if (copy_from_user(&dpd, rq->ifr_data, sizeof(dpd))) {
	printk("DPD: no copy from user\n");
	return -EFAULT;
    }

    // in net/core/dev.c:dev_ioctl, at the line containing 'cmd >=
    // SIOCDEVPRIVATE' we find the lines that put locks around
    // dev_ifsioc, since we are going to hang around in the ioctl for
    // a while and we explicitly want multiple things in the ioctl at
    // the same time, we have to unlock these things.  This is ugly, I
    // guess an alternative would be to create a /proc entry for the
    // device and access things through /proc rather than ioctl, but for
    // now we do this and pray :)

    // need to modify kernel/kmod.c to export dev_probe_{lock,unlock}
    if (dpd_debug_ioctl_locking) printk("DPD: ioctl unlocking ...\n");
    rtnl_unlock();

    if (dpd.subcmd == driver_packet_dump_test) {
	ret = do_driver_packet_dump_test(&dpd, dd_privdata);
    } else if (dpd.subcmd == driver_packet_dump_setup) {
	ret = do_driver_packet_dump_setup(&dpd, dd_privdata);
    } else if (dpd.subcmd == driver_packet_dump_finish) {
	ret = do_driver_packet_dump_finish(dd_privdata);
    } else if (dpd.subcmd == driver_packet_dump_file) {
	ret = do_driver_packet_dump_file(&dpd, dd_privdata);
	if (ret == 0) {
	    if (copy_to_user(rq->ifr_data,&dpd,sizeof(dpd))) {
		ret = -EFAULT;
	    }
	}
    } else if (dpd.subcmd == driver_packet_dump_getstats) {
	struct driver_dump_privdata *dd_priv = dd_privdata;
	// printk("DPD: getstats\n");
	dpd.copy_packets = dd_priv->copy_packets;
	dpd.copy_bytes = dd_priv->copy_bytes;
	dpd.drop_packets = dd_priv->drop_packets;
	dpd.drop_bytes = dd_priv->drop_bytes;
	if (copy_to_user(rq->ifr_data,&dpd,sizeof(dpd))) {
	    ret = -EFAULT;
	} else {
	    ret = 0;
	}
    } else {
	printk("dpd unknown cmd %d\n",dpd.subcmd);
	ret = -EINVAL;
    }
    if (dpd_debug_ioctl_locking) printk("DPD: ioctl re-locking ...\n");
    rtnl_lock();

    if (0) printk("DPD: ioctl returning %d\n",ret);
    return ret;
}
