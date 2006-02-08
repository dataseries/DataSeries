// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    test program for DataSeries
*/

#include <stdio.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <openssl/opensslv.h>
#include <openssl/evp.h>
#include <zlib.h>
#include <lzoconf.h>
#include <HashTable.H>
#include <MersenneTwisterRandom.H>
#include <Clock.H>

#include <Extent.H>
#include <ExtentField.H>
#include <DataSeriesModule.H>

#ifndef HPUX_ACC
using namespace std;
#endif

const string pslist[] = 
{
"CMD",
"init [2] ",
"[keventd]",
"[ksoftirqd_CPU0]",
"[kswapd]",
"[bdflush]",
"[kupdated]",
"[i2oevtd]",
"[kreiserfsd]",
"dhclient -pf /var/run/dhclient.wlan0.pid wlan0",
"/sbin/syslogd",
"/sbin/klogd",
"[kapmd]",
"/usr/sbin/apmd -P /etc/apm/apmd_proxy --proxy-timeout 30",
"/usr/sbin/cupsd",
"[exim3]",
"/usr/sbin/nmbd -D",
"/usr/sbin/smbd -D",
"/usr/sbin/sshd",
"/usr/bin/X11/xfs -daemon",
"[xfs-xtt]",
"/usr/sbin/ntpd",
"/usr/sbin/usbmgr",
"[khubd]",
"/usr/sbin/noflushd -n 3 /dev/hda",
"[atd]",
"/usr/sbin/cron",
"/usr/bin/vmnet-bridge -d /var/run/vmnet-bridge-0.pid /dev/vmnet0 eth0",
"/usr/bin/vmnet-natd -d /var/run/vmnet-natd-8.pid -m /var/run/vmnet-natd-8.mac -c /etc/vmware/vmnet8/nat/nat.conf",
"/usr/sbin/apache",
"-zsh",
"/sbin/getty 38400 tty2",
"/sbin/getty 38400 tty3",
"/sbin/getty 38400 tty4",
"/sbin/getty 38400 tty5",
"/sbin/getty 38400 tty6",
"/usr/bin/vmnet-netifup -d /var/run/vmnet-netifup-vmnet8.pid /dev/vmnet8 vmnet8",
"/usr/bin/vmnet-netifup -d /var/run/vmnet-netifup-vmnet1.pid /dev/vmnet1 vmnet1",
"/usr/bin/vmnet-dhcpd -cf /etc/vmware/vmnet8/dhcpd/dhcpd.conf -lf /etc/vmware/vmnet8/dhcpd/dhcpd.leases -pf /var/run/vmnet-dhcpd-vmnet8.pid vmnet8",
"/usr/bin/vmnet-dhcpd -cf /etc/vmware/vmnet1/dhcpd/dhcpd.conf -lf /etc/vmware/vmnet1/dhcpd/dhcpd.leases -pf /var/run/vmnet-dhcpd-vmnet1.pid vmnet1",
"/usr/bin/vmware-nmbd -D -l /dev/null -s /etc/vmware/vmnet1/smb/smb.conf -f /var/run/vmware-nmbd-vmnet1.pid",
"/usr/bin/vmware-nmbd -D -l /dev/null -s /etc/vmware/vmnet1/smb/smb.conf -f /var/run/vmware-nmbd-vmnet1.pid",
"/bin/sh /opt/beta/XFree86/4.3.0/bin/startx",
"xinit /home/anderse/.xinitrc -- /home/anderse/.xserverrc",
"[X]",
"/bin/sh /home/anderse/.Xclients",
"[xterm]",
"xclock -digital -norender -padding 0 -geometry +1113+0 -fg yellow -bg navyblue -fn -misc-fixed-medium-r-normal--12-*-*-*-*-*-*-* -update 1",
"xapm -fn -misc-fixed-medium-r-normal--10-70-*-*-c-60-iso8859-1 -geometry 100x13-0+0 -bw 0 -bg black -fg white",
"fvwm2",
"/usr/lib/fvwm/2.4.15/FvwmPager 7 4 none 0 8 0 8",
"zsh",
"kdeinit: Running...                    ",
"kdeinit: dcopserver --nosid --suicide  ",
"kdeinit: klauncher                     ",
"kdeinit: kded                          ",
"[xterm]",
"zsh",
"cgoban",
"[xterm]",
"zsh",
"[xterm]",
"zsh",
"[xterm]",
"zsh",
"[xterm]",
"zsh",
"/usr/bin/emacs Extent.C",
"[xterm]",
"zsh",
"[xterm]",
"zsh",
"[apache]",
"[apache]",
"[apache]",
"[apache]",
"[apache]",
"[xterm]",
"zsh",
"[xterm]",
"zsh",
"[xterm]",
"zsh",
"[man]",
"/usr/bin/less",
"/usr/bin/gdb -fullname test-debug",
"/home/anderse/projects/DataSeries/cpp/test-debug",
"/usr/sbin/irattach /dev/ttyS1 -s",
"[xterm]",
"zsh",
"[xterm]",
"zsh",
"SRT2txt /home/anderse/projects/srtLite/regression_tests/snake-920626.srt.gz",
"less",
"[xterm]",
"zsh",
"[xterm]",
"konqueror",
"[xterm]",
"zsh",
"[xterm]",
"zsh",
"[xterm]",
"zsh",
"[xterm]",
"zsh",
"/usr/bin/ispell -a -m -B",
"[xterm]",
"zsh",
"[xterm]",
"zsh",
"[xterm]",
"zsh",
"[gnome-freecell]",
"[gnome-freecell]",
"less 1495",
"/usr/bin/perl ./random-pics ggood",
"[xdpyinfo] <defunct>",
"ps -efl",
"perl -ne s/^.{75}//;chomp;$_ = ...; print;",
};
int npslist = sizeof(pslist)/sizeof(const string);


void
test_varcompress()
{
    typedef ExtentType::int32 int32;
    typedef ExtentType::byte byte;
    Extent::ByteArray packed;
    packed.resize(4);
    *(int32 *)packed.begin() = 0;
    int psn = 0;
    map<string,int> ispacked;
    while(packed.size() < 1024*1024) {
	if (psn == npslist) {
	    psn = 0;
	    break;
	}
	if (ispacked[pslist[psn]] == 0) {
	    ispacked[pslist[psn]] = 1;
	    int pos = packed.size();
	    int32 datasize = pslist[psn].size();
	    int32 roundup = datasize + (12 - (datasize % 8)) % 8;
	    packed.resize(packed.size() + 4 + roundup);
	    byte *offset = packed.begin() + pos;
	    *(int32 *)offset = datasize;
	    offset += 4;
	    AssertAlways(((unsigned long)offset % 8) == 0,
			 ("?!\n"));
	    memcpy(offset,pslist[psn].data(),datasize);
	}
	++psn;
    }
    Clock::Tdbl start_time,end_time;
    Extent::ByteArray compressed;
    printf("packing %d bytes on non-duplicate strings:\n",
	   packed.size());
    start_time = Clock::tod();
    Extent::packBZ2(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    printf("  bz2 compress -> %d, %.2fus\n",compressed.size(),end_time-start_time);
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packZLib(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    printf("  zlib compress -> %d, %.2fus\n",compressed.size(),end_time-start_time);
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packLZO(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    printf("  lzo compress -> %d, %.2fus\n",compressed.size(),end_time-start_time);
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packLZF(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    printf("  lzf compress -> %d, %.2fus\n",compressed.size(),end_time-start_time);

    packed.resize(4);
    *(int32 *)packed.begin() = 0;
    psn = 0;
    int reps = 0;
    while(packed.size() < 1024*1024) {
	if (psn == npslist) {
	    psn = 0;
	    reps += 1;
	}
	int pos = packed.size();
	int32 datasize = pslist[psn].size();
	int32 roundup = datasize + (12 - (datasize % 8)) % 8;
	packed.resize(packed.size() + 4 + roundup);
	byte *offset = packed.begin() + pos;
	*(int32 *)offset = datasize;
	offset += 4;
	AssertAlways(((unsigned long)offset % 8) == 0,
		     ("?!\n"));
	memcpy(offset,pslist[psn].data(),datasize);
	++psn;
    }
    printf("packing %d bytes of %d repetitions\n",
	   packed.size(),reps);
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packBZ2(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    printf("  bz2 compress -> %d, %.2fus\n",compressed.size(),end_time-start_time);
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packZLib(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    printf("  zlib compress -> %d, %.2fus\n",compressed.size(),end_time-start_time);
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packLZO(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    printf("  lzo compress -> %d, %.2fus\n",compressed.size(),end_time-start_time);
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packLZF(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    printf("  lzf compress -> %d, %.2fus\n",compressed.size(),end_time-start_time);
}

void
test_primitives()
{
    typedef ExtentType::int32 int32;
    typedef ExtentType::byte byte;
    Extent::ByteArray packed;

    const int hash_speed_test = 9*1024*1024;
    const int reps = 20;
    packed.resize(hash_speed_test);
    for(int i=0;i<hash_speed_test;i++) {
	packed[i] = (byte)(hash_speed_test & 0xFF);
    }
    struct rusage hash_start, hash_end;
    double elapsed;
#if OPENSSL_VERSION_NUMBER >= 0x00907000
    EVP_MD_CTX mdctx;
    unsigned char md_value[EVP_MAX_MD_SIZE];
    unsigned int md_len;
    {
	AssertAlways(getrusage(RUSAGE_SELF,&hash_start)==0,("?!"));
	for(int i=0;i<reps;i++) {
	    EVP_MD_CTX_init(&mdctx);
	    EVP_DigestInit_ex(&mdctx, EVP_sha1(), NULL);
	    EVP_DigestUpdate(&mdctx, packed.begin(), packed.size());
	    EVP_DigestFinal_ex(&mdctx, md_value, &md_len);
	}
	AssertAlways(getrusage(RUSAGE_SELF,&hash_end)==0,("?!"));
	elapsed = (hash_end.ru_utime.tv_sec - hash_start.ru_utime.tv_sec) + (hash_end.ru_utime.tv_usec - hash_start.ru_utime.tv_usec)/1.0e6;
	printf("SHA-1 Hash of %d * %d bytes in %.6gs, %.4g MB/s\n",
	       reps, hash_speed_test,elapsed,reps * hash_speed_test / (1e6 *elapsed));
    }
    {
	AssertAlways(getrusage(RUSAGE_SELF,&hash_start)==0,("?!"));
	for(int i=0;i<reps;i++) {
	    EVP_MD_CTX_init(&mdctx);
	    EVP_DigestInit_ex(&mdctx, EVP_md5(), NULL);
	    EVP_DigestUpdate(&mdctx, packed.begin(), packed.size());
	    EVP_DigestFinal_ex(&mdctx, md_value, &md_len);
	}
	AssertAlways(getrusage(RUSAGE_SELF,&hash_end)==0,("?!"));
	elapsed = (hash_end.ru_utime.tv_sec - hash_start.ru_utime.tv_sec) + (hash_end.ru_utime.tv_usec - hash_start.ru_utime.tv_usec)/1.0e6;
	printf("MD5 Hash of %d * %d bytes in %.6gs, %.4g MB/s\n",
	       reps, hash_speed_test,elapsed,reps * hash_speed_test / (1e6 *elapsed));
    }
#endif
    {
	AssertAlways(getrusage(RUSAGE_SELF,&hash_start)==0,("?!"));
	uLong adler = adler32(0L, Z_NULL, 0);
	for(int i=0;i<reps;i++) {
	    adler = adler32(adler,packed.begin(), packed.size());
	}
	AssertAlways(getrusage(RUSAGE_SELF,&hash_end)==0,("?!"));
	elapsed = (hash_end.ru_utime.tv_sec - hash_start.ru_utime.tv_sec) + (hash_end.ru_utime.tv_usec - hash_start.ru_utime.tv_usec)/1.0e6;
	printf("Adler Hash(%ld) of %d * %d bytes in %.6gs, %.4g MB/s\n",
	       adler,reps, hash_speed_test,elapsed,reps * hash_speed_test / (1e6 *elapsed));
    }
    {
	AssertAlways(getrusage(RUSAGE_SELF,&hash_start)==0,("?!"));
	uLong adler = crc32(0L, Z_NULL, 0);
	for(int i=0;i<reps;i++) {
	    adler = crc32(adler,packed.begin(), packed.size());
	}
	AssertAlways(getrusage(RUSAGE_SELF,&hash_end)==0,("?!"));
	elapsed = (hash_end.ru_utime.tv_sec - hash_start.ru_utime.tv_sec) + (hash_end.ru_utime.tv_usec - hash_start.ru_utime.tv_usec)/1.0e6;
	printf("CRC32(%ld) of %d * %d bytes in %.6gs, %.4g MB/s\n",
	       adler, reps, hash_speed_test,elapsed,reps * hash_speed_test / (1e6 *elapsed));
    }
    {
	AssertAlways(getrusage(RUSAGE_SELF,&hash_start)==0,("?!"));
	uLong adler = lzo_adler32(0L, Z_NULL, 0);
	for(int i=0;i<reps;i++) {
	    adler = lzo_adler32(adler,packed.begin(), packed.size());
	}
	AssertAlways(getrusage(RUSAGE_SELF,&hash_end)==0,("?!"));
	elapsed = (hash_end.ru_utime.tv_sec - hash_start.ru_utime.tv_sec) + (hash_end.ru_utime.tv_usec - hash_start.ru_utime.tv_usec)/1.0e6;
	printf("lzo_adler(%ld) of %d * %d bytes in %.6gs, %.4g MB/s \n",
	       adler,reps, hash_speed_test,elapsed,reps * hash_speed_test / (1e6 *elapsed));
    }
    {
	AssertAlways(getrusage(RUSAGE_SELF,&hash_start)==0,("?!"));
	uLong adler = lzo_crc32(0L, Z_NULL, 0);
	for(int i=0;i<reps;i++) {
	    adler = lzo_crc32(adler,packed.begin(), packed.size());
	}
	AssertAlways(getrusage(RUSAGE_SELF,&hash_end)==0,("?!"));
	elapsed = (hash_end.ru_utime.tv_sec - hash_start.ru_utime.tv_sec) + (hash_end.ru_utime.tv_usec - hash_start.ru_utime.tv_usec)/1.0e6;
	printf("lzo_crc(%ld) of %d * %d bytes in %.6gs, %.4g MB/s\n",
	       adler, reps, hash_speed_test,elapsed,reps * hash_speed_test / (1e6 *elapsed));
    }
    {
	AssertAlways(getrusage(RUSAGE_SELF,&hash_start)==0,("?!"));
	unsigned int hash = 1972;
	for(int i=0;i<reps;i++) {
	    hash = BobJenkinsHash(hash, packed.begin(), packed.size());
	}
	AssertAlways(getrusage(RUSAGE_SELF,&hash_end)==0,("?!"));
	elapsed = (hash_end.ru_utime.tv_sec - hash_start.ru_utime.tv_sec) + (hash_end.ru_utime.tv_usec - hash_start.ru_utime.tv_usec)/1.0e6;
	printf("bjhash of %d * %d bytes in %.6gs, %.4g MB/s\n",
	       reps, hash_speed_test,elapsed,reps * hash_speed_test / (1e6 *elapsed));
    }
    {
	Extent::ByteArray copy;
	copy.resize(packed.size());
	AssertAlways(getrusage(RUSAGE_SELF,&hash_start)==0,("?!"));
	for(int i=0;i<reps;i++) {
	    memcpy(copy.begin(),packed.begin(),packed.size());
	}
	AssertAlways(getrusage(RUSAGE_SELF,&hash_end)==0,("?!"));
	elapsed = (hash_end.ru_utime.tv_sec - hash_start.ru_utime.tv_sec) + (hash_end.ru_utime.tv_usec - hash_start.ru_utime.tv_usec)/1.0e6;
	printf("memcpy of %d * %d bytes in %.6gs, %.4g MB/s\n",
	       reps, hash_speed_test,elapsed,reps * hash_speed_test / (1e6 *elapsed));
    }	
}

void
test_extentpackunpack()
{
    printf("test_extentpackunpack - start\n");
    ExtentTypeLibrary typelib;

    typelib.registerType("<ExtentType name=\"test type\">\n"
			 "  <field type=\"bool\" name=\"test1\" />\n"
			 "  <field type=\"int32\" name=\"input1\" pack_relative=\"input1\" />\n"
			 "  <field type=\"int32\" name=\"input2\" pack_relative=\"input1\" />\n"
			 "  <field type=\"int64\" name=\"int64-1\" pack_relative=\"int64-1\" />\n"
			 "  <field type=\"int64\" name=\"int64-2\" pack_relative=\"int64-2\" />\n"
			 "  <field type=\"double\" name=\"double1\" pack_scale=\"1e-6\" pack_relative=\"double1\" />\n"
			 "  <field type=\"variable32\" name=\"var1\"/>\n"
			 "  <field type=\"variable32\" name=\"var2\"/>\n"
			 "</ExtentType>\n");

    ExtentSeries testseries(typelib,"test type");
    Extent testextent(testseries);
    testseries.setExtent(testextent);
    const int nrecords = 937;
    testseries.createRecords(nrecords);

    Int32Field int1(testseries,"input1");
    Int32Field int2(testseries,"input2");
    Int64Field int64_1(testseries,"int64-1");
    Int64Field int64_2(testseries,"int64-2");
    DoubleField double1(testseries,"double1");
    Variable32Field var1(testseries,"var1");
    Variable32Field var2(testseries,"var2");

    Extent::ByteArray variablestuff;
    variablestuff.resize(nrecords * 2 + 5);
    for(int i=0;i<nrecords * 2 + 5;i++) {
	variablestuff[i] = (char)(i&0xFF);
    }
    for(int i=0;i<nrecords;i++) {
	int1.set(i);
	int2.set(nrecords-i);
	int64_1.set((ExtentType::int64)i * (ExtentType::int64)1000000 * (ExtentType::int64)1000000);
	int64_2.set((ExtentType::int64)i * (ExtentType::int64)19721776 * (ExtentType::int64)1000000);
	double1.set((double)i/1000000.0);
	AssertAlways(var1.size() == 0,("?!\n"));
	var1.set(variablestuff.begin(),i+1);
	AssertAlways(var1.size() == i+1,("??"));
	AssertAlways(memcmp(var1.val(),variablestuff.begin(),i+1)==0,("??"));
	AssertAlways(var2.size() == 0,("?!\n"));
	var2.set(variablestuff.begin(),2*i+1);
	AssertAlways(var2.size() == 2*i+1,("??"));
	AssertAlways(memcmp(var2.val(),variablestuff.begin(),2*i+1)==0,("??"));
	++testseries.pos;
    }

    testseries.pos.reset(testseries.extent());
    for(int i=0;i<nrecords;i++) {
	AssertAlways(int1.val() == i,("?? %d",int1.val()));
	AssertAlways(int2.val() == nrecords-i,("??"));;
	AssertAlways(int64_1.val() == (ExtentType::int64)i * (ExtentType::int64)1000000 * (ExtentType::int64)1000000,("??"));
	AssertAlways(int64_2.val() == (ExtentType::int64)i * (ExtentType::int64)19721776 * (ExtentType::int64)1000000,("??"));
	AssertAlways((int)round(double1.val() * 1000000.0) == i,("??"));
	AssertAlways(var1.size() == i+1,("??"));
	AssertAlways(memcmp(var1.val(),variablestuff.begin(),i+1)==0,("??"));
	AssertAlways(var2.size() == 2*i+1,("??"));
	AssertAlways(memcmp(var2.val(),variablestuff.begin(),2*i+1)==0,("??"));
	++testseries.pos;
    }

    Extent::ByteArray packed;
    testextent.packData(packed,Extent::compress_zlib);
    Extent unpackextent(testseries);
    unpackextent.unpackData(typelib, packed, false);

    testseries.setExtent(unpackextent);
    for(int i=0;i<nrecords;i++) {
	AssertAlways(int1.val() == i,("?? %d",int1.val()));
	AssertAlways(int2.val() == nrecords-i,("??"));;
	AssertAlways(int64_1.val() == (ExtentType::int64)i * (ExtentType::int64)1000000 * (ExtentType::int64)1000000,("??"));
	AssertAlways(int64_2.val() == (ExtentType::int64)i * (ExtentType::int64)19721776 * (ExtentType::int64)1000000,("??"));
	AssertAlways((int)round(double1.val() * 1000000.0) == i,("??"));
	AssertAlways(var1.size() == i+1,("??"));
	AssertAlways(memcmp(var1.val(),variablestuff.begin(),i+1)==0,("??"));
	AssertAlways(var2.size() == 2*i+1,("??"));
	AssertAlways(memcmp(var2.val(),variablestuff.begin(),2*i+1)==0,("??"));
	++testseries.pos;
    }
    printf("unpacked bytes %d, packed %d\n",unpackextent.extentsize(),packed.size());
    printf("test_extentpackunpack - end\n");
}

#define REVERSE_INT32(Int32ValuE) \
    ((Int32ValuE >> 24) & 0xff) | ((Int32ValuE >> 8) & 0xff00) \
  | ((Int32ValuE << 8) & 0xff0000) | ((Int32ValuE << 24) & 0xff000000)

void
test_byteflip()
{
    unsigned char b[4];

    *(unsigned int *)b = 0x12345678;

    struct rusage test_start, test_end;
    double elapsed;
    unsigned int sum = 0;
    getrusage(RUSAGE_SELF,&test_start);
    for(int i=0;i<200000000;i++) {
	sum += *(unsigned int *)b;
	unsigned char f = b[0];
	b[0] = b[3];
	b[3] = f;
	unsigned char g = b[1];
	b[1] = b[2];
	b[2] = g;
    }
    getrusage(RUSAGE_SELF,&test_end);
    elapsed = (test_end.ru_utime.tv_sec - test_start.ru_utime.tv_sec) + (test_end.ru_utime.tv_usec - test_start.ru_utime.tv_usec)/1.0e6;
    printf("via character flip: sum %d in %.6g seconds\n",sum,elapsed);

    sum = 0;
    *(unsigned int *)b = 0x12345678;
    getrusage(RUSAGE_SELF,&test_start);
    for(int i=0;i<200000000;i++) {
	sum += *(unsigned int *)b;
	unsigned int v = *(unsigned int *)b;
	*(unsigned int *)b = REVERSE_INT32(v);
    }
    getrusage(RUSAGE_SELF,&test_end);
    elapsed = (test_end.ru_utime.tv_sec - test_start.ru_utime.tv_sec) + (test_end.ru_utime.tv_usec - test_start.ru_utime.tv_usec)/1.0e6;
    printf("via int shift: sum %d in %.6g seconds\n",sum,elapsed);

    sum = 0;
    *(unsigned int *)b = 0x12345678;
    getrusage(RUSAGE_SELF,&test_start);
    for(int i=0;i<200000000;i++) {
	sum += *(unsigned int *)b;
	unsigned int v = *(unsigned int *)b;
	*(unsigned int *)b = ((v >> 24) & 0xFF) | ((v>>8) & 0xFF00) |
	    ((v & 0xFF00) << 8) | ((v & 0xFF) << 24);
    }
    getrusage(RUSAGE_SELF,&test_end);
    elapsed = (test_end.ru_utime.tv_sec - test_start.ru_utime.tv_sec) + (test_end.ru_utime.tv_usec - test_start.ru_utime.tv_usec)/1.0e6;
    printf("via int shift2: sum %d in %.6g seconds\n",sum,elapsed);

    sum = 0;
    *(unsigned int *)b = 0x12345678;
    getrusage(RUSAGE_SELF,&test_start);
    for(int i=0;i<200000000;i++) {
	sum += *(unsigned int *)b;
	unsigned int v = *(unsigned int *)b;
	*(unsigned int *)b = ((v >> 24) & 0xFF) + ((v>>8) & 0xFF00) +
	    ((v & 0xFF00) << 8) + ((v & 0xFF) << 24);
    }
    getrusage(RUSAGE_SELF,&test_end);
    elapsed = (test_end.ru_utime.tv_sec - test_start.ru_utime.tv_sec) + (test_end.ru_utime.tv_usec - test_start.ru_utime.tv_usec)/1.0e6;
    printf("via int shift3: sum %d in %.6g seconds\n",sum,elapsed);

    sum = 0;
    *(unsigned int *)b = 0x12345678;
    getrusage(RUSAGE_SELF,&test_start);
    for(int i=0;i<200000000;i++) {
	sum += *(unsigned int *)b;
	unsigned int v = *(unsigned int *)b;
	*(unsigned int *)b = ((v >> 24) & 0xFF) | ((v>>8) & 0xFF00) |
	    ((v << 8) & 0xFF0000) | ((v << 24) & 0xFF000000);
    }
    getrusage(RUSAGE_SELF,&test_end);
    elapsed = (test_end.ru_utime.tv_sec - test_start.ru_utime.tv_sec) + (test_end.ru_utime.tv_usec - test_start.ru_utime.tv_usec)/1.0e6;
    printf("via int shift4: sum %d in %.6g seconds\n",sum,elapsed);

    sum = 0;
    *(unsigned int *)b = 0x12345678;
    getrusage(RUSAGE_SELF,&test_start);
    for(int i=0;i<200000000;i++) {
	sum += *(unsigned int *)b;
	Extent::flip4bytes((Extent::byte *)b);
    }
    getrusage(RUSAGE_SELF,&test_end);
    elapsed = (test_end.ru_utime.tv_sec - test_start.ru_utime.tv_sec) + (test_end.ru_utime.tv_usec - test_start.ru_utime.tv_usec)/1.0e6;
    printf("via Extent::flip4bytes: sum %d in %.6g seconds\n",sum,elapsed);
    
}
 
double elapsed(struct rusage &start, struct rusage &end)
{
    return end.ru_utime.tv_sec - start.ru_utime.tv_sec + (end.ru_utime.tv_usec - start.ru_utime.tv_usec)/1.0e6;
}

class DoubleAccessor {
public:
    DoubleAccessor(char *_base_offset, bool _maybe_null, double _default_value)
	: base_offset(_base_offset), maybe_null(_maybe_null),
	  default_value(_default_value)
    { }
    bool isnull() { return *base_offset & 0x1 ? true : false; }
    void setnull1(bool null) { 
	if (null) { 
	    *base_offset = (char)(*base_offset | 0x1); 
	} else { 
	    *base_offset = (char)(*base_offset & ~0x1); 
	} 
    }
    void setnull2(bool null) { setnull1(null); if (null) { set1(default_value); } }
    double get1() { return *(double *)(base_offset + 8); }
    void set1(double v) { *(double *)(base_offset + 8) = v; }
    double get2() { if (isnull()) { return default_value; } else { return get1(); } }
    void set2(double v) { set1(v); setnull1(false); }

    char *base_offset;
    bool maybe_null;
    double default_value;
};

void
test_nullsupport()
{
    printf("test_nullsupport - start\n");
    const int nents = 20000000;
    char *array = new char[16*nents];
    char *endofarray = array + 16 * nents;

    for(int i=0;i<2;i++) {
	struct rusage start, end;
	DoubleAccessor access(array, MTRandom.randDouble() < 2, -1);
	printf("repetition %d:\n",i);

	getrusage(RUSAGE_SELF,&start);
	for(access.base_offset = array; 
	    access.base_offset != endofarray; 
	    access.base_offset += 16) {
	    access.set2(MTRandom.randDouble());
	}
	getrusage(RUSAGE_SELF,&end);
	printf("  initial set: %.6g seconds\n",elapsed(start,end));

	double sum = 0;
	getrusage(RUSAGE_SELF,&start);
	for(access.base_offset = array; 
	    access.base_offset != endofarray; 
	    access.base_offset += 16) {
	    sum += access.get1();
	}
	getrusage(RUSAGE_SELF,&end);
	printf("  get1: %.6g seconds (%.3f)\n",elapsed(start,end),sum);

	sum = 0;
	getrusage(RUSAGE_SELF,&start);
	for(access.base_offset = array; 
	    access.base_offset != endofarray; 
	    access.base_offset += 16) {
	    sum += access.get2();
	}
	getrusage(RUSAGE_SELF,&end);
	printf("  get2: %.6g seconds (%.3f)\n",elapsed(start,end),sum);

	sum = 0;
	getrusage(RUSAGE_SELF,&start);
	for(access.base_offset = array; 
	    access.base_offset != endofarray; 
	    access.base_offset += 16) {
	    if (access.isnull()) {
		sum += 1;
	    }
	}
	getrusage(RUSAGE_SELF,&end);
	printf("  isnull: %.6g seconds (%.3f)\n",elapsed(start,end),sum);

	getrusage(RUSAGE_SELF,&start);
	for(access.base_offset = array; 
	    access.base_offset != endofarray; 
	    access.base_offset += 16) {
	    access.set1(1);
	}
	getrusage(RUSAGE_SELF,&end);
	printf("  set1: %.6g seconds\n",elapsed(start,end));

	getrusage(RUSAGE_SELF,&start);
	for(access.base_offset = array; 
	    access.base_offset != endofarray; 
	    access.base_offset += 16) {
	    access.set2(2);
	}
	getrusage(RUSAGE_SELF,&end);
	printf("  set2: %.6g seconds\n",elapsed(start,end));

	getrusage(RUSAGE_SELF,&start);
	for(access.base_offset = array; 
	    access.base_offset != endofarray; 
	    access.base_offset += 16) {
	    access.setnull1(true);
	}
	getrusage(RUSAGE_SELF,&end);
	printf("  setnull1: %.6g seconds\n",elapsed(start,end));

	getrusage(RUSAGE_SELF,&start);
	for(access.base_offset = array; 
	    access.base_offset != endofarray; 
	    access.base_offset += 16) {
	    access.setnull2(true);
	}
	getrusage(RUSAGE_SELF,&end);
	printf("  setnull2: %.6g seconds\n",elapsed(start,end));

	getrusage(RUSAGE_SELF,&start);
	for(access.base_offset = array; 
	    access.base_offset != endofarray; 
	    access.base_offset += 16) {
	    double v = MTRandom.randDouble();
	    if (v < 0.5) {
		access.setnull2(true);
	    } else {
		access.set2(v);
	    }
	}
	getrusage(RUSAGE_SELF,&end);
	printf("  randset: %.6g seconds\n",elapsed(start,end));
	
	sum = 0;
	getrusage(RUSAGE_SELF,&start);
	for(access.base_offset = array; 
	    access.base_offset != endofarray; 
	    access.base_offset += 16) {
	    if (access.isnull()) {
		sum += 1;
	    }
	}
	getrusage(RUSAGE_SELF,&end);
	printf("  isnull: %.6g seconds (%.3f)\n",elapsed(start,end),sum);

	sum = 0;
	getrusage(RUSAGE_SELF,&start);
	for(access.base_offset = array; 
	    access.base_offset != endofarray; 
	    access.base_offset += 16) {
	    sum += access.get1();
	}
	getrusage(RUSAGE_SELF,&end);
	printf("  get1: %.6g seconds (%.3f)\n",elapsed(start,end),sum);

	sum = 0;
	getrusage(RUSAGE_SELF,&start);
	for(access.base_offset = array; 
	    access.base_offset != endofarray; 
	    access.base_offset += 16) {
	    sum += access.get2();
	}
	getrusage(RUSAGE_SELF,&end);
	printf("  get2: %.6g seconds (%.3f)\n",elapsed(start,end),sum);

	sum = 0;
	access.default_value = 0;
	getrusage(RUSAGE_SELF,&start);
	for(access.base_offset = array; 
	    access.base_offset != endofarray; 
	    access.base_offset += 16) {
	    sum += access.get2();
	}
	getrusage(RUSAGE_SELF,&end);
	printf("  get2(def0): %.6g seconds (%.3f)\n",elapsed(start,end),sum);
    }
    printf("test_nullsupport - done\n");
}

bool
randBool(MersenneTwisterRandom &rand)
{
    return (rand.randInt() & 0x1) == 1;
}

void
fillVariableData(MersenneTwisterRandom &rand, char *variabledata, int vdsize)
{
    for(int i=0;i<vdsize;i++) {
	int v = rand.randInt() & 0xF;
	if (v < 10) {
	    variabledata[i] = (char)('0' + v);
	} else {
	    variabledata[i] = (char)('A' + (v - 10));
	}
    }
}

void
test_makecomplexfile()
{
    printf("test_makecomplexfile...\n");
    string complextype(
"<ExtentType name=\"complex test type\">\n"
"  <field type=\"bool\" name=\"bool\" />\n"
"  <field type=\"bool\" name=\"null_bool\" opt_nullable=\"yes\"/>\n"
"  <field type=\"byte\" name=\"byte\" />\n"
"  <field type=\"byte\" name=\"null_byte\" opt_nullable=\"yes\"/>\n"
"  <field type=\"int32\" name=\"int32\" />\n"
"  <field type=\"int32\" name=\"null_int32\" opt_nullable=\"yes\"/>\n"
"  <field type=\"int64\" name=\"int64\" />\n"
"  <field type=\"int64\" name=\"null_int64\" opt_nullable=\"yes\"/>\n"
"  <field type=\"double\" name=\"double\" />\n"
"  <field type=\"double\" name=\"null_double\" opt_nullable=\"yes\"/>\n"
"  <field type=\"double\" name=\"base_double\" opt_doublebase=\"100000\"/>\n"
"  <field type=\"variable32\" name=\"variable32\" />\n"
"  <field type=\"variable32\" name=\"null_variable32\" opt_nullable=\"yes\"/>\n"
"</ExtentType>");
    ExtentTypeLibrary library;
    ExtentType *outputtype = library.registerType(complextype);
    DataSeriesSink output("test.ds");
    output.writeExtentLibrary(library);
    ExtentSeries outputseries(*outputtype);
    OutputModule outmodule(output,outputseries,outputtype,30000);

    BoolField f_bool(outputseries,"bool");
    BoolField f_bool_null(outputseries,"null_bool",Field::flag_nullable);
    ByteField f_byte(outputseries,"byte");
    ByteField f_byte_null(outputseries,"null_byte",Field::flag_nullable);
    Int32Field f_int32(outputseries,"int32");
    Int32Field f_int32_null(outputseries,"null_int32",Field::flag_nullable);
    Int64Field f_int64(outputseries,"int64");
    Int64Field f_int64_null(outputseries,"null_int64",Field::flag_nullable);
    DoubleField f_double(outputseries,"double");
    DoubleField f_double_null(outputseries,"null_double",Field::flag_nullable);
    DoubleField f_double_base(outputseries,"base_double",DoubleField::flag_allownonzerobase);
    Variable32Field f_variable32(outputseries,"variable32");
    Variable32Field f_variable32_null(outputseries,"null_variable32",Field::flag_nullable);

    MersenneTwisterRandom rand(1781);
    for(int i=0;i<10000;++i) {
	outmodule.newRecord();
	f_bool.set(randBool(rand));
	if (randBool(rand)) {
	    f_bool_null.setNull(true);
	} else {
	    f_bool_null.set(randBool(rand));
	    AssertAlways(f_bool_null.isNull() == false,("internal error\n"));
	}

	f_byte.set((ExtentType::byte)(rand.randInt() & 0xFF));
	if (randBool(rand)) {
	    f_byte_null.setNull(true);
	} else {
	    f_byte_null.set((ExtentType::byte)(rand.randInt() & 0xFF));
	}

	f_int32.set(rand.randInt());
	if (randBool(rand)) {
	    f_int32_null.setNull(true);
	} else {
	    f_int32_null.set(rand.randInt());
	}

	f_int64.set(rand.randLongLong());
	if (randBool(rand)) {
	    f_int64_null.setNull(true);
	} else {
	    f_int64_null.set(rand.randLongLong());
	}

	f_double.set(rand.randDoubleOpen53());
	if (randBool(rand)) {
	    f_double_null.setNull(true);
	} else {
	    f_double_null.set(rand.randDoubleOpen53());
	}
	f_double_base.setabs(rand.randDoubleOpen53() * 1000000.0);

	const int vdsize = 32;
	char variabledata[vdsize];
	fillVariableData(rand,variabledata,vdsize);
	f_variable32.set(variabledata,vdsize);
	if (randBool(rand)) {
	    f_variable32_null.setNull(true);
	} else {
	    fillVariableData(rand,variabledata,vdsize);
	    f_variable32_null.set(variabledata,vdsize);
	}
    }	
    outmodule.flushExtent();
    printf("test_makecomplexfile - Done\n");
}

int
main(int argc, char *argv[])
{
    test_extentpackunpack();
    test_makecomplexfile();
    test_nullsupport();
    test_byteflip();
    test_varcompress();
    test_primitives();
}
