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
#if DATASERIES_ENABLE_LZO
#include <lzoconf.h>
#endif

#include <boost/bind.hpp>

#include <Lintel/LintelAssert.hpp>
#include <Lintel/HashTable.hpp>
#include <Lintel/MersenneTwisterRandom.hpp>
#include <Lintel/Clock.hpp>
#include <Lintel/Stats.hpp>

#include <DataSeries/cryptutil.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>
#include <DataSeries/DataSeriesModule.hpp>

using namespace std;
using boost::format;

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
    cout << format("packing %d bytes on non-duplicate strings:\n")
	% packed.size();
    start_time = Clock::tod();
    Extent::packBZ2(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    cout << format("  bz2 compress -> %d, %.2fus\n")
	% compressed.size() % (end_time-start_time);
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packZLib(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    cout << format("  zlib compress -> %d, %.2fus\n")
	% compressed.size() % (end_time-start_time);
#if DATASERIES_ENABLE_LZO
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packLZO(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    cout << format("  lzo compress -> %d, %.2fus\n")
	% compressed.size() % (end_time-start_time);
#endif
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packLZF(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    cout << format("  lzf compress -> %d, %.2fus\n")
	% compressed.size() % (end_time-start_time);

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
	SINVARIANT(((unsigned long)offset % 8) == 0);
	memcpy(offset,pslist[psn].data(),datasize);
	++psn;
    }
    cout << format("packing %d bytes of %d repetitions\n")
	% packed.size() % reps;
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packBZ2(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    cout << format("  bz2 compress -> %d, %.2fus\n")
	% compressed.size() % (end_time-start_time);
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packZLib(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    cout << format("  zlib compress -> %d, %.2fus\n")
	% compressed.size() % (end_time-start_time);
#if DATASERIES_ENABLE_LZO
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packLZO(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    cout << format("  lzo compress -> %d, %.2fus\n")
	% compressed.size() % (end_time-start_time);
#endif
    compressed.resize(0);
    start_time = Clock::tod();
    Extent::packLZF(packed.begin(),packed.size(),compressed,9);
    end_time = Clock::tod();
    cout << format("  lzf compress -> %d, %.2fus\n")
	% compressed.size() % (end_time-start_time);
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
#if DATASERIES_ENABLE_LZO
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
#endif
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
    unpackextent.unpackData(packed, false);

    testseries.setExtent(unpackextent);
    for(int i=0;i<nrecords;i++) {
	AssertAlways(int1.val() == i,("?? %d %d",int1.val(),i));
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
    cout << format("unpacked bytes %d, packed %d\n")
	% unpackextent.extentsize() % packed.size();
    printf("test_extentpackunpack - end\n");
}

#define REVERSE_INT32(Int32ValuE) \
    ((Int32ValuE >> 24) & 0xff) | ((Int32ValuE >> 8) & 0xff00) \
  | ((Int32ValuE << 8) & 0xff0000) | ((Int32ValuE << 24) & 0xff000000)

void doit_charflip(uint32_t *buf, unsigned buflen)
{
    for(unsigned i=0;i<buflen;++i) {
	unsigned char *b = (unsigned char *)(buf+i);

	unsigned char f = b[0];
	b[0] = b[3];
	b[3] = f;
	unsigned char g = b[1];
	b[1] = b[2];
	b[2] = g;
    }
}

void doit_intshift1(uint32_t *buf, unsigned buflen)
{
    for(unsigned i=0;i<buflen;++i) {
	buf[i] = REVERSE_INT32(buf[i]);
    }
}

void doit_intshift2(uint32_t *buf, unsigned buflen)
{
    for(unsigned i=0;i<buflen;++i) {
	uint32_t v = buf[i];
	buf[i] = ((v >> 24) & 0xFF) | ((v>>8) & 0xFF00) |
	    ((v & 0xFF00) << 8) | ((v & 0xFF) << 24);
    }
}

void doit_intshift3(uint32_t *buf, unsigned buflen)
{
    for(unsigned i=0;i<buflen;++i) {
	uint32_t v = buf[i];
	buf[i] = ((v >> 24) & 0xFF) + ((v>>8) & 0xFF00) +
	    ((v & 0xFF00) << 8) + ((v & 0xFF) << 24);
    }
}

void doit_intshift4(uint32_t *buf, unsigned buflen)
{
    for(unsigned i=0;i<buflen;++i) {
	uint32_t v = buf[i];
	buf[i] = ((v >> 24) & 0xFF) | ((v>>8) & 0xFF00) |
	    ((v << 8) & 0xFF0000) | ((v << 24) & 0xFF000000);
    }
}

#if __GNUC__ >= 2 && (defined(__i386__) || defined(__x86_64__))
void doit_bswap_i486(uint32_t *buf, unsigned buflen)
{
    for(unsigned i=0;i<buflen;++i) {
	register uint32_t tmp = buf[i];
	asm("bswap %0" : "=r" (tmp) : "0" (tmp));
	buf[i] = tmp;
    }
}
#endif

#ifdef bswap_32
void doit_bswap_32(uint32_t *buf, unsigned buflen)
{
    for(unsigned i=0;i<buflen;++i) {
	buf[i] = bswap_32(buf[i]);
    }
}
#endif

void
onebytefliptest(uint32_t *buf, int buflen, int reps,
		void (*fn)(uint32_t *, unsigned), const string &fnname,
		uint32_t &expected_sum, Stats &timing, Stats &flip4_time, bool first_run = false)
{
    if (timing.count() > 2 && timing.mean()-timing.conf95() > (flip4_time.mean() + flip4_time.conf95())) {
	printf("   skipping %s, mean time %.6g-%.6g > flip4+conf95 %.6g + %.6g\n",
	       fnname.c_str(), timing.mean(), timing.conf95(), flip4_time.mean(), flip4_time.conf95());
	return;
    }
    uint32_t sum = 0;
    struct rusage test_start, test_end;
    getrusage(RUSAGE_SELF,&test_start);
    for(int i=0;i<reps;i++) {
	fn(buf,buflen);
	for(int j=0;j<buflen;++j) {
	    sum += buf[j];
	}
    }
    getrusage(RUSAGE_SELF,&test_end);

    double elapsed = (test_end.ru_utime.tv_sec - test_start.ru_utime.tv_sec) + (test_end.ru_utime.tv_usec - test_start.ru_utime.tv_usec)/1.0e6;
    if (first_run) {
	expected_sum = sum;
    } 
    AssertAlways(sum == expected_sum, ("bad %s byteswap? %d != %d", fnname.c_str(), sum, expected_sum));
    timing.add(elapsed);
    printf("   via %s: sum %d in %.6g seconds; mean %.6gs +- %.6gs\n",fnname.c_str(), sum, elapsed, timing.mean(), timing.stddev());
}

bool in_range(double point, double min, double max)
{
    return point >= min && point <= max;
}

bool range_overlap(double a_min, double a_max, double b_min, double b_max)
{
    return in_range(a_min, b_min, b_max) || in_range(a_max, b_min, b_max) ||
	in_range(b_min, a_min, a_max) || in_range(b_max, a_min, a_max);
}


void
test_byteflip()
{
    static const unsigned rounds = 5;
    // reps + bufsize takes ~1 second for each test on a 2.8GhZ p4 Xeon
    static const unsigned reps = 2 * 100;
    static const unsigned bufsize = 1000000;

    uint32_t buf[bufsize];

    for(unsigned i=0; i < bufsize; ++i) {
	buf[i] = MTRandom.randInt();
    }

    Stats flip4_time;
    uint32_t flip4_sum;
    printf("Initial execution:\n");
    onebytefliptest(buf, bufsize, reps, Extent::run_flip4bytes, "Extent::flip4bytes", flip4_sum, flip4_time, flip4_time, true);

    const unsigned max_runtimes = 10;
    Stats runtimes[max_runtimes];
    uint32_t expected_sum = flip4_sum;

    for(unsigned i = 0; i < rounds; ++i) {
	printf("\nRound %d:\n", i);
	onebytefliptest(buf, bufsize, reps, doit_charflip, "character flip", expected_sum, runtimes[0], flip4_time);
	onebytefliptest(buf, bufsize, reps, doit_intshift1, "integer shift 1", expected_sum, runtimes[1], flip4_time);
	onebytefliptest(buf, bufsize, reps, doit_intshift2, "integer shift 2", expected_sum, runtimes[2], flip4_time);
	onebytefliptest(buf, bufsize, reps, doit_intshift3, "integer shift 3", expected_sum, runtimes[3], flip4_time);
	onebytefliptest(buf, bufsize, reps, doit_intshift4, "integer shift 4", expected_sum, runtimes[4], flip4_time);

#if __GNUC__ >= 2 && (defined(__i386__) || defined(__x86_64__))
	onebytefliptest(buf, bufsize, reps, doit_bswap_i486, "bswap_i486", expected_sum, runtimes[5], flip4_time);
#endif
#ifdef bswap_32
	onebytefliptest(buf, bufsize, reps, doit_bswap_32, "bswap_32", expected_sum, runtimes[6], flip4_time);
#endif
	
	onebytefliptest(buf, bufsize, reps, Extent::run_flip4bytes, "Extent::flip4bytes", flip4_sum, flip4_time, flip4_time);
    }

#ifndef COMPILE_DEBUG
    // With debugging turned on, even with the functions marked
    // inline, it seems gcc still generates a function call, which
    // makes elapsed_flip4 much slower

    unsigned bestidx = 0;
    for(unsigned i = 1; i < max_runtimes; ++i) {
	if (runtimes[i].count() == 0)
	    continue;
	if (runtimes[i].mean() < runtimes[bestidx].mean()) 
	    bestidx = i;
    }
    double bestmean = runtimes[bestidx].mean();
    double bestconf95 = runtimes[bestidx].conf95();
    // 0.005 means that if bestmean and flip4mean are within 1% of
    // each other, that will be considered acceptable.  Got a run
    // where the times differered by 6ms (~.1%), but had been stable
    // enough that the confidence intervals were too small to generate
    // an overlap.
    if (bestconf95 < bestmean * 0.005) {
	bestconf95 = bestmean * 0.005;
    }
    double flip4mean = flip4_time.mean();
    double flip4conf95 = flip4_time.conf95();
    if (flip4conf95 < flip4mean * 0.005) {
	flip4conf95 = flip4mean * 0.005;
    }
    AssertAlways(range_overlap(flip4mean - flip4conf95, flip4mean + flip4conf95,
			       bestmean - bestconf95, bestmean + bestconf95),
		 ("Error, flip4_time (%.6g +- %.6g) does not overlap with best time (%.6g +- %.6g); something is weird",
		  flip4mean, flip4conf95, bestmean, bestconf95));
    printf("flip4 performance of (%.6g +- %.6g) verified to overlap with best time (%.6g +- %.6g)\n",
	   flip4mean, flip4conf95, bestmean, bestconf95);
#endif
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
    const int nents = 10000000;
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
    delete [] array;
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
    const ExtentType *outputtype = library.registerType(complextype);
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

void
test_doublebase_nullable()
{
    printf("test_doublebase_nullable...\n");
    string dbntype_xml(
"<ExtentType name=\"doublebase nullable test type\">\n"
"  <field type=\"double\" name=\"double\" opt_nullable=\"yes\" opt_doublebase=\"1000000\" pack_scale=\"1000\" />\n"
"</ExtentType>");

    ExtentTypeLibrary library;
    const ExtentType *dbntype = library.registerType(dbntype_xml);
    ExtentSeries dbnseries(*dbntype);
    DoubleField f_double(dbnseries,"double",Field::flag_nullable | DoubleField::flag_allownonzerobase);
    
    Extent *cur_extent = new Extent(*dbntype);
    dbnseries.setExtent(cur_extent);
    
    dbnseries.newRecord();
    f_double.setNull();
    AssertAlways(f_double.isNull(), ("bad"));
    dbnseries.newRecord();
    f_double.set(1000000);
    AssertAlways(!f_double.isNull(), ("bad"));
    dbnseries.newRecord();
    f_double.setabs(1000000);
    AssertAlways(!f_double.isNull(), ("bad"));

    dbnseries.pos.reset(cur_extent);
    AssertAlways(dbnseries.morerecords(), ("bad"));

    AssertAlways(f_double.isNull(), ("bad"));
    AssertAlways(0 == f_double.val(), ("bad"));
    AssertAlways(1000000 == f_double.absval(), ("bad")); // changed semantics to have offset in absval always
    
    ++dbnseries;
    AssertAlways(dbnseries.morerecords(), ("bad"));
    AssertAlways(false == f_double.isNull(), ("bad"));
    AssertAlways(1000000 == f_double.val(), ("bad"));
    AssertAlways(2000000 == f_double.absval(), ("bad"));
    
    ++dbnseries;
    AssertAlways(dbnseries.morerecords(), ("bad"));
    AssertAlways(false == f_double.isNull(), ("bad"));
    AssertAlways(0 == f_double.val(), ("bad"));
    AssertAlways(1000000 == f_double.absval(), ("bad"));
    delete cur_extent;
}    

void
test_compactnull()
{
    MersenneTwisterRandom rand;
    cout << format("test_compactnull - start seed=%d\n") % rand.seed_used;
    ExtentTypeLibrary typelib;

    // One real bool, 7 hidden ones
    typelib.registerType("<ExtentType name=\"Test::CompactNulls\" pack_null_compact=\"non_bool\" >\n"
			 "  <field type=\"bool\" name=\"bool\" opt_nullable=\"yes\" />\n"
			 "  <field type=\"byte\" name=\"byte\" opt_nullable=\"yes\" />\n"
			 "  <field type=\"int32\" name=\"int32\" opt_nullable=\"yes\" pack_relative=\"int32\" />\n"
			 "  <field type=\"int32\" name=\"int32b\" opt_nullable=\"yes\" pack_relative=\"int32\" />\n"
			 "  <field type=\"int64\" name=\"int64\" opt_nullable=\"yes\" pack_relative=\"int64\" />\n"
			 "  <field type=\"double\" name=\"double\" opt_nullable=\"yes\" />\n"
			 "  <field type=\"variable32\" name=\"variable32\" opt_nullable=\"yes\" pack_unique=\"yes\" />\n"
			 "</ExtentType>\n");

    ExtentSeries series1(typelib,"Test::CompactNulls");
    Extent extent1(series1);
    series1.setExtent(extent1);

    int nrecords = 1000 + rand.randInt(10000);
    series1.createRecords(nrecords);

    BoolField f_bool(series1, "bool", Field::flag_nullable);
    ByteField f_byte(series1, "byte", Field::flag_nullable);
    Int32Field f_int32(series1, "int32", Field::flag_nullable);
    Int32Field f_int32b(series1, "int32b", Field::flag_nullable);
    Int64Field f_int64(series1, "int64", Field::flag_nullable);
    DoubleField f_double(series1, "double", Field::flag_nullable);
    Variable32Field f_variable32(series1, "variable32", Field::flag_nullable);

    Extent::ByteArray variablestuff;
    variablestuff.resize(nrecords * 4 + 5);
    for(int i=0;i<nrecords * 2 + 5;i++) {
	variablestuff[i] = (char)(i&0xFF);
    }
    /// Test 1: all null
    
    // Test filling in a value and then nulling.
    for(int i=1;i<=nrecords;i++) {
	f_bool.set(true);
	f_bool.setNull();
	f_byte.set(i & 0xFF);
	f_byte.setNull();
	f_int32.set(i);
	f_int32.setNull();
	f_int32b.set(i*10);
	f_int32b.setNull();
	f_int64.set(i*7731);
	f_int64.setNull();
	f_double.set(i+10000);
	f_double.setNull();
	f_variable32.set(variablestuff.begin()+i,i+1);
	f_variable32.setNull();
	++series1.pos;
    }    

    Extent::ByteArray packed;
    extent1.packData(packed, Extent::compress_none);

    cout << format("all null: %d rows, original bytes %d, packed %d\n")
	% nrecords % extent1.extentsize() % packed.size();
    uint32_t overhead = 48 + (4 - (nrecords % 4)) % 4;
    INVARIANT(packed.size() == static_cast<size_t>(overhead + nrecords), 
	      "size check failed");

    ExtentSeries series2(typelib, "Test::CompactNulls");
    Extent unpack1(series2);
    unpack1.unpackData(packed, false);

    series1.setExtent(unpack1);
    for(int i=0;i<nrecords;i++) {
	INVARIANT(f_bool.isNull() && f_byte.isNull() && f_int32.isNull()
		  && f_int32b.isNull() && f_int64.isNull() 
		  && f_double.isNull() && f_variable32.isNull(), "??");
	++series1.pos;
    }
    
    /// Test 2: random nulls

    cout << "all null uncompact passed\n";

    series1.setExtent(extent1);
    // Fill at random...
    for(int i=1;i<=nrecords;i++) {
	f_bool.set(true);
	if (rand.randInt(2)) f_bool.setNull();
	f_byte.set(i & 0xFF);
	if (rand.randInt(2)) f_byte.setNull();
	f_int32.set(i);
	if (rand.randInt(2)) f_int32.setNull();
	f_int32b.set(rand.randInt());
	if (rand.randInt(2)) f_int32b.setNull();
	f_int64.set(i*7731);
	if (rand.randInt(2)) f_int64.setNull();
	f_double.set(i+10000);
	if (rand.randInt(2)) f_double.setNull();
	f_variable32.set(variablestuff.begin()+i,i+1);
	if (rand.randInt(2)) f_variable32.setNull();
	++series1.pos;
    }

    packed.clear();
    extent1.packData(packed, Extent::compress_lzf);
    cout << format("random null: %d rows, original bytes %d, packed %d\n")
	% nrecords % extent1.extentsize() % packed.size();

    BoolField g_bool(series2, "", Field::flag_nullable);
    ByteField g_byte(series2, "byte", Field::flag_nullable);
    Int32Field g_int32(series2, "int32", Field::flag_nullable);
    Int32Field g_int32b(series2, "int32b", Field::flag_nullable);
    Int64Field g_int64(series2, "int64", Field::flag_nullable);
    DoubleField g_double(series2, "double", Field::flag_nullable);
    Variable32Field g_variable32(series2, "variable32", Field::flag_nullable);
    unpack1.unpackData(packed, false);
    series1.setExtent(extent1);
    series2.setExtent(unpack1);
    for(int i=0;i<nrecords;i++) {
	g_bool.setFieldName("bool"); // slow and inefficient, but for testing
	INVARIANT((f_bool.isNull() && g_bool.isNull())
		  || (!f_bool.isNull() && !g_bool.isNull() &&
		      f_bool.val() == g_bool.val()), 
		  format("bad@%d %d %d/%d %d") 
		  % i % f_bool.isNull() % g_bool.isNull()
		  % f_bool.val() % g_bool.val());
	INVARIANT((f_byte.isNull() && g_byte.isNull())
		  || (!f_byte.isNull() && !g_byte.isNull() &&
		      f_byte.val() == g_byte.val()), "bad");
	INVARIANT((f_int32.isNull() && g_int32.isNull())
		  || (!f_int32.isNull() && !g_int32.isNull() &&
		      f_int32.val() == g_int32.val()), "bad");
	INVARIANT((f_int32b.isNull() && g_int32b.isNull())
		  || (!f_int32b.isNull() && !g_int32b.isNull() &&
		      f_int32b.val() == g_int32b.val()), "bad");
	INVARIANT((f_int64.isNull() && g_int64.isNull())
		  || (!f_int64.isNull() && !g_int64.isNull() &&
		      f_int64.val() == g_int64.val()), "bad");
	INVARIANT((f_double.isNull() && g_double.isNull())
		  || (!f_double.isNull() && !g_double.isNull() &&
		      f_double.val() == g_double.val()), "bad");
	INVARIANT((f_variable32.isNull() && g_variable32.isNull())
		  || (!f_variable32.isNull() && !g_variable32.isNull() &&
		      f_variable32.stringval() == g_variable32.stringval()), "bad");
	++series1.pos;
	++series2.pos;
    }
    cout << "random null passed\n";
    
    cout << "test_compactnull - end\n";
}

void
test_empty_field_name()
{
    MersenneTwisterRandom rand;
    cout << format("test_empty_field_name - start seed=%d\n") % rand.seed_used;
    ExtentTypeLibrary typelib;

    // One real bool, 7 hidden ones
    typelib.registerType("<ExtentType name=\"Test::EmptyFieldName\" >\n"
			 "  <field type=\"int32\" name=\"int32a\" />\n"
			 "  <field type=\"int32\" name=\"int32b\" />\n"
			 "</ExtentType>\n");

    ExtentSeries series1(typelib,"Test::EmptyFieldName");
    Extent extent1(series1);
    series1.setExtent(extent1);

    int nrecords = 1000 + rand.randInt(10000);
    series1.createRecords(nrecords);
    Int32Field f_int32a(series1, "int32a");
    Int32Field f_int32b(series1, "int32b");

    for(int i=0; i < nrecords; ++i) {
	f_int32a.set(rand.randInt());
	f_int32b.set(rand.randInt());
	series1.next();
	SINVARIANT(series1.more());
    }

    series1.setExtent(extent1);
    Int32Field f_switch(series1, "");
    int counta = 0, countb = 0;
    for(int i=0; i < nrecords; ++i) {
	SINVARIANT(series1.more());
	bool which = rand.randInt(2) == 0;
	if (which) {
	    ++counta;
	    if (f_switch.getName() != "int32a" || rand.randInt(2) == 0) {
		f_switch.setFieldName("int32a");
	    }
	    SINVARIANT(f_switch.val() == f_int32a.val());
	} else {
	    ++countb;
	    if (f_switch.getName() != "int32b" || rand.randInt(2) == 0) {
		f_switch.setFieldName("int32b");
	    }
	    SINVARIANT(f_switch.val() == f_int32b.val());
	}
    }
    SINVARIANT(counta > nrecords / 3 && countb > nrecords / 3);
    cout << "passed empty_field_name tests\n";
}

void
test_extentseriescleanup()
{
    AssertBoostFnBefore(boost::bind(AssertBoostThrowExceptionFn,
				    _1,_2,_3,_4));

    bool caught = false;
    try {
	FATAL_ERROR("foo");
    } catch (AssertBoostException &e) {
	SINVARIANT(e.msg == "foo");
	caught = true;
    }
    SINVARIANT(caught);
    caught = false;
    try {
	{ 
	    ExtentSeries tmp;
	    BoolField *tmp2 = new BoolField(tmp, "buz");
	    tmp2 = NULL;
	}
    } catch (AssertBoostException &e) {
	AssertBoostClearFns();

	SINVARIANT(e.expression == "my_fields.size() == 0");
	SINVARIANT(e.msg == "You still have fields such as buz live on a series over type unset type.  You have to delete dynamically allocated fields before deleting the ExtentSeries.  Class member variables and static ones are automatically deleted in the proper order.");
	caught = true;
    }
    SINVARIANT(caught);
    cout << "Passed extent-series cleanup tests.\n";
}

int
main(int argc, char *argv[])
{
    Extent::setReadChecksFromEnv(true);

    runCryptUtilChecks();
    test_byteflip();
    test_primitives();
    test_extentpackunpack();
    test_makecomplexfile();
    test_nullsupport();
    test_varcompress();
    test_doublebase_nullable();
    test_compactnull();
    test_extentseriescleanup();
}
