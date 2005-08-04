/* -*-C++-*-
*******************************************************************************
*
* File:         dsread.C
* RCS:          $Header: /mount/cello/cvs/DataSeries/src/process/dsread.C,v 1.1 2004/09/28 05:08:32 anderse Exp $
* Description:  A program with no purpose other than to do profiling and benchmarking.
* Author:       Eric Anderson
* Created:      Sun Sep 26 21:32:26 2004
* Modified:     Mon Sep 27 21:21:24 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2004, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#include <DataSeriesModule.H>

#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>

static inline double 
timediff(struct timeval &end,struct timeval &start)
{
    return end.tv_sec - start.tv_sec + (end.tv_usec - start.tv_usec) * 1e-6;
}

static inline double 
timediff(struct rusage &end, struct rusage &start)
{
    return timediff(end.ru_utime,start.ru_utime) + timediff(end.ru_stime,start.ru_stime);
}

extern bool dataseries_enable_preuncompress_check, dataseries_enable_postuncompress_check, dataseries_enable_unpack_variable32_check;

int
main(int argc, char *argv[])
{
    if (true) {
	printf("read test 1...\n");
	struct rusage rusage_start;
	AssertAlways(getrusage(RUSAGE_SELF,&rusage_start)==0,
		     ("getrusage failed: %s\n",strerror(errno)));
	SourceModule source;
	
	for(int i=1;i<argc;++i) {
	    source.addSource(argv[i]);
	}
	DataSeriesModule::getAndDelete(source);
	struct rusage rusage_end;
	AssertAlways(getrusage(RUSAGE_SELF,&rusage_end)==0,
		     ("getrusage failed: %s\n",strerror(errno)));
	printf("extents: %.2f MB in %.2f secs decode time, %.2f unaccounted\n",
	       (double)(source.total_uncompressed_bytes)/(1024.0*1024),
	       source.decode_time, timediff(rusage_end,rusage_start)-source.decode_time);
    }

    if (false) {
	printf("read test 2...\n");
	struct rusage rusage_start;
	AssertAlways(getrusage(RUSAGE_SELF,&rusage_start)==0,
		     ("getrusage failed: %s\n",strerror(errno)));
	SourceModule source;
	
	for(int i=1;i<argc;++i) {
	    source.addSource(argv[i]);
	}
	DataSeriesModule::getAndDelete(source);
	struct rusage rusage_end;
	AssertAlways(getrusage(RUSAGE_SELF,&rusage_end)==0,
		     ("getrusage failed: %s\n",strerror(errno)));
	printf("extents: %.2f MB in %.2f secs decode time, %.2f unaccounted\n",
	       (double)(source.total_uncompressed_bytes)/(1024.0*1024),
	       source.decode_time, timediff(rusage_end,rusage_start)-source.decode_time);
    }

    dataseries_enable_preuncompress_check = false;
    dataseries_enable_postuncompress_check = false;
    if (false) {
	printf("read test 3 (no pre/post check)...\n");
	struct rusage rusage_start;
	AssertAlways(getrusage(RUSAGE_SELF,&rusage_start)==0,
		     ("getrusage failed: %s\n",strerror(errno)));
	SourceModule source;
	
	for(int i=1;i<argc;++i) {
	    source.addSource(argv[i]);
	}
	DataSeriesModule::getAndDelete(source);
	struct rusage rusage_end;
	AssertAlways(getrusage(RUSAGE_SELF,&rusage_end)==0,
		     ("getrusage failed: %s\n",strerror(errno)));
	printf("extents: %.2f MB in %.2f secs decode time (no pre/post checking), %.2f unaccounted\n",
	       (double)(source.total_uncompressed_bytes)/(1024.0*1024),
	       source.decode_time, timediff(rusage_end,rusage_start)-source.decode_time);
    }

    dataseries_enable_unpack_variable32_check = false;
    if (false) {
	printf("read test 4 (no pre/post/var32 checks)...\n");
	struct rusage rusage_start;
	AssertAlways(getrusage(RUSAGE_SELF,&rusage_start)==0,
		     ("getrusage failed: %s\n",strerror(errno)));
	SourceModule source;
	
	for(int i=1;i<argc;++i) {
	    source.addSource(argv[i]);
	}
	DataSeriesModule::getAndDelete(source);
	struct rusage rusage_end;
	AssertAlways(getrusage(RUSAGE_SELF,&rusage_end)==0,
		     ("getrusage failed: %s\n",strerror(errno)));
	printf("extents: %.2f MB in %.2f secs decode time (no pre/post/var32 checking), %.2f unaccounted\n",
	       (double)(source.total_uncompressed_bytes)/(1024.0*1024),
	       source.decode_time, timediff(rusage_end,rusage_start)-source.decode_time);
    }
}
    
