// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation
*/

#include <Lintel/LintelAssert.hpp>
#include <DataSeries/commonargs.hpp>

void
getPackingArgs(int *argc, char *argv[], commonPackingArgs *commonArgs)
{
    for(int cur_arg = 1;cur_arg < *argc;++cur_arg) {
	if (!(argv[cur_arg][0] == '-' && argv[cur_arg][1] == '-')) {
	    continue;
	} 
	if (argv[cur_arg][2] == '\0') {
	    break;
	}
	if (strcmp(argv[cur_arg],"--disable-lzo") == 0) {
	    commonArgs->compress_modes &= ~Extent::compress_lzo;
	} else if (strcmp(argv[cur_arg],"--disable-gz") == 0) {
	    commonArgs->compress_modes &= ~Extent::compress_zlib;
	} else if (strcmp(argv[cur_arg],"--disable-bz2") == 0) {
	    commonArgs->compress_modes &= ~Extent::compress_bz2;
	} else if (strcmp(argv[cur_arg],"--disable-lzf") == 0) {
	    commonArgs->compress_modes &= ~Extent::compress_lzf;
	} else if (strcmp(argv[cur_arg],"--compress-lzo") == 0) {
	    commonArgs->compress_modes = Extent::compress_lzo;
	} else if (strcmp(argv[cur_arg],"--compress-gz") == 0) {
	    commonArgs->compress_modes = Extent::compress_zlib;
	} else if (strcmp(argv[cur_arg],"--compress-bz2") == 0) {
	    commonArgs->compress_modes = Extent::compress_bz2;
	} else if (strcmp(argv[cur_arg],"--compress-lzf") == 0) {
	    commonArgs->compress_modes = Extent::compress_lzf;
	} else if (strcmp(argv[cur_arg],"--compress-none") == 0) {
	    commonArgs->compress_modes = 0;
	} else if (strcmp(argv[cur_arg],"--enable-lzo") == 0) {
	    commonArgs->compress_modes |= Extent::compress_lzo;
	} else if (strcmp(argv[cur_arg],"--enable-gz") == 0) {
	    commonArgs->compress_modes |= Extent::compress_zlib;
	} else if (strcmp(argv[cur_arg],"--enable-bz2") == 0) {
	    commonArgs->compress_modes |= Extent::compress_bz2;
	} else if (strcmp(argv[cur_arg],"--enable-lzf") == 0) {
	    commonArgs->compress_modes |= Extent::compress_lzf;
	} else if (strncmp(argv[cur_arg],"--compress-level=",17) == 0) {
	    commonArgs->compress_level = atoi(argv[cur_arg]+17);
	    AssertAlways(commonArgs->compress_level > 0 && commonArgs->compress_level < 10,
			 ("compression level %d (%s) invalid, should be 1..9\n",
			  commonArgs->compress_level,argv[cur_arg]));
	} else if (strncmp(argv[cur_arg],"--extent-size=",14) == 0) {
	    commonArgs->extent_size = atoi(argv[cur_arg]+14);
	    AssertAlways(commonArgs->extent_size >= 1024,
			 ("extent size %d (%s), < 1024 doesn't make sense\n",
			  commonArgs->extent_size,argv[cur_arg]));
	} else {
	    continue; // ignore unrecognized arguments
	}
	for(int i=cur_arg+1;i< *argc;i++) {
	    argv[i-1] = argv[i];
	}
	--*argc;
	--cur_arg;
    }
    if (commonArgs->extent_size < 0) {
	// the analysis work in the paper shows that 64-96k is the
	// peak decompression rate, with a sacrifice of a slight
	// amount from the maximum compression ratio.  Compression
	// ratio flattens out somewhere around 512k, but if we are not
	// using bz2, then the goal is likely speed rather than
	// compression, so default to a smaller extent size.  For bz2,
	// we are going for maximal compression, which is flat past
	// 8MB.
	commonArgs->extent_size = 64*1024;
	if (commonArgs->compress_modes & Extent::compress_bz2) {
	    commonArgs->extent_size = 16*1024*1024;
	}
    }
}

static const std::string packing_opts = 
  "    --{disable,compress,enable}-{lzf,lzo,gz,bz2} (default --enable-*)\n"
  "    --compress-none --compress-level=[0-9] (default 9)\n"
  "    --extent-size=[>=1024] (default 16*1024*1024 if bz2 is enabled, 64*1024 otherwise)\n";

const std::string &packingOptions() 
{
    return packing_opts;
}

