// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    common argument handling for the converters
*/

#include <DataSeries/Extent.H>

struct commonPackingArgs {
    int compress_level;
    int compress_modes;
    int extent_size;
    commonPackingArgs() 
	: compress_level(9), 
	  compress_modes(Extent::compress_all), 
  	  extent_size(-1)
    { }
};

// ignores unrecognized arguments, stops getting arguments at a --
void getPackingArgs(int *argc, char *argv[], commonPackingArgs *commonArgs);
const std::string &packingOptions();

