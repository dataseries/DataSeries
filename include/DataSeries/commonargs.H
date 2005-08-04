/* -*-C++-*-
*******************************************************************************
*
* File:         commonargs.H
* RCS:          $Header: /mount/cello/cvs/DataSeries/include/DataSeries/commonargs.H,v 1.2 2003/09/06 00:54:20 anderse Exp $
* Description:  common argument handling for the converters
* Author:       Eric Anderson
* Created:      Tue May 27 13:39:16 2003
* Modified:     Wed Sep  3 18:54:32 2003 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#include <Extent.H>

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

