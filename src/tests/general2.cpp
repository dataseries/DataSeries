// -*-C++-*-
/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <inttypes.h>

// g++ 4.3.2 debian lenny x86_64 will cast the large double in general.cpp to 255 if it
// can inline the test, otherwise it cases it to 135.  Forcing this function into a separate
// file fixes this problem.
uint8_t dblToUint8(double v) {
    return static_cast<uint8_t>(v);
}

