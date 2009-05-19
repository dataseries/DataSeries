// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Common definitions for ExtentReader and ExtentWriter.
*/

#ifndef EXTENT_IO_H
#define EXTENT_IO_H

struct ExtentDataHeader {
    uint32_t fixedDataSize; // (compressed) size
    uint32_t variableDataSize; // (compressed) size
    uint8_t fixedDataCompressed; // effectively a bool.
    uint8_t variableDataCompressed; // effectively a bool
} __attribute__((__packed__));

#endif
