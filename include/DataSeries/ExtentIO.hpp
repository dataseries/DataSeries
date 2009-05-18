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
    uint8_t fixedDataCompressed;
    uint8_t variableDataCompressed;
    uint32_t fixedDataSize; // compressed size of it's compressed
    uint32_t variableDataSize; // compressed size if it's compressed
} __attribute__((__packed__));

#endif
