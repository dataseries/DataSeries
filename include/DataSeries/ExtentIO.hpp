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

// TODO-tomer: rename ExtentReader/Writer to
// TemporaryFileReader/TemporaryFileWriter, and make this
// TemporaryExtentHeader.  Remove the attribute packed.

struct ExtentDataHeader {
    // TODO-tomer: add TemporaryFileHeader (or some better name so you
    // can use it for the network exchange also); header_size and
    // magic number will go in there.
    // TODO-tomer: add uint32_t header_size to this and check it matches

    // TODO-tomer: naming convention is variable_name
	ExtentDataHeader() : fixedDataSize(0), variableDataSize(0), fixedDataCompressed(0), variableDataCompressed(0) {}
    uint32_t fixedDataSize; // (compressed) size
    uint32_t variableDataSize; // (compressed) size
    uint8_t fixedDataCompressed; // effectively a bool.
    uint8_t variableDataCompressed; // effectively a bool
} __attribute__((__packed__));

#endif
