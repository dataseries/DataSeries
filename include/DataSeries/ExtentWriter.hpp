// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A simple class for serializing extents to temporary files.
    This class supports LZF compression.
*/

#ifndef __DATASERIES_EXTENTWRITER_H
#define __DATASERIES_EXTENTWRITER_H

#include <string>

#include <DataSeries/Extent.hpp>

class ExtentWriter {
public:
    ExtentWriter(const std::string &fileName, bool compress=true);
    virtual ~ExtentWriter();

    /** Write the specified extent to a file. */
    void writeExtent(Extent *extent);

    /** Close and flush the file. */
    void close();

private:
    void writeExtentBuffers(Extent::ByteArray &fixedData,
                            Extent::ByteArray &variableData);
    void compressBuffer(Extent::ByteArray &source, Extent::ByteArray &destination);
    void writeBuffer(const void *buffer, size_t size);

    int fd;
    bool compress;
};

#endif
