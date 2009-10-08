// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A simple class for serializing temporary extents to temporary files. It is used in
    conjunction with ExtentReader.
    This class supports LZF compression.
*/

#ifndef __DATASERIES_EXTENTWRITER_H
#define __DATASERIES_EXTENTWRITER_H

#include <string>

#include <DataSeries/Extent.hpp>

class ExtentWriter {
public:
    ExtentWriter(const std::string &fileName, bool compress, bool is_socket);
    ExtentWriter(int fd, bool compress, bool is_socket);
    ExtentWriter(bool compress, bool is_socket);

    virtual ~ExtentWriter();

    void setFileDescriptor(int fd);

    /** Write the specified extent to a file. */
    void writeExtent(Extent *extent);

    /** Close the file. */
    void close();

    double getThroughput();
    uint64_t getTotalSize();
    double getTotalTime();

private:
    void writeExtentBuffers(bool fixedDataCompressed,
                            bool variableDataCompressed,
                            Extent::ByteArray &fixedData,
                            Extent::ByteArray &variableData);
    bool compressBuffer(Extent::ByteArray &source, Extent::ByteArray &destination);
    void writeBufferToSocket(void *buffer, size_t size);

    std::string fileName;
    int fd;
    bool compress;
    bool is_socket;
    size_t extent_index;

    size_t total_size;
    double total_time;
};

#endif
