// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file

    A dataseries module for reading temporary dataseries files that
    were probably written by ExtentWriter.

    This class supports LZF compression.
*/

// TODO-tomer: no __ at the beginning of ndefs, HPP at end
#ifndef DATASERIES_EXTENTREADER_HPP
#define DATASERIES_EXTENTREADER_HPP

#include <sys/socket.h>

#include <string>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentType.hpp>

class ExtentDataHeader;

class ExtentReader : public DataSeriesModule {
public:
    ExtentReader(const std::string &file_name, const ExtentType &extentType);
    ExtentReader(int fd, const ExtentType &extentType, bool is_socket = true);

    virtual ~ExtentReader();

    /** Read the next extent from the file. The caller is responsible for deallocating
        (via delete) the returned object. */
    Extent *getExtent();

    /** Close the file. */
    void close();

    uint64_t getTotalSize();
    double getTotalTime();
    double getThroughput();

private:
    void readExtentBuffers(const ExtentDataHeader &header, Extent::ByteArray &fixedData,
                           Extent::ByteArray &variableData);
    void decompressBuffer(Extent::ByteArray &source, Extent::ByteArray &destination);
    bool readBuffer(void *buffer, size_t size) {
        return is_socket ? readBufferFromSocket(buffer, size) : readBufferFromFile(buffer, size);
    }

    bool readBufferFromFile(void *buffer, size_t size);
    bool readBufferFromSocket(void *buffer, size_t size);

    int fd;
    const ExtentType &extentType; // TODO-tomer extent_type (and many others)
    size_t offset;
    bool is_socket;

    double total_time;
};

#endif
