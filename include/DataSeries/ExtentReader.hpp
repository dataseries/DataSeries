// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A simple class for reading temporary extents from files. It is used in
    conjunction with ExtentWriter.
    This class supports LZF compression.
*/

#ifndef __DATASERIES_EXTENTREADER_H
#define __DATASERIES_EXTENTREADER_H

#include <string>

#include <DataSeries/Extent.hpp>
#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/ExtentType.hpp>

class ExtentReader : public DataSeriesModule {
public:
    ExtentReader(const std::string &fileName, const ExtentType &extentType);
    virtual ~ExtentReader();

    /** Read the next extent from the file. The caller is responsible for deallocating
        (via delete) the returned object. */
    Extent *getExtent();

    /** Close the file. */
    void close();

private:
    void readExtentBuffers(bool compress,
                           size_t compressedFixedDataSize,
                           size_t compressedVariableDataSize,
                           Extent::ByteArray &fixedData,
                           Extent::ByteArray &variableData);
    void decompressBuffer(Extent::ByteArray &source, Extent::ByteArray &destination);
    bool readBuffer(void *buffer, size_t size);

    int fd;
    const ExtentType &extentType;
    size_t offset;
};

#endif
