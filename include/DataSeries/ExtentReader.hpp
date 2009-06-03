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

#include <string>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentType.hpp>

class ExtentDataHeader;

class ExtentReader : public DataSeriesModule {
public:
    ExtentReader(const std::string &file_name, const ExtentType &extentType);
    virtual ~ExtentReader();

    /** Read the next extent from the file. The caller is responsible for deallocating
        (via delete) the returned object. */
    Extent *getExtent();

    /** Close the file. */
    void close();

private:
    void readExtentBuffers(const ExtentDataHeader &header, Extent::ByteArray &fixedData,
                           Extent::ByteArray &variableData);
    void decompressBuffer(Extent::ByteArray &source, Extent::ByteArray &destination);
    bool readBuffer(void *buffer, size_t size);

    int fd;
    const ExtentType &extentType; // TODO-tomer extent_type (and many others)
    size_t offset;
};

#endif
