// -*-C++-*-
/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

// TODO-shirant: document *no memcpy* and explain why this is not optimized inline in the normal
// source module? - use doxygen triple-slash comments

/** @file
    DataSeries Module for reading a single DataSeries file with no compression and correct endianess
*/

#ifndef __SIMPLE_SOURCE_MODULE_H
#define __SIMPLE_SOURCE_MODULE_H

#include <string>

#include <DataSeries/DataSeriesModule.hpp>

class ExtentType;

class SimpleSourceModule : public DataSeriesModule {
public:
    SimpleSourceModule(const std::string &filename);
    virtual ~SimpleSourceModule();
    virtual Extent* getExtent();

private:
    void init();
    void openFile();
    void closeFile();
    bool readFile(Extent::ByteArray &data, size_t amount, size_t dataOffset = 0); // false indicates EOF

    // TODO-shirant: confirm with Eric: 100 or 120 column limit on each row? Regardless, the
    // following line overflows...
    bool readExtent(/* out */ std::string &typeName, /* out */ Extent::ByteArray &fixedData, /* out */ Extent::ByteArray &variableData);

    std::string filename;
    int fd;
    off64_t offset;
    uint64_t extentCount;
    const ExtentType *commonExtentType; // we really only support one type (this is how we'll know we've reached the end)
    ExtentTypeLibrary library;
    bool done;
};

#endif
