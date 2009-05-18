/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

extern "C" {
#include <lzf.h>
}

#include <boost/format.hpp>

#include <Lintel/LintelLog.hpp>

#include <DataSeries/ExtentIO.hpp>
#include <DataSeries/ExtentReader.hpp>

ExtentReader::ExtentReader(const std::string &fileName, const ExtentType &extentType)
    : fd(-1), extentType(extentType), offset(0) {
    fd = open(fileName.c_str(), O_RDONLY | O_LARGEFILE);
    INVARIANT(fd >= 0, boost::format("Error opening file '%s' for read: %s")
              % fileName % strerror(errno));
}

ExtentReader::~ExtentReader() {
    this->close();
}

Extent *ExtentReader::getExtent() {
    ExtentDataHeader header;
    if (!readBuffer(&header, sizeof(header))) {
        return NULL;
    }

    Extent *extent = new Extent(extentType);
    readExtentBuffers(header, extent->fixeddata, extent->variabledata);
    return extent;
}

void ExtentReader::close() {
    if (fd == -1) {
        return;
    }
    CHECKED(::close(fd) == 0,
            boost::format("Close failed: %s") % strerror(errno));
    fd = -1;
}

void ExtentReader::readExtentBuffers(const ExtentDataHeader &header,
                                     Extent::ByteArray &fixedData,
                                     Extent::ByteArray &variableData) {
    if (!header.fixedDataCompressed) {
        fixedData.resize(header.fixedDataSize);
        readBuffer(fixedData.begin(), header.fixedDataSize);
    } else {
        Extent::ByteArray compressedFixedData;
        compressedFixedData.resize(header.fixedDataSize);
        CHECKED(readBuffer(compressedFixedData.begin(), header.fixedDataSize), "Unable to read fixed data");
        decompressBuffer(compressedFixedData, fixedData);
    }

    if (!header.variableDataCompressed) {
        variableData.resize(header.variableDataSize);
        readBuffer(variableData.begin(), header.variableDataSize);
    } else {
        Extent::ByteArray compressedVariableData;
        compressedVariableData.resize(header.variableDataSize);
        CHECKED(readBuffer(compressedVariableData.begin(), header.variableDataSize), "Unable to read variable data");
        decompressBuffer(compressedVariableData, variableData);
    }
}

void ExtentReader::decompressBuffer(Extent::ByteArray &source, Extent::ByteArray &destination) {
    destination.resize(*(reinterpret_cast<uint32_t*>(source.begin())));
    unsigned int ret = lzf_decompress(source.begin() + sizeof(uint32_t), source.size() - sizeof(uint32_t),
                                      destination.begin(), destination.size());
    INVARIANT((size_t)ret == destination.size(),
              boost::format("Decompressed data has incorrect size (%s != %s)") %
              ret % destination.size());
    LintelLogDebug("extentreader", boost::format("LZF: %s + %s => %s") %
                   source.size() % sizeof(uint32_t) % destination.size());
}

bool ExtentReader::readBuffer(void *buffer, size_t size) {
    ssize_t ret = pread64(fd, buffer, size, offset);
    INVARIANT(ret != -1, boost::format("Error reading %s bytes: %s") %
              size % strerror(errno));
    if (ret == 0) {
        return false;
    }
    offset += size;
    INVARIANT((size_t)ret == size, boost::format("Partial read %s of %s bytes: %s")
              % ret % size % strerror(errno));
    return true;
}
