#include <DataSeries/ExtentReader.hpp>

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include <boost/format.hpp>

#include <Lintel/LintelLog.hpp>

extern "C" {
#include <lzf.h>
}

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
    uint32_t headers[3];
    if (!readBuffer(headers, sizeof(headers))) {
        return NULL;
    }
    LintelLogDebug("extentreader", boost::format("Reading extent from disk (comrpessed/fixed/variable): %lu/%lu/%lu") % headers[0] % headers[1] % headers[2]);

    Extent *extent = new Extent(extentType);
    extent->fixeddata.resize(headers[1]);
    extent->variabledata.resize(headers[2]);
    readExtentBuffers(headers[0] == 1, headers[1], headers[2], extent->fixeddata, extent->variabledata);
    return extent;
}

void ExtentReader::close() {
    if (fd == -1) return;
    CHECKED(::close(fd) == 0,
            boost::format("Close failed: %s") % strerror(errno));
    fd = -1;
}

void ExtentReader::readExtentBuffers(bool compress,
                                     size_t compressedFixedDataSize,
                                     size_t compressedVariableDataSize,
                                     Extent::ByteArray &fixedData,
                                     Extent::ByteArray &variableData) {
    if (!compress) {
        fixedData.resize(compressedFixedDataSize);
        variableData.resize(compressedVariableDataSize);

        readBuffer(fixedData.begin(), compressedFixedDataSize);
        readBuffer(variableData.begin(), compressedVariableDataSize);
        return;
    }

    Extent::ByteArray compressedFixedData;
    Extent::ByteArray compressedVariableData;

    compressedFixedData.resize(compressedFixedDataSize);
    compressedVariableData.resize(compressedVariableDataSize);

    CHECKED(readBuffer(compressedFixedData.begin(), compressedFixedDataSize), "Unable to read fixed data");
    CHECKED(readBuffer(compressedVariableData.begin(), compressedVariableDataSize), "Unable to read variable data");

    decompressBuffer(compressedFixedData, fixedData);
    decompressBuffer(compressedVariableData, variableData);
}

void ExtentReader::decompressBuffer(Extent::ByteArray &source, Extent::ByteArray &destination) {
    destination.resize(*((uint32_t*)source.begin()));
    unsigned int ret = lzf_decompress(source.begin() + sizeof(uint32_t), source.size() - sizeof(uint32_t),
                                      destination.begin(), destination.size());
    INVARIANT((size_t)ret == destination.size(),
              boost::format("Decompressed data has incorrect size (%ld != %lu)") %
              ret % destination.size());
    LintelLogDebug("extentreader", boost::format("LZF: %lu + %lu => %lu") %
                   (unsigned long)source.size() % sizeof(uint32_t) % (unsigned long)destination.size());
}

bool ExtentReader::readBuffer(void *buffer, size_t size) {
    ssize_t ret = pread64(fd, buffer, size, offset);
    INVARIANT(ret != -1, boost::format("Error reading %lu bytes: %s") %
              size % strerror(errno));
    if (ret == 0) return false;
    offset += size;
    INVARIANT((size_t)ret == size, boost::format("Partial read %ld of %lu bytes: %s")
              % ret % size % strerror(errno));
    return true;
}
