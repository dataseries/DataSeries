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
    : fd(-1), extentType(extentType), offset(0), is_socket(false), total_time(0.0) {
    fd = open(fileName.c_str(), O_RDONLY | O_LARGEFILE);
    INVARIANT(fd >= 0, boost::format("Error opening file '%s' for read: %s")
              % fileName % strerror(errno));
    CHECKED(posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL) == 0, "fadvise failed.");
}

ExtentReader::ExtentReader(int fd, const ExtentType &extentType, bool is_socket)
    : fd(fd), extentType(extentType), offset(0), is_socket(is_socket), total_time(0.0) {
}

ExtentReader::~ExtentReader() {
    this->close();
}

Extent *ExtentReader::getExtent() {
    Clock::Tfrac start_clock = Clock::todTfrac();
    Extent *extent = NULL;

    ExtentDataHeader header;
    if (readBuffer(&header, sizeof(header))) {
        extent = new Extent(extentType);
        readExtentBuffers(header, extent->fixeddata, extent->variabledata);
    }

    Clock::Tfrac stop_clock = Clock::todTfrac();
    total_time += Clock::TfracToDouble(stop_clock - start_clock);

    return extent;
}

void ExtentReader::close() {
    if (fd == -1) {
        return;
    }
    LintelLogDebug("ExtentReader", boost::format("Finished reading from file descriptor. Throughput was %s MB/s.") %  ((double)offset / total_time / (1 << 20)));
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
}

bool ExtentReader::readBufferFromSocket(void *buffer, size_t size) {
    size_t bytes_left = size;
    uint8_t *byte_buffer = reinterpret_cast<uint8_t*>(buffer);
    while (bytes_left > 0) {
        ssize_t bytes_received = recv(fd, byte_buffer, bytes_left, 0);
        INVARIANT(bytes_received != -1, "Unable to read from socket.");
        if (bytes_received == 0) {
            INVARIANT(bytes_left == size, "Partial read.");
            return false;
        }
        bytes_left -= bytes_received;
        byte_buffer += bytes_received;
    }
    offset += size;
    return true;
}

bool ExtentReader::readBufferFromFile(void *buffer, size_t size) {
    ssize_t ret = read(fd, buffer, size); // pread64(fd, buffer, size, offset);
    INVARIANT(ret != -1, boost::format("Error reading %s bytes: %s") %
              size % strerror(errno));
    if (ret == 0) {
        return false;
    }
    offset += ret;
    INVARIANT((size_t)ret == size, boost::format("Partial read %s of %s bytes: %s")
              % ret % size % strerror(errno));
    return true;
}
