/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/socket.h>

extern "C" {
#include <lzf.h>
}

#include <boost/format.hpp>

#include <Lintel/Clock.hpp>
#include <Lintel/LintelLog.hpp>

#include <DataSeries/ExtentIO.hpp>
#include <DataSeries/ExtentWriter.hpp>

ExtentWriter::ExtentWriter(const std::string &fileName, bool compress, bool is_socket)
    : fileName(fileName), fd(-1), compress(compress), is_socket(is_socket), extent_index(0), total_size(0), total_time(0.0) {
    fd = open(fileName.c_str(), O_CREAT | O_TRUNC | O_WRONLY | O_LARGEFILE, 0640);
    INVARIANT(fd >= 0, boost::format("Error opening file '%s' for write: %s")
              % fileName % strerror(errno));
    CHECKED(posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) == 0, "fadvise failed.");
}

ExtentWriter::ExtentWriter(int fd, bool compress, bool is_socket)
    : fd(fd), compress(compress), is_socket(is_socket), extent_index(0), total_size(0), total_time(0.0) {
}

ExtentWriter::ExtentWriter(bool compress, bool is_socket)
    : fd(-1), compress(compress), is_socket(is_socket), extent_index(0), total_size(0), total_time(0.0) {
}

void ExtentWriter::setFileDescriptor(int fd) {
    this->fd = fd;
}

ExtentWriter::~ExtentWriter() {
}

void ExtentWriter::writeExtent(Extent *extent) {
    Clock::Tfrac start_clock = Clock::todTfrac();

    if (extent == NULL) {
        Extent::ByteArray emptyArray;
        writeExtentBuffers(false, false, emptyArray, emptyArray);
    } else if (!compress) {
        writeExtentBuffers(false, false, extent->fixeddata, extent->variabledata);
    } else {
        Extent::ByteArray fixedData;
        Extent::ByteArray variableData;
        bool fixedDataCompressed = compressBuffer(extent->fixeddata, fixedData);
        bool variableDataCompressed = compressBuffer(extent->variabledata, variableData);
        writeExtentBuffers(fixedDataCompressed,
                           variableDataCompressed,
                           fixedDataCompressed ? fixedData : extent->fixeddata,
                           variableDataCompressed ? variableData : extent->variabledata);
    }

    Clock::Tfrac stop_clock = Clock::todTfrac();
    total_time += Clock::TfracToDouble(stop_clock - start_clock);
    ++extent_index;
}

void ExtentWriter::writeExtentBuffers(bool fixedDataCompressed,
                                      bool variableDataCompressed,
                                      Extent::ByteArray &fixedData,
                                      Extent::ByteArray &variableData) {
    ExtentDataHeader header;
    header.fixedDataCompressed = fixedDataCompressed ? 1 : 0;
    header.variableDataCompressed = variableDataCompressed ? 1 : 0;
    header.fixedDataSize = fixedData.size();
    header.variableDataSize = variableData.size();

    size_t size = sizeof(header) + fixedData.size() + variableData.size();

    if (is_socket) {
        writeBufferToSocket(&header, sizeof(header));
        writeBufferToSocket(fixedData.begin(), fixedData.size());
        writeBufferToSocket(variableData.begin(), variableData.size());
    } else {
        struct iovec iov[3];
        iov[0].iov_base = &header;
        iov[0].iov_len = sizeof(header);
        iov[1].iov_base = fixedData.begin();
        iov[1].iov_len = fixedData.size();
        iov[2].iov_base = variableData.begin();
        iov[2].iov_len = variableData.size();

        ssize_t ret = writev(fd, iov, sizeof(iov) / sizeof(struct iovec));

        INVARIANT(ret != -1,
                  boost::format("Error on write of %s bytes: %s")
                  % (unsigned long)size % strerror(errno));
        INVARIANT((size_t)ret == size,
                  boost::format("Partial write %s bytes out of %s bytes (disk full?): %s")
                  % ret % size % strerror(errno));
    }
    total_size += size;
}

void ExtentWriter::writeBufferToSocket(void *buffer, size_t size) {
    uint8_t *data = static_cast<uint8_t*>(buffer);
    while (size > 0) {
        ssize_t ret = ::send(fd, data, size, 0);
        INVARIANT(ret > 0, "Unable to write buffer to socket.");
        LintelLogDebug("ExtentWriter", boost::format("Wrote %s/%s bytes.") % ret % size);
        size -= ret;
        data += ret;
    }
}

bool ExtentWriter::compressBuffer(Extent::ByteArray &source, Extent::ByteArray &destination) {
    destination.resize(source.size() + sizeof(uint32_t), false);
    (reinterpret_cast<uint32_t*>(destination.begin()))[0] = source.size();

    unsigned int ret = lzf_compress(source.begin(), source.size(),
                                    destination.begin() + sizeof(uint32_t),
                                    destination.size() - sizeof(uint32_t));
    if (ret == 0) {
        return false;
    }
    destination.resize(ret + sizeof(uint32_t));
    return true;
}

double ExtentWriter::getThroughput() {
    return (total_time == 0.0) ? 0.0 : (double)total_size / total_time / (1 << 20);
}

double ExtentWriter::getTotalTime() {
    return total_time;
}

uint64_t ExtentWriter::getTotalSize() {
    return total_size;
}

void ExtentWriter::close() {
    if (fd == -1) {
        return;
    }
    LintelLogDebug("ExtentWriter", "Finished writing to file descriptor.");
    CHECKED(::close(fd) == 0, boost::format("Close failed: %s") % strerror(errno));
    fd = -1;
}
