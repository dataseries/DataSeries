/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>

extern "C" {
#include <lzf.h>
}

#include <boost/format.hpp>

#include <Lintel/Clock.hpp>
#include <Lintel/LintelLog.hpp>

#include <DataSeries/ExtentIO.hpp>
#include <DataSeries/ExtentWriter.hpp>

ExtentWriter::ExtentWriter(const std::string &fileName, bool compress)
    : fd(-1), compress(compress), extent_index(0), total_size(0), total_time(0.0) {
    fd = open(fileName.c_str(), O_CREAT | O_WRONLY | O_LARGEFILE, 0640);
    INVARIANT(fd >= 0, boost::format("Error opening file '%s' for write: %s")
              % fileName % strerror(errno));
    CHECKED(posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) == 0, "fadvise failed.");
}

ExtentWriter::ExtentWriter(int fd, bool compress)
    : fd(fd), compress(compress), extent_index(0), total_size(0), total_time(0.0) {
}

ExtentWriter::ExtentWriter(bool compress)
    : fd(-1), compress(compress), extent_index(0), total_size(0), total_time(0.0) {
}

void ExtentWriter::setFileDescriptor(int fd) {
    this->fd = fd;
}

ExtentWriter::~ExtentWriter() {
    this->close();
}

void ExtentWriter::writeExtent(Extent *extent) {
    Clock::Tfrac start_clock = Clock::todTfrac();

    if (!compress) {
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

    struct iovec iov[3];
    iov[0].iov_base = &header;
    iov[0].iov_len = sizeof(header);
    iov[1].iov_base = fixedData.begin();
    iov[1].iov_len = fixedData.size();
    iov[2].iov_base = variableData.begin();
    iov[2].iov_len = variableData.size();

    ssize_t ret = writev(fd, iov, sizeof(iov) / sizeof(struct iovec));

    size_t size = sizeof(header) + fixedData.size() + variableData.size();
    INVARIANT(ret != -1,
              boost::format("Error on write of %s bytes: %s")
              % (unsigned long)size % strerror(errno));
    INVARIANT((size_t)ret == size,
              boost::format("Partial write %s bytes out of %s bytes (disk full?): %s")
              % ret % size % strerror(errno));

    total_size += size;
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

void ExtentWriter::close() {
    if (fd == -1) {
        return;
    }
    CHECKED(::close(fd) == 0, boost::format("Close failed: %s") % strerror(errno));
    fd = -1;
    if (total_time == 0.0) {
        LintelLogDebug("ExtentWriter", boost::format("Finished writing. [%s]") % this);
    } else {
        LintelLogDebug("ExtentWriter", boost::format("Finished writing. Throughput was %s MB/s. [%s]") % ((double)total_size / total_time / (1 << 20)) % this);
    }
}
