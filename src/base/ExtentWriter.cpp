#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

extern "C" {
#include <lzf.h>
}

#include <boost/format.hpp>

#include <Lintel/LintelLog.hpp>

#include <DataSeries/ExtentWriter.hpp>

ExtentWriter::ExtentWriter(const std::string &fileName, bool compress)
    : fd(-1), compress(compress) {
    // TODO-tomer: 0666 -> 0640?
    fd = open(fileName.c_str(), O_CREAT | O_WRONLY | O_LARGEFILE, 0666);
    INVARIANT(fd >= 0, boost::format("Error opening file '%s' for write: %s")
              % fileName % strerror(errno));
}

ExtentWriter::~ExtentWriter() {
    this->close();
}

void ExtentWriter::writeExtent(Extent *extent) {
    if (!compress) {
        writeExtentBuffers(extent->fixeddata, extent->variabledata);
        return;
    }

    // compress the data using LZF

    Extent::ByteArray fixedData;
    Extent::ByteArray variableData;
    compressBuffer(extent->fixeddata, fixedData);
    compressBuffer(extent->variabledata, variableData);
    writeExtentBuffers(fixedData, variableData);
}

void ExtentWriter::writeExtentBuffers(Extent::ByteArray &fixedData,
                                      Extent::ByteArray &variableData) {
    uint32_t headers[3];
    headers[0] = compress ? 1 : 0;
    headers[1] = fixedData.size();
    headers[2] = variableData.size();

    // Could use writeev (i.e., scatter gather) to do these three writes...
    writeBuffer(headers, sizeof(headers));
    writeBuffer(fixedData.begin(), fixedData.size());
    writeBuffer(variableData.begin(), variableData.size());

    // TODO-tomer: cast fixage
    LintelLogDebug("extentwriter", boost::format("Wrote extent to file (header: %lu bytes, "
                   "fixed data: %lu bytes, variable data: %lu bytes)") %
                   (unsigned long)sizeof(headers) %
                   (unsigned long)fixedData.size() %
                   (unsigned long)variableData.size());
}

void ExtentWriter::writeBuffer(const void *buffer, size_t size) {
    ssize_t ret = write(fd, buffer, size);
    INVARIANT(ret != -1,
              boost::format("Error on write of %lu bytes: %s")
              % (unsigned long)size % strerror(errno));
    INVARIANT((size_t)ret == size,
              boost::format("Partial write %ld bytes out of %lu bytes (disk full?): %s")
              % (long)ret % (unsigned long)size % strerror(errno));
}

void ExtentWriter::compressBuffer(Extent::ByteArray &source, Extent::ByteArray &destination) {
    destination.resize(source.size() + sizeof(uint32_t), false);
    // TODO-tomer: cast
    ((uint32_t*)destination.begin())[0] = source.size();
    unsigned int ret = lzf_compress(source.begin(), source.size(),
                                    destination.begin() + sizeof(uint32_t),
                                    destination.size() - sizeof(uint32_t));
    INVARIANT(ret != 0, "LZF compression failed");
    destination.resize(ret + sizeof(uint32_t));
    LintelLogDebug("extentwriter", boost::format("LZF: %lu => %lu + %lu bytes") %
            (unsigned long)source.size() % sizeof(uint32_t) % ((unsigned long)destination.size() - sizeof(uint32_t)));
}

void ExtentWriter::close() {
    if (fd == -1) {
        return;
    }
    CHECKED(::close(fd) == 0,
            boost::format("Close failed: %s") % strerror(errno));
    fd = -1;
}
