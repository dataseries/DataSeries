/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

extern "C" {
#include <lzf.h>
}

#include <boost/format.hpp>

#include <Lintel/Clock.hpp>
#include <Lintel/LintelLog.hpp>

#include <DataSeries/ExtentIO.hpp>
#include <DataSeries/ExtentWriter.hpp>

ExtentWriter::ExtentWriter(const std::string &fileName, bool compress)
    : fd(-1), compress(compress), extent_index(0) {
    fd = open(fileName.c_str(), O_CREAT | O_WRONLY | O_LARGEFILE, 0640);
    INVARIANT(fd >= 0, boost::format("Error opening file '%s' for write: %s")
              % fileName % strerror(errno));
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

    LintelLogDebug("ExtentWriter", boost::format("Wrote extent #%s (%s seconds, %s bytes).") % extent_index % Clock::TfracToDouble(stop_clock - start_clock) % extent->size());
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

    // Could use writeev (i.e., scatter gather) to do these three writes...
    writeBuffer(&header, sizeof(header));
    writeBuffer(fixedData.begin(), fixedData.size());
    writeBuffer(variableData.begin(), variableData.size());

    // TODO-tomer: use the ExtentWriter convention for debug's on a class.
    LintelLogDebug("extentwriter", boost::format("Wrote extent to file (header: %s bytes, "
                   "fixed data: %s bytes, variable data: %s bytes)") %
                   sizeof(header) %
                   fixedData.size() %
                   variableData.size());
}

void ExtentWriter::writeBuffer(const void *buffer, size_t size) {
    ssize_t ret = write(fd, buffer, size);

    INVARIANT(ret != -1,
              boost::format("Error on write of %s bytes: %s")
              % (unsigned long)size % strerror(errno));
    INVARIANT((size_t)ret == size,
              boost::format("Partial write %s bytes out of %s bytes (disk full?): %s")
              % ret % size % strerror(errno));
}

bool ExtentWriter::compressBuffer(Extent::ByteArray &source, Extent::ByteArray &destination) {
    destination.resize(source.size() + sizeof(uint32_t), false);
    (reinterpret_cast<uint32_t*>(destination.begin()))[0] = source.size();

    LintelLogDebug("extentwriter", boost::format("Compressing %s bytes via LZF") % source.size());
    unsigned int ret = lzf_compress(source.begin(), source.size(),
                                    destination.begin() + sizeof(uint32_t),
                                    destination.size() - sizeof(uint32_t));
    if (ret == 0) {
        return false;
    }
    destination.resize(ret + sizeof(uint32_t));
    LintelLogDebug("extentwriter", boost::format("LZF: %s => %s + %s bytes") %
            source.size() % sizeof(uint32_t) % ((unsigned long)destination.size() - sizeof(uint32_t)));
    return true;
}

void ExtentWriter::close() {
    if (fd == -1) {
        return;
    }
    CHECKED(::close(fd) == 0, boost::format("Close failed: %s") % strerror(errno));
    fd = -1;
}
