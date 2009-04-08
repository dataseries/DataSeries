// TODO-shirant: The following is the preferred style for includes (group by project, list projects
// from lowest level to highest level, list alphabetically within a project):
// #include <stdio.h>
// #include <fcntl.h>
//
// #include <Lintel/LintelLog.hpp>
//
// #include <DataSeries/Extent.hpp>
// #include <DataSeries/SimpleSourceModule.hpp>

#include <DataSeries/SimpleSourceModule.hpp>
#include <DataSeries/Extent.hpp>
#include <Lintel/LintelLog.hpp>
#include <stdio.h>
#include <fcntl.h>

using namespace std;

// TODO-shirant: Why are these (FileHeader and ExtentHeaer) defined in the cpp? These look like
// general(ish) datastructures?
// Think if this actually works on 32 AND 64 bit given alignment and packing rules.  Maybe ask Eric
struct FileHeader {
    char version[4];
    int32_t magic1;
    int64_t magic2;
    double magic3;
    double magic4;
    double magic5;
};

struct ExtentHeader {
    uint32_t compressedFixedDataSize;
    uint32_t compressedVariableDataSize;
    uint32_t recordCount;
    uint32_t decompressedVariableDataSize;
    uint32_t compressedAdler32Digest;
    uint32_t partlyUnpackedBjhashDigest;
    uint8_t fixedDataCompressionType;
    uint8_t variableDataCompressionType;
    uint8_t extentTypeNameLength;
    uint8_t zero;
};

SimpleSourceModule::SimpleSourceModule(const string &filename) :
        filename(filename), fd(-1), offset(0), extentCount(0) {
    init(); // better than working in the constructor due to gcc/gdb bug (no debug info for local variables in constructors)
}

void SimpleSourceModule::init() {
    openFile();
    Extent::ByteArray data;
    INVARIANT(readFile(data, sizeof(FileHeader)), "unable to read the DS file's header");
    FileHeader *header = reinterpret_cast<FileHeader*>(data.begin());
    INVARIANT(header->magic1 == 0x12345678, "invalid DS file (or wrong endianess)");

    library.registerType(ExtentType::getDataSeriesXMLType());
    library.registerType(ExtentType::getDataSeriesIndexTypeV0()); // not really needed - not going to look at the index

    // we can read the first extent (the XML type extent) now that we registered the type with our library
    Extent *typeExtent = getExtent();
    SINVARIANT(typeExtent != NULL);

    // iterate over all the types in the XML type index and register them with our library
    ExtentSeries series(typeExtent); // a series of one extent
    Variable32Field typevar(series, "xmltype");
    for (; series.pos.morerecords(); ++series.pos) {
        string xmltype = typevar.stringval();
        library.registerType(xmltype);
    }
    delete typeExtent;
}

SimpleSourceModule::~SimpleSourceModule() {
    closeFile();
}

void SimpleSourceModule::openFile() {
    SINVARIANT(fd == -1);
    fd = open(filename.c_str(), O_RDONLY | O_LARGEFILE);
    INVARIANT(fd >= 0, boost::format("error opening file '%s' for read: %s")
        % filename % strerror(errno));
}

void SimpleSourceModule::closeFile() {
    SINVARIANT(fd != 0);
    close(fd);
}

bool SimpleSourceModule::readFile(Extent::ByteArray &data, size_t amount, size_t dataOffset) {
    data.resize(dataOffset + amount, false);
    bool result = Extent::checkedPread(fd, offset, data.begin() + dataOffset, amount, true);
    offset += amount; // we're moving forward in the file
    return result;
}

bool SimpleSourceModule::readExtent(string &typeName, Extent::ByteArray &fixedData, Extent::ByteArray &variableData) {
    Extent::ByteArray headerData;

    // read the extent header
    if (!readFile(headerData, sizeof(ExtentHeader))) {
        return false; // couldn't find any additional data extents
    }
    ExtentHeader *header = reinterpret_cast<ExtentHeader*>(headerData.begin());

    // sanity checks
    SINVARIANT(header->compressedVariableDataSize == header->decompressedVariableDataSize - 4);
    INVARIANT(header->fixedDataCompressionType == 0 && header->variableDataCompressionType == 0,
            "compressed extents are not supported with this source module");

    // read the type name
    Extent::ByteArray typeNameData;
    SINVARIANT(readFile(typeNameData, header->extentTypeNameLength));
    typeName = string((char*)typeNameData.begin(), header->extentTypeNameLength);

    // TODO-shirant: Is there a / should there be a macro for alignment stuff? Should 4 be a const
    // or defined elsewhere (so e.g. it can be 8 on a 64 bit machine)?
    // Perhaps ask Eric
    offset += (4 - offset % 4) % 4; // 4-byte alignment

    // read the fixed data
    SINVARIANT(readFile(fixedData, header->compressedFixedDataSize));

    offset += (4 - offset % 4) % 4;

    // TODO: we can further optimize by consolidating the last two readFile calls (we can't consolidate with the next one due to the 4-byte zero)

    // read the variable data
    SINVARIANT(readFile(variableData, header->compressedVariableDataSize, 4));
    *reinterpret_cast<int32_t*>(variableData.begin()) = 0;

    return true;
}

Extent* SimpleSourceModule::getExtent() {
    Extent::ByteArray fixedData;
    Extent::ByteArray variableData;

    off64_t extentOffset = offset;
    string extentTypeName;
    INVARIANT(readExtent(extentTypeName, fixedData, variableData), "the file is done and we haven't even reached the index");

    const ExtentType *extentType = library.getTypeByName(extentTypeName);
    ++extentCount;

    if (extentType == &ExtentType::getDataSeriesIndexTypeV0()) {
        return NULL; // this is the index extent so we're done
    }
    Extent *extent = new Extent(*extentType);
    extent->extent_source = filename;
    extent->extent_source_offset = extentOffset;
    extent->fixeddata.swap(fixedData);
    extent->variabledata.swap(variableData);
    return extent;
}
