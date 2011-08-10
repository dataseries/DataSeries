#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/DataSeriesModule.hpp>
#include <string>

const std::string type_string = "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Test::Garbage\" version=\"1.0\" >\n    <field type=\"int64\" name=\"number\" />\n</ExtentType>";

void printIndex(off64_t offset, Extent &extent) {
    std::cout << offset << "\t" << extent.type.getName()
              << "\t" << extent.size() << "\n";
}

int main(int argc, char *argv[]) {
    ExtentTypeLibrary library;
    const ExtentType &type = library.registerTypeR(type_string);

    DataSeriesSink sink("extent-write-callback.ds", Extent::compress_lzf);
    ExtentSeries series(type);
    OutputModule output(sink, series, type, 4 * 1024);
    Int64Field number(series, "number");

    sink.setExtentWriteCallback(&printIndex);
    sink.writeExtentLibrary(library);
    
    for(uint64_t i = 0; i < 64 * 1024; i++) {
        output.newRecord();
        number.set(i);
    }
}
