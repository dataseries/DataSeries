#include <string>

#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/DataSeriesModule.hpp>

using namespace std;

const string type_string = 
        "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Test::Garbage\" version=\"1.0\" >\n"
        "  <field type=\"int64\" name=\"number\" />\n"
        "</ExtentType>\n";

void printIndex(off64_t offset, Extent &extent) {
    cout << offset << "\t" << extent.getTypePtr()->getName() << "\t" << extent.size() << "\n";
}

int main(int argc, char *argv[]) {
    ExtentTypeLibrary library;
    const ExtentType::Ptr type = library.registerTypePtr(type_string);

    DataSeriesSink sink("extent-write-callback.ds", Extent::compression_algs[Extent::compress_mode_lzf].compress_flag);
    ExtentSeries series(type);
    OutputModule output(sink, series, type, 4 * 1024);
    Int64Field number(series, "number");

    sink.setExtentWriteCallback(&printIndex);
    sink.writeExtentLibrary(library);
    
    for (uint64_t i = 0; i < 64 * 1024; i++) {
        output.newRecord();
        number.set(i);
    }
}
