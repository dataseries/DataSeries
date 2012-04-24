#include <sys/types.h>
#include <sys/wait.h>

#include <string>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/DataSeriesModule.hpp>

const std::string type_string = "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Test::Garbage\" version=\"1.0\" >\n    <field type=\"int64\" name=\"number\" />\n</ExtentType>";

int main(int argc, char *argv[]) {
    int child = fork();
    if (child == 0) {
        ExtentTypeLibrary library;
        const ExtentType::Ptr type = library.registerTypePtr(type_string);

        DataSeriesSink sink("incomplete-ds-file.ds", Extent::compress_lzf);
        ExtentSeries series(type);
        OutputModule output(sink, series, type, 4 * 1024);
        Int64Field number(series, "number");

        sink.writeExtentLibrary(library);
    
        for (uint64_t i = 0; i < 64 * 1024; i++) {
            output.newRecord();
            number.set(i);

            if (i == 50 * 1024) {
                output.flushExtent();
                sink.flushPending();
            }
            assert(i < 50 * 1024);
        }
    }

    int error = waitpid(child, NULL, 0);
    if (error != child) {
        return -1;
    }
    return 0;
}
