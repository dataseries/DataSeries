#include <boost/format.hpp>

#include <Lintel/LintelLog.hpp>

#include <DataSeries/DataSeriesSink.hpp>

using namespace std;
using boost::format;

const string extent_type_xml = 
"<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"File-Rotation\" version=\"1.0\" >\n"
"  <field type=\"int32\" name=\"thread\" />\n"
"  <field type=\"int32\" name=\"count\" />\n"
"</ExtentType>\n";

void writeExtent(DataSeriesSink &output, const ExtentType &type, uint32_t thread,
                 uint32_t &count, uint32_t rows) {
    ExtentSeries s(new Extent(type));
    Int32Field f_thread(s, "thread");
    Int32Field f_count(s, "count");
    for (uint32_t i = 0; i < rows; ++i) {
        s.newRecord();
        f_thread.set(thread);
        f_count.set(count);
        ++count;
    }
    output.writeExtent(*s.getExtent(), NULL);
    delete s.getExtent();
    s.clearExtent();
}

void simpleFileRotation() {
    ExtentTypeLibrary library;
    const ExtentType &type = library.registerTypeR(extent_type_xml);

    DataSeriesSink output(Extent::compress_lzf);

    uint32_t count;
    for (uint32_t i=0; i < 10; ++i) {
        LintelLog::info(format("simple round %d") % i);
        output.open(str(format("simple-fr-%d.ds") % i));
        output.writeExtentLibrary(library);
        writeExtent(output, type, 0, count, i+5);
        output.close();
    }
}

int main(int argc, char *argv[]) {
    INVARIANT(argc == 2, format("Usage: %s (simple)") % argv[0]);

    string mode(argv[1]);
    if (mode == "simple") {
        simpleFileRotation();
    } else {
        FATAL_ERROR("unknown mode, expected simple");
    }
}
