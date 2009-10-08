#include <iostream>
#include <fstream>
#include <string>

#include <Lintel/LintelLog.hpp>

#include <DataSeries/commonargs.hpp>
#include <DataSeries/DataSeriesModule.hpp>

using namespace std;
using boost::format;

const string text_xml(
  "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Gensort\" version=\"1.0\" comment=\"\" >\n"
  "  <field type=\"fixedwidth\" name=\"key\" size=\"10\" />\n"
  "  <field type=\"fixedwidth\" name=\"value\" size=\"90\" />\n"
  "</ExtentType>"
  );

ExtentSeries series;
OutputModule *outmodule;
FixedWidthField key(series, "key");
FixedWidthField value(series, "value");

int
main(int argc,char *argv[])
{
    LintelLog::parseEnv();
    commonPackingArgs packing_args;
    getPackingArgs(&argc, argv, &packing_args);

    INVARIANT(argc == 3,
        format("Usage: %s [ds-common-args] inname outdsname")
        % argv[0]);

    ifstream infile(argv[1]);
    INVARIANT(infile.is_open(), format("Unable to open file %s") % argv[1]);

    DataSeriesSink outds(argv[2], packing_args.compress_modes, packing_args.compress_level);
    ExtentTypeLibrary library;
    const ExtentType *extent_type = library.registerType(text_xml);
    series.setType(*extent_type);

    LintelLogDebug("gensort2ds", format("Creating output file with extent size: %s") % packing_args.extent_size);

    outmodule = new OutputModule(outds, series, extent_type, packing_args.extent_size);
    outds.writeExtentLibrary(library);

    int64_t record_count = 0;

    uint8_t record[100];
    infile.read((char*)record, 100);
    while (!infile.eof()) {
        outmodule->newRecord();
        key.set(record + 0);
        value.set(record + 10);
        ++record_count;
        infile.read((char*)record, 100);
    }

    delete outmodule;
    outds.close();

    cout << "Finished writing " << record_count << " records" << endl;

    infile.close();
    return 0;
}

