// TODO-elie : use program options, use proper tyeps (int32_t, etc.),
// come up with a better name, and put invariants in the right places

#include <string>

#include <stdio.h>

#include <boost/format.hpp>

#include <Lintel/LintelLog.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/commonargs.hpp>


using namespace std;
using boost::format;

// TODO-elie: fill in the descriptive fields (e.g. units, epoch)
const string text_xml
(
 "<ExtentType namespace=\"ssd.hopl.hp.com\" name=\"Genread\" version=\"1.0\" "
 "comment=\"Data on recieving network traffic\" >\n"
 "   <field type=\"int32\" name=\"timedelta\" />\n"
 "   <field type=\"int32\" name=\"bytes\" />\n"
 "</ExtentType>"
 );

int32_t convertFile(string fileName, string outName);

int main(int argc, char ** argv) {
    SINVARIANT(argc==3);
    string file(argv[1]);
    string output(argv[2]);
    SINVARIANT(convertFile(file, output) == 0);
}

// TODO-elie : check return value of calls to fopen 
int32_t convertFile(string fileName, string outName) {
    FILE *inf = fopen(fileName.c_str(), "r");

    DataSeriesSink sink(outName, Extent::compress_gz, 7);
    ExtentTypeLibrary library;
    const ExtentType *extent_type = library.registerType(text_xml);
    ExtentSeries series;
    Int32Field f_time(series, "timedelta");
    Int32Field f_amount(series, "bytes");
    series.setType(*extent_type);
    OutputModule *outmodule = new OutputModule(sink, series, extent_type,
					       1024 * 1024);
    sink.writeExtentLibrary(library);

    int32_t time, amount;
    int32_t res;
    
    res = fscanf(inf, "%d %d", &time, &amount);
    while(res == 0) {
	fscanf(inf, "%*s");
	res = fscanf(inf, "%d %d", &time, &amount);	
	if(res==EOF) {
	    return 0;
	}
    }
    
    do {
	outmodule->newRecord();
	f_time.set(time);
	f_amount.set(amount);
	res = fscanf(inf, "%d %d", &time, &amount);		
    } while(res==2);
    fclose(inf);
    delete outmodule;
    sink.close();
    return 0;
}

