#include <string>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/commonargs.hpp>

using namespace std;

static string extent_type_xml(
"<ExtentType name=\"Test::Simple\" namespace=\"ssd.hpl.hp.com\""
"	    version=\"1.0\">"
"  <field type=\"double\" name=\"a\" />"
"  <field type=\"double\" name=\"b\" />"
"</ExtentType>"
);

int main(int argc, char **argv)
{
    commonPackingArgs packing_args;

    getPackingArgs(&argc, argv, &packing_args);

    string dsfn("expr.ds");

    DataSeriesSink dsout(dsfn, packing_args.compress_modes,
			 packing_args.compress_level);

    ExtentTypeLibrary library;
    const ExtentType *extent_type = library.registerType(extent_type_xml);
    dsout.writeExtentLibrary(library);
    
    ExtentSeries extent_series(*extent_type);
    DoubleField a(extent_series, "a");
    DoubleField b(extent_series, "b");

    const unsigned extent_size = 16 * 1024;
    OutputModule output(dsout, extent_series, extent_type, extent_size);

    for (double ax = 0.0; ax < 20.0; ax += 0.5) {
	for (double bx = 0.0; bx < 20.0; bx += 0.5) {
	    output.newRecord();
	    a.set(ax);
	    b.set(bx);
	}
    }
	    
    output.close();
    dsout.close();

    return 0;
}
