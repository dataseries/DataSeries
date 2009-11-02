#include <string>

#include <boost/format.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/commonargs.hpp>

using namespace std;

static string extent_type_xml(
"<ExtentType name=\"Test::Simple\" namespace=\"ssd.hpl.hp.com\""
"	    version=\"1.0\">"
"  <field type=\"double\" name=\"a\" />"
"  <field type=\"double\" name=\"b\" />"
"  <field type=\"double\" name=\"c\" />"
"  <field type=\"variable32\" name=\"d\" />"
"  <field type=\"variable32\" name=\"e\" />"
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
    const ExtentType &extent_type(library.registerTypeR(extent_type_xml));
    dsout.writeExtentLibrary(library);
    
    ExtentSeries extent_series(extent_type);
    DoubleField a(extent_series, "a");
    DoubleField b(extent_series, "b");
    DoubleField c(extent_series, "c");
    Variable32Field d(extent_series, "d");
    Variable32Field e(extent_series, "e");

    const unsigned extent_size = 16 * 1024;
    OutputModule output(dsout, extent_series, extent_type, extent_size);

    for (double ax = 0.0; ax < 20.0; ax += 0.5) {
	for (double bx = 0.0; bx < 20.0; bx += 0.5) {
	    output.newRecord();
	    a.set(ax);
	    b.set(bx);
	    c.set(ax * bx);
	    d.set((boost::format("%1%") % ax).str());
	    e.set((boost::format("%1%") % bx).str());
	}
    }
	    
    output.close();
    dsout.close();

    return 0;
}
