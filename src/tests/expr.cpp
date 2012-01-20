#include <string>

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/format.hpp>

#include <Lintel/StringUtil.hpp>

#include <DataSeries/commonargs.hpp>
#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/DSExpr.hpp>

using namespace std;


void makeFile() {
static string extent_type_xml(
"<ExtentType name=\"Test::Simple\" namespace=\"ssd.hpl.hp.com\" version=\"1.0\">"
"  <field type=\"double\" name=\"a\" />"
"  <field type=\"double\" name=\"b\" />"
"  <field type=\"double\" name=\"c\" />"
"  <field type=\"variable32\" name=\"d\" />"
"  <field type=\"variable32\" name=\"e\" />"
"</ExtentType>"
);

    string dsfn("expr.ds");

    DataSeriesSink dsout(dsfn, Extent::compress_lzf, 1);

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
}

DSExprParser::Selector firstMatch(vector<ExtentSeries *> *series, const string &in_name) {
    string name = in_name;
    if (prefixequal(name, "remove.")) {
        name = name.substr(7);
    }
    BOOST_FOREACH(ExtentSeries *e, *series) {
        if (e->getType()->hasColumn(name)) {
            return make_pair(e, name);
        }
    }
    return DSExprParser::Selector(); // make_pair(static_cast<ExtentSeries *>(NULL), string());
}

void testSeriesSelect() {
    static string extent_type_1_xml(
"<ExtentType name=\"Test1\" namespace=\"ssd.hpl.hp.com\" version=\"1.0\" >"
"  <field type=\"int32\" name=\"a\" />"
"  <field type=\"int32\" name=\"b\" />"
"</ExtentType>");
    static string extent_type_2_xml(
"<ExtentType name=\"Test2\" namespace=\"ssd.hpl.hp.com\" version=\"1.0\" >"
"  <field type=\"int32\" name=\"c\" />"
"  <field type=\"int32\" name=\"d\" />"
"</ExtentType>");

    ExtentTypeLibrary library;

    const ExtentType &extent_type_1(library.registerTypeR(extent_type_1_xml));
    const ExtentType &extent_type_2(library.registerTypeR(extent_type_2_xml));

    Extent e1(extent_type_1), e2(extent_type_2);
    ExtentSeries series1(&e1), series2(&e2);
    Int32Field a(series1, "a"), b(series1, "b"), c(series2, "c"), d(series2, "d");

    vector<ExtentSeries *> all_series;
    all_series.push_back(&series1);
    all_series.push_back(&series2);
    
    DSExpr *expr = DSExpr::make(boost::bind(firstMatch, &all_series, _1), 
                                "(a*remove.c) + (remove.b*d)");

    series1.newRecord();
    series2.newRecord();

    SINVARIANT(expr->valInt64() == 0);
    a.set(1);
    c.set(1);
    SINVARIANT(expr->valInt64() == 1);
    b.set(2);
    d.set(2);
    SINVARIANT(expr->valInt64() == 5);
    c.set(0);
    SINVARIANT(expr->valInt64() == 4);
    d.set(0);
    SINVARIANT(expr->valInt64() == 0);

    delete expr;
}

int main(int argc, char **argv) {
    testSeriesSelect();
    makeFile();

    return 0;
}
