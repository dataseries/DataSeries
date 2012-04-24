#include <string>

#include <boost/bind.hpp>
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
    const ExtentType::Ptr extent_type(library.registerTypePtr(extent_type_xml));
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
    for(vector<ExtentSeries *>::iterator i = series->begin(); i != series->end(); ++i) {
        if ((**i).getType()->hasColumn(name)) {
            return make_pair(*i, name);
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

    const ExtentType::Ptr extent_type_1(library.registerTypePtr(extent_type_1_xml));
    const ExtentType::Ptr extent_type_2(library.registerTypePtr(extent_type_2_xml));

    ExtentSeries series1(extent_type_1), series2(extent_type_2);
    series1.newExtent();
    series2.newExtent();
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
    cout << "Series Select passed.\n";
}

void testNullExpr() {
    static string extent_type_xml(
"<ExtentType name=\"Test1\" namespace=\"ssd.hpl.hp.com\" version=\"1.0\" >"
"  <field type=\"int32\" name=\"a\" opt_nullable=\"yes\" />"
"  <field type=\"int32\" name=\"b\" opt_nullable=\"yes\" />"
"</ExtentType>");

    ExtentTypeLibrary library;

    const ExtentType::Ptr extent_type(library.registerTypePtr(extent_type_xml));

    ExtentSeries series(extent_type);
    series.newExtent();
    Int32Field a(series, "a", Field::flag_nullable), b(series, "b", Field::flag_nullable);

    boost::scoped_ptr<DSExpr> expr1(DSExpr::make(series, "1000"));
    boost::scoped_ptr<DSExpr> expr2(DSExpr::make(series, "\"abc\""));
    boost::scoped_ptr<DSExpr> expr3(DSExpr::make(series, "a"));
    boost::scoped_ptr<DSExpr> expr4(DSExpr::make(series, "-a"));
    boost::scoped_ptr<DSExpr> expr5(DSExpr::make(series, "a+b"));

    series.newRecord();
    a.set(1); 
    b.set(1);
    SINVARIANT(!expr1->isNull() && !expr2->isNull() && !expr3->isNull() && !expr4->isNull()
               && !expr5->isNull());
    a.setNull();
    SINVARIANT(!expr1->isNull() && !expr2->isNull() && expr3->isNull() && expr4->isNull()
               && expr5->isNull());
    b.setNull();
    SINVARIANT(!expr1->isNull() && !expr2->isNull() && expr3->isNull() && expr4->isNull()
               && expr5->isNull());
    a.set(1);
    SINVARIANT(!expr1->isNull() && !expr2->isNull() && !expr3->isNull() && !expr4->isNull()
               && expr5->isNull());
    cout << "Null Expr passed.\n";
}

int main(int argc, char **argv) {
    testSeriesSelect();
    testNullExpr();
    makeFile();

    return 0;
}
