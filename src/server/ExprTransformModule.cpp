#include <Lintel/StringUtil.hpp>

#include <DataSeries/DSExpr.hpp>

#include "DSSModule.hpp"

class ExprTransformModule : public OutputSeriesModule {
public:
    ExprTransformModule(DataSeriesModule &source, const vector<ExprColumn> &expr_columns)
        : source(source), expr_columns(expr_columns)
    { }

    void firstExtent(Extent &in) {
        string output_xml(str(format("<ExtentType name=\"expr-transform -> %s\""
                                     " namespace=\"server.example.com\" version=\"1.0\">\n")
                              % output_table_name));

        BOOST_FOREACH(ExprColumn &column, expr_columns) {
            string extra;
            if (column.column_type == "variable32") {
                extra = "pack_unique=\"yes\"";
            }
            output_xml.append(str(format("  <field type=\"%s\" name=\"%s\" %s/>\n")
                                  % column.column_type % column.column_name % extra));
        }
        output_xml.append("</ExtentType>\n");
        
        ExtentTypeLibrary lib;
        output_series.setType(lib.registerTypeR(output_xml));
    }

    ExtentSeries *fieldNameToSeries(const string &field_name) {
        if (prefixequal(field_name, "in.")) {
            return
    }

    virtual Extent *getExtent() {
        while (true) {
            Extent *in = source.getExtent();
            if (in == NULL) {
                return returnOutputSeries();
            }
            if (input_series.getType() == NULL) {
                firstExtent(*in);
            }

            if (output_series.getExtent() == NULL) {
                output_series.setExtent(new Extent(*output_series.getType()));
            }

            // ...

            if (output_series.getExtent()->size() > 96*1024) {
                return returnOutputSeries();
            }
        }
    }        
};

OutputSeriesModule::OSMPtr dataseries::makeExprTransformModule
(DataSeriesModule &source, const vector<ExprColumn> &expr_columns) {
    return OutputSeriesModule::OSMPtr(new ExprTransformModule(source, expr_columns));
}

