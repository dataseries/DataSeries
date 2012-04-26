#include <DataSeries/DSExpr.hpp>

#include "DSSModule.hpp"

class SelectModule : public OutputSeriesModule {
public:
    SelectModule(DataSeriesModule &source, const string &where_expr_str)
        : source(source), where_expr_str(where_expr_str), copier(input_series, output_series)
    { }

    virtual ~SelectModule() { }

    virtual Extent::Ptr getSharedExtent() {
        while (true) {
            Extent::Ptr in = source.getSharedExtent();
            if (in == NULL) {
                return returnOutputSeries();
            }
            if (input_series.getTypePtr() == NULL) {
                input_series.setType(in->getTypePtr());
                output_series.setType(in->getTypePtr());

                copier.prep();
                where_expr.reset(DSExpr::make(input_series, where_expr_str));
            }

            if (!output_series.hasExtent()) {
                output_series.newExtent();
            }
        
            for (input_series.setExtent(in); input_series.more(); input_series.next()) {
                if (where_expr->valBool()) {
                    output_series.newRecord();
                    copier.copyRecord();
                }
            }
            if (output_series.getExtentRef().size() > 96*1024) {
                return returnOutputSeries();
            }
        }
    }

    DataSeriesModule &source;
    string where_expr_str;
    ExtentSeries input_series;
    ExtentRecordCopy copier;
    boost::shared_ptr<DSExpr> where_expr;
};

DataSeriesModule::Ptr 
dataseries::makeSelectModule(DataSeriesModule &source, const string &where_expr_str) {
    return DataSeriesModule::Ptr(new SelectModule(source, where_expr_str));
}

