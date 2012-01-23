#include <boost/bind.hpp>

#include <Lintel/StringUtil.hpp>

#include <DataSeries/DSExpr.hpp>

#include "DSSModule.hpp"

class ExprTransformModule : public OutputSeriesModule {
public:
    ExprTransformModule(DataSeriesModule &source, const vector<ExprColumn> &expr_columns,
                        const string &output_table_name)
        : OutputSeriesModule(), source(source), input_series(), previous_row_series(),
          previous_row(), copier(output_series, previous_row_series), expr_columns(expr_columns),
          output_table_name(output_table_name)
    { }

    bool hasColumn(ExtentSeries &series, const string &field_name) {
        return series.getType()->hasColumn(field_name);
    }

    DSExprParser::Selector fieldNameToSeries(const string &name) {
        if (prefixequal(name, "in.") && hasColumn(input_series, name.substr(3))) {
            return make_pair(&input_series, name.substr(3));
        } else if (prefixequal(name, "out.") && hasColumn(output_series, name.substr(4))) {
            return make_pair(&output_series, name.substr(4));
        } else if (prefixequal(name, "prev.") && hasColumn(previous_row_series, name.substr(5))) {
            return make_pair(&previous_row_series, name.substr(5));
        } else if (hasColumn(input_series, name)) {
            return make_pair(&input_series, name);
        } else if (hasColumn(output_series, name)) {
            return make_pair(&output_series, name);
        } else {
            return DSExprParser::Selector();
        }
    }

    void firstExtent(Extent &in) {
        input_series.setExtent(in);
        string output_xml(str(format("<ExtentType name=\"expr-transform -> %s\""
                                     " namespace=\"server.example.com\" version=\"1.0\">\n")
                              % output_table_name));

        BOOST_FOREACH(ExprColumn &column, expr_columns) {
            string extra;
            if (column.type == "variable32") {
                extra = "pack_unique=\"yes\"";
            }
            output_xml.append(str(format("  <field type=\"%s\" name=\"%s\" %s/>\n")
                                  % column.type % column.name % extra));
        }
        output_xml.append("</ExtentType>\n");
        
        ExtentTypeLibrary lib;
        output_series.setType(lib.registerTypeR(output_xml));
        previous_row_series.setType(*output_series.getType());
        previous_row.reset(new Extent(*output_series.getType()));
        previous_row_series.setExtent(previous_row.get());
        previous_row_series.newRecord();
        copier.prep();

        DSExprParser::FieldNameToSelector field_name_to_selector
            = boost::bind(&ExprTransformModule::fieldNameToSeries, this, _1);
        boost::scoped_ptr<DSExprParser> parser(DSExprParser::MakeDefaultParser());
        
        outputs.reserve(expr_columns.size());
        for(size_t i = 0; i < expr_columns.size(); ++i) {
            const ExprColumn &column(expr_columns[i]);
            boost::shared_ptr<DSExpr> expr(parser->parse(field_name_to_selector, column.expr));
            GeneralField::Ptr field = GeneralField::make(output_series, column.name);
            
            outputs.push_back(Output(expr, field, 
                                     output_series.getType()->getFieldType(column.name)));
        }
    }

    virtual Extent *getExtent() {
        while (true) {
            if (input_series.getExtent() == NULL) {
                Extent *in = source.getExtent();
                if (in == NULL) {
                    return returnOutputSeries();
                }
                if (input_series.getType() == NULL) {
                    firstExtent(*in);
                }
                input_series.setExtent(in);
            }

            SINVARIANT(output_series.getExtent() == NULL);
            output_series.setExtent(new Extent(*output_series.getType()));

            while (input_series.more()) {
                output_series.newRecord();
                BOOST_FOREACH(Output &output, outputs) {
                    GeneralValue val;
                    switch(output.type) 
                        {
                        case ExtentType::ft_bool: val.setBool(output.expr->valBool()); break;
                        case ExtentType::ft_byte: val.setByte(output.expr->valInt64()); break;
                        case ExtentType::ft_int32: val.setInt32(output.expr->valInt64()); break;
                        case ExtentType::ft_int64: val.setInt64(output.expr->valInt64()); break;
                        case ExtentType::ft_double: val.setDouble(output.expr->valDouble()); break;
                        case ExtentType::ft_variable32: 
                            val.setVariable32(output.expr->valString()); break;
                        case ExtentType::ft_fixedwidth:
                            FATAL_ERROR("should have failed to create type");
                        default:
                            FATAL_ERROR("unknown type");
                        }
                    output.field->set(val);
                }
                input_series.next();

                if (previous_row->variabledata.size() > 16*1024) {
                    // limit unbounded memory usage.
                    previous_row->clear();
                    previous_row_series.newRecord();
                }
                copier.copyRecord();

                if (output_series.getExtent()->size() > 96*1024) {
                    return returnOutputSeries();
                }
            }
                
            if (!input_series.more()) {
                delete input_series.getExtent();
                input_series.clearExtent();
            }
        }
    }

    struct Output {
        Output(boost::shared_ptr<DSExpr> expr, GeneralField::Ptr field, ExtentType::fieldType type)
            : expr(expr), field(field), type(type) { }

        boost::shared_ptr<DSExpr> expr;
        GeneralField::Ptr field;
        ExtentType::fieldType type;
    };

    DataSeriesModule &source;
    ExtentSeries input_series, previous_row_series;
    boost::scoped_ptr<Extent> previous_row;
    ExtentRecordCopy copier;
    vector<ExprColumn> expr_columns;
    vector<Output> outputs;
    string output_table_name;
};

OutputSeriesModule::OSMPtr dataseries::makeExprTransformModule
(DataSeriesModule &source, const vector<ExprColumn> &expr_columns,
 const string &output_table_name) {
    return OutputSeriesModule::OSMPtr
        (new ExprTransformModule(source, expr_columns, output_table_name));
}

