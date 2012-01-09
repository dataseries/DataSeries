#include "DSSModule.hpp"

class TableDataModule : public RowAnalysisModule {
public:
    TableDataModule(DataSeriesModule &source_module, TableData &into, uint32_t max_rows)
        : RowAnalysisModule(source_module), into(into), max_rows(max_rows)
    { 
        into.rows.reserve(max_rows < 4096 ? max_rows : 4096);
        into.more_rows = false;
    }

    virtual void firstExtent(const Extent &e) {
        series.setType(e.getType());
        const ExtentType &extent_type(e.getType());
        fields.reserve(extent_type.getNFields());
        for (uint32_t i = 0; i < extent_type.getNFields(); ++i) {
            string field_name(extent_type.getFieldName(i));
            fields.push_back(GeneralField::make(series, field_name));
            into.columns.push_back(TableColumn(field_name, 
                                               extent_type.getFieldTypeStr(field_name)));
        }
        into.__isset.columns = true;
    }

    virtual void processRow() {
        if (into.rows.size() == max_rows) {
            into.more_rows = true;
            into.__isset.more_rows = true;
            return;
        }
        into.rows.resize(into.rows.size() + 1);
        vector<string> &row(into.rows.back());
        row.reserve(fields.size());
        BOOST_FOREACH(GeneralField::Ptr g, fields) {
            row.push_back(g->val().valString());
        }
    }

    TableData &into;
    uint32_t max_rows;
    vector<GeneralField::Ptr> fields;
};

DataSeriesModule::Ptr dataseries::makeTableDataModule(DataSeriesModule &source_module,
                                                      TableData &into, uint32_t max_rows) {
    return DataSeriesModule::Ptr(new TableDataModule(source_module, into, max_rows));
}
