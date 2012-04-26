#ifndef DATASERIES_SERVERMODULES_HPP
#define DATASERIES_SERVERMODULES_HPP

#include <DataSeries/DataSeriesModule.hpp>

#include "ThrowError.hpp"
#include "OutputSeriesModule.hpp"
#include "RenameCopier.hpp"
#include "gen-cpp/DataSeriesServer.h"

// TODO: consider re-doing a lot of the other modules in this form, most of them don't benefit
// from inheritence.

namespace dataseries {
    struct SortColumnImpl {
        SortColumnImpl(GeneralField::Ptr field, bool sort_less, NullMode null_mode)
            : field(field), sort_less(sort_less), null_mode(null_mode)
        { 
            TINVARIANT(null_mode == NM_First || null_mode == NM_Last);
        }
        GeneralField::Ptr field;
        bool sort_less; // a < b ==> sort_less
        NullMode null_mode;
    };

    struct UM_UnionTable : dataseries::UnionTable {
        UM_UnionTable() : UnionTable(), source(), series(), copier(), order_fields() { }
        UM_UnionTable(const dataseries::UnionTable &ut, DataSeriesModule::Ptr source) 
            : UnionTable(ut), source(source), series(), copier(), order_fields() { }
    
        ~UM_UnionTable() throw () {}
    
        DataSeriesModule::Ptr source;
        ExtentSeries series;
        RenameCopier::Ptr copier;
        std::vector<SortColumnImpl> order_fields;
    };


    DataSeriesModule::Ptr makeTeeModule(DataSeriesModule &source_module, 
                                        const std::string &output_path);
    DataSeriesModule::Ptr makeTableDataModule(DataSeriesModule &source_module,
                                              TableData &into, uint32_t max_rows);
    OutputSeriesModule::OSMPtr makeHashJoinModule
      (DataSeriesModule &a_input, int32_t max_a_rows, DataSeriesModule &b_input,
       const std::map<std::string, std::string> &eq_columns,
       const std::map<std::string, std::string> &keep_columns,
       const std::string &output_table_name);

    OutputSeriesModule::OSMPtr makeStarJoinModule
      (DataSeriesModule &fact_input, const std::vector<Dimension> &dimensions,
       const std::string &output_table_name,
       const std::map<std::string, std::string> &fact_columns,
       const std::vector<DimensionFactJoin> &dimension_fact_join_in,
       const HashMap< std::string, boost::shared_ptr<DataSeriesModule> > &dimension_modules);

    DataSeriesModule::Ptr makeSelectModule(DataSeriesModule &source, 
                                           const std::string &where_expr_str);
    OutputSeriesModule::OSMPtr makeProjectModule(DataSeriesModule &source, 
                                                 const std::vector<std::string> &keep_columns);
    DataSeriesModule::Ptr makeSortedUpdateModule
      (DataSeriesModule &base_input, DataSeriesModule &update_input,
       const std::string &update_column, const std::vector<std::string> &primary_key);

    OutputSeriesModule::OSMPtr makeUnionModule
      (const std::vector<dataseries::UM_UnionTable> &in_sources, 
       const std::vector<SortColumn> &order_columns,
       const std::string &output_table_name);

    OutputSeriesModule::OSMPtr makeSortModule
      (DataSeriesModule &source, const std::vector<SortColumn> &sort_by);

    OutputSeriesModule::OSMPtr makeExprTransformModule
    (DataSeriesModule &source, const std::vector<ExprColumn> &expr_columns,
     const std::string &output_table_name);
}

#endif
