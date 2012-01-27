// -*- C++ -*-

namespace cpp dataseries

struct TableColumn {
    1: required string name;
    2: required string type;
}

struct NullableString {
    1: optional string v;
}

struct TableData {
    1: required list<list<NullableString>> rows;
    2: optional list<TableColumn> columns;
    3: optional bool more_rows;
}

struct Dimension {
    // separate dimension_name and source_table allows for multiple dimensions generated
    // from a single table.
    1: required string dimension_name;
    2: required string source_table;
    3: required list<string> key_columns;
    4: required list<string> value_columns;
    5: required i32 max_rows;
}

enum DFJ_MissingAction {
    DFJ_InvalidEnumConst = 0;
    DFJ_SkipRow = 1;
    DFJ_Unchanged = 2;
}

struct DimensionFactJoin {
    1: required string dimension_name; 
    2: required list<string> fact_key_columns; // assume same order as in Dimension
    3: required map<string, string> extract_values; // value number to column name
    4: required DFJ_MissingAction missing_dimension_action;
}

struct UnionTable {
    1: required string table_name;
    2: required map<string, string> extract_values;
}

enum SortMode {
    SM_InvalidEnumConst = 0;
    SM_Ascending = 1;
    SM_Decending = 2;
}

enum NullMode {
    NM_InvalidEnumConst = 0;
    NM_Default = 1; // As if the entry isn't null, take the default "0" value.
    NM_First = 2; // nulls are < everyting
    NM_Last = 3; // nulls are > everything
}

struct SortColumn {
    1: required string column;
    2: required SortMode sort_mode;
    3: required NullMode null_mode;
}

struct ExprColumn {
    1: required string name;
    2: required string type;
    3: required string expr;
}

service DataSeriesServer {
    void ping();
    void shutdown();
    bool hasTable(string table_name);

    void importDataSeriesFiles(list<string> source_paths, string extent_type_name, 
                               string dest_table);
    void importCSVFiles(list<string> source_paths, string xml_desc, string dest_table, 
                        string field_separator, string comment_prefix);
    void importSQLTable(string dsn, string src_table, string dest_table);
    void importData(string dest_table, string xml_desc, TableData data);

    void mergeTables(list<string> source_tables, string dest_table);

    TableData getTableData(string source_table, i32 max_rows = 1000000, string where_expr = '');

    // Table a will be loaded into memory; join will be on all column pairs in eq_columns
    // keep columns sources a.<name> or b.<name> will be mapped to the dest name.

    // eq_columns contains the name of columns to be compared and keep_columns keys specfies the
    // columns which will be copied with value as destination column name. e.g.
    // SELECT a.store_name AS Store, b.sales AS Sales FROM a, b where a.id_for_store = b.store_id
    // would have eq_columns = { id_for_store => store_id }, and
    // keep_columns = { a.store_name => Store, b.sales => Sales}
    void hashJoin(string a_table, string b_table, string out_table,
                  map<string, string> eq_columns, map<string, string> keep_columns,
                  i32 max_a_rows = 1000000);

    void starJoin(string fact_table, list<Dimension> dimensions, string out_table,
                  map<string, string> fact_columns, list<DimensionFactJoin> dimension_fact_join,
                  i32 max_dimension_rows = 1000000);

    void selectRows(string in_table, string out_table, string where_expr);

    void projectTable(string in_table, string out_table, list<string> keep_columns);

    // Transform table has logically three sources of input:
    // 1) The input table
    // 2) The output table
    // 3) The previous output row table.
    //
    // Transform table scans the input table calculating one output row per input row.
    // For each output row, the expressions are evaluated in the list order and stored
    // in the output row, then the output row is copied to the previous output row, and
    // the input row is advanced.  
    //
    // For looking up expressions in either the input or output tables, the following order is
    // used, and the first match is found:
    // 1) If the name starts with in., strip off the in., and lookup the name in the input table
    // 2) If the name starts with out., strip off the out., and lookup the name in the output table
    // 3) If the name starts with prev., strip off the prev., and lookup the name in the previous
    //    row table
    // 3) Lookup the name in the input table
    // 4) Lookup the name in the output table
    //
    // At the current time, int32 output columns will be evaluated as int64 and truncated;
    // fixed width columns cannot be used.
    void transformTable(string in_table, string out_table, list<ExprColumn> expr_columns);

    // order_columns are from the output names; All output names must share 
    void unionTables(list<UnionTable> in_tables, list<string> order_columns, string out_table);

    void sortTable(string in_table, string out_table, list<SortColumn> by);

    // Replace base table with a merge between base_table and update_from.  The tables are
    // both assumed to be uniquely sorted based on primary_key.  The 
    // update applied is based on the value in update_column.  update_column should be a byte
    // column with the following possible values:

    // 1) insert the row: once base_table.row > update_table.row insert row and
    //    advance update_from

    // 2) replace the row: once base_table.row == update_table.row replace
    //    base_table.row with update_table.row, advance both.  
    //    If base_table.row > update_table.row, insert row, advance update_from

    // 3) delete the row: once base_table.row == update_table.row, advance both (copy neither)
    //    If base_table.row > update_table.row, advance update_from
    void sortedUpdateTable(string base_table, string update_from, string update_column,
                           list<string> primary_key);
}

exception InvalidTableName {
    1: required string table_name;
    2: required string why;
}

exception RequestError {
    1: required string why;
}
