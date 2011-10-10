// -*- C++ -*-

namespace cpp dataseries

struct TableColumn {
    1: required string name;
    2: required string type;
}

struct TableData {
    1: required list<list<string>> rows;
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

service DataSeriesServer {
    void ping();
    bool hasTable(string table_name);

    void importDataSeriesFiles(list<string> source_paths, string extent_type, string dest_table);
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
    // order_columns are from the output names; All output names must share 
    void unionTables(list<UnionTable> in_tables, list<string> order_columns, string out_table);

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
