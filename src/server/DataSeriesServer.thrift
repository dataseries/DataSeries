// -*- C++ -*-

namespace cpp dataseries

struct TableData {
    list<list<string>> rows;
    bool more_rows;
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
    void hashJoin(string a_table, string b_table, string out_table,
                  map<string, string> eq_columns, map<string, string> keep_columns,
                  i32 max_a_rows = 1000000);

    void selectRows(string in_table, string out_table, string where_expr);

    void projectTable(string in_table, string out_table, list<string> keep_columns);

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
