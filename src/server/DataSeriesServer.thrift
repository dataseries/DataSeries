// -*- C++ -*-

namespace cpp dataseries

struct TableData {
    list<list<string>> rows;
    bool more_rows;
}

service DataSeriesServer {
    void ping();
    void importDataSeriesFiles(list<string> source_paths, string extent_type, string dest_table);
    void importCSVFiles(list<string> source_paths, string xml_desc, string dest_table, 
                        string field_separator, string comment_prefix);
    void importSQLTable(string dsn, string src_table, string dest_table);
    void importData(string dest_table, string xml_desc, TableData data);

    void mergeTables(list<string> source_tables, string dest_table);

    TableData getTableData(string source_table, i32 max_rows);

    // Table a will be loaded into memory; join will be on all column pairs in eq_columns
    // keep columns sources a.<name> or b.<name> will be mapped to the dest name.
    void hashJoin(string a_table, string b_table, string out_table,
                  map<string, string> eq_columns, map<string, string> keep_columns,
                  i32 max_a_rows = 1000000);
}

exception InvalidTableName {
    1: required string table_name;
    2: required string why;
}

exception RequestError {
    1: required string why;
}
