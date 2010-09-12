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
    void mergeTables(list<string> source_tables, string dest_table);
    TableData getTableData(string source_table, i32 max_rows);
    void test();
}

exception InvalidTableName {
    1: required string table_name;
    2: required string why;
}

exception RequestError {
    1: required string why;
}
