#!/usr/bin/perl -w
use strict;
use Data::Dumper;

use lib "$ENV{BUILD_OPT}/DataSeries.server/src/server/gen-perl";
use lib "$ENV{HOME}/projects/thrift/lib/perl/lib";

use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::BufferedTransport;

use DataSeriesServer;
use Text::Table;

my $socket = new Thrift::Socket('localhost', 49476);
$socket->setRecvTimeout(1000*1000);
my $transport = new Thrift::BufferedTransport($socket, 4096, 4096);
my $protocol = new Thrift::BinaryProtocol($transport);
my $client = new DataSeriesServerClient($protocol);

$transport->open();
$client->ping();
print "Post Ping\n";

# tryImportCSV();
# tryImportSql();
# tryImportData();
# tryHashJoin();
# trySelect();
# tryProject();
# tryUpdate();
trySimpleStarJoin();
# tryStarJoin();
# tryUnion();

sub tryImportCSV {
    my $csv_xml = <<'END';
<ExtentType name="test-csv2ds" namespace="simpl.hpl.hp.com" version="1.0">
  <field type="bool" name="bool" />
  <field type="byte" name="byte" />
  <field type="int32" name="int32" />
  <field type="int64" name="int64" />
  <field type="double" name="double" />
  <field type="variable32" name="variable32" pack_unique="yes" />
</ExtentType>
END

    $client->importCSVFiles(["/home/anderse/projects/DataSeries/check-data/csv2ds-1.csv"],
                            $csv_xml, 'csv2ds-1', ",", "#");
    print "imported\n";

    my $ret = getTableData('csv2ds-1');
    print Dumper($ret);
}

sub tryImportSql {
    $client->importSQLTable('', 'scrum_task', 'scrum_task');

    my $ret = getTableData('scrum_task');
    print Dumper($ret);
}

sub tryImportData {
    my $data_xml = <<'END';
<ExtentType name="test-import" namespace="simpl.hpl.hp.com" version="1.0">
  <field type="bool" name="bool" />
  <field type="byte" name="byte" />
  <field type="int32" name="int32" />
  <field type="int64" name="int64" />
  <field type="double" name="double" />
  <field type="variable32" name="variable32" pack_unique="yes" />
</ExtentType>
END

    $client->importData("test-import", $data_xml, new TableData
                        ({ 'rows' =>
                           [[ 'on', 5, 1371, 111111, 11.0, "abcde" ],
                            [ 'on', 5, 1371, 111111, 11.0, "1q2w3e" ],
#                            [ 'on', 6, 1385, 112034, 12.0, "mnop" ],
                            [ 'off', 7, 12345, 999999999999, 123.456, "fghij" ] ]}));
}

sub tryHashJoin {
    tryImportData();

    importData('join-data', [ 'join_int32' => 'int32', 'join_str' => 'variable32' ],
               [[ 1371, "123" ],
                [ 1371, "456" ],
                [ 1371, "789" ],
                [ 9321, "xyz" ],
                [ 12345, "fghij" ] ]);

    print "\n----- Table A ----\n";
    print Dumper(getTableData("test-import"));
    print "\n---- Table B ----\n";
    print Dumper(getTableData("join-data"));


    $client->hashJoin('test-import', 'join-data', 'test-hash-join',
                      { 'int32' => 'join_int32' }, { 'a.int32' => 'table-a:join-int32',
                                                     'a.variable32' => 'table-a:extra-variable32',
                                                     'b.join_int32' => 'table-b:join-int32',
                                                     'b.join_str' => 'table-b:extra-variable32'});
    print "\n---- HashJoin Output ----\n";
    print Dumper(getTableData("test-hash-join"));
}

sub trySelect {
    tryImportData();
    $client->selectRows("test-import", "test-select", "int32 == 1371");
    print Dumper(getTableData('test-select'));

    print Dumper(getTableData('test-import', undef, 'int32 == 1371'));
}

sub tryProject {
    tryImportData();

    $client->projectTable('test-import', 'test-project', [ qw/bool int32 variable32/ ]);
    print Dumper(getTableData('test-project'));
}

sub tryUpdate {
    my $base_fields = <<'END';
  <field type="variable32" name="type" pack_unique="yes" />
  <field type="int32" name="count" />
END

    my $base_xml = <<"END";
<ExtentType name="base-table" namespace="simpl.hpl.hp.com" version="1.0">
$base_fields
</ExtentType>
END

    my $update_xml = <<"END";
<ExtentType name="base-table" namespace="simpl.hpl.hp.com" version="1.0">
  <field type="byte" name="update-op" />
$base_fields
</ExtentType>
END

    $client->importData("base-table", $base_xml, new TableData
                        ({ 'rows' =>
                           [[ "abc", 5 ],
                            [ "def", 6 ],
                            [ "ghi", 0 ],
                            [ "jkl", 7 ], ]}));

    $client->importData("update-table", $update_xml, new TableData
                        ({ 'rows' =>
                           [[ 1, "aaa", 3 ],
                            [ 2, "abc", 9 ],
                            [ 3, "def", 0 ],
                            [ 2, "ghi", 5 ] ]}));

    $client->sortedUpdateTable('base-table', 'update-table', 'update-op', [ 'type' ]);
    print Dumper(getTableData("base-table"));

    $client->importData("update-table", $update_xml, new TableData
                        ({ 'rows' =>
                           [[ 3, "aaa", 0 ],
                            [ 2, "mno", 1 ],
                            [ 1, "pqr", 2 ] ]}));
    $client->sortedUpdateTable('base-table', 'update-table', 'update-op', [ 'type' ]);
    print Dumper(getTableData("base-table"));

    # TODO: Add test for empty update..., and in general for empty extents in all the ops
}

sub tryStarJoin {
    tryImportData(); # columns bool, byte, int32, int64, double, variable32; 3 rows

    my $data_xml = <<'END';
<ExtentType name="dim-int" namespace="simpl.hpl.hp.com" version="1.0">
  <field type="int32" name="key_1" />
  <field type="int32" name="key_2" />
  <field type="variable32" name="val_1" pack_unique="yes" />
  <field type="variable32" name="val_2" pack_unique="yes" />
</ExtentType>
END

    printTable("test-import");
    $client->importData("dim-int", $data_xml, new TableData
                        ({ 'rows' =>
                           [[ 1371, 12345, "abc", "def" ],
                            [ 12345, 1371, "ghi", "jkl" ],
                            [ 999, 999, "mno", "pqr" ]] }));
    printTable("dim-int");

    # Same table dim-int, is used to create 2 different dimensions. In practice we could have
    # different table and selectively use columns from each table.
    my $dim_1 = new Dimension({ dimension_name => 'dim_int_1',
                                source_table => 'dim-int',
                                key_columns => ['key_1'],
                                value_columns => ['val_1', 'val_2'],
                                max_rows => 1000 });
    my $dim_2 = new Dimension({ dimension_name => 'dim_int_2',
                                source_table => 'dim-int',
                                key_columns => ['key_2'],
                                value_columns => ['val_1', 'val_2'],
                                max_rows => 1000 });

    my $dfj_1 = new DimensionFactJoin({ dimension_name => 'dim_int_1',
                                        fact_key_columns => [ 'int32' ],
                                        extract_values => { 'val_1' => 'dfj_1.dim1_val1' },
                                        missing_dimension_action => DFJ_MissingAction::DFJ_Unchanged });

    my $dfj_2 = new DimensionFactJoin({ dimension_name => 'dim_int_1',
                                        fact_key_columns => [ 'int32' ],
                                        extract_values => { 'val_1' => 'dfj_2.dim1_val1',
                                                            'val_2' => 'dfj_2.dim1_val2' },
                                        missing_dimension_action => DFJ_MissingAction::DFJ_Unchanged });

    my $dfj_3 = new DimensionFactJoin({ dimension_name => 'dim_int_2',
                                        fact_key_columns => [ 'int32' ],
                                        extract_values => { 'val_2' => 'dfj_3.dim1_val2' },
                                        missing_dimension_action => DFJ_MissingAction::DFJ_Unchanged });

    my $dfj_4 = new DimensionFactJoin({ dimension_name => 'dim_int_2',
                                        fact_key_columns => [ 'int32' ],
                                        extract_values => { 'val_1' => 'dfj_4.dim1_val1' },
                                        missing_dimension_action => DFJ_MissingAction::DFJ_Unchanged });

    $client->starJoin('test-import', [$dim_1, $dim_2], 'test-star-join',
                      { 'int32' => 'f.int_32', 'int64' => 'f.int_64'}, [$dfj_1, $dfj_2, $dfj_3, $dfj_4]);

    printTable("test-star-join");
    # print Dumper(getTableData("test-star-join"));
}



sub trySimpleStarJoin {
    # tryImportData(); # columns bool, byte, int32, int64, double, variable32; 3 rows

    my $person_xml = <<'END';
<ExtentType name="person-details" namespace="simpl.hpl.hp.com" version="1.0">
  <field type="variable32" name="Name" pack_unique="yes" />
  <field type="variable32" name="Country" pack_unique="yes" />
  <field type="variable32" name="State" pack_unique="yes" />
</ExtentType>
END

    my $country_xml = <<'END';
<ExtentType name="country-details" namespace="simpl.hpl.hp.com" version="1.0">
  <field type="variable32" name="Name" pack_unique="yes" />
  <field type="variable32" name="Capital" pack_unique="yes" />
  <field type="variable32" name="Currency" pack_unique="yes" />
</ExtentType>
END

    my $state_xml = <<'END';
<ExtentType name="country-details" namespace="simpl.hpl.hp.com" version="1.0">
  <field type="variable32" name="Name" pack_unique="yes" />
  <field type="variable32" name="Capital" pack_unique="yes" />
  <field type="variable32" name="TimeZone" pack_unique="yes" />
</ExtentType>
END

    $client->importData("person-details", $person_xml, new TableData
                        ({ 'rows' =>
                           [[ "John", "United States", "California" ],
                            [ "Adam", "United States", "Colarado" ],
                            [ "Ram", "India", "Karnataka"],
                            [ "Shiva", "India", "Maharastra"]] }));

    $client->importData("country-details", $country_xml, new TableData
                        ({ 'rows' =>
                           [[ "India", "New Delhi", "INR" ],
                            [ "United States", "Wahsington, D.C.", "Dollar"]] }));

    $client->importData("state-details", $state_xml, new TableData
                        ({ 'rows' =>
                           [[ "California", "Sacramento", "PST (UTC.8), PDT (UTC.7)" ],
                            [ "Colarado", "Denver", "MST=UTC-07, MDT=UTC-06" ],
                            [ "Karnataka", "Bangalore", "IST" ],
                            [ "Maharastra", "Mumbai", "IST" ]] }));

    printTable("person-details");
    printTable("country-details");
    printTable("state-details");

    # Same table dim-int, is used to create 2 different dimensions. In practice we could have
    # different table and selectively use columns from each table.
    my $dim_country = new Dimension({ dimension_name => 'dim_country',
                                      source_table => 'country-details',
                                      key_columns => ['Name'],
                                      value_columns => ['Capital'],
                                      max_rows => 1000 });

    my $dim_state = new Dimension({ dimension_name => 'dim_state',
                                    source_table => 'state-details',
                                    key_columns => ['Name'],
                                    value_columns => ['Capital'],
                                    max_rows => 1000 });

    my $dfj_1 = new DimensionFactJoin({ dimension_name => 'dim_country',
                                        fact_key_columns => ['Country'],
                                        extract_values => { 'Capital' => 'CountryCapital' },
                                        missing_dimension_action => DFJ_MissingAction::DFJ_Unchanged });

    my $dfj_2 = new DimensionFactJoin({ dimension_name => 'dim_state',
                                        fact_key_columns => [ 'State' ],
                                        extract_values => { 'Capital' => 'StateCapital' },
                                        missing_dimension_action => DFJ_MissingAction::DFJ_Unchanged });

    $client->starJoin('person-details', [$dim_country, $dim_state], 'test-star-join',
                      { 'Name' => 'Name', 'Country' => 'Country', 'State' => 'State'}, [$dfj_1, $dfj_2]);

    printTable("test-star-join");
}

sub unionTable {
    return new UnionTable({ 'table_name' => $_[0], 'extract_values' => $_[1] });
}

sub tryUnion {
    # extra column tests discard; different names tests rename
    importData('union-input-1', [ 'col1' => 'int32', 'col2' => 'variable32', 'col3' => 'byte',
                                  'col4' => 'int32' ],
               [ [ 100, "abc", 3, 1 ],
                 [ 2000, "ghi", 4, 4 ],
                 [ 3000, "def", 5, 5 ],
                 [ 12345, "ghi", 17, 7 ] ]);
    importData('union-input-2', [ 'cola' => 'int32', 'colb' => 'variable32', 'colc' => 'int32' ],
               [ [ 100, "def", 2 ],
                 [ 2000, "def", 3 ],
                 [ 12345, "efg", 6 ],
                 [ 12345, "ghi", 8 ],
                 [ 12345, "jkl", 9 ],
                 [ 20000, "abc", 10 ]]);
    $client->unionTables([ unionTable('union-input-1', { 'col1' => 'int', 'col2' => 'string',
                                                         'col4' => 'order' }),
                           unionTable('union-input-2', { 'cola' => 'int', 'colb' => 'string',
                                                         'colc' => 'order' }) ],
                         [ qw/int string/ ], 'union-output');
    print Dumper(getTableData("union-output"));
}

sub importData {
    my ($table_name, $table_desc, $rows) = @_;

    my $data_xml;
    if (ref $table_desc) {
        $data_xml = qq{<ExtentType name="$table_name" namespace="simpl.hpl.hp.com" version="1.0">\n};
        for (my $i=0; $i < @$table_desc; $i += 2) {
            my $name = $table_desc->[$i];
            my $desc = $table_desc->[$i+1];
            my $extra = $desc eq 'variable32' ? 'pack_unique="yes" ' : '';
            $data_xml .= qq{  <field type="$desc" name="$name" $extra/>\n};
        }
        $data_xml .= "</ExtentType>\n";
        print "Importing with $data_xml\n";
    } else {
        $data_xml = $table_desc;
    }
    $client->importData($table_name, $data_xml, new TableData({ 'rows' => $rows }));
}

sub getTableData {
    my ($table_name, $max_rows, $where_expr) = @_;

    $max_rows ||= 1000;
    $where_expr ||= '';
    my $ret = eval { $client->getTableData($table_name, $max_rows, $where_expr); };
    if ($@) {
        print Dumper($@);
        die "fail";
    }
    return $ret;
}

sub printTable($) {
    my ($table) = @_;
    my $table_refr = getTableData($table);

    my @columns;
    foreach my $col (@{$table_refr->{'columns'}}) {
        push(@columns, $col->{'name'} . "\n" . $col->{'type'});
    }

    my $tb = Text::Table->new(@columns);
    foreach my $row (@{$table_refr->{'rows'}}) {
        $tb->add(@{$row});
    }
    print "Table: $table \n\n";
    print $tb, "\n\n";
}

# print "pong\n";
# $client->importDataSeriesFiles(["/home/anderse/projects/DataSeries/check-data/nfs.set6.20k.ds"],
# 			       "NFS trace: attr-ops", "import");
# print "import\n";
# $client->mergeTables(["import", "import"], "2ximport");
# $client->test();
# print "test\n";


