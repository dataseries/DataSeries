#!/usr/bin/perl -w
use strict;
use Data::Dumper;

use lib '/home/anderse/build/opt-debian-5.0-x86_64/DataSeries.dblike/src/server/gen-perl';
use lib '/home/anderse/projects/thrift/lib/perl/lib';

use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::BufferedTransport;

use DataSeriesServer;

my $socket = new Thrift::Socket('localhost', 49476);
my $transport = new Thrift::BufferedTransport($socket, 4096, 4096);
my $protocol = new Thrift::BinaryProtocol($transport);
my $client = new DataSeriesServerClient($protocol);

$transport->open();
$client->ping();

# tryImportCSV();
# tryImportSql();
# tryImportData();
tryHashJoin();

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
                            [ 'off', 7, 12345, 999999999999, 123.456, "fghij" ] ]}));

    print Dumper(getTableData("test-import"));
}

sub tryHashJoin {
    tryImportData();

    my $data_xml = <<'END';
<ExtentType name="xxtest-import" namespace="simpl.hpl.hp.com" version="1.0">
  <field type="int32" name="int32" />
  <field type="variable32" name="variable32" pack_unique="yes" />
</ExtentType>
END

    $client->importData("join-data", $data_xml, new TableData
                        ({ 'rows' =>
                           [[ 1371, "123" ],
                            [ 1371, "456" ],
                            [ 1371, "789" ],
                            [ 12345, "fghij" ] ]}));
    print Dumper(getTableData("join-data"));


    $client->hashJoin('test-import', 'join-data', 'test-hash-join',
                      { 'int32' => 'int32' }, { 'a.int32' => 'table-a:join-int32',
                                                'a.variable32' => 'table-a:extra-variable32',
                                                'b.int32' => 'table-b:join-int32',
                                                'b.variable32' => 'table-b:extra-variable32'});

    print Dumper(getTableData("test-hash-join"));
}

sub getTableData {
    my ($table_name) = @_;

    my $ret = eval { $client->getTableData($table_name, 1000); };
    if ($@) {
        print Dumper($@);
        die "fail";
    }
    return $ret;
}

# print "pong\n";
# $client->importDataSeriesFiles(["/home/anderse/projects/DataSeries/check-data/nfs.set6.20k.ds"],
# 			       "NFS trace: attr-ops", "import");
# print "import\n";
# $client->mergeTables(["import", "import"], "2ximport");
# $client->test();
# print "test\n";


