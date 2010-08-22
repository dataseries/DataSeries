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

my $ret = eval { $client->getTableData('csv2ds-1', 1000); };
if ($@) {
    print Dumper($@);
    die "fail";
}
print Dumper($ret);

# print "pong\n";
# $client->importDataSeriesFiles(["/home/anderse/projects/DataSeries/check-data/nfs.set6.20k.ds"],
# 			       "NFS trace: attr-ops", "import");
# print "import\n";
# $client->mergeTables(["import", "import"], "2ximport");
# $client->test();
# print "test\n";


