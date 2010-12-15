#!/usr/bin/perl -w
use strict;
use Data::Dumper;
use Date::Parse;

use lib '/home/anderse/build/opt-debian-5.0-x86_64/DataSeries.server/src/server/gen-perl';
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

updateDateConv(34, '2010-10-04', '2010-10-22');

sub updateDateConv {
    my ($sprint, $start, $end) = @_;

    my $istart = str2time($start);
    my $iend = str2time($end);
    my $delta = $iend - $istart;
    die "??" unless $istart > 0 && $delta > 86400 && $delta <= 6*7*86400;
    $istart += 12*60*60; # avoid DST issues, center date
    $iend += 12*60*60;
    my $daynum = 0;
    my $prevday = -1;

    my @rows;
    for(my $i = $istart; $i <= $iend; $i += 86400/2) {
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)
	    = localtime($i);
	next if $mday == $prevday;
	$prevday = $mday;
	my $date = sprintf("%04d-%02d-%02d", $year + 1900, $mon+1, $mday);
	if ($wday == 0 || $wday == 6) {
            $daynum += 0.0001; # tiny update for sorting
        } else {
            $daynum = sprintf("%d", $daynum);
            ++$daynum;
        }
	print "$sprint $daynum $date\n";
        push (@rows, [ 2, $sprint, $date, $daynum ]);
    }

    my $update_xml = <<'END';
<ExtentType name ="sprint_days" namespace="simpl.hpl.hp.com" version="1.0">
  <field type="byte" name="update_op" />
  <field type="int32" name="sprint_number" />
  <field type="variable32" name="date" />
  <field type="double" name="day" />
</ExtentType>
END
    print "import...\n";
    $client->importData('sprint_dayconv_raw.update', $update_xml, new TableData({ rows => \@rows}));
    print Dumper(getTableData('sprint_dayconv_raw.update'));
    print "sut...\n";
    $client->sortedUpdateTable('sprint_dayconv_raw', 'sprint_dayconv_raw.update',
                               'update_op', [ 'sprint_number', 'date' ]);
    print Dumper(getTableData('sprint_dayconv_raw'));
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
