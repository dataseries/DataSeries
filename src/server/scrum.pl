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
    sql_exec("delete from sprint_dayconv_raw where sprint_number = $sprint_number");
    my $prevday = -1;

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
	print "$sprint_number $daynum $date\n";
	sql_exec("replace into sprint_dayconv_raw (adate, day, sprint_number) values ('$date', $daynum, $sprint_number);");
    }

}

