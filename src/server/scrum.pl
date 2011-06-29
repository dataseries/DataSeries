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
$socket->setRecvTimeout(100000);
my $transport = new Thrift::BufferedTransport($socket, 4096, 4096);
my $protocol = new Thrift::BinaryProtocol($transport);
my $client = new DataSeriesServerClient($protocol);

$transport->open();

my $ret = eval {
    updateDateConv(19, '2009-07-06', '2009-07-31');
    scrumTaskRestricted(19);
};

if ($@) { 
    print Dumper($@);
    die "fail";
}

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
	my $date_str = sprintf("%04d-%02d-%02d", $year + 1900, $mon+1, $mday);
        my $date = str2time($date_str);
	if ($wday == 0 || $wday == 6) {
            $daynum += 0.0001; # tiny update for sorting
        } else {
            $daynum = sprintf("%d", $daynum);
            ++$daynum;
        }
	print "$sprint $daynum $date_str $date\n";
        push (@rows, [ 2, $sprint, $date, $daynum ]);
    }

    my $update_xml = <<'END';
<ExtentType name ="sprint_days" namespace="simpl.hpl.hp.com" version="1.0">
  <field type="byte" name="update_op" />
  <field type="int32" name="sprint_number" />
  <field type="int64" units="microseconds" epoch="unix" name="date" />
  <field type="double" name="day" />
</ExtentType>
END
    print "import...\n";
    $client->importData('sprint_dayconv_raw.update', $update_xml, new TableData({ rows => \@rows}));
    print "gtd...\n";
    print Dumper(getTableData('sprint_dayconv_raw.update'));
    print "sut...\n";
    $client->sortedUpdateTable('sprint_dayconv_raw', 'sprint_dayconv_raw.update',
                               'update_op', [ 'sprint_number', 'date' ]);
    print "sel...\n";
    $client->selectRows('sprint_dayconv_raw', 'sprint_dayconv', "sprint_number == $sprint");
}

# sql create or replace view scrum_task_restricted as 
#    select id, title, state, created, estimated_days, finished, actual_days, sprint, assigned_to,
#        (select day from sprint_dayconv where adate = created) as created_day, 
#        (select day from sprint_dayconv where adate = finished) as end_day 
#    from scrum_task
#    where sprint = $sprint_number and !(state = 'canceled' and finished is null)
sub scrumTaskRestricted {
    my ($sprint) = @_;

    if (!$client->hasTable('scrum_task')) {
        print "import...\n";
        $client->importSQLTable('', 'scrum_task', 'scrum_task');
    }

    print "sel...\n";
    # TODO: need fn.isNull(finished); default is 0 so that will happen to work
    $client->selectRows('scrum_task', 'scrum_task_restricted.1', 
                        "sprint == $sprint && !(state == \"canceled\" && finished == 0)");

    print "gtd...\n";
#    print Dumper(getTableData('scrum_task_restricted.1'));
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
