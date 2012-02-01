#!/usr/bin/perl -w
use strict;
use Data::Dumper;
use Date::Parse;
use Text::Table;

use lib "$ENV{BUILD_OPT}/DataSeries/src/server/gen-perl";
use lib "$ENV{HOME}/projects/thrift/lib/perl/lib";

use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::BufferedTransport;

use DataSeriesServer;

my $force_rebuild = 0;

my $socket = new Thrift::Socket('localhost', 49476);
$socket->setRecvTimeout(100000);
my $transport = new Thrift::BufferedTransport($socket, 4096, 4096);
my $protocol = new Thrift::BinaryProtocol($transport);
my $client = new DataSeriesServerClient($protocol);

$transport->open();

my $ret = eval {
    updateDateConv(19, '2009-07-06', '2009-07-31');
    scrumTaskRestricted(19);
    remainingWork();
};

if ($@) { 
    print Dumper($@);
    die "fail";
}

sub updateDateConv {
    my ($sprint, $start, $end) = @_;

    return if !$force_rebuild && $client->hasTable('sprint_dayconv');
    my $istart = str2time($start);
    my $iend = str2time($end);
    my $delta = $iend - $istart;
    die "??" unless $istart > 0 && $delta > 86400 && $delta <= 6*7*86400;
    $istart += 12*60*60; # avoid DST issues, center date
    $iend += 12*60*60;
    my $daynum = 0;
    my $prevday = -1;

    my @rows = ([ new NullableString({ v => 2 }), new NullableString({ v => $sprint}),
                  new NullableString(), new NullableString() ]);
    # Could make it work with addition of 24hrs but incrementing 12 hrs and skipping duplicated
    # days is safer to avoid DST issues.
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
        # TODO: *1m because units for sql2ds of times is microseconds
        # if we had a expression transform operation on the tables then we'd be fine, and
        # that's what we should do.
        push (@rows, [ map { new NullableString({'v' => $_}); }
                       (2, $sprint, $date*1000*1000, $daynum) ]);
    }

    my $update_xml = <<'END';
<ExtentType name ="sprint_days" namespace="simpl.hpl.hp.com" version="1.0">
  <field type="byte" name="update_op" />
  <field type="int32" name="sprint_number" />
  <field type="int64" units="microseconds" epoch="unix" name="date" opt_nullable="yes" />
  <field type="double" name="day" opt_nullable="yes" />
</ExtentType>
END
    print "import...\n";
    $client->importData('sprint_dayconv_raw.update', $update_xml, new TableData({ rows => \@rows}));
    print "gtd...\n";
    printTable('sprint_dayconv_raw.update');
    print "sut...\n";
    $client->sortedUpdateTable('sprint_dayconv_raw', 'sprint_dayconv_raw.update',
                               'update_op', [ 'sprint_number', 'date' ]);
    print "sel...\n";
    $client->selectRows('sprint_dayconv_raw', 'sprint_dayconv', "sprint_number == $sprint");
}

sub selfMap {
    my @ret;
    foreach $_ (@_) {
        push(@ret, $_, $_);
    }
    return @ret;
}

# sql create or replace view scrum_task_restricted as 
#    select id, title, state, created, estimated_days, finished, actual_days, sprint, assigned_to,
#        (select day from sprint_dayconv where adate = created) as created_day, 
#        (select day from sprint_dayconv where adate = finished) as end_day 
#    from scrum_task
#    where sprint = $sprint_number and !(state = 'canceled' and finished is null)
sub scrumTaskRestricted {
    my ($sprint) = @_;

    if ($force_rebuild || !$client->hasTable('scrum_task')) {
        print "import...\n";
        $client->importSQLTable('', 'scrum_task', 'scrum_task');
    }
    return if !$force_rebuild && $client->hasTable('scrum_task_restricted');

    print "sel...\n";
    # TODO: need fn.isNull(finished); default is 0 so that will happen to work
    $client->selectRows('scrum_task', 'scrum_task_restricted.1', 
                        "sprint == $sprint && !(state == \"canceled\" && finished == 0)");
#    print Dumper(getTableData('scrum_task_restricted.1'));
#
#    print Dumper(getTableData('sprint_dayconv')->{columns});

    my $dim = new Dimension({ dimension_name => 'dayconv',
                              source_table => 'sprint_dayconv',
                              key_columns => ['date'],
                              value_columns => ['day'],
                              max_rows => 1000 });
    my $dfj_created = new DimensionFactJoin({ dimension_name => 'dayconv',
                                              fact_key_columns => ['created'],
                                              extract_values => { 'day' => 'created_day' },
                                              missing_dimension_action
                                                => DFJ_MissingAction::DFJ_Unchanged });
    my $dfj_finished = new DimensionFactJoin({ dimension_name => 'dayconv',
                                               fact_key_columns => ['finished'],
                                               extract_values => { 'day' => 'finished_day' },
                                               missing_dimension_action
                                                 => DFJ_MissingAction::DFJ_Unchanged });
    $client->starJoin('scrum_task_restricted.1', [$dim], 'scrum_task_restricted',
                      { selfMap(qw/id title state created estimated_days finished actual_days
                                   sprint assigned_to/) }, [$dfj_created, $dfj_finished]);

    print "gtd...\n";
    # printTable('scrum_task_restricted');
}

sub exprColumn {
    return new ExprColumn({ name => $_[0], type => $_[1], expr => $_[2] });
}

# remaining work on each day
# sql create or replace view sprint_remain as select day, (select sum(estimated_days) as sum_estimated_days from scrum_task_restricted where created <= adate and (finished > adate or finished is null)) as remain from sprint_dayconv

# remaining work on each day with foreknowledge
# sql create or replace view sprint_remain_fore as select day, (select sum(estimated_days) as sum_estimated_days from scrum_task_restricted where finished > adate or finished is null) as remain from sprint_dayconv 
sub remainingWork {
    # For the first one, sort by created, separate sort by finished; union created and finished
    # after inverting estimated_days for finished, then run accumulation over days in
    # sprint_dayconv
    #
    # For the second one, sort by 0, finished, otherwise same as above
    #
    # TODO: need null_mode => NM_First/NM_Last[/NM_Drop]
    my $sc_created = new SortColumn({ column => 'created', sort_mode => SortMode::SM_Ascending,
                                      null_mode => NullMode::NM_First });
    my $sc_finished = new SortColumn({ column => 'finished', sort_mode => SortMode::SM_Ascending,
                                      null_mode => NullMode::NM_Last });

    $client->transformTable('scrum_task_restricted', 'remaining-work.extract',
                            [ exprColumn('created', 'int64', 'created'),
                              exprColumn('finished', 'int64', 'finished'),
                              exprColumn('zero', 'int64', 'zero'),
                              exprColumn('estimated_days', 'double', 'estimated_days'),
                              exprColumn('minus_estimated_days', 'double', '- estimated_days') ]);

    $client->sortTable('remaining-work.extract', 'remaining-work.sort-created',
                       [ $sc_created ]);
    $client->sortTable('remaining-work.extract', 'remaining-work.sort-finished',
                       [ $sc_finished ]);


    printTable('remaining-work.sort-finished');
#    $client->sortTable('scrum_task_restricted', 'remaining-work.sort-created',
    # TODO: need $client->deriveTable(table, [expr => col])
    # 
}

sub getTableData ($;$$) {
    my ($table_name, $max_rows, $where_expr) = @_;

    $max_rows ||= 1000;
    $where_expr ||= '';
    my $ret = eval { $client->getTableData($table_name, $max_rows, $where_expr); };
    if ($@) {
        print Dumper($@);
        die "fail";
    }
    my @rows = map {
        my $r = $_;
        [ map { defined $_->{v} ? $_->{v} : undef } @$r ]
    } @{$ret->{rows}};

    $ret->{rows} = \@rows;
    return $ret;
}

sub printTable ($) {
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
