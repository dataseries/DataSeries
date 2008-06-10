#!/usr/bin/perl
use Time::Local;
use Cwd;
use bigint;

$MONTH{Jan} = 0; $MONTH{Feb} = 1; $MONTH{Mar} = 2; $MONTH{Apr} = 3; $MONTH{May} = 4; $MONTH{Jun} = 5; $MONTH{Jul} = 6; $MONTH{Aug} = 7; $MONTH{Sep} = 8; $MONTH{Oct} = 9; $MONTH{Nov} = 10; $MONTH{Dec} = 11;

$trace_count = 0;
$average_length = 0;
$total_length = 0;
open (TIME, $ARGV[0]) or die "can't open file ($!)\n";
#previous entry...
$old_end_tfracs = 0;
while (defined ($the_line = <TIME>)) {
    $line_copy = $the_line;
    chomp($the_line);
    print "Line:$the_line\n";
    unless ($the_line =~ /tracedate/) {
	next;
    }
    $trace_count++;
    @time_line = split /=\s*/, $the_line;
    @time_line = split /\;/, $time_line[1];
    if ($#time_line > 0) {
	#1992 trace
	print "1992 trace\n";
	@tmp_time = split /(\"\s+)+/, $time_line[1];
	@time_line = ($time_line[0], @tmp_time);
	foreach $cur_elem (@time_line) {
	    print "$cur_elem\n";
	    $cur_elem =~ s/\s*\"//g;
	}
    } else {
	#1996,1998,1999 trace
	print "1996,1998,1999 trace\n";
	@time_line = split /\"/, $time_line[0];
    }
    $count = 0;
    foreach $time_foo (@time_line) {
	print "$count:$time_foo\n";
	$count++;
    }
    ($start_time, $first_end_time, $empty_item1, $rel_end_time, $empty_item2, $earliest_start) = @time_line;
    print ("strt:$start_time  first_end:$first_end_time  relendt:$rel_end_time\n");
    @start_time = split /\s+/, $start_time;
    foreach $time_ele (@start_time) {
	print "$time_ele\n";
    }
    @hour_min_sec = split /\:/,$start_time[3];
    foreach $hms (@hour_min_sec) {
	print "$hms\n";
    }
    print "final: $hour_min_sec[2], $hour_min_sec[1], $hour_min_sec[0], $start_time[2], $MONTH{$start_time[1]}, $start_time[4]\n";
    $epoc_start_time = timelocal($hour_min_sec[2], $hour_min_sec[1],
				 $hour_min_sec[0], $start_time[2],
				 $MONTH{$start_time[1]}, $start_time[4]);
    
    $epoc_start_time_tfracs = $epoc_start_time << 32;
    $epoc_end_time = $epoc_start_time_tfracs + $rel_end_time;
    $diff = ($epoc_start_time_tfracs - $old_end_tfracs)/2**32;
    $pretty_start = localtime $epoc_start_time_tfracs >> 32;
    $pretty_end = localtime $epoc_end_time >> 32;
    print "start time read: $time_line[0]\n";
    print "start time computed: $pretty_start\n";
    print "end time computed: $pretty_end\n";
    print ("$old_end_tfracs $diff $epoc_start_time_tfracs $epoc_end_time\n");
    print ("DIFF:$diff\n");
    print ("tracelength:$rel_end_time\n");
    #320 << 32 is 5min20sec from 1992 traces
#    if (abs($rel_end_time - $average_length) > (320 << 32) && $average_length != 0) {
#	print "too much stddev in average_length\n";
#	print "curLength:$rel_end_time\naverageLength:$average_length\n";
#	exit(1);
#    }
#    if ($diff < 0) {
#	print ("diff is negative...\n");
#	exit(1);
#    }
    $total_length += $rel_end_time;
    $average_length = $total_length / $trace_count;
#    if ($diff > ($average_length >> 32)) {
#	print "diff is greater than half the length of a single trace.  There is a gap...\n";
#	exit(1);
#    }
    $new_base = 0;
    if ($diff < 0) {
	$new_base = (1 << 32)*4/10;
    } else {
	$new_base = 0;
    }
    $epoc_start_time_tfracs += $new_base;
    $frac_hash{$line_copy} = [$epoc_start_time_tfracs, $new_base, $earliest_start];
    $old_end_tfracs = $epoc_end_time;
    $pretty_start = localtime $epoc_start_time_tfracs >> 32;
    $pretty_end = localtime $epoc_end_time >> 32;
    $pretty_earliest = localtime $earliest_start >> 32;
    print ("$pretty_start\n$pretty_end\n$pretty_earliest\n");
}
my $when = localtime 695808304;
print "timestamp turns a billion at $when\n";
foreach $cur_key (keys (%frac_hash)) {
    print "key: $cur_key  val:$frac_hash{$cur_key}[0] two:$frac_hash{$cur_key}[1]\n";
}
foreach $curArg (@ARGV[1..$#ARGV]) {
    if (-d $curArg) {
	processDir($curArg);
    } else {
	print "Argument $curArg wasn't a directory\n";
    }
}

sub processDir {
    my ($curWorkingDir, $dirToProcess, $DIRHANDLE);
    $dirToProcess = $_[0];
    print "curarg is $dirToProcess\n";
    chdir($dirToProcess) || die "cannot cd to $dirToProcess ($!)\n";
    $curWorkingDir = getcwd();
    print "curworkdir is $curWorkingDir\n";

    opendir($DIRHANDLE, ".")|| die "cannot open $dirToProcess";
    $statfileCount = 0;
    while (defined ($name = readdir($DIRHANDLE))) {
	print "Name is $name\n";
	if ((-f $name) && ($name =~/info$/)) {
	    print "matched $curWorkingDir/$name\n";
	    $statfileCount++;
	    processFile($name);
	} else {
	    print "not a DS file.\n";
	    if (0 && -d $name) {
		print "dirname is $name\n";
		unless ($name =~ /^\./) {
		    print "good dirname is $name\n";
		    processDir($name);
		}
	    }
	}
    }
    chdir("..");
}

sub processFile {
    $file_name = $_[0];
    print "opening $file_name for read/write\n";
    open(INFOFILE, "+>>$file_name") or die "Can't open file for RW (!$)";
    seek(INFOFILE, 0, 0);
    $first_line = <INFOFILE>;
    #chomp($first_line);
    print "first line: $first_line\n";
    #foreach $cur_key (keys (%frac_hash)) {
    #	print "$first_line\n$cur_key";
    #    }

    if (defined ($frac_hash{$first_line})) {
	print "defined\n";
	$real_start = $frac_hash{$first_line}[0];
	$offset = $frac_hash{$first_line}[1];
	$earliest = $frac_hash{$first_line}[2];
	if ($earliest > 0 && $earliest < $real_start) {
	    print "suspect IO earliest: $earliest < $real_start\n";
	    $real_start = $earliest;
	    $offset = 0;
	}
	$format_start = sprintf "%f", $real_start;
	print "real_start: $format_start offset: $offset\n";
	seek(INFOFILE, 0, 0);
	truncate(INFOFILE, 0);
	print INFOFILE "$first_line";
	print INFOFILE "$format_start $offset";
    } else {
	print "$_ not found in hash\n";
    }
    close INFOFILE;
}
