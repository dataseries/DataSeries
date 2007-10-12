package DataSeries::LSFPlots::AllCpu;
use strict;
use vars '@ISA';
use DataSeries::LSFPlots::Default;
use Data::Dumper;

$DataSeries::LSFPlots::Default::class = 'DataSeries::LSFPlots::AllCpu';

@ISA = qw/DataSeries::LSFPlots::Default/;

sub groups {
    return 'all';
}

sub do_individual_plot {
    my ($this,$group,$groupent) = @_;

    return 1 if $group eq 'all';
    return 0;
}

sub post_read_parse_op {
    my ($this,$farmload) = @_;

    my @percent_usage;
    my $maxrun = 0;
    foreach my $v (@{$farmload->{all}->{all}}) {
	$maxrun = $v->[RUN] if $v->[RUN] > $maxrun;
    }
    foreach my $v (@{$farmload->{all}->{all}}) {
	my @tmp = @$v;
	$tmp[PEND] = -1;
	my $run = $tmp[RUN];
	if ($run <= 0.05*$maxrun) {
	    $tmp[RUN] = 0;
	    $tmp[USER] = 0;
	    $tmp[SYSTEM] = 0;
	    $tmp[IDLE] = 0;
	} else {
	    if (($tmp[USER] + $tmp[SYSTEM]) > $run) {

		# accounting can sometimes be broken, either as a
		# result of jobs re-running or running multi-cpu but
		# we think they are single cpu.  In this case our
		# numbers are wrong; fix them up so they don't look
		# insane.

		$tmp[IDLE] = 0;
		$tmp[SYSTEM] = $run - $tmp[USER];
		if ($tmp[SYSTEM] < 0) {
		    $tmp[USER] = $run;
		    $tmp[SYSTEM] = 0;
		}
	    }
	    $tmp[RUN] = 100*$tmp[RUN]/$maxrun;
	    $tmp[USER] = 100*$tmp[USER]/$run;
	    $tmp[SYSTEM] = 100*$tmp[SYSTEM]/$run;
	    if (($tmp[USER] + $tmp[SYSTEM]) > 100.0001)  { #.0001 because of some weird roundoff error
		die "Odd ", join(" ",@$v), "//", join(" ", @tmp), "\n";
	    }
	    $tmp[IDLE] = 100*$tmp[IDLE]/$run;
	}
	push(@percent_usage,\@tmp);
    }
    $farmload->{percents}->{all} = \@percent_usage;
}

sub do_plots {
    my ($this,$farmload) = @_;

    $this->plotcompare('cpu','all',[qw/all/],[qw/run cpu idle/]);
    $this->plotcompare('cpupercent','percents',[qw/all/],[qw/idle/]);
}

1;
