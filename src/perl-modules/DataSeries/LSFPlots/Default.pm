package DataSeries::LSFPlots::Default;
use strict;
use warnings;
use Carp;

require Exporter;

use vars qw(@ISA @EXPORT);
@ISA    = qw(Exporter);
@EXPORT = qw(TIME PEND RUN USER SYSTEM IDLE);

# offsets into entries in farmload array's
use constant {
    'TIME' => 0,
    'PEND' => 1,
    'RUN' => 2,
    'USER' => 3,
    'SYSTEM' => 4,
    'IDLE' => 5,
};

use vars '$class';

$class = 'DataSeries::LSFPlots::Default';

sub new {
    my($class) = @_;
    return bless {}, $class;
}

### defaults for command line options...

sub indexfile { return undef; }
sub plotdir { return undef; }
sub starttime { return undef; }
sub endtime { return undef; }
sub intervallen { return undef; }
sub windowlen { return undef; }
sub maxintervals { return undef; }
sub saveraw { return undef; }
sub readraw { return undef; }
sub avgintervals { return undef; }
sub mwavgintervals { return undef; }
sub mwmaxintervals { return undef; }
sub cumulative { return 0; }
sub groups { return 'all,production,sequence,team,queue,hostgroup,team_group,cluster,username'; }

### revert to main package functions

sub plotone {
    my $this = shift; # group, subent
    &main::plotoneindividual(@_);
}

sub plotcompare {
    my $this = shift; # plotname, group, ents, [lines]
    &main::docompare(@_);
}

sub plotmulticompare {
    my $this = shift; # plot-base, group, ents-regex, [maxkeeprel]
    &main::domulticompare(@_);
}

### controllable options...

sub keep_data { # do  you want to skip some of the data as it is read in?
#    my($this,$group,$subent) = @_;
    return 1;
}

# called after the data is read, but before any averaging/cumulativing
# is done
sub post_read_parse_op { my($this,$farmload) = @_; }

# called after data has been read, averaged/cumulatived, and summarized
sub post_read_summary_op { my($this,$farmload) = @_; }

sub multicompare_maxkeeprel {
    return 0.05;
}

sub do_individual_plot {
    my($this,$group,$groupent) = @_;
    return 1; # 1 to plot individuals
}

# called to make all of the special plots, you can explicitly make 
# individual plots using this, or by overriding the do_individual_plot
# function

sub do_plots { 
    my($this,$farmload) = @_;

#    warn "DataSeries::LSFPlots::Default::do_plots doesn't generate any plots";
}

### utilities for child modules

sub scale_data {
    my($this,$data,$scalefactor) = @_;

    return $data if $scalefactor == 1;
    my @ret;
    foreach my $d (@$data) {
	my @ent = (@{$d});
	for(my $k=1;$k<6;++$k) {
	    $ent[$k] *= $scalefactor;
	}
	push(@ret,\@ent);
    }
    return \@ret;
}

sub sum_data {
    my $this = shift;

    if (@_ == 0) {
	return [];
    } elsif (@_ == 1) {
	return $_[0];
    }
    foreach $_ (@_) {
	croak "undefined array as argument to sum_data\n"
	    unless defined $_;
    }
    my @data = map { [@{$_}] } @_; # duplicate pointers
    my $curtime = $data[0]->[0]->[0];
    my $interval = $data[0]->[1]->[0] - $data[0]->[0]->[0];
    map { $curtime = $_->[0]->[0] if $_->[0]->[0] < $curtime } @data;
    my @retdata;
    while(@data > 0) {
	my @ent = ($curtime,0,0,0,0,0,0);
	my $foundany = 0;
	foreach my $dent (@data) {
	    next if @$dent == 0;
	    die "internal error $curtime $dent->[0]->[0]"
		if $dent->[0]->[0] < $curtime;
	    next unless $dent->[0]->[0] == $curtime;
	    $foundany = 1;
	    for(my $i=1;$i<6;++$i) {
		$ent[$i] += $dent->[0]->[$i];
	    }
	    shift @$dent;
	}
	die "internal $curtime" unless $foundany;
	push(@retdata,\@ent);
	$curtime += $interval;
	@data = grep(@$_ > 0,@data);
    }
    return \@retdata;
}

1;
