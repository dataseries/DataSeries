package DataSeries::LSFPlots::Productions;
use strict;
use vars '@ISA';
use DataSeries::LSFPlots::Default;

$DataSeries::LSFPlots::Default::class = 'DataSeries::LSFPlots::Productions';

@ISA = qw/DataSeries::LSFPlots::Default/;

sub plotdir {
    return "/tmp/productions";
}

sub groups {
    return 'all,production';
}

sub do_plots {
    my ($this,$farmload) = @_;

    $this->plotmulticompare('productions','production',qr/./o);
}

1;


