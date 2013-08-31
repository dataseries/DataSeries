#!/usr/bin/perl -w
use strict;
$_ = <STDIN>;
die "?" unless m!^command: .*/home/anderse/datasets/animation-bear/(nfs-[12]/set-\d+)/\*\.ds\)$!o;
my $dataset = $1;
while(<STDIN>) {
    next unless /^(\S+) for interval len of (\S+)s with samples every (\S+)s$/o;
    my ($type, $interval_len, $sample_freq) = ($1,$2,$3);
    $_ = <STDIN>;
    die "?" unless /^(\d+) data points, mean/o;
    my ($nmeasure) = ($1);
    $_ = <STDIN>;
    die "?1" unless /^\s+quantiles about every \d+ data points:/o;
    while (<STDIN>) {
	last if /^\s+tails:/o;
	die "?" unless /^\s+(\d+)\%: (.+)$/o;
	my $first_quant = $1;
	$_ = $2;
	my @qvals = split(/, /o, $_);
	my $quant = $first_quant / 100;
	foreach my $qval (@qvals) {
	    print "replace into ip_bw_rolling (dataset, type, interval_len, sample_freq, nmeasure, quantile, val) values ('$dataset', '$type', $interval_len, $sample_freq, $nmeasure, $quant, $qval);\n";
	    $quant += 0.01;
	}
    }
    die "$_ ?2" unless /^\s+tails: (.+)$/o;
    $_ = $1;
    my @tails = split(/, /o);
    foreach my $tail (@tails) {
	die "? $tail" unless $tail =~ /^(\d+(?:\.\d+)?)\%: (\d+(?:\.\d+)?)$/o;
	my $quant = $1 / 100;
	my $qval = $2;
	print "replace into ip_bw_rolling (dataset, type, interval_len, sample_freq, nmeasure, quantile, val) values ('$dataset', '$type-tail', $interval_len, $sample_freq, $nmeasure, $quant, $qval);\n";
    }
    # don't insert for now; will just discuss in text.
}

__END__

create table ip_bw_rolling (
   dataset varchar(32) not null,
   type varchar(12) not null,
   interval_len double not null,
   sample_freq double not null,
   nmeasure double not null,
   quantile double not null,
   val double not null,
   primary key idx1 (dataset, type, interval_len, quantile)
);
