#!/usr/bin/perl -w
use strict;
$_ = <STDIN>;
die "?" unless m!^command:.*nfsdsanalysis -g /home/anderse/datasets/animation-bear/(nfs-[12]/set-\d+)/\*\.ds\)$!o;
my $dataset = $1;

while(<STDIN>) {
    last if /^Begin-virtual void UniqueFileHandles::printResult/o;
}

$_ = <STDIN>;
die "??" unless /^found (\d+) unique filehandles/o;
$_ = <STDIN>; 
die "??" unless /^file size quantiles:$/o;

parseQuantile();

sub parseQuantile {

    $_ = <STDIN>;
    die "?" unless /^(\d+) data points, mean/o;
    my $ndata = $1;
    return if $ndata == 0;
    $_ = <STDIN>;
    die "?" unless /^\s+quantiles about every/o;
    for (my $i=1; $i < 100; $i += 10) {
	$_ = <STDIN>;
	chomp;
	die "'$_' ? $i " unless /^\s*$i%: (.+)$/;
	my $quantiles = $1;
	my @quantiles = split(/,\s+/o, $quantiles);
	my $max = $i + 10;
	$max = 100 if $max == 101;
	for(my $j = $i; $j < $max; ++$j) {
	    die "?" unless @quantiles > 0;
	    my $quant = shift @quantiles;
	    print "insert into nfs_filesize_quantile (dataset, quantile, value) values ('$dataset', $j, $quant);\n";
	}
	die "??" unless @quantiles == 0;
    }
}

__END__

create table nfs_filesize_quantile (
   dataset varchar(32) not null,
   quantile double not null,
   value double not null,	 
   primary key idx1 (dataset, quantile)
);

