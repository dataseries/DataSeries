#!/usr/bin/perl -w
use strict;
$_ = <STDIN>;
die "?" unless m!^command:.*nfsdsanalysis (.+) /home/anderse/datasets/animation-bear/(nfs-[12]/set-\d+)/\*\.ds\)$!o;
my ($args, $dataset) = ($1,$2);
my @args = split(/\s+/o,$args);
die "?? @args" unless @args % 2 == 0;

my @configs;
for(my $i=0; $i<@args; $i += 2) {
    die "??" unless $args[$i] eq '-i';
    push(@configs, $args[$i+1]);
}

my $confignum = 0;
while(<STDIN>) {
    next unless /^Begin-virtual void Sequentiality::printResult/o;
    die "??" if $confignum >= @configs;
    parseResult($configs[$confignum]);
    ++$confignum;
}

die "??" unless $confignum == @configs;

sub parseResult {
    my($config) = @_;

    $_ = <STDIN>;
    die "??" unless /^Analysis conf/o;
    $_ = <STDIN>;
    die "??" unless m!^Reorder count: (\d+)/(\d+),!o;
    my ($reorder, $total) = ($1,$2);

    my $unknown_file_size_count;
    while (<STDIN>) {
	if (/^(.+) distribution:/o) {
	    parseQuantile($config, $1);
	} elsif (/^(\w+):$/o) {
	    parseQuantile($config, $1);
	} elsif (/^unknown file size count (\d+)$/o) {
	    $unknown_file_size_count = $1;
	} elsif (/^End-virtual void Sequentiality/o) {
	    last;
	}
    }
    print "insert into nfs_sequentiality_general (dataset, config, reorder, total, unknown_file_size) values ('$dataset', '$config', $reorder, $total, $unknown_file_size_count);\n";
}


sub parseQuantile {
    my ($config, $type) = @_;

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
	    print "insert into nfs_sequentiality_quantile (dataset, config, type, quantile, value) values ('$dataset', '$config', '$type', $j, $quant);\n";
	}
	die "??" unless @quantiles == 0;
    }
}

__END__

create table nfs_sequentiality_general (
   dataset varchar(32) not null,
   config varchar(64) not null,
   reorder bigint not null,
   total bigint not null,
   unknown_file_size bigint not null,
   primary key idx1 (dataset, config)
);

create table nfs_sequentiality_quantile (
   dataset varchar(32) not null,
   config varchar(64) not null,
   type varchar(64) not null,
   quantile double not null,
   value double not null,	 
   primary key idx1 (dataset, config, type, quantile)
);

