#!/usr/bin/perl -w
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#

# Note this is somewhat slow and you have to have pre-installed all
# the DataSeries programs in your path for it to work.

# hack to check dirpathlookup results

my $file="nfs-2.set-1.20k.ds";

open(DATA, "ds2txt --skip-all --select attr \\* $file |") or die "?";

my %parent;

while(<DATA>) {
    @F = split(/\s+/o);
    next if $F[4] eq 'null';
    my($fn,$fh,$pfh) = ($F[2],$F[3],$F[4]);
    $parent{$fh} = [$pfh, $fn];
}

foreach my $k (sort keys %parent) {
    my @path = path($k);
    my $count = @path;
    print "$count $k -> @path\n";
}

sub path {
    my($cur) = @_;

    return ("FH:$cur") unless defined $parent{$cur};
    return (path($parent{$cur}->[0]),$parent{$cur}->[1]);
}
