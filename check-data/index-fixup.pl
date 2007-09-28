#!/usr/bin/perl -w
use strict;

die "??" unless $0 =~ m!^(/.*)/check-data/index-fixup.pl!;

my $topdir = $1;

while(<STDIN>) {
    s,$topdir,/home/anderse/projects/DataSeries,o;
    s,^(/home/anderse/projects/DataSeries/check-data/\S+) \d+$,$1 ########,o;
    print;
}


