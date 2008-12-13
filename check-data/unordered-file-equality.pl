#!/usr/bin/perl -w
use strict;

open(F1, $ARGV[0]) or die "Can't open $ARGV[0] for read: $!";
open(F2, $ARGV[1]) or die "Can't open $ARGV[1] for read: $!";
my @f1 = <F1>;
my @f2 = <F2>;

my %found1;
map { ++$found1{$_} } @f1;
my %found2;
map { ++$found2{$_} } @f2;

my $lines1 = 0;
foreach $_ (@f1) {
    ++$lines1;
    die "missing line found in $ARGV[0] in $ARGV[1]:\n$_"
	unless $found2{$_};
    --$found2{$_};
}

my $lines2 = 0;
foreach $_ (@f2) {
    ++$lines2;
    die "missing line found in $ARGV[1] in $ARGV[0]:\n$_"
	unless $found1{$_};
    --$found1{$_};
}

die "huh" unless $lines1 == $lines2 && $lines1 == @f1 && $lines1 == @f2;

print "$ARGV[0] is the same set of lines as $ARGV[1]\n";
