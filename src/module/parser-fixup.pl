#!/usr/bin/perl -w
use strict;

open(F1, "$ARGV[0]/$ARGV[1].hpp") or die "bad";
my @data = <F1>;
open(F2, "$ARGV[0]/stack.hh") or die "bad";
my @stack = <F2>;
open(F3, "$ARGV[0]/location.hh") or die "bad";
my @location = <F3>;
close(F1); 
close(F2);
close(F3);
open(OUT, ">$ARGV[0]/$ARGV[1].hpp") or die "bad";
my ($stack, $location) = (0,0);
foreach $_ (@data) {
    if (/^#include "stack.hh"$/o) {
	print OUT @stack;
	++$stack;
    } elsif (/^#include "location.hh"$/o) {
	print OUT @location;
	++$location ;
    } else {
	print OUT $_;
    }
}
close(OUT);
die "?? stack include count wrong" unless $stack == 1;
die "?? location include count wrong" unless $location == 1;

