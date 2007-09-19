#!/usr/bin/perl -w
use strict;

my @data = readFile("$ARGV[0]/$ARGV[1].hpp");
my @stack = readFile("$ARGV[0]/stack.hh");
my @location = readFile("$ARGV[0]/location.hh");
my @position = readFile("$ARGV[0]/position.hh");

open(OUT, ">$ARGV[0]/$ARGV[1].hpp") or die "bad";
my ($stack, $location) = (0,0);
foreach $_ (@data) {
    if (/^#include "stack.hh"$/o) {
	print OUT @stack;
	++$stack;
    } elsif (/^#include "location.hh"$/o) {
	++$location;
	my $position = 0;
	foreach $_ (@location) {
	    if (/^# include "position.hh"$/o) {
		++$position;
		print OUT @position;
	    } else {
		print OUT $_;
	    }
	}
	die "?? position include count wrong" unless $position == 1;
    } elsif (/^#line/o) {
	# drop
    } else {
	print OUT $_;
    }
}
close(OUT);
die "?? stack include count wrong" unless $stack == 1;
die "?? location include count wrong" unless $location == 1;

sub readFile {
    my($path) = @_;

    open(F, $path) or die "can't read $path: $!";
    my @ret = <F>;
    close(F);
    return @ret;
}
