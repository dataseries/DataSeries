#!/usr/bin/perl -w
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
# 
# See the file named COPYING for license details
# 
# Test program for debugging issues in time conversion

use strict;

use Math::BigFloat lib => 'GMP';

if (0) {
    my $p_s = Math::BigFloat->new("1208544069");
    my $p_ns = Math::BigFloat->new("460581886");

    my $s = $p_s + $p_ns / (10**9);

    my $s_tfrac = $s * (2.0**32);
    print "$s; $s_tfrac\n";

    my $q_s = -$p_s - 1;
    my $q_ns = 10**9 - $p_ns;

    my $s2 = $q_s + $q_ns / (10**9);
    my $s2_tfrac = $s2 * (2.0**32);
    print "$s2; $s2_tfrac\n";

    my $s3 = $s2_tfrac + (10**(-20));
    print "$s3\n";

    $s_tfrac->ffround(0);
    $s2_tfrac->ffround(0);
    print "$s_tfrac; $s2_tfrac\n";
    die "??" unless $s_tfrac == -$s2_tfrac;

    my $a = $p_ns * 4 * 1024 * 1024 * 1024 / (1000*1000*1000);
    print "XX $a\n";
}

if (1) {
    for (my $i=0;$i<100;++$i) {
	my $frac32 = rand_i64();
	my $sns = $frac32 * 10**9 / 2**32;
	print "X $frac32 $sns\n" if 0;
	$sns->ffround(0);
	print "Hi $frac32 $sns\n" if 0;
	my $ofrac32 = $sns * 2**32 / 10**9;
	print "    check_one_frac32_convert(nsec, ${frac32}LL, ${sns}LL, ${ofrac32}LL);\n";
    }
}

sub rand_i64 {
    my $a = rand_u32();
    my $b = rand_u32();
    my $sign = 1;
    if ($a > 2**31) {
	$sign = -1;
	$a -= 2**31;
    }
    return $sign * ($a * (2**32) + $b);
}

sub rand_u32 {
    my $a = int(rand(65536));
    my $b = int(rand(65536));
    my $c = $a * 65536 + $b;
    return Math::BigFloat->new($c);
}

