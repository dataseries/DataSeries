package DataSeries::LSFcommon;
use strict;
use Date::Parse;
use Carp;
use DataSeries::Crypt;

use vars qw/@ISA @EXPORT_OK %EXPORT_TAGS/;
require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw/rr_rj_mismatch_keep_first rr_rj_mismatch_rewrite 
    findswdata mdr setuptimes decodetime findfiles/;
%EXPORT_TAGS = (all => \@EXPORT_OK);

$LSFCommon::verbose = 1;

my $rr_rj_neq_ok_1 = decode('4c3aba05d7ab6232dc6a21b899466c63');
my $rr_rj_neq_ok_2 = decode('a725515bc6272f6707684a76b3cbe6ca');
my $rr_rj_neq_ok_3 = decode('a2e3386cad3c9872ca212d3c7572fcb5');
my $rr_rj_neq_ok_4 = decode('2d0668134867ddcc712e2ad0cb14959d');

sub rr_rj_mismatch_keep_first {
    my($prod_a,$seq_a,$shot_a,$prod_b,$seq_b,$shot_b) = @_;

    return 1 if $seq_b eq $rr_rj_neq_ok_1 && ($shot_b eq $rr_rj_neq_ok_1 ||
					      $shot_b eq $rr_rj_neq_ok_2);
    return 1 if $seq_b eq $rr_rj_neq_ok_3;
    return 1 if $seq_b eq $rr_rj_neq_ok_4 && $shot_a eq $shot_b;
}

sub rr_rj_mismatch_rewrite {
    my($prod_a,$seq_a,$shot_a,$prod_b,$seq_b,$shot_b,$rrid,$metas) = @_;

    carp "??" unless defined $prod_a;
    return ($prod_a,$seq_a,$shot_a) 
	if $prod_a eq $prod_b && $seq_a eq $seq_b && $shot_a eq $shot_b;
    return ($prod_a,$seq_a,$shot_a)
	if rr_rj_mismatch_keep_first($prod_a,$seq_a,$shot_a,$prod_b,$seq_b,$shot_b);
    return ($prod_b,$seq_b,$shot_b)
	if rr_rj_mismatch_keep_first($prod_b,$seq_b,$shot_b,$prod_a,$seq_a,$shot_a);
    if (mdr::candecrypt()) {
	warn "mismatch $prod_a $seq_a $shot_a // $prod_b $seq_b $shot_b ;; $rrid/$metas";
	# this is really just a guess :(
	return ($prod_a,$seq_a,$shot_a);
    } else {
	warn "mismatch $prod_a $seq_a $shot_a // $prod_b $seq_b $shot_b";
	# this is really just a guess :(
	return ($prod_a,$seq_a,$shot_a);
    }
}

sub setuptimes {
    my($starttime,$centertime,$endtime,$windowlen) = @_;

    $starttime = decodetime($starttime);
    $centertime = decodetime($centertime);
    $endtime = decodetime($endtime);

    $windowlen = 86400 unless defined $windowlen;
    if ($windowlen =~ /^(\d+)m$/o) {
	$windowlen = $1 * 60;
    } elsif ($windowlen =~ /^(\d+)h$/o) {
	$windowlen = $1 * 3600;
    } elsif ($windowlen =~ /^(\d+)d$/o) {
	$windowlen = $1 * 24 * 3600;
    }
    die "?? $windowlen" unless $windowlen =~ /^\d+$/o;
    if (defined $centertime) {
	$starttime = $centertime - $windowlen/2;
	$endtime = $centertime + $windowlen/2;
    }
    if (defined $starttime && ! defined $endtime) {
	$endtime = $starttime + $windowlen;
    }

    if (defined $endtime && !defined $starttime) {
	$starttime = $endtime - $windowlen;
    }

    if (!defined $starttime && !defined $endtime) {
	$endtime = time - 5*60;
	$starttime = $endtime - $windowlen;
    }

    die "?? $starttime $endtime" 
	unless defined $starttime && defined $endtime && $endtime > $starttime;
    print STDERR "Time range: ", scalar localtime($starttime), " .. ", scalar localtime($endtime), "\n"
	if $LSFCommon::verbose;

    return ($starttime,$endtime);
}

sub decodetime {
    my $time = $_[0];
    return undef unless defined $time;
    return $time if $time =~ /^\d+$/o;
    my $tmp = str2time($time);
    die "Unable to interpret $time"
	unless defined $tmp;
    return $tmp;
}

1;
