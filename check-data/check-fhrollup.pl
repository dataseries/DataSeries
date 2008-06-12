#!/usr/bin/perl -w
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#

# Note this is intensely slow and you have to have pre-installed all
# the DataSeries programs in your path for it to work.

# hack to check fhrollup results; can have a difference in the lowest
# bit because the analysis code computes in the raw time space and
# this does it in sec.nsec

use Math::BigFloat;

my $fh="62000000e280200d20000000001f080d934e470d32c9075c62000000e2802000";
#$fh="62000000e280200d200000000014979bec99270d32c9075c62000000e2802000";
# $fh="4597580159784506200000000158974559784506fa0a0162cf8e8a0075703200";
my $file="nfs-2.set-1.20k.ds";

open(RECORDS, "ds2txt --skip-all --select attr \\* $file |") or die "?";

my ($sum,$count) = (0,0);

while(<RECORDS>) {
    @F = split(/\s+/o);
    next unless $F[3] eq $fh;
#    print "HI $_";
    my ($request_id, $reply_id) = split(/\s+/o);
    my $request_at = getAt($request_id);
    my $reply_at = getAt($reply_id);

    $sum += $reply_at - $request_at;
    ++$count;
}

printf("sum %.3fus, count %d, avglat %.3fus\n",
       $sum * 1e6, $count, $sum * 1e6 / $count);

sub getAt {
    my($record_id) = @_;

    open(GET_AT, qq{ds2txt --skip-all --select common record_id,packet_at --printSpec='type="Trace::NFS::common" name="packet_at" print_format="sec.nsec" units="2^-32 seconds" epoch="unix"' $file  |}) or die "?";

    while(<GET_AT>) {
	my @F = split;
	die "??" unless @F == 2;
	if ($F[0] == $record_id) {
	    close(GET_AT);
#	    print "HO $_";
	    return Math::BigFloat->new($F[1]);
	}
    }

    die "??";
}

