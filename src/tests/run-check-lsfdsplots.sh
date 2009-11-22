#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

../process/dsextentindex --compress-lzf --new Batch::LSF::Grizzly cluster_name,submit_time,end_time lsfdsplots.index.ds $1/check-data/lsb.acct.2007-01-01-p1.ds 

PERL5LIB=$1/src/perl-modules:$2/share/perl5:$PERL5LIB perl ../analysis/lsfdsplots --indexfile=lsfdsplots.index.ds --psonly --plotdir=check-lsfdsplots --groups=all --starttime=1167681250 --endtime=1167681547 --lsfdsanalysis=../analysis/lsfdsanalysis

# Gnuplot generates different postscript for different versions; this seems the best
# approach to checking that we're getting something valid out.
grep -v '^%%CreationDate:' check-lsfdsplots/all/all.ps | grep -v ' /CreationDate' >check-lsfdsplots/all/all.ps-nodate
if cmp check-lsfdsplots/all/all.ps-nodate $1/check-data/lsfdsplots.ps.ref.gnuplot4.0; then
    echo "plot ok, gnuplot 4.0 equivalent"
elif cmp check-lsfdsplots/all/all.ps-nodate $1/check-data/lsfdsplots.ps.ref.gnuplot4.2; then
    echo "plot ok, gnuplot 4.2 equivalent"
else
    echo "Can't find gnuplot reference data consistent with input"
    exit 1
fi

exit 0
