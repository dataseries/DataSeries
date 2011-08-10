#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

[ ! -f lsfdsplots.index.ds ] || rm lsfdsplots.index.ds
../process/dsextentindex --compress-lzf --new Batch::LSF::Grizzly cluster_name,submit_time,end_time lsfdsplots.index.ds $1/check-data/lsb.acct.2007-01-01-p1.ds 

PERL5LIB=$1/src/perl-modules:$2/share/perl5:$PERL5LIB perl ../analysis/lsfdsplots --indexfile=lsfdsplots.index.ds --psonly --plotdir=check-lsfdsplots --groups=all --starttime=1167681250 --endtime=1167681547 --lsfdsanalysis=../analysis/lsfdsanalysis

# Gnuplot generates different postscript for different versions; this seems the best
# approach to checking that we're getting something valid out.
grep -v '^%%CreationDate:' check-lsfdsplots/all/all.ps | grep -v ' /CreationDate' | grep -v ' /Author' | sed 's/patchlevel [0-9]/patchlevel X/' >check-lsfdsplots/all/all.ps-nodate
for i in $1/check-data/lsfdsplots.ps.ref.*; do
    if cmp check-lsfdsplots/all/all.ps-nodate $i; then
        echo "plot ok, matches $i"
        exit 0
    fi
done

echo "Can't find gnuplot reference data consistent with input"
exit 1
