#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script
set -e

for i in nfs-1.set-0.ip  nfs-2.set-0.ip  nfs-2.set-3.ip; do
    # long ranges for -b are just ignored because we don't get a long
    # enough time.
    ../analysis/ipdsanalysis -b 60,15,5,1,0.1,0.01,0.001 -d 0.001 $1/check-data/$i.ds >check.$i.tmp
    perl $1/check-data/clean-timing.pl < check.$i.tmp >check.$i.txt
    cmp check.$i.txt $1/check-data/check.$i.ref
    rm check.$i.tmp
done

    