#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

SRC=$1

do_check() {
    perl $SRC/check-data/clean-timing.pl < $1.tmp >$1.txt
    cmp $1.txt $SRC/check-data/$1.ref
    rm $1.tmp
}

# Odd order for arguments to preserve ordering in the reference files.
# switching -b, -c would require fixing the reference files.

../analysis/nfs/nfsdsanalysis -a -c 1 -b \
    -d 64000000c20f0300200000000127ef590364240c4fdf0058400000009a871600 \
    -d a63c6021c20f0300200000000117721df1e3040064000000400000009a871600 \
    -d e8535201c2cca305200000000079de25139fc708d6390035400000006bfc0800 \
    -e e8535201c2cca3052000000000393dcb82c6fc08d6390035400000006bfc0800 \
    -e $SRC/check-data/nfs.set6.20k.testfhs \
    $SRC/check-data/nfs.set6.20k.ds >check.nfsdsanalysis.tmp
do_check check.nfsdsanalysis

../analysis/nfs/nfsdsanalysis -a -c 1 -b \
    -d 059f13004fb46a0d2000000002725fa8b2c39a0060f8073d059f13004fb46a00 \
    -d 059f13004fb46a0d2000000004de9a890a4af00ebadc0762059f13004fb46a00 \
    -d 059f13004fb46a0d2000000004de9a85074af00ebadc0762059f13004fb46a00 \
    -e 87d874012ece4f0520000000040ba0561d83d10060f8073e87d874012ece4f00 \
    $SRC/check-data/nfs-2.set-0.20k.ds >nfs-2.set-0.20k.tmp
do_check nfs-2.set-0.20k

../analysis/nfs/nfsdsanalysis -a -c 1 -b \
    -d d5554d0089f4a8052000000000f71819cd080a051aaa0717d5554d0089f4a800 \
    -d b3aace985185f1002000000004521cffd6c2be0afa058e006ceb3200580bf600 \
    -e ee8d3c02707bea0b20000000001603676d27261200fb07e16200000083ecb200 \
    $SRC/check-data/nfs-2.set-1.20k.ds >nfs-2.set-1.20k.tmp 
do_check nfs-2.set-1.20k

if [ `whoami` = anderse -a -f ../analysis/nfs/set-5/subset.500k.ds ]; then
    ../analysis/nfs/nfsdsanalysis -c 2,no_print_base,test,print_zero_rates ../analysis/nfs/set-5/subset.500k.ds | perl $SRC/check-data/clean-timing.pl >subset.500k.out
    cmp subset.500k.out ../analysis/nfs/set-5/subset.500k.out
fi

exit 0
