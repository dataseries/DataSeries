#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

../analysis/nfs/nfsdsanalysis -a -j -l 1 -u 64000000c20f0300200000000127ef590364240c4fdf0058400000009a871600 -u a63c6021c20f0300200000000117721df1e3040064000000400000009a871600 -u e8535201c2cca305200000000079de25139fc708d6390035400000006bfc0800 -z e8535201c2cca3052000000000393dcb82c6fc08d6390035400000006bfc0800 -z $1/check-data/nfs.set6.20k.testfhs $1/check-data/nfs.set6.20k.ds >check.nfsdsanalysis.tmp
perl $1/check-data/clean-timing.pl < check.nfsdsanalysis.tmp >check.nfsdsanalysis.txt
cmp check.nfsdsanalysis.txt $1/check-data/check.nfsdsanalysis.ref
rm check.nfsdsanalysis.tmp

../analysis/nfs/nfsdsanalysis -a -j -l 1 -u 059f13004fb46a0d2000000002725fa8b2c39a0060f8073d059f13004fb46a00 -u 059f13004fb46a0d2000000004de9a890a4af00ebadc0762059f13004fb46a00 -u 059f13004fb46a0d2000000004de9a85074af00ebadc0762059f13004fb46a00 $1/check-data/nfs-2.set-0.20k.ds -z 87d874012ece4f0520000000040ba0561d83d10060f8073e87d874012ece4f00 >nfs-2.set-0.20k.tmp
perl $1/check-data/clean-timing.pl < nfs-2.set-0.20k.tmp > nfs-2.set-0.20k.check
cmp nfs-2.set-0.20k.check $1/check-data/nfs-2.set-0.20k.ref

exit 0
