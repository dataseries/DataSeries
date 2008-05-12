#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

../analysis/lsfdsanalysis -a 60:1167681240:1167681600:all,production: -a 120:1167681240:1167681600:all,sequence: -a 60:1167681240:1167681600:all,production:'start_time > 1167681360' $1/check-data/lsb.acct.2007-01-01-p1.ds  >check.lsfdsanalysis.tmp
perl $1/check-data/clean-timing.pl < check.lsfdsanalysis.tmp >check.lsfdsanalysis.txt
cmp check.lsfdsanalysis.txt $1/check-data/check.lsfdsanalysis.ref
rm check.lsfdsanalysis.tmp

exit 0
