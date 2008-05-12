#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

rm test.index.1.ds test.index.2.ds || true
../process/dsextentindex --compress-lzf --new I/O enter_driver,machine_id,disk_offset,is_read test.index.1.ds $1/check-data/h03126.ds-littleend
../process/ds2txt --skip-index test.index.1.ds >test.index.tmp
perl $1/check-data/index-fixup.pl <test.index.tmp >test.index.1.ds.txt
cmp test.index.1.ds.txt $1/check-data/test.index.1a.ref
../process/dsextentindex --compress-lzf test.index.1.ds $1/check-data/h03126.ds-bigend
../process/ds2txt --skip-index test.index.1.ds > test.index.tmp
perl $1/check-data/index-fixup.pl < test.index.tmp >test.index.1.ds.txt
cmp test.index.1.ds.txt $1/check-data/test.index.1b.ref
../process/dsextentindex --compress-lzf --new Batch::LSF::Grizzly cluster_name,submit_time,end_time test.index.2.ds $1/check-data/lsb.acct.2007-01-01-p1.ds 
../process/ds2txt --skip-index test.index.2.ds > test.index.tmp
perl $1/check-data/index-fixup.pl < test.index.tmp >test.index.2.ds.txt
cmp test.index.2.ds.txt $1/check-data/test.index.2.ref
rm test.index.tmp

exit 0
