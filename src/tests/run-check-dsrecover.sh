#!/bin/sh -x
#
# (c) Copyright 2010, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

# recover a valid file and compare expected outputs
../process/dsrecover $1/check-data/h03126.ds-littleend test1.ds

../process/ds2txt --skip-all $1/check-data/h03126.ds-littleend > expected.txt
../process/ds2txt --skip-all test1.ds > recovered.txt
cmp expected.txt recovered.txt

# recover an invalid file and compare expected outputs
./generate-incomplete-ds
../process/dsrecover test.ds test2.ds
../process/ds2txt --skip-all test2.ds > recovered.txt
cmp $1/check-data/check.dsrecover.txt recovered.txt

# cleanup and exit
rm test.ds test1.ds test2.ds recovered.txt expected.txt
exit 0