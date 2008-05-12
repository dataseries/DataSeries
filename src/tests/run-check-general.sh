#!/bin/sh
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e

./general
../process/ds2txt --skip-index test.ds >test.txt
../process/ds2txt --skip-index $1/check-data/complex.ds-littleend >complex-little.txt
../process/ds2txt --skip-index $1/check-data/complex.ds-bigend >complex-big.txt
cmp $1/check-data/complex.txt test.txt
cmp $1/check-data/complex.txt complex-little.txt
cmp $1/check-data/complex.txt complex-big.txt
rm test.ds test.txt complex-little.txt complex-big.txt

exit 0
