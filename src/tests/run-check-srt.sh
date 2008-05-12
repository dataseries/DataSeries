#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

# As of 2008-05-12, this test is known to fail.

../process/srt2ds --compress-lzf $1/check-data/hourly.03126.srt.bz2 h03126.ds 
../process/cmpsrtds $1/check-data/hourly.03126.srt.bz2 h03126.ds 
../process/cmpsrtds $1/check-data/hourly.03126.srt.bz2 $1/check-data/h03126.ds-littleend 
../process/cmpsrtds $1/check-data/hourly.03126.srt.bz2 $1/check-data/h03126.ds-bigend 
rm h03126.ds

exit 0
