#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

perl ../dstypes2cxx -o b --ds2txt=../process/ds2txt $1/check-data/lsb.acct.2007-01-01-p1.ds >check.dstypes2cxx.txt
perl ../dstypes2cxx -o r --ds2txt=../process/ds2txt $1/check-data/lsb.acct.2007-01-01-p1.ds >>check.dstypes2cxx.txt
perl ../dstypes2cxx -o d -p 'DataSeries: ExtentIndex'=dsextent --ds2txt=../process/ds2txt $1/check-data/lsb.acct.2007-01-01-p1.ds >>check.dstypes2cxx.txt
cmp check.dstypes2cxx.txt $1/check-data/check.dstypes2cxx.ref

exit 0
