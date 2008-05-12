#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

rm dsselect.test.1 || true 2>/dev/null
../process/dsselect --compress-gz --extent-size=524288 I/O enter_driver,bytes,machine_id,driver_type,disk_offset $1/check-data/h03126.ds-littleend dsselect.test.1 >/dev/null
gunzip < $1/check-data/dsselect.test.1.gz >dsselect.test.1.ref
../process/ds2txt --skip-index dsselect.test.1 >dsselect.test.1.txt
cmp dsselect.test.1.ref dsselect.test.1.txt

exit 0
