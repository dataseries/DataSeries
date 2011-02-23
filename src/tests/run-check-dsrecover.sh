#!/bin/sh -x
#
# (c) Copyright 2010, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 


echo Stage one - recover valid faile
# recover a valid file and compare expected outputs
../process/dsrecover $1/check-data/h03126.ds-littleend test-recover.ds

../process/ds2txt --skip-all $1/check-data/h03126.ds-littleend > expected.txt
../process/ds2txt --skip-all test-recover.ds > recovered.txt
cmp expected.txt recovered.txt

echo Stage two - recover file truncated by process abort
# recover an invalid file and compare expected outputs
./generate-incomplete-ds
../process/dsrecover incomplete-ds-file.ds recovered-ds-file.ds
../process/ds2txt --skip-all recovered-ds-file.ds > recovered.txt
cmp $1/check-data/check.dsrecover.txt recovered.txt

# cleanup
rm test-recover.ds incomplete-ds-file.ds recovered-ds-file.ds recovered.txt expected.txt

echo Stage three - test corruption failures
for testcase in 100-zeros-at-50K stomp-end two-extents ; do
    ../process/dsrecover $1/check-data/recovery/corrupt-$testcase.ds recovered-$testcase.ds
    cmp recovered-$testcase.ds $1/check-data/recovery/recovered-$testcase.ds
    rm recovered-$testcase.ds
done

echo Stage four - test unrecoverable corruption
if ../process/dsrecover $1/check-data/recovery/corrupt-stomp-beginning.ds recovered-stomp-beginning.ds ; then
    echo Should have errored out on unrecoverable corruption
    exit 1
fi

if [ -a recovered-stomp-beginning.ds ] ; then
    echo Should not have recovered from corruption at beginning of file.
    exit 1
fi



exit 0
