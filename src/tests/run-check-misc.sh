#!/bin/sh
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e

./misc
../process/ds2txt --skip-index test.ds >test.txt
cmp $1/check-data/complex.txt test.txt

../process/ds2txt --skip-index $1/check-data/complex.ds-littleend >complex-little.txt
cmp $1/check-data/complex.txt complex-little.txt

if [ "$2" = "LZO-ON" ]; then
    ../process/ds2txt --skip-index $1/check-data/complex.ds-bigend >complex-big.txt
    cmp $1/check-data/complex.txt complex-big.txt
    rm complex-big.txt
elif [ "$2" = "LZO-OFF" ]; then
    echo "unable to run bigend test, LZO compression is disabled."
else
    echo "? '$2'"
    exit 1
fi

rm test.ds test.txt complex-little.txt 

exit 0
