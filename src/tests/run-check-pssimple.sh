#!/bin/sh
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

# the index may change as data will compress differently on big/little
# endian machines, so we can't compare with it in the text output

set -e
pwd
../process/pssimple2ds --extent-size=10485760 $1/check-data/pssimple.5.bz2 pss5.ds
../process/ds2txt --skip-index pss5.ds >pss5.txt
../process/ds2txt --skip-index $1/check-data/pss5.ds-littleend >pss5-little.txt
../process/ds2txt --skip-index $1/check-data/pss5.ds-bigend >pss5-big.txt
cmp pss5.txt pss5-little.txt
cmp pss5.txt pss5-big.txt
rm pss5.txt pss5-little.txt pss5-big.txt pss5.ds

exit 0
