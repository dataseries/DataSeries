#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

LINTEL_REGRESSION_TEST_INC_DIR=$1/src/perl-modules perl ../analysis/lsfdsplots --indexfile=test.index.2.ds --psonly --plotdir=check-lsfdsplots --groups=all --starttime=1167681250 --endtime=1167681547 --lsfdsanalysis=../analysis/lsfdsanalysis

# TODO: come up with another way to test this; the following test
# doesn't work since different versions of gnuplot generate very
# different postscript.  (First found on gnuplot 4.2 with debian lenny
# vs. 4.0 with debian etch)  Visualization looks the same.

# grep -v '^%%CreationDate:' check-lsfdsplots/all/all.ps >check-lsfdsplots/all/all.ps-nodate
# cmp check-lsfdsplots/all/all.ps-nodate $1/check-data/lsfdsplots.ps.ref

exit 0
