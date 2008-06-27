#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e

SRC=$1

./expr
../process/ds2txt --skip-index expr.ds >expr.tmp
perl $SRC/check-data/clean-timing.pl <expr.tmp >expr.txt

run() {
  ../process/dsstatgroupby 'Test::Simple' basic a where "$1" group by b basic b where "$1" group by a from expr.ds > "$2".tmp
  perl $SRC/check-data/clean-timing.pl <"$2".tmp >"$2".txt
}

# TODO: collapse all of these into a single test run with a single output file
# and do all the comparisons in one go.  Leads to fewer regression test files.

run "a != b" expr-neq
run "a == b" expr-eq
run "a > b" expr-gt
run "a < b" expr-lt
run "a >= b" expr-geq
run "a <= b" expr-leq

for x in '' '-neq' '-eq' '-gt' '-lt' '-geq' '-leq'; do
    echo $x
    cmp $SRC/check-data/expr$x.txt expr$x.txt
done

#rm expr.ds expr.txt expr-neq.txt expr-eq.txt expr-gt.txt \
#   expr-lt.txt expr-geq.txt expr-leq.txt

exit 0
