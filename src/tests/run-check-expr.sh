#!/bin/sh
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e

./expr
../process/ds2txt --skip-index expr.ds >expr.txt

run() {
  ../process/dsstatgroupby 'Test::Simple' basic a where "$1" group by b basic b where "$1" group by a from expr.ds > "$2"
}

run "a != b" expr-neq.txt
run "a == b" expr-eq.txt
run "a > b" expr-gt.txt
run "a < b" expr-lt.txt
run "a >= b" expr-geq.txt
run "a <= b" expr-leq.txt

for x in '' '-neq' '-eq' '-gt' '-lt' '-geq' '-leq'; do
    echo $x
    cmp $1/check-data/expr$x.txt expr$x.txt
done

#rm expr.ds expr.txt expr-neq.txt expr-eq.txt expr-gt.txt \
#   expr-lt.txt expr-geq.txt expr-leq.txt

exit 0
