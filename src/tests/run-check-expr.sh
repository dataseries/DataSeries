#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e

SRC=$1

# create our data file
./expr
../process/ds2txt --skip-index expr.ds |
  perl $SRC/check-data/clean-timing.pl >expr.txt

# run our tests
run() {
  echo "expr: $1 start" >>expr.txt
  ../process/ds2txt --skip-all --where 'Test::Simple' "$1" expr.ds |
    perl $SRC/check-data/clean-timing.pl >>expr.txt
  echo "expr: $1 end" >>expr.txt
}

run "a != b"
run "a == b"
run "a > b"
run "a < b"
run "a >= b"
run "a <= b"
run "a * - b == - c"
run "(a == b) && (b == c)"
run "(a == b) || (a == c)"

cmp $SRC/check-data/expr.txt expr.txt
rm -f expr.ds expr.txt

exit 0
