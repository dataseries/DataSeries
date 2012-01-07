#!/bin/sh
#
# (c) Copyright 2007-2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# script for rebuilding the dsexpr flex/bison bits.

set -e
set -x
echo "version check, from debian squeeze, known to work."
echo "Failure was on getting both debian etch and RHEL4"
echo "to work at the same time."
echo "TODO: re-test and see if we can get it to rebuild properly with more versions"
echo "Alternately, always try to rebuild, but keep around the generated version for"
echo "Cases where it doesn't work correctly."
[ "`flex --version`" = "flex 2.5.35" ]
[ "`bison --version | head -1`" = "bison (GNU Bison) 2.4.1" ]

flex -o module/DSExprScan.cpp module/DSExprScan.ll
bison -d -o module/DSExprParse.cpp module/DSExprParse.yy
perl module/expr-parser-fixup.pl module DSExprParse
rm module/stack.hh module/location.hh module/position.hh
