#!/bin/sh -x
#
# (c) Copyright 2007-2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# script for rebuilding the dsexpr flex/bison bits.

set -e
echo "version check, from debian etch, known to work."
echo "was unable to get the flex and bison stuff to work on both"
echo "debian etch and RHEL4 with flex 2.5.4 and bison 1.875"
[ "`flex --version`" = "flex 2.5.33" ]
[ "`bison --version | head -1`" = "bison (GNU Bison) 2.3" ]
flex -o module/DSExprScan.cpp module/DSExprScan.ll
bison -d -o module/DSExprParse.cpp module/DSExprParse.yy
perl module/expr-parser-fixup.pl module DSExprParse
rm module/stack.hh module/location.hh module/position.hh
