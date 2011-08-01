#!/bin/sh -x
#
# (c) Copyright 2009, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script for TypeFilterModule
# 
# Reads in a DS file and extracts particular extents based on filters

set -e

SRC=$1

./extent-write-callback > extent-write-callback.txt

cmp $SRC/check-data/extent-write-callback.txt extent-write-callback.txt

rm -f extent-write-callback.txt
rm -f extent-write-callback.ds

exit 0
