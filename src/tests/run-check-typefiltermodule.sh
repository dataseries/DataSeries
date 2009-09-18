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

[ ! -f sortedindex.ds ] || rm sortedindex.ds
[ ! -f unsortedindex.ds ] || rm unsortedindex.ds

# run program and compare
./typefiltermodule $SRC/check-data/nfs-2.set-0.20k.ds > typefiltermodule.txt

cmp $SRC/check-data/typefiltermodule.txt typefiltermodule.txt

rm -f typefiltermodule.txt

exit 0
