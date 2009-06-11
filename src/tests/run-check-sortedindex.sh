#!/bin/sh -x
#
# (c) Copyright 2009, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script for SortedIndexModule

set -e

SRC=$1

[ ! -f sortedindex.ds ] || rm sortedindex.ds

# create data file
../process/dsextentindex --compress-lzf --new 'NFS trace: common' packet-at sortedindex.ds $SRC/check-data/nfs.set6.20k.ds

# run program and compare
./sortedindex > sortedindex.txt

cmp $SRC/check-data/sortedindex.txt sortedindex.txt
rm -f sortedindex.ds sortedindex.txt

exit 0
