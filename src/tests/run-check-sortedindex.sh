#!/bin/sh -x
#
# (c) Copyright 2009, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script for SortedIndexModule
# 
# Generates three index files for testing purposes:
#    * sortedindex - multiple extents indexed from a single file
#    * unsortedindex - used to check that unsorted indices generate error

set -e

SRC=$1

[ ! -f sortedindex.ds ] || rm sortedindex.ds
[ ! -f unsortedindex.ds ] || rm unsortedindex.ds

# create data files
../process/dsextentindex --compress-lzf --new 'NFS trace: common' packet-at sortedindex.ds $SRC/check-data/nfs.set6.20k.ds
../process/dsextentindex --compress-lzf --new 'NFS trace: common' source unsortedindex.ds $SRC/check-data/nfs.set6.20k.ds

# run program and compare
./sortedindex > sortedindex.txt

cmp $SRC/check-data/sortedindex.txt sortedindex.txt

rm -f sortedindex.ds unsortedindex.ds sortedindex.txt 

exit 0
