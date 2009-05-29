#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script
set -e

DATADIR=
for dir in $HOME/nobackup-data $HOME/datasets; do
    [ -f $dir/animation-bear/nfs-2/set-0/236500-236542.ds ] && DATADIR=$dir
done

if [ -z "$DATADIR" ]; then
    echo "Unfortunately, this test is uninteresting unless we use a reasonable size dataset"
    echo "No test possible, can't find nfs-2/set-0/236500-236542.ds"
    exit 0
fi

../analysis/nfs/ipnfscrosscheck $DATADIR/animation-bear/nfs-2/set-0/236500-236542.ds > ipnfscrosscheck.tmp
sed "s,$DATADIR,DATADIR,g" <ipnfscrosscheck.tmp >ipnfscrosscheck.txt
cmp ipnfscrosscheck.txt $1/check-data/ipnfscrosscheck.ref
