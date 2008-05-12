#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

rm ellard.ds ellard.ds.txt ellard-data.txt ellard-analysis.txt || true
../process/ellardnfs2ds $1/check-data/ellard-raw-data.txt ellard.ds
../process/ds2txt --skip-index ellard.ds >ellard.ds.txt
cmp ellard.ds.txt $1/check-data/ellard.ds.txt.ref
../process/ds2ellardnfs ellard.ds >ellard-data.txt
cmp ellard-data.txt $1/check-data/ellard-raw-data.txt 
../process/ellardanalysis ellard.ds >ellard-analysis.txt
cmp ellard-analysis.txt $1/check-data/ellard-analysis.txt.ref

exit 0
