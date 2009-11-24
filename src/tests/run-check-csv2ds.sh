#!/bin/sh
set -e

SRC=$1

../process/csv2ds --compress-lzf --xml-desc-file=$SRC/check-data/csv2ds-1.xml $SRC/check-data/csv2ds-1.csv csv2ds-1.ds
../process/ds2txt --skip-index csv2ds-1.ds >csv2ds-1.txt
cmp csv2ds-1.txt $SRC/check-data/csv2ds-1.txt.ref
