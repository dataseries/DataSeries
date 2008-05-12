#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

rm wcweb.ds wcweb.raw wcweb.analysis || true
../process/wcweb2ds $1/check-data/wc_day50_4.head wcweb.ds
../process//ds2wcweb wcweb.ds >wcweb.raw
cmp $1/check-data/wc_day50_4.head wcweb.raw
../analysis/wcanalysis wcweb.ds >wcweb.analysis 2>/dev/null
cmp wcweb.analysis $1/check-data/wc_day50_4.analysis

exit 0
