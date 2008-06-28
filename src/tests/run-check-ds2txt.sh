#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e

SRC=$1

../process/ds2txt --skip-all --select common record_id,packet_at,dest --printSpec='type="Trace::NFS::common" name="packet_at" print_format="sec.nsec" units="2^-32 seconds" epoch="unix"' --printSpec='type="Trace::NFS::common" name="dest" print_format="ipv4"' $SRC/check-data/nfs-2.set-1.20k.ds >ds2txt.test.tmp

head -30 <ds2txt.test.tmp >ds2txt.test.txt
cmp ds2txt.test.txt $1/check-data/ds2txt.test.ref
rm -f ds2txt.test.tmp ds2txt.test.txt

../process/ds2txt --skip-all --select common record_id,packet_at,dest --printSpec='type="Trace::NFS::common" name="packet_at" print_format="sec.nsec" units="2^-32 seconds" epoch="unix"' --printSpec='type="Trace::NFS::common" name="dest" print_format="ipv4"' --where Trace::NFS::common 'record_id < 38549988050' $SRC/check-data/nfs-2.set-1.20k.ds >ds2txt-where.test.txt
cmp ds2txt-where.test.txt $1/check-data/ds2txt-where.test.ref
rm -f ds2txt-where.test.tmp

