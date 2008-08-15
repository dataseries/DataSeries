#!/bin/sh -x
#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# test script

set -e 

../process/dsstatgroupby 'I/O' basic '1000*(return_to_driver - leave_driver)' group by 'device_number' basic 'bytes/1024' group by 'machine_id' from $1/check-data/h03126.ds-littleend >test.dsstatgroupby-tmp
perl $1/check-data/clean-timing.pl <test.dsstatgroupby-tmp | sort >test.dsstatgroupby.1
perl $1/check-data/unordered-file-equality.pl test.dsstatgroupby.1 $1/check-data/test.dsstatgroupby.1.ref

../process/dsstatgroupby 'Batch::LSF' basic 'start_time - submit_time' where 'start_time - submit_time > 50000' group by 'production' basic 'cpu_time/(end_time-start_time)' group by production quantile 'start_time - submit_time' where 'start_time - submit_time > 50000' group by production from $1/check-data/lsb.acct.2007-01-01-p1.ds >test.dsstatgroupby.tmp
perl $1/check-data/clean-timing.pl <test.dsstatgroupby.tmp | sort >test.dsstatgroupby.2
perl $1/check-data/unordered-file-equality.pl test.dsstatgroupby.2 $1/check-data/test.dsstatgroupby.2.ref
rm test.dsstatgroupby.tmp

exit 0
