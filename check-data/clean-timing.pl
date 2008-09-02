#!/usr/bin/perl -w
use strict;
while(<STDIN>) {
    s/ \d+\.\d{2} secs decode time/ 0.00 secs decode time/o;
    s/^decode seconds: +\d+\.\d{2}/decode seconds:      0.00/o;
    # dsstatgroupby
    s/^# wait fraction *: +\d+\.\d{2}$/# wait fraction:       0.00/o;
    # lsf analysis
    s/^wait fraction *: +\d+\.\d{2}$/wait fraction:       0.00/o;
    # nfs analysis
    s/^wait fraction *:( +\d+\.\d{2}){4}$/wait fraction:       0.00     0.00     0.00     0.00/o;
    print;
}
