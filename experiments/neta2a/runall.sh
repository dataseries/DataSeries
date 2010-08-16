#!/bin/bash

for i in `seq 0 0`; do
    ./runone.sh 8 4000 60 $i shared | tee logs/`hostname`
done