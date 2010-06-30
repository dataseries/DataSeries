#!/bin/bash
home="/home/krevate/projects/DataSeries/experiments/neta2a"
mynodeindex=$1
nodeindex=$2
host=$3
port=$4
dataamountpernode=$5

echo "nodeindex $mynodeindex: start server for $nodeindex at $host on port $port to read $dataamountpernode MB"
echo "dd if=/dev/zero bs=1000000 count=$dataamountpernode | nc -p $port -l | ./reader $[$dataamountpernode*1000000] &> $home/logs/$mynodeindex.$nodeindex.s"
dd if=/dev/zero bs=1000000 count=$dataamountpernode | nc -p $port -l | ./reader $[$dataamountpernode*1000000] &> $home/logs/$mynodeindex.$nodeindex.s