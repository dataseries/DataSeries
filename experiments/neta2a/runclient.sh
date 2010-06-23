#!/bin/bash
home="/home/krevate/projects/DataSeries/experiments/neta2a"
mynodeindex=$1
nodeindex=$2
host=$3
port=$4

#echo "nodeindex $mynodeindex: connect to $nodeindex at $host on port $port"
dd if=/dev/zero bs=1000000 count=4000 | nc $host $port | ./reader &> $home/logs/$nodeindex.$mynodeindex.c