#!/bin/bash

home="/home/krevate/projects/DataSeries/experiments/neta2a";
outputdir="/mnt/fileserver-1/b/user/ekrevat/";
nodelist="$home/nodelist.txt";
myhostname=`hostname`;
mynodeindex=`grep -n $myhostname $nodelist | cut -d: -f 1`;
let mynodeindex=$mynodeindex-1;


for i in 32; do
    if [ $mynodeindex -eq 0 ]; then
	echo "PRIMARY NODE"
    fi
    for j in 0 20 40 60 80; do
	for k in `seq 0 1`; do
	./runone.sh $i 16000 $j $k | tee ${outputdir}logs/`hostname`
	./runone.sh $i 16000 $j $k shared | tee ${outputdir}logs/`hostname`
	done
    done
done