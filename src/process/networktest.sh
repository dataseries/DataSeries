#!/bin/bash

CURRENT_EXPERIMENT_COUNT=$1
CURRENT_NODE=$2
NODES=('10.10.10.10' '10.10.10.11' '10.10.10.13' '10.10.10.14' '10.10.10.15' '10.10.10.16' '10.10.10.17' '10.10.10.18' '10.10.10.19' '10.10.10.20')
PORTS=()
COUNT=${#NODES[@]}

if [ $CURRENT_NODE -ge $CURRENT_EXPERIMENT_COUNT ]; then
	echo "This node will not participate in the experiment."
	exit 0
fi

for ((j=0; j < $CURRENT_EXPERIMENT_COUNT; j++)); do
	PORTS[$j]=$((13130 + $j))
done

NODE=${NODES[${CURRENT_NODE}]}

echo $NODE will wait for these nodes to connect:
for ((j=$CURRENT_NODE - 1; j >= 0; j--)); do
	echo datapump "\"${CURRENT_NODE}->$j\"" 1000 "|" nc -l -p ${PORTS[$j]} "> /dev/null &"
	datapump "${CURRENT_NODE}->$j" 1000 | nc -l -p ${PORTS[$j]} > /dev/null &
done

sleep 3

echo $NODE will connect to these nodes:
for ((j=$CURRENT_NODE + 1; j < $CURRENT_EXPERIMENT_COUNT; j++)); do
	echo datapump "\"${CURRENT_NODE}->$j\"" 1000 "|" nc ${NODES[${j}]} ${PORTS[$CURRENT_NODE]} "> /dev/null &"
	datapump "${CURRENT_NODE}->$j" 1000 | nc ${NODES[${j}]} ${PORTS[$CURRENT_NODE]} > /dev/null &
done