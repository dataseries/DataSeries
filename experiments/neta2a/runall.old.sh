#!/bin/bash
home="/home/krevate/projects/DataSeries/experiments/neta2a";
nodelist="$home/nodelist.txt";
serverclientnetbarnode="pds-11";
myhostname=`hostname`;
mynodeindex=`grep -n $myhostname $nodelist | cut -d: -f 1`;
let mynodeindex=$mynodeindex-1;
portbase=5000;
myport=$(( $portbase + $mynodeindex ));
involved=-1;

if [ -n "$1" ];
then
  numnodes=$1
else
  numnodes=`cat $nodelist | wc -l`
fi

if [ -n "$2" ];
then
  dataamount=$2
else
  dataamount=4000
fi
dataamountpernode=$[$dataamount/$numnodes]

for host in `cat $nodelist`;
do
  if [ $host == `hostname` ]; then
    involved=1;
  fi
done

if [ $involved -lt 0 ]; then
  echo "Not involved";
  exit
fi

echo "Running all-to-all with $dataamount MB over $numnodes nodes ($dataamountpernode MB per node)"

#start servers first
foundme=-1;
nodeindex=-1;
for host in `head -$numnodes $nodelist`;
do
  let nodeindex=$nodeindex+1
  port=$(( portbase + $nodeindex ));
  if [ $host == `hostname` ]; then
    foundme=1;
  elif [ $foundme -gt 0 ]; then
    #Create server to everyone with higher nodeindex
    echo "nodeindex $mynodeindex: start server for $nodeindex at $host on port $port"
    $home/runserver.sh $mynodeindex $nodeindex $host $port $dataamountpernode &
  fi
done

#barrier
$home/net_call_bar $serverclientnetbarnode
echo " "

#start clients second
foundme=-1;
nodeindex=-1;
for host in `head -$numnodes $nodelist`;
do
  let nodeindex=$nodeindex+1;
  if [ $host == `hostname` ]; then
    foundme=1;
  elif [ $foundme -le 0 ]; then
    #Create client to everyone with lower nodeindex
    echo "nodeindex $mynodeindex: connect to $nodeindex at $host on port $myport"
    $home/runclient.sh $mynodeindex $nodeindex $host $myport $dataamountpernode &
  fi
done