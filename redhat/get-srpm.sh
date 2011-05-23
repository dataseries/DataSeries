#!/bin/sh
. redhat/get-version.sh
if [ -z "$1" ]; then
    echo "Usage: $0 <os>-<version>-<arch>"
    exit 1
fi
OS=`echo $1 | sed 's/-.*//'`
echo "$rpm_topdir/SRPMS/DataSeries-$OS-$VERSION-1.src.rpm"
