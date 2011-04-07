#!/bin/sh
. redhat/get-version.sh

rpm_topdir=`grep '^._topdir ' $HOME/.rpmmacros | awk '{print $2}'`

[ "$rpm_topdir" == "" ] && rpm_topdir=/usr/src/redhat

echo "Using rpm_topdir = $rpm_topdir"

cp ../DataSeries-$VERSION.tar.gz $rpm_topdir/SOURCES/DataSeries-$VERSION.tar.gz
