#!/bin/sh
set -e -x
case `pwd` in
   */DataSeries | */DataSeries.0.20[0-9][0-9].[0-9][0-9].[0-9][0-9])
      : 
      ;;
   *) echo "Being executed in the wrong directory, should be in .../DataSeries"
      exit 1
      ;;
esac

if [ -d .git ]; then
    `dirname $0`/../../Lintel/dist/make-release-changelog.sh
fi

if [ ! -f Release.info -o ! -f ChangeLog ]; then
    echo "Error: Ought to either both Release.info and Changelog.mtn from a release or should have just created it from monotone repository"
    exit 1
fi

. redhat/get-version.sh

cwd=`pwd`
dir=`basename $cwd`
cd ..
[ "$dir" = "DataSeries-$VERSION" -o -d DataSeries-$VERSION ] || ln -s $dir DataSeries-$VERSION
tar cvvfhz DataSeries-$VERSION.tar.gz --exclude=DataSeries-$VERSION/.git DataSeries-$VERSION/
[ "$dir" = "DataSeries-$VERSION" ] || rm DataSeries-$VERSION

mkdir -p $rpm_topdir/SOURCES
cp DataSeries-$VERSION.tar.gz $rpm_topdir/SOURCES/DataSeries-$VERSION.tar.gz

