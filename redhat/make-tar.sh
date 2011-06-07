#!/bin/sh
case `pwd` in
   */DataSeries | */DataSeries.0.20[0-9][0-9].[0-9][0-9].[0-9][0-9])
      : 
      ;;
   *) echo "Being executed in the wrong directory, should be in .../DataSeries"
      exit 1
      ;;
esac

if [ -d _MTN ]; then
    `dirname $0`/../../Lintel/dist/make-release-changelog.sh
fi

if [ ! -f Release.info -o ! -f Changelog.mtn ]; then
    echo "Error: Ought to either both Release.info and Changelog.mtn from a release or should have just created it from monotone repository"
    exit 1
fi

. redhat/get-version.sh

cwd=`pwd`
dir=`basename $cwd`
cd ..
[ "$dir" == "DataSeries-$VERSION" ] || ln -s $dir DataSeries-$VERSION
tar cvvfhz DataSeries-$VERSION.tar.gz --exclude=DataSeries-$VERSION/_MTN DataSeries-$VERSION/
[ "$dir" == "DataSeries-$VERSION" ] || rm DataSeries-$VERSION
cp DataSeries-$VERSION.tar.gz $rpm_topdir/SOURCES/DataSeries-$VERSION.tar.gz

