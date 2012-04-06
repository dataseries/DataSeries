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

. $1/version
../Lintel/dist/make-release-changelog.sh

cwd=`pwd`
dir=`basename $cwd`
cd ..
[ "$dir" = "DataSeries-$RELEASE_VERSION" ] || ln -snf $dir DataSeries-$RELEASE_VERSION
tar cvvfhz DataSeries-$RELEASE_VERSION.tar.gz --exclude=DataSeries-$RELEASE_VERSION/.git --exclude=\*\~ DataSeries-$RELEASE_VERSION/
[ "$dir" = "DataSeries-$RELEASE_VERSION" ] || rm DataSeries-$RELEASE_VERSION

mv DataSeries-$RELEASE_VERSION.tar.gz /var/www/pb-sources
