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
    if [ -f Release.info ]; then
	echo "Warning, overwriting Release.info with current information from monotone"
	rm Release.info
    fi
    echo "Monotone-Revision: `mtn automate get_base_revision_id`" >Release.info
    echo "Creation-Date: `date +%Y-%m-%d-%H-%M`" >>Release.info
    echo "BEGIN_EXTRA_STATUS" >>Release.info
    mtn status >>Release.info
    echo "END_EXTRA_STATUS" >>Release.info
    mtn log >Changelog.mtn
fi

if [ ! -f Release.info -o ! -f Changelog.mtn ]; then
    echo "Error: Ought to either both Release.info and Changelog.mtn from a release or should have just created it from monotone repository"
    exit 1
fi

. redhat/get-version.sh
perl redhat/patch-spec.pl $VERSION $RELEASE || exit 1

CHECK_VERSION=`grep Version: redhat/DataSeries.spec | awk '{print $2}'`
if [ "$CHECK_VERSION" = "" -o "$CHECK_VERSION" = "0." ]; then
    echo "Missing version in DataSeries.spec"
    exit 1
fi

if [ "$CHECK_VERSION" != "$VERSION" ]; then
    echo "Bad version in redhat/DataSeries.spec; $CHECK_VERSION != $VERSION"
    exit 1
fi

cwd=`pwd`
dir=`basename $cwd`
cd ..
[ "$dir" == "DataSeries-$VERSION" ] || ln -s $dir DataSeries-$VERSION
tar cvvfhz DataSeries-$VERSION.tar.gz DataSeries-$VERSION/
[ "$dir" == "DataSeries-$VERSION" ] || rm DataSeries-$VERSION

