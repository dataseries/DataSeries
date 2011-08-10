#!/bin/sh
perl redhat/patch-spec.pl $PATCH_SPEC_OS $VERSION $RELEASE >redhat/DataSeries.spec || exit 1

CHECK_VERSION=`grep Version: redhat/DataSeries.spec | awk '{print $2}'`
if [ "$CHECK_VERSION" = "" -o "$CHECK_VERSION" = "0." ]; then
    echo "Missing version in DataSeries.spec"
    exit 1
fi

if [ "$CHECK_VERSION" != "$VERSION" ]; then
    echo "Bad version in redhat/DataSeries.spec; $CHECK_VERSION != $VERSION"
    exit 1
fi
