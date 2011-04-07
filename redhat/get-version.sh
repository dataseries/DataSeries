if [ ! -f debian/changelog ]; then
    echo "$0: Missing debian/changelog; can't extract consistent version"
    exit 1
fi
VERSION=`perl -ne 'if (/^dataseries \(([0-9\.]+)\) /o) { print "$1\n"; exit(0);} ' <debian/changelog`
if [ -z "$VERSION" ]; then
    echo "$0: missing version in debian/changelog"
    exit 1
fi
export VERSION
RELEASE=`perl -ne 'if (/ $ENV{VERSION}-(\d+)$/o) { print "$1\n"; exit(0);} ' <redhat/DataSeries.spec.in`

if [ -z "$RELEASE" ]; then
    echo "$0: missing release for version $VERSION in redhat/DataSeries.spec.in"
    exit 1
fi
