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

rpm_topdir=`grep '^._topdir ' $HOME/.rpmmacros | awk '{print $2}'`

[ "$rpm_topdir" == "" ] && rpm_topdir=/usr/src/redhat

if [ -f /etc/redhat-release ]; then
    if [ `grep Fedora /etc/redhat-release | wc -l` = 1 ]; then
        PATCH_SPEC_OS=fedora
    elif [ `grep CentOS /etc/redhat-release | wc -l` = 1 ]; then
        PATCH_SPEC_OS=centos
    elif [ `grep 'Red Hat Enterprise' /etc/redhat-release | wc -l` = 1 ]; then
        PATCH_SPEC_OS=rhel
    fi
fi

[ ! -z "$PATCH_SPEC_OS" ] || PATCH_SPEC_OS=unknown

