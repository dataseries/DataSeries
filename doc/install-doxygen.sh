#!/bin/sh
#
# (c) Copyright 2007-2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# install doxygen docs

# TODO: update this be general so it can be used by DataSeries also.

if [ $# != 2 ]; then 
    echo "Usage: $0 <doxygen-binary-build-dir> <target-share-dir>"
    exit 1
fi

if [ ! -d "$1/doxygen/html" ]; then
    echo "Missing $1/doxygen/html"
    exit 1
fi

BUILD="$1"
TARGET="$2"
echo "Install html to $TARGET/doc/DataSeries"
mkdir -p "$TARGET/doc/DataSeries"
cp -rp "$BUILD/doxygen/html" "$TARGET/doc/DataSeries"
echo "Install man pages to $TARGET/man/man3"
mkdir -p "$TARGET/man/man3"
cp -rp "$BUILD/doxygen/man/man3"/* "$TARGET/man/man3"
