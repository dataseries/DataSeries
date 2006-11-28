#                                               -*- Autoconf -*-
#
#  (c) Copyright 2005, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# Process this file with autoconf to produce a configure script.

AC_INIT(DataSeries, 1.0, [dataseries-bugs@cello.hpl.hp.com])

AC_PREREQ(2.57)

AC_REVISION($Revision: 1.2 $)
AC_CANONICAL_SYSTEM
AM_SANITY_CHECK
AM_INIT_AUTOMAKE

COM_HP_HPL_LINTEL_OPTMODE()

AM_MAINTAINER_MODE
COM_HP_HPL_ACINCLUDE(lintel dataseries)

## programs...
AC_PROG_CXX
AC_PROG_INSTALL
AC_PROG_LD
AC_SUBST(LD)

## library bits...
AC_ENABLE_SHARED
AC_ENABLE_STATIC
AC_LIBTOOL_SETUP
AC_PROG_LIBTOOL
AC_SUBST(LIBTOOL_DEPS)

AC_LANG(C++)

HPL_REQUIRELIB_LINTEL
HPL_REQUIRELIB_XML2

HPL_WITHLIB_SRT

AC_LANG(C)

HPL_WITHLIB_LZF
HPL_WITHLIB_LZO
HPL_WITHLIB_ZLIB
HPL_WITHLIB_BZ2
HPL_WITHLIB_PCAP

## flags
AC_SYS_LARGEFILE

#
# Libtool library revision control info
# See the libtool documentation under the heading "Libtool's versioning
# system" in order to understand the meaning of these fields
#
# Here are a set of rules to help you update your library version
# information:
#
#  1. Start with version information of `0:0:0' for each libtool library.
#  2. Update the version information only immediately before a public
#     release of your software. More frequent updates are unnecessary, and
#     only guarantee that the current interface number gets larger faster.
#  3. If the library source code has changed at all since the last update,
#     then increment revision (`c:r:a' becomes `c:r+1:a').
#  4. If any interfaces have been added, removed, or changed since the last
#     update, increment current, and set revision to 0.
#  5. If any interfaces have been added since the last public release, then
#     increment age.
#  6. If any interfaces have been removed since the last public release,
#     then set age to 0.

LIBDATASERIES_VERSION=0:0:0

AC_SUBST(LIBDATASERIES_VERSION)

AC_CONFIG_FILES([Makefile include/Makefile src/Makefile src/dataseries-config])

AC_OUTPUT

COM_HP_HPL_LINTEL_OPTMODE_MESSAGES()

if test "$with_srt" = 'yes'; then
    :
else
    AC_MSG_WARN([skipping building of SRT related programs, e.g. srt2dataseries and cmpsrtds])
fi

if test "$with_pcap" = 'yes'; then
    :
else
    AC_MSG_WARN([skipping building of libpcap related programs, e.g. driverdump])
fi

if test "$with_lzf" = 'yes'; then
    :
else
    AC_MSG_WARN(building DataSeries without LZF compression support)
fi

if test "$with_lzo" = 'yes'; then
    :
else
    AC_MSG_WARN(building DataSeries without LZO compression support)
fi

if test "$with_zlib" = 'yes'; then
    :
else
    AC_MSG_WARN(building DataSeries without ZLIB compression support)
fi

if test "$with_bz2" = 'yes'; then
    :
else
    AC_MSG_WARN(building DataSeries without BZ2 compression support)
fi

