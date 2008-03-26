#                                               -*- Autoconf -*-
#
#  (c) Copyright 2005, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# Process this file with autoconf to produce a configure script.

AC_INIT(DataSeries, 1.0, [dataseries-bugs@cello.hpl.hp.com])

AC_PREREQ(2.57)

AC_REVISION($Revision: 6369 $)
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
AM_PROG_AS
AC_SUBST(LD)

HPL_PERL_SUBSTS
EXPAND_VARIABLE

## library bits...
AC_ENABLE_SHARED
AC_ENABLE_STATIC
AC_LIBTOOL_SETUP
AC_PROG_LIBTOOL
AC_SUBST(LIBTOOL_DEPS)

AC_LANG(C++)

HPL_REQUIRELIB_LINTEL
AC_MSG_CHECKING([for lintel boost support...])
if lintel-config --has-feature boost; then
   AC_MSG_RESULT(ok)
else
   AC_MSG_RESULT(failed)
   AC_MSG_FAILURE([DataSeries requires that lintel have Boost support, but it is missing from lintel-config --features])
fi
HPL_REQUIRELIB_XML2

HPL_WITHLIB_SRT

AC_LANG(C)
HPL_WITHLIB_PCRE
HPL_WITHLIB_DMALLOC

AM_CONDITIONAL(WITH_LZF, true)
HPL_WITHLIB_LZO
HPL_WITHLIB_ZLIB
HPL_WITHLIB_BZ2
HPL_WITHLIB_PCAP
HPL_REQUIRELIB_CRYPTO
HPL_WITHLIB_DMALLOC

AC_ARG_ENABLE(paper,
  [  --enable-paper    build binaries for the paper -- default NO],
  [enable_paper=$enableval],
  [enable_paper=no])

AM_CONDITIONAL(ENABLE_PAPER, test "$enable_paper" = yes)

AC_ARG_ENABLE(example,
  [  --enable-example    build binaries for the examples -- default NO],
  [enable_example=$enableval],
  [enable_example=no])

AM_CONDITIONAL(ENABLE_EXAMPLE, test "$enable_example" = yes)

AM_CONDITIONAL(WITH_LINUX, test "`uname -s`" = Linux)
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

test "x$exec_prefix" = xNONE && exec_prefix='${prefix}'
expanded_bindir=`expand_variable $bindir`
AC_SUBST(expanded_bindir)

WITH_PERL_XML_PARSER
WITH_PERL_DATE_PARSE

AC_CONFIG_FILES([Makefile include/Makefile src/Makefile src/perl-modules/Makefile src/dataseries-config src/dstypes2cxx src/analysis/lsfdsplots src/analysis/dsdecrypt])

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

