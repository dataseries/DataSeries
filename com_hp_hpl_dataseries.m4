#
#  (c) Copyright 2005, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#

# autoconf macros used for dataseries

AC_DEFUN([HPL_REQUIRELIB_DATASERIES],
[
if test "$have_lintel" = yes; then
    :
else
HPL_REQUIRELIB_LINTEL
fi
if test "$have_lintel" = yes; then
    :
else
    AC_MSG_FAILURE([dataseries requires lintel, which hasn't been detected])
fi
AC_LANG_ASSERT(C++)
AC_MSG_CHECKING(for dataseries-config)
DATASERIES_CFLAGS=
DATASERIES_LIBS=
DATASERIES_LIBTOOL=
have_dataseries=no
# this ordering of tests causes the build to prefer the version of Dataseries
# installed into the prefix, rather than the one in the user's path
save_exec_prefix=$exec_prefix
if test $exec_prefix = NONE; then
    exec_prefix=$prefix
fi
DATASERIES_CONFIG=`eval echo $bindir`/dataseries-config
exec_prefix=$save_exec_prefix
DATASERIES_VERSION=`$DATASERIES_CONFIG --version 2>/dev/null`
if test "$DATASERIES_VERSION" = ""; then
    DATASERIES_CONFIG=dataseries-config
fi
DATASERIES_VERSION=`$DATASERIES_CONFIG --version 2>/dev/null`

if test "$DATASERIES_VERSION" = ""; then
    AC_MSG_RESULT(failed)
else
    AC_MSG_RESULT(success)
    DATASERIES_CFLAGS=`$DATASERIES_CONFIG --cflags`
    DATASERIES_LIBTOOL=`$DATASERIES_CONFIG --libtool-libs`
    DATASERIES_LIBS=`$DATASERIES_CONFIG --libs`
    save_dataseries_cppflags=$CPPFLAGS
    CPPFLAGS="$CPPFLAGS $DATASERIES_CFLAGS"
    AC_CHECK_HEADER(Extent.H,have_dataseries=yes,have_dataseries=no)
    CPPFLAGS=$save_dataseries_cppflags
    if test $have_dataseries = yes; then
        have_dataseries=no
        save_dataseries_libs=$LIBS
        LIBS="$DATASERIES_LIBS $LINTEL_NOGCLIBS $LIBS"
        AC_CHECK_LIB(DataSeries,dataseriesVersion,have_dataseries=yes,have_dataseries=no,,$LINTEL_LIB)
        LIBS=$save_dataseries_libs
    fi
    AC_MSG_CHECKING(for Dataseries)
    if test $have_dataseries = no; then
        AC_MSG_RESULT(failed)
        AC_MSG_NOTICE(found dataseries-config as $DATASERIES_CONFIG.)
        AC_MSG_NOTICE(cflags: $DATASERIES_CFLAGS)
        AC_MSG_NOTICE(lib: $DATASERIES_LIBS)
        AC_MSG_FAILURE(but couldn't get compilation to work; this is broken)
        exit 1
    fi
    AC_MSG_RESULT(success -- version $DATASERIES_VERSION)
fi

if test "$have_dataseries" = 'yes'; then
    :
else
    AC_MSG_FAILURE(Couldn't find a working version of Dataseries, aborting)
    exit 1
fi

AC_SUBST(DATASERIES_CFLAGS)
AC_SUBST(DATASERIES_LIBTOOL)
AC_SUBST(DATASERIES_LIBS)
])

AC_DEFUN([HPL_WITHLIB_SRT],
[
AC_ARG_WITH(srt,
  [  --without-srt           disable SRT support],
  [with_srt=$withval],
  [with_srt='yes'])
AC_LANG_ASSERT(C++)
have_srt_hdr=no
have_srt_lib=no
SRT_CFLAGS=
SRT_LIBS=
SRT_LIBTOOL=
if test ! "$with_srt" = 'no'; then
    save_srt_cppflags=$CPPFLAGS
    save_srt_libs=$LIBS
    CPPFLAGS="$CPPFLAGS $LINTEL_CFLAGS"
    AC_CHECK_HEADER(SRTTrace.H,have_srt_hdr=yes,)
    if test $have_srt_hdr = no; then
    	AC_CHECK_HEADER($prefix/include/SRTTrace.H,have_srt_hdr=yes;SRT_CFLAGS="-I$prefix/include",)
    fi
    CPPFLAGS=$save_srt_cppflags
    # important to use different functions for the library checks or the check
    # will be incorrectly cached.
    AC_CHECK_LIB(SRTlite,srtVersion,have_srt_lib=yes;SRT_LIBS="-lSRTlite -lz -lbz2",,$LINTEL_LIBS -lz -lbz2)
    if test $have_srt_lib = no; then
    	srt_libs_save=$LIBS
    	LIBS="-L$prefix/lib $LIBS"
    	AC_CHECK_LIB(SRTlite,srtVersionCheck2,have_srt_lib=yes;SRT_LIBS="-L$prefix/lib -lSRTlite -lz -lbz2",,$LINTEL_LIBS -lz -lbz2)
    	LIBS=$srt_libs_save
    fi
    
    if test $have_srt_hdr = no -o $have_srt_lib = no; then
    	echo "*** WARNING: skipping building of SRT accessing tools"
    	SRT_CFLAGS=
    	SRT_LIBS=
    	with_srt=no
    else
    	with_srt=yes
	SRT_LIBTOOL=$SRT_LIBS
    fi
fi

AC_SUBST(SRT_CFLAGS)
AC_SUBST(SRT_LIBS)
AM_CONDITIONAL(WITH_SRT, test $with_srt = yes)
])


AC_DEFUN([HPL_WITHLIB_LZF],
[
AC_ARG_WITH(lzf,
  [  --without-lzf           disable LZF compression support],
  [with_lzf=$withval],
  [with_lzf='yes'])

LZF_LIBS=''
if test "$with_lzf" = yes; then
    have_lzf_hdr=no
    have_lzf_lib=no
    AC_LANG_ASSERT(C)
    AC_CHECK_HEADER(lzf.h,have_lzf_hdr=yes,)
    AC_CHECK_LIB(lzf,lzf_compress,have_lzf_lib=yes;LZF_LIBS=-llzf)
    
    if test $have_lzf_hdr = yes -a $have_lzf_lib = yes; then
    	with_lzf=yes
    else
        with_lzf=no
    	LZF_LIBS=''
    fi
fi
AC_SUBST(LZF_LIBS)
AM_CONDITIONAL(WITH_LZF, test $with_lzf = yes)
])


AC_DEFUN([HPL_WITHLIB_LZO],
[
AC_ARG_WITH(lzo,
  [  --without-lzo           disable LZO compression support],
  [with_lzo=$withval],
  [with_lzo='yes'])

LZO_LIBS=''
if test "$with_lzo" = yes; then
    have_lzo_hdr=no
    have_lzo_lib=no
    AC_LANG_ASSERT(C)
    AC_CHECK_HEADER(lzo1x.h,have_lzo_hdr=yes,)
    AC_CHECK_LIB(lzo,lzo1x_999_compress_level,have_lzo_lib=yes;LZO_LIBS=-llzo)

    if test $have_lzo_hdr = yes -a $have_lzo_lib = yes; then
    	with_lzo=yes
    else
        with_lzo=no
    	LZO_LIBS=''
    fi
fi
AC_SUBST(LZO_LIBS)
AM_CONDITIONAL(WITH_LZO, test $with_lzo = yes)
])


AC_DEFUN([HPL_WITHLIB_ZLIB],
[
AC_ARG_WITH(zlib,
  [  --without-zlib           disable ZLIB compression support],
  [with_zlib=$withval],
  [with_zlib='yes'])

ZLIB_LIBS=''
if test "$with_zlib" = yes; then
    have_zlib_hdr=no
    have_zlib_lib=no
    AC_LANG_ASSERT(C)
    AC_CHECK_HEADER(zlib.h,have_zlib_hdr=yes,)
    AC_CHECK_LIB(z,compress2,have_zlib_lib=yes;ZLIB_LIBS=-lz)
    
    if test $have_zlib_hdr = yes -a $have_zlib_lib = yes; then
    	with_zlib=yes
    else
    	with_zlib=no
    	ZLIB_LIBS=''
    fi
fi

AC_SUBST(ZLIB_LIBS)
AM_CONDITIONAL(WITH_ZLIB, test $with_zlib = yes)
])


AC_DEFUN([HPL_WITHLIB_BZ2],
[
AC_ARG_WITH(bz2,
  [  --without-bz2           disable BZ2 compression support],
  [with_bz2=$withval],
  [with_bz2='yes'])

BZ2_LIBS=''
if test "$with_bz2" = yes; then
    have_bz2_hdr=no
    have_bz2_lib=no
    AC_LANG_ASSERT(C)
    AC_CHECK_HEADER(bzlib.h,have_bz2_hdr=yes,)
    AC_CHECK_LIB(bz2,BZ2_bzBuffToBuffCompress,have_bz2_lib=yes;BZ2_LIBS=-lbz2)
    
    if test $have_bz2_hdr = yes -a $have_bz2_lib = yes; then
    	with_bz2=yes
    else
        with_bz2=no
	BZ2_LIBS=''
    fi
fi

AC_SUBST(BZ2_LIBS)
AM_CONDITIONAL(WITH_BZ2, test $with_bz2 = yes)
])

