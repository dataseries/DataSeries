#
#  (c) Copyright 2005, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# automake input file

if WITH_LZF
DATASERIES_ENABLE_LZF=1
else
DATASERIES_ENABLE_LZF=0
endif

if WITH_LZO
DATASERIES_ENABLE_LZO=1
else
DATASERIES_ENABLE_LZO=0
endif

if WITH_ZLIB
DATASERIES_ENABLE_ZLIB=1
else
DATASERIES_ENABLE_ZLIB=0
endif

if WITH_BZ2
DATASERIES_ENABLE_BZ2=1
else
DATASERIES_ENABLE_BZ2=0
endif

bpmodulesdir = $(datadir)/bp_modules
nobase_bpmodules_DATA = BatchParallel/ds2txt.pm

AM_CPPFLAGS = -I$(top_srcdir)/include/DataSeries @SRT_CFLAGS@ @LINTEL_CFLAGS@ @XML2_CFLAGS@ -DDATASERIES_ENABLE_LZF=$(DATASERIES_ENABLE_LZF) -DDATASERIES_ENABLE_LZO=$(DATASERIES_ENABLE_LZO) -DDATASERIES_ENABLE_ZLIB=$(DATASERIES_ENABLE_ZLIB) -DDATASERIES_ENABLE_BZ2=$(DATASERIES_ENABLE_BZ2)
AM_LDFLAGS = @OPTMODE_LDFLAGS@

lib_LTLIBRARIES = libDataSeries.la
bin_SCRIPTS = dataseries-config

libDataSeries_la_LDFLAGS = -version-info @LIBDATASERIES_VERSION@ 
libDataSeries_la_LIBADD = @LINTEL_LIBTOOL@ @XML2_LIBTOOL@ @LZF_LIBS@ @LZO_LIBS@ @ZLIB_LIBS@ @BZ2_LIBS@

libDataSeries_la_SOURCES = \
	base/DataSeriesFile.C \
	base/Extent.C \
	base/ExtentField.C \
	base/ExtentSeries.C \
	base/ExtentType.C \
	base/GeneralField.C \
	process/commonargs.C \
	module/DStoTextModule.C \
	module/DataSeriesModule.C \
	module/IndexSourceModule.C \
	module/MinMaxIndexModule.C \
	module/PrefetchBufferModule.C \
	module/RowAnalysisModule.C \
	module/SequenceModule.C \
	module/TypeIndexModule.C

bin_PROGRAMS = ds2txt pssimple2ds dsextentindex lvpvprocessusage textindex

LDADD = $(top_builddir)/src/libDataSeries.la @LINTEL_NOGCLIBS@
ds2txt_SOURCES = process/ds2txt.C
pssimple2ds_SOURCES = process/pssimple2ds.C
dsextentindex_SOURCES = process/dsextentindex.C
lvpvprocessusage_SOURCES = process/lvpvprocessusage.C
textindex_SOURCES = process/textindex.C

CHECK_TARGETS = ran.check-pssimple ran.check-test

if WITH_SRT
bin_PROGRAMS += srt2dataseries cmpsrtds

srt2dataseries_SOURCES = process/srt2dataseries.C
srt2dataseries_LDADD = @SRT_LIBS@ $(top_builddir)/src/libDataSeries.la 

cmpsrtds_SOURCES = process/cmpsrtds.C
cmpsrtds_LDADD = @SRT_LIBS@ $(top_builddir)/src/libDataSeries.la 

CHECK_TARGETS += ran.check-srt2dataseries
endif

check_PROGRAMS = test
test_LDADD =  $(top_builddir)/src/libDataSeries.la @LINTEL_NOGCLIBS@ -lssl -lcrypto

check-local: $(CHECK_TARGETS)

# can't do a simple dataseries file compare because we put random data 
# into the extentseries at the end.

ran.check-srt2dataseries: srt2dataseries cmpsrtds
	./srt2dataseries $(top_srcdir)/check-data/hourly.03126.srt.bz2 h03126.ds 
	./cmpsrtds $(top_srcdir)/check-data/hourly.03126.srt.bz2 h03126.ds 
	./cmpsrtds $(top_srcdir)/check-data/hourly.03126.srt.bz2 $(top_srcdir)/check-data/h03126.ds-littleend 
	./cmpsrtds $(top_srcdir)/check-data/hourly.03126.srt.bz2 $(top_srcdir)/check-data/h03126.ds-bigend 
	rm h03126.ds
	touch $@

# the index may change as data will compress differently on big/little
# endian machines, so we can't compare with it in the text output
ran.check-pssimple: pssimple2ds ds2txt
	./pssimple2ds $(top_srcdir)/check-data/pssimple.5.bz2 pss5.ds
	./ds2txt --skip-index pss5.ds >pss5.txt
	./ds2txt --skip-index $(top_srcdir)/check-data/pss5.ds-littleend >pss5-little.txt
	./ds2txt --skip-index $(top_srcdir)/check-data/pss5.ds-bigend >pss5-big.txt
	cmp pss5.txt pss5-little.txt
	cmp pss5.txt pss5-big.txt
	rm pss5.txt pss5-little.txt pss5-big.txt pss5.ds
	touch $@

ran.check-test: test ds2txt
	./test
	./ds2txt --skip-index test.ds >test.txt
	./ds2txt --skip-index $(top_srcdir)/check-data/complex.ds-littleend >complex-little.txt
	./ds2txt --skip-index $(top_srcdir)/check-data/complex.ds-bigend >complex-big.txt
	cmp $(top_srcdir)/check-data/complex.txt test.txt
	cmp $(top_srcdir)/check-data/complex.txt complex-little.txt
	cmp $(top_srcdir)/check-data/complex.txt complex-big.txt
	rm test.ds test.txt complex-little.txt complex-big.txt
	touch $@
