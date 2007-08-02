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

if WITH_CRYPTO
WITH_CRYPTO=1
else
WITH_CRYPTO=0
endif

bpmodulesdir = $(datadir)/bp_modules
nobase_bpmodules_DATA = BatchParallel/ds2txt.pm

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/src/process -I$(top_srcdir)/src/liblzf-1.6 @SRT_CFLAGS@ @LINTEL_CFLAGS@ @XML2_CFLAGS@ -DDATASERIES_ENABLE_LZF=$(DATASERIES_ENABLE_LZF) -DDATASERIES_ENABLE_LZO=$(DATASERIES_ENABLE_LZO) -DDATASERIES_ENABLE_ZLIB=$(DATASERIES_ENABLE_ZLIB) -DDATASERIES_ENABLE_BZ2=$(DATASERIES_ENABLE_BZ2) -DWITH_CRYPTO=$(WITH_CRYPTO)
AM_LDFLAGS = @OPTMODE_LDFLAGS@

lib_LTLIBRARIES = libDataSeries.la
bin_SCRIPTS = dataseries-config

libDataSeries_la_LDFLAGS = -version-info @LIBDATASERIES_VERSION@ 
libDataSeries_la_LIBADD = @LINTEL_LIBTOOL@ @LINTELPTHREAD_LIBTOOL@ @XML2_LIBTOOL@ @LZO_LIBS@ @ZLIB_LIBS@ @BZ2_LIBS@ -lcrypto

libDataSeries_la_SOURCES = \
	base/DataSeriesFile.C \
	base/Extent.C \
	base/ExtentField.C \
	base/ExtentSeries.C \
	base/ExtentType.C \
	base/GeneralField.C \
	process/commonargs.C \
	module/DSStatGroupBy.C \
	module/DStoTextModule.C \
	module/DataSeriesModule.C \
	module/IndexSourceModule.C \
	module/MinMaxIndexModule.C \
	module/PrefetchBufferModule.C \
	module/RowAnalysisModule.C \
	module/SequenceModule.C \
	module/TypeIndexModule.C \
	module/cryptutil.C  \
	lex.DSStatGroupBy_yy.c \
	liblzf-1.6/lzf_c.c \
	liblzf-1.6/lzf_d.c 

BUILT_SOURCES = lex.DSStatGroupBy_yy.c

bin_PROGRAMS = ds2txt pssimple2ds dsextentindex lvpvprocessusage textindex \
	ipdsanalysis indexnfscommon dsselect dsrepack dsstatgroupby 

LDADD = libDataSeries.la 
ds2txt_SOURCES = process/ds2txt.C
pssimple2ds_SOURCES = process/pssimple2ds.C
dsextentindex_SOURCES = process/dsextentindex.C
lvpvprocessusage_SOURCES = process/lvpvprocessusage.C
textindex_SOURCES = process/textindex.C
ipdsanalysis_SOURCES = process/ipdsanalysis.C process/sourcebyrange.C # process/nfsdsanalysis-common.C 
indexnfscommon_SOURCES = process/indexnfscommon.C
dsselect_SOURCES = process/dsselect.C
dsrepack_SOURCES = process/dsrepack.C
dsstatgroupby_SOURCES = process/dsstatgroupby.C

##### WITH_PCAP
if WITH_PCAP
bin_PROGRAMS += network-driverdump 
network_driverdump_SOURCES = raw-collection/network-driverdump/driverdump.C

bin_PROGRAMS += nettrace2ds 

nettrace2ds_SOURCES = process/nettrace2ds.C

nettrace2ds_LDADD = libDataSeries.la -lpcap
nobase_bpmodules_DATA += BatchParallel/nettrace2ds.pm 
endif

CHECK_TARGETS = ran.check-pssimple ran.check-test ran.check-dsselect

##### WITH_SRT
if WITH_SRT
bin_PROGRAMS += srt2ds cmpsrtds

srt2ds_SOURCES = process/srt2ds.C
srt2ds_LDADD = @SRT_LIBS@ libDataSeries.la  

cmpsrtds_SOURCES = process/cmpsrtds.C
cmpsrtds_LDADD = @SRT_LIBS@ libDataSeries.la 

CHECK_TARGETS += ran.check-srt2ds

nobase_bpmodules_DATA += BatchParallel/srt2ds.pm
endif

##### WITH_PCRE
if WITH_PCRE
bin_PROGRAMS += bacct2ds
bacct2ds_SOURCES = process/bacct2ds.C
bacct2ds_LDADD = @PCRE_LIBS@ libDataSeries.la

nobase_bpmodules_DATA += BatchParallel/bacct2ds.pm
endif

##### BUILT Sources

lex.DSStatGroupBy_yy.c: module/DSStatGroupByLex.l
	flex $(top_srcdir)/src/module/DSStatGroupByLex.l

##### Testing...

check_PROGRAMS = test
test_SOURCES = test.C 
test_LDADD =  libDataSeries.la 

check-local: $(CHECK_TARGETS)

# can't do a simple dataseries file compare because we put random data 
# into the extentseries at the end.

ran.check-srt2ds: srt2ds cmpsrtds
	./srt2ds $(top_srcdir)/check-data/hourly.03126.srt.bz2 h03126.ds 
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

ran.check-dsselect: dsselect ds2txt
	@-rm dsselect.test.1
	./dsselect --compress-gz I/O enter_driver,bytes,machine_id,driver_type,disk_offset ~/projects/DataSeries/check-data/h03126.ds-littleend dsselect.test.1 >/dev/null
	gunzip < $(top_srcdir)/check-data/dsselect.test.1.gz >dsselect.test.1.ref
	./ds2txt --skip-index dsselect.test.1 >dsselect.test.1.txt
	cmp dsselect.test.1.ref dsselect.test.1.txt
	touch $@


if ENABLE_PAPER
bin_PROGRAMS += srtstat srtcsvstat

srtstat_SOURCES = paper/srtstat.C
srtstat_LDADD = libDataSeries.la

srtcsvstat_SOURCES = paper/srtcsvstat.C
srtcsvstat_LDADD = @LINTEL_LIBTOOL@
endif

if ENABLE_EXAMPLE
bin_PROGRAMS += merge-example

merge_example_SOURCES = example/merge-example.cpp
endif
