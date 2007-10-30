#
#  (c) Copyright 2005-2007, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# automake input file

SUBDIRS = perl-modules

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
nobase_bpmodules_DATA = BatchParallel/ds2txt.pm BatchParallel/dsrepack.pm BatchParallel/nonmod/gpgkeys.pm

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/src -I$(top_srcdir)/src/liblzf-1.6 @SRT_CFLAGS@ @LINTEL_CFLAGS@ @XML2_CFLAGS@ @PCRE_CFLAGS@ -DDATASERIES_ENABLE_LZF=$(DATASERIES_ENABLE_LZF) -DDATASERIES_ENABLE_LZO=$(DATASERIES_ENABLE_LZO) -DDATASERIES_ENABLE_ZLIB=$(DATASERIES_ENABLE_ZLIB) -DDATASERIES_ENABLE_BZ2=$(DATASERIES_ENABLE_BZ2) 
AM_LDFLAGS = @OPTMODE_LDFLAGS@ @DMALLOC_THR_LIBS@

lib_LTLIBRARIES = libDataSeries.la
bin_SCRIPTS = dataseries-config dstypes2cxx

libDataSeries_la_LDFLAGS = -version-info @LIBDATASERIES_VERSION@ 
libDataSeries_la_LIBADD = @LINTEL_LIBTOOL@ @LINTELPTHREAD_LIBTOOL@ @XML2_LIBTOOL@ @LZO_LIBS@ @ZLIB_LIBS@ @BZ2_LIBS@ @DMALLOC_LIBS@ @CRYPTO_LIBS@ -lrt 

# TODO transition all the source files to .cpp, includes to .hpp so we
# might be able to build on windows.

libDataSeries_la_SOURCES = \
	base/DataSeriesFile.C \
	base/Extent.C \
	base/ExtentField.C \
	base/ExtentSeries.C \
	base/ExtentType.C \
	base/GeneralField.C \
	process/commonargs.C \
	module/DSExpr.cpp \
	module/DSExprParse.cpp \
	module/DSExprScan.cpp \
	module/DSStatGroupByModule.C \
	module/DStoTextModule.C \
	module/DataSeriesModule.C \
	module/IndexSourceModule.C \
	module/MinMaxIndexModule.C \
	module/PrefetchBufferModule.C \
	module/RowAnalysisModule.C \
	module/SequenceModule.C \
	module/TypeIndexModule.C \
	module/cryptutil.C  \
	liblzf-1.6/lzf_c.c \
	liblzf-1.6/lzf_d.c 

bin_PROGRAMS =

LDADD = libDataSeries.la

# General purpose
bin_PROGRAMS += dsextentindex dsselect dsrepack dsstatgroupby
dsextentindex_SOURCES = process/dsextentindex.C
dsselect_SOURCES = process/dsselect.C
dsrepack_SOURCES = process/dsrepack.C
dsstatgroupby_SOURCES = process/dsstatgroupby.C

# Converters
bin_PROGRAMS += ds2txt pssimple2ds iphost2ds
ds2txt_SOURCES = process/ds2txt.C
pssimple2ds_SOURCES = process/pssimple2ds.C
iphost2ds_SOURCES = process/iphost2ds.C

# Type-specific processing
bin_PROGRAMS += textindex indexnfscommon
textindex_SOURCES = process/textindex.C
indexnfscommon_SOURCES = process/indexnfscommon.C

# Type specific analysis
bin_PROGRAMS += lvpvprocessusage ipdsanalysis lsfdsanalysis nfsdsanalysis
bin_SCRIPTS += analysis/dsdecrypt
lvpvprocessusage_SOURCES = process/lvpvprocessusage.C
ipdsanalysis_SOURCES = process/ipdsanalysis.C process/sourcebyrange.C 
lsfdsanalysis_SOURCES = analysis/lsfdsanalysis.C \
	analysis/lsfdsanalysis-common.C analysis/lsfdsanalysis-mod1.C 
nfsdsanalysis_SOURCES = analysis/nfs/common.cpp analysis/nfs/mod1.cpp \
  analysis/nfs/mod2.cpp analysis/nfs/mod3.cpp analysis/nfs/mod4.cpp \
  analysis/nfs/nfsdsanalysis.cpp process/sourcebyrange.C

CHECK_TARGETS = ran.check-pssimple ran.check-test ran.check-dsselect \
	ran.check-dsextentindex ran.check-dsstatgroupby \
	ran.check-lsfdsanalysis ran.check-dstypes2cxx

### Conditional things

if HAS_DATE_PARSE
bin_SCRIPTS += analysis/lsfdsplots 

# Dunno why autoconf doesn't do this for us, it works only if lsfdsplots
# isn't in a subdir.
analysis/lsfdsplots: $(top_builddir)/config.status $(srcdir)/analysis/lsfdsplots.in
	cd $(top_builddir) && $(SHELL) ./config.status $(subdir)/$@

CHECK_TARGETS += ran.check-lsfdsplots
endif

analysis/dsdecrypt: $(top_builddir)/config.status $(srcdir)/analysis/dsdecrypt.in
	cd $(top_builddir) && $(SHELL) ./config.status $(subdir)/$@


##### WITH_PCAP
if WITH_PCAP
bin_PROGRAMS += network-driverdump 
network_driverdump_SOURCES = raw-collection/network-driverdump/driverdump.C

bin_PROGRAMS += nettrace2ds 

nettrace2ds_SOURCES = process/nettrace2ds.C

nettrace2ds_LDADD = libDataSeries.la -lpcap
nobase_bpmodules_DATA += BatchParallel/nettrace2ds.pm 
endif

##### WITH_SRT
if WITH_SRT
bin_PROGRAMS += srt2ds cmpsrtds 
bin_SCRIPTS += process/dssrt2txt

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
bin_SCRIPTS += raw-collection/lsf/lsflog-timesplit

bacct2ds_SOURCES = process/bacct2ds.C
bacct2ds_LDADD = @PCRE_LIBS@ libDataSeries.la

nobase_bpmodules_DATA += BatchParallel/bacct2ds.pm
endif

rebuild_DSExpr:
	@echo "version check, from debian etch, known to work."
	@echo "was unable to get the flex and bison stuff to work on both"
	@echo "debian etch and RHEL4 with flex 2.5.4 and bison 1.875"
	[ "`flex --version`" = "flex 2.5.33" ]
	[ "`bison --version | head -1`" = "bison (GNU Bison) 2.3" ]
	flex -o $(top_srcdir)/src/module/DSExprScan.cpp $(top_srcdir)/src/module/DSExprScan.ll
	bison -d -o $(top_srcdir)/src/module/DSExprParse.cpp $(top_srcdir)/src/module/DSExprParse.yy
	perl $(top_srcdir)/src/module/expr-parser-fixup.pl $(top_srcdir)/src/module DSExprParse
	rm $(top_srcdir)/src/module/stack.hh $(top_srcdir)/src/module/location.hh $(top_srcdir)/src/module/position.hh

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
	./pssimple2ds --extent-size=10485760 $(top_srcdir)/check-data/pssimple.5.bz2 pss5.ds
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
	./dsselect --compress-gz --extent-size=524288 I/O enter_driver,bytes,machine_id,driver_type,disk_offset ~/projects/DataSeries/check-data/h03126.ds-littleend dsselect.test.1 >/dev/null
	gunzip < $(top_srcdir)/check-data/dsselect.test.1.gz >dsselect.test.1.ref
	./ds2txt --skip-index dsselect.test.1 >dsselect.test.1.txt
	cmp dsselect.test.1.ref dsselect.test.1.txt
	touch $@

ran.check-dsextentindex: dsextentindex ds2txt
	@-rm test.index.1.ds test.index.2.ds
	./dsextentindex --compress-lzf --new I/O enter_driver,machine_id,disk_offset,is_read test.index.1.ds $(top_srcdir)/check-data/h03126.ds-littleend
	./ds2txt --skip-index test.index.1.ds | perl $(top_srcdir)/check-data/index-fixup.pl $(top_srcdir) >test.index.1.ds.txt
	cmp test.index.1.ds.txt $(top_srcdir)/check-data/test.index.1a.ref
	./dsextentindex --compress-lzf test.index.1.ds $(top_srcdir)/check-data/h03126.ds-bigend
	./ds2txt --skip-index test.index.1.ds | perl $(top_srcdir)/check-data/index-fixup.pl >test.index.1.ds.txt
	cmp test.index.1.ds.txt $(top_srcdir)/check-data/test.index.1b.ref
	./dsextentindex --compress-lzf --new Batch::LSF::Grizzly cluster_name,submit_time,end_time test.index.2.ds $(top_srcdir)/check-data/lsb.acct.2007-01-01-p1.ds 
	./ds2txt --skip-index test.index.2.ds | perl $(top_srcdir)/check-data/index-fixup.pl >test.index.2.ds.txt
	cmp test.index.2.ds.txt $(top_srcdir)/check-data/test.index.2.ref
	touch $@

ran.check-dsstatgroupby: dsstatgroupby
	./dsstatgroupby 'I/O' basic '1000*(return_to_driver - leave_driver)' group by 'device_number' basic 'bytes/1024' group by 'machine_id' from ~/projects/DataSeries/check-data/h03126.ds-littleend | perl $(top_srcdir)/check-data/clean-timing.pl >test.dsstatgroupby.1
	cmp test.dsstatgroupby.1 $(top_srcdir)/check-data/test.dsstatgroupby.1.ref
	./dsstatgroupby 'Batch::LSF' basic 'start_time - submit_time' where 'start_time - submit_time > 50000' group by 'production' basic 'cpu_time/(end_time-start_time)' group by production quantile 'start_time - submit_time' where 'start_time - submit_time > 50000' group by production from ~/projects/DataSeries/check-data/lsb.acct.2007-01-01-p1.ds | perl $(top_srcdir)/check-data/clean-timing.pl >test.dsstatgroupby.2
	cmp test.dsstatgroupby.2 $(top_srcdir)/check-data/test.dsstatgroupby.2.ref
	touch $@

ran.check-lsfdsanalysis: lsfdsanalysis
	./lsfdsanalysis -a 60:1167681240:1167681600:all,production: -a 120:1167681240:1167681600:all,sequence: -a 60:1167681240:1167681600:all,production:'start_time > 1167681360' $(top_srcdir)/check-data/lsb.acct.2007-01-01-p1.ds  | perl $(top_srcdir)/check-data/clean-timing.pl >check.lsfdsanalysis.txt
	cmp check.lsfdsanalysis.txt $(top_srcdir)/check-data/check.lsfdsanalysis.ref
	touch $@

ran.check-dstypes2cxx: dstypes2cxx
	perl ./dstypes2cxx --ds2txt=$(top_builddir)/src/ds2txt $(top_srcdir)/check-data/lsb.acct.2007-01-01-p1.ds >check.dstypes2cxx.txt
	perl ./dstypes2cxx -o r --ds2txt=$(top_builddir)/src/ds2txt $(top_srcdir)/check-data/lsb.acct.2007-01-01-p1.ds >>check.dstypes2cxx.txt
	perl ./dstypes2cxx -o d -p 'DataSeries: ExtentIndex'=dsextent --ds2txt=$(top_builddir)/src/ds2txt $(top_srcdir)/check-data/lsb.acct.2007-01-01-p1.ds >>check.dstypes2cxx.txt
	
	cmp check.dstypes2cxx.txt $(top_srcdir)/check-data/check.dstypes2cxx.ref
	touch $@

if HAS_DATE_PARSE
ran.check-lsfdsplots: ran.check-dsextentindex analysis/lsfdsplots
	LINTEL_REGRESSION_TEST_INC_DIR=$(top_srcdir)/src/perl-modules perl ./analysis/lsfdsplots --indexfile=test.index.2.ds --psonly --plotdir=check-lsfdsplots --groups=all --starttime=1167681250 --endtime=1167681547 --lsfdsanalysis=$(top_builddir)/src/lsfdsanalysis
	grep -v '^%%CreationDate:' check-lsfdsplots/all/all.ps >check-lsfdsplots/all/all.ps-nodate
	cmp check-lsfdsplots/all/all.ps-nodate $(top_srcdir)/check-data/lsfdsplots.ps.ref
	touch $@
endif

if ENABLE_PAPER
bin_PROGRAMS += srtstat srtstat2 srtcsvstat

srtstat_SOURCES = paper/srtstat.C
srtstat_LDADD = libDataSeries.la

srtstat2_SOURCES = paper/srtstat2.C
srtstat2_LDADD = libDataSeries.la

srtcsvstat_SOURCES = paper/srtcsvstat.C
srtcsvstat_LDADD = @LINTEL_LIBTOOL@
endif

if ENABLE_EXAMPLE
bin_PROGRAMS += merge-example

merge_example_SOURCES = example/merge-example.cpp
endif
