#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/process directory

MACRO(DATASERIES_SIMPLE_TEST testname)
  DATASERIES_PROGRAM_NOINST(${testname})
  ADD_TEST(${testname} ./${testname} ${ARGN})
ENDMACRO(DATASERIES_SIMPLE_TEST)

MACRO(DATASERIES_SCRIPT_TEST testname)
  ADD_TEST(${testname} ${CMAKE_CURRENT_SOURCE_DIR}/run-check-${testname}.sh ${DataSeries_SOURCE_DIR} ${ARGN})
ENDMACRO(DATASERIES_SCRIPT_TEST)

### Simple exit(0) program tests
DATASERIES_SIMPLE_TEST(time-field)
DATASERIES_SIMPLE_TEST(pack-pad-record)
DATASERIES_SIMPLE_TEST(pack-field-ordering)

### Script tests of public programs
# *** WARNING, don't use | in any of the test scripts; if you do, then
# *** the error conditions are not checked properly.

DATASERIES_SCRIPT_TEST(dsselect)
DATASERIES_SCRIPT_TEST(dsextentindex BZ2-${BZIP2_ENABLED})
DATASERIES_SCRIPT_TEST(dsstatgroupby)
DATASERIES_SCRIPT_TEST(lsfdsanalysis)
DATASERIES_SCRIPT_TEST(nfsdsanalysis)
DATASERIES_SCRIPT_TEST(ellard)
DATASERIES_SCRIPT_TEST(worldcup)
DATASERIES_SCRIPT_TEST(ds2txt)
DATASERIES_SCRIPT_TEST(ipnfscrosscheck)

### Script tests of testing-only programs
DATASERIES_PROGRAM_NOINST(general)
DATASERIES_SCRIPT_TEST(general LZO-${LZO_ENABLED})

DATASERIES_PROGRAM_NOINST(expr)
DATASERIES_SCRIPT_TEST(expr)

### Conditional tests...

IF(LZO_ENABLED)
    # TODO: repack the nfs*.ip.ds files with only gz and lzf
    DATASERIES_SCRIPT_TEST(ipdsanalysis)
ENDIF(LZO_ENABLED)

IF(BUNZIP2_PROGRAM_ENABLED)
    DATASERIES_SCRIPT_TEST(pssimple)
ENDIF(BUNZIP2_PROGRAM_ENABLED)

IF(PERL_XML_PARSER_ENABLED)
    DATASERIES_SCRIPT_TEST(dstypes2cxx)
ENDIF(PERL_XML_PARSER_ENABLED)

IF(PERL_DATE_PARSE_ENABLED AND PERL_CRYPT_RIJNDAEL_ENABLED)
    DATASERIES_SCRIPT_TEST(lsfdsplots)
ENDIF(PERL_DATE_PARSE_ENABLED AND PERL_CRYPT_RIJNDAEL_ENABLED)

IF(SRT_ENABLED)
    DATASERIES_SCRIPT_TEST(srt)
ENDIF(SRT_ENABLED)

