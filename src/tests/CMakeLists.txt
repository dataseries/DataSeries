#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/process directory

MACRO(DATASERIES_SIMPLE_TEST testname)
  DATASERIES_PROGRAM_NOINST(${testname})
  ADD_TEST(${testname} ./${testname})
ENDMACRO(DATASERIES_SIMPLE_TEST)

MACRO(DATASERIES_SCRIPT_TEST testname)
  ADD_TEST(${testname} ${CMAKE_CURRENT_SOURCE_DIR}/run-check-${testname}.sh ${DataSeries_SOURCE_DIR})
ENDMACRO(DATASERIES_SCRIPT_TEST)

DATASERIES_SIMPLE_TEST(time-field)
DATASERIES_SIMPLE_TEST(pack-pad-record)
DATASERIES_SIMPLE_TEST(pack-field-ordering)

# *** WARNING, don't use | in any of the test scripts; if you do, then
# *** the error conditions are not checked properly.

DATASERIES_SCRIPT_TEST(pssimple)

DATASERIES_PROGRAM_NOINST(general)
DATASERIES_SCRIPT_TEST(general)

DATASERIES_SCRIPT_TEST(dsselect)
DATASERIES_SCRIPT_TEST(dsextentindex)
DATASERIES_SCRIPT_TEST(dsstatgroupby)
DATASERIES_SCRIPT_TEST(lsfdsanalysis)
DATASERIES_SCRIPT_TEST(nfsdsanalysis)
DATASERIES_SCRIPT_TEST(ellard)
DATASERIES_SCRIPT_TEST(worldcup)
DATASERIES_SCRIPT_TEST(ds2txt)

IF(PERL_XML_PARSER_ENABLED)
    DATASERIES_SCRIPT_TEST(dstypes2cxx)
ENDIF(PERL_XML_PARSER_ENABLED)

IF(PERL_DATE_PARSE_ENABLED AND PERL_CRYPT_RIJNDAEL_ENABLED)
    DATASERIES_SCRIPT_TEST(lsfdsplots)
ENDIF(PERL_DATE_PARSE_ENABLED AND PERL_CRYPT_RIJNDAEL_ENABLED)

IF(SRT_ENABLED)
    DATASERIES_SCRIPT_TEST(srt)
ENDIF(SRT_ENABLED)



