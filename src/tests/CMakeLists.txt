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
DATASERIES_SIMPLE_TEST(pack-bug ${CMAKE_SOURCE_DIR}/check-data/nfs-2.set-1.20k.ds)
DATASERIES_SIMPLE_TEST(sub-extent-pointer)
DATASERIES_SIMPLE_TEST(shared-bare-pointer)
DATASERIES_PROGRAM_NOINST(general general2.cpp)
ADD_TEST(general ./general)

### Script tests of public programs
# *** WARNING, don't use | in any of the test scripts; if you do, then
# *** the error conditions are not checked properly.

DATASERIES_SCRIPT_TEST(dsextentindex BZ2-${BZIP2_ENABLED})
DATASERIES_SCRIPT_TEST(dsstatgroupby)
DATASERIES_SCRIPT_TEST(lsfdsanalysis)
DATASERIES_SCRIPT_TEST(ellard)
DATASERIES_SCRIPT_TEST(worldcup)
DATASERIES_SCRIPT_TEST(ds2txt)
DATASERIES_SCRIPT_TEST(ipnfscrosscheck)
DATASERIES_SCRIPT_TEST(ipdsanalysis)

### Script tests of testing-only programs
DATASERIES_PROGRAM_NOINST(expr)
DATASERIES_SCRIPT_TEST(expr)

DATASERIES_PROGRAM_NOINST(typefiltermodule)
DATASERIES_SCRIPT_TEST(typefiltermodule)

DATASERIES_PROGRAM_NOINST(extent-write-callback)
DATASERIES_SCRIPT_TEST(extent-write-callback)

DATASERIES_PROGRAM_NOINST(generate-incomplete-ds)
DATASERIES_SCRIPT_TEST(dsrecover)

DATASERIES_PROGRAM_NOINST(file-rotation)
DATASERIES_SCRIPT_TEST(file-rotation)


# TODO; misc test also depends on bzip2
DATASERIES_PROGRAM_NOINST(misc)
DATASERIES_SCRIPT_TEST(misc LZO-${LZO_ENABLED})

### Conditional tests...

IF (LINTEL_program-options_ENABLED)
    DATASERIES_SCRIPT_TEST(dsselect)
    DATASERIES_SCRIPT_TEST(csv2ds)
ENDIF (LINTEL_program-options_ENABLED)

IF(BZIP2_ENABLED)
    DATASERIES_SCRIPT_TEST(pssimple)
ENDIF(BZIP2_ENABLED)

IF(PERL_XML_PARSER_ENABLED)
    DATASERIES_SCRIPT_TEST(dstypes2cxx)
ENDIF(PERL_XML_PARSER_ENABLED)

IF(LSFDSPLOTS_ENABLED)
    DATASERIES_SCRIPT_TEST(lsfdsplots ${CMAKE_INSTALL_PREFIX})
ENDIF(LSFDSPLOTS_ENABLED)

IF(SRT_ENABLED)
    DATASERIES_SCRIPT_TEST(srt)
ENDIF(SRT_ENABLED)

IF(BOOST_FOREACH_ENABLED)
    DATASERIES_PROGRAM_NOINST(sortedindex)
    DATASERIES_SCRIPT_TEST(sortedindex)
ENDIF(BOOST_FOREACH_ENABLED)

IF(CRYPTO_ENABLED)
    DATASERIES_SCRIPT_TEST(nfsdsanalysis)
ENDIF(CRYPTO_ENABLED)
### Long tests

DATASERIES_SIMPLE_TEST(byteflip)
