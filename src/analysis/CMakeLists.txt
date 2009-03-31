#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/analysis directory

DATASERIES_PROGRAM(wcanalysis)
DATASERIES_PROGRAM(lsfdsanalysis 
	lsfdsanalysis-common.cpp lsfdsanalysis-mod1.cpp)
DATASERIES_PROGRAM(ipdsanalysis ../process/sourcebyrange.cpp)

IF(PERL_CRYPT_RIJNDAEL_ENABLED)
    LINTEL_INSTALL_CONFIG_PROGRAM(dsdecrypt)
ENDIF(PERL_CRYPT_RIJNDAEL_ENABLED)

IF(PERL_DATE_PARSE_ENABLED)
    LINTEL_INSTALL_CONFIG_PROGRAM(lsfdsplots)
ENDIF(PERL_DATE_PARSE_ENABLED)

DATASERIES_PROGRAM(fgrepanalysis)
DATASERIES_PROGRAM(simplefgrepanalysis)

# example of linking a module to a library
# FIND_LIBRARY(PCRE_LIBRARY NAMES pcre PATHS /usr/lib /usr/local/lib)
# TARGET_LINK_LIBRARIES(simplefgrepanalysis ${PCRE_LIBRARY})

ADD_SUBDIRECTORY(nfs)

