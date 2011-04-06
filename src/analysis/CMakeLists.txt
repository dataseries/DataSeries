#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/analysis directory

DATASERIES_PROGRAM(wcanalysis)
DATASERIES_PROGRAM(lsfdsanalysis lsfdsanalysis-common.cpp lsfdsanalysis-mod1.cpp)
DATASERIES_PROGRAM(ipdsanalysis ../process/sourcebyrange.cpp)
DATASERIES_PROGRAM(lvpvprocessusage)

IF(PERL_CRYPT_RIJNDAEL_ENABLED)
    LINTEL_INSTALL_CONFIG_PROGRAM(dsdecrypt)
ENDIF(PERL_CRYPT_RIJNDAEL_ENABLED)

IF(PERL_DATE_PARSE_ENABLED)
    LINTEL_INSTALL_CONFIG_PROGRAM(lsfdsplots)
ENDIF(PERL_DATE_PARSE_ENABLED)

ADD_SUBDIRECTORY(nfs)

