#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/perl-modules directory

SET(PERL_MODULE_FILES)

IF(PERL_CRYPT_RIJNDAEL_ENABLED)
    LIST(APPEND PERL_MODULE_FILES DataSeries/Crypt.pm)
    IF(PERL_DATE_PARSE_ENABLED)
        LIST(APPEND PERL_MODULE_FILES
    	    DataSeries/LSFcommon.pm
    	    DataSeries/LSFPlots/AllCpu.pm
    	    DataSeries/LSFPlots/Default.pm
    	    DataSeries/LSFPlots/Productions.pm 
        )
    ENDIF(PERL_DATE_PARSE_ENABLED)
ENDIF(PERL_CRYPT_RIJNDAEL_ENABLED)

LINTEL_INSTALL_FILE_PATH(${CMAKE_INSTALL_PREFIX}/share/perl5
	${PERL_MODULE_FILES})
