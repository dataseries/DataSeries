#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/BatchParallel directory

SET(BATCH_PARALLEL_FILES
	ds2txt.pm
	dsrepack.pm
	ellardnfs2ds.pm
	nonmod/gpgkeys.pm
)

IF(SRT_ENABLED)
    LIST(APPEND BATCH_PARALLEL_FILES srt2ds.pm)
ENDIF(SRT_ENABLED)

IF(PCRE_ENABLED)
    LIST(APPEND BATCH_PARALLEL_FILES bacct2ds.pm)
ENDIF(PCRE_ENABLED)

IF(PCAP_ENABLED AND PERL_FILESYS_STATVFS_ENABLED)
    LIST(APPEND BATCH_PARALLEL_FILES nettrace2ds.pm)
ENDIF(PCAP_ENABLED AND PERL_FILESYS_STATVFS_ENABLED)

LINTEL_INSTALL_FILE_PATH(${CMAKE_INSTALL_PREFIX}/share/bp_modules/BatchParallel
	${BATCH_PARALLEL_FILES})
