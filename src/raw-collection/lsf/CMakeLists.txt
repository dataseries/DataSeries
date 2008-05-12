#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/raw-collection/lsf directory

# Doesn't actually depend on it, but if you're not building bacct2ds,
# then it's probably not useful.

IF(PCRE_ENABLED)
    LINTEL_INSTALL_CONFIG_PROGRAM(lsflog-timesplit)
ENDIF(PCRE_ENABLED)


