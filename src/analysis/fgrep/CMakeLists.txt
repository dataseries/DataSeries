#
# (c) Copyright 2009, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/analysis/fgrep directory


DATASERIES_PROGRAM(fgrepanalysis BoyerMooreHorspool.cpp)

# example of linking a module to a library
# FIND_LIBRARY(PCRE_LIBRARY NAMES pcre PATHS /usr/lib /usr/local/lib)
# TARGET_LINK_LIBRARIES(fgrepanalysis ${PCRE_LIBRARY})
