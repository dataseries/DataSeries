#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/example directory

DATASERIES_PROGRAM_NOINST(merge-example)
LINTEL_BOOST_EXTRA(BOOST_REGEX boost/regex.hpp boost_regex)
IF (BOOST_REGEX_ENABLED AND BOOST_FOREACH_ENABLED)
    DATASERIES_PROGRAM_NOINST(writing_a_file)
    TARGET_LINK_LIBRARIES(writing_a_file ${BOOST_REGEX_LIBRARIES})
ENDIF (BOOST_REGEX_ENABLED AND BOOST_FOREACH_ENABLED)

DATASERIES_PROGRAM_NOINST(processing)
DATASERIES_PROGRAM_NOINST(running_multiple_modules)
