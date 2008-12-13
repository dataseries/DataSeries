#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/example directory

DATASERIES_PROGRAM_NOINST(merge-example)
DATASERIES_PROGRAM_NOINST(writing_a_file)
DATASERIES_PROGRAM_NOINST(processing)
DATASERIES_PROGRAM_NOINST(running_multiple_modules)
# TODO-eric: fix the next two bits
TARGET_LINK_LIBRARIES(writing_a_file /cygdrive/c/boost/trunk/stage/lib/libboost_regex.dll.a)
TARGET_LINK_LIBRARIES(running_multiple_modules /cygdrive/c/boost/trunk/stage/lib/libboost_program_options.dll.a)
