#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/mapreduce directory

DATASERIES_PROGRAM(mapreducedemo Record.cpp Mapper.cpp Reducer.cpp)
DATASERIES_PROGRAM(mapreducetest)
