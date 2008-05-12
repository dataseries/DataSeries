#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# Utility macros for DataSeries

MACRO(DATASERIES_PROGRAM_NOINST program_name)
    ADD_EXECUTABLE(${program_name} ${program_name}.cpp ${ARGN})
    ADD_DEPENDENCIES(${program_name} DataSeries)
    TARGET_LINK_LIBRARIES(${program_name} DataSeries)
ENDMACRO(DATASERIES_PROGRAM_NOINST)

MACRO(DATASERIES_PROGRAM program_name)
    ADD_EXECUTABLE(${program_name} ${program_name}.cpp ${ARGN})
    ADD_DEPENDENCIES(${program_name} DataSeries)
    TARGET_LINK_LIBRARIES(${program_name} DataSeries)
    INSTALL(TARGETS ${program_name} DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
ENDMACRO(DATASERIES_PROGRAM)

