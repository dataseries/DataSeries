#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# Utility macros for DataSeries

INCLUDE(LintelDocs)

MACRO(DATASERIES_PROGRAM_NOINST program_name)
    ADD_EXECUTABLE(${program_name} ${program_name}.cpp ${ARGN})
    ADD_DEPENDENCIES(${program_name} DataSeries)
    TARGET_LINK_LIBRARIES(${program_name} DataSeries ${LINTEL_LIBRARIES})
ENDMACRO(DATASERIES_PROGRAM_NOINST)

# Generates a dataseries program, pass in the name of the executable; the assumption is that
# the main source file has the same name.  You can pass additional source files after the
# program name.
MACRO(DATASERIES_PROGRAM program_name)
    ADD_EXECUTABLE(${program_name} ${program_name}.cpp ${ARGN})
    ADD_DEPENDENCIES(${program_name} DataSeries)
    TARGET_LINK_LIBRARIES(${program_name} DataSeries)
    INSTALL(TARGETS ${program_name} DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
    IF(DEFINED DATASERIES_POD2MAN_RELEASE)
        LINTEL_POD2MAN(${program_name}.cpp 1 ${DATASERIES_POD2MAN_RELEASE} "-" ${program_name}.1)
    ENDIF(DEFINED DATASERIES_POD2MAN_RELEASE)
ENDMACRO(DATASERIES_PROGRAM)

MACRO(DATASERIES_INSTALL_CONFIG_PROGRAM program_name)
    LINTEL_INSTALL_CONFIG_PROGRAM(${program_name})
    IF(DEFINED DATASERIES_POD2MAN_RELEASE)
        LINTEL_POD2MAN(${program_name}.in 1 ${DATASERIES_POD2MAN_RELEASE} "-" ${program_name}.1)
    ENDIF(DEFINED DATASERIES_POD2MAN_RELEASE)
ENDMACRO(DATASERIES_INSTALL_CONFIG_PROGRAM)
