#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# Main cmake control file

# ELSEIF came with 2.4.4
CMAKE_MINIMUM_REQUIRED(VERSION 2.4.4 FATAL_ERROR)

# The below comments refer to a feature change in CMake version 2.6,
# which is explained in the first example at 
# cmake.org/Wiki/CMake/Policies#Fixing_an_Interface_Breaks_Work-Arounds
IF(COMMAND CMAKE_POLICY)
    CMAKE_POLICY(SET CMP0003 NEW)
    CMAKE_POLICY(SET CMP0005 OLD) # we escape a string definition in src/CMakeLists.txt
    # once we are cmake 2.6 and above only we can set this to new and remove the escaping
ENDIF(COMMAND CMAKE_POLICY)

# Also sets DataSeries_SOURCE_DIR to the full path to the root of your the DataSeries source directory
PROJECT(DataSeries)

SET(DATASERIES_VERSION 0.0)
SET(DATASERIES_ABI_VERSION 0)

# Set this to the path where the Lintel directory files are located
# (should help cmake find required includes for different Lintel builds)
SET(LINTEL_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../Lintel/")

# CMAKE_MODULE_PATH specifies the first places CMake looks for packages and includes.
SET(CMAKE_MODULE_PATH "${LINTEL_DIR}/cmake-modules" "${CMAKE_INSTALL_PREFIX}/share/cmake-modules" ${DataSeries_SOURCE_DIR} ${CMAKE_MODULE_PATH} )

INCLUDE(LintelCMakeUtil)
INCLUDE(DataSeriesCMakeUtil)

INCLUDE("CMakeConfig.txt")

ENABLE_TESTING()
 
ADD_SUBDIRECTORY(include/DataSeries)
ADD_SUBDIRECTORY(src)
ADD_SUBDIRECTORY(doc)

SET(CMAKE_MODULE_FILES
	DataSeriesCMakeUtil.cmake
	FindDataSeries.cmake
        FindThrift.cmake # TODO-reviewer: should this go into lintel?
)

INSTALL(FILES ${CMAKE_MODULE_FILES}
	DESTINATION ${CMAKE_INSTALL_PREFIX}/share/cmake-modules)

IF(LINTEL_FIND_ALL_NOTFOUND)
    MESSAGE("")
    MESSAGE("************************************")
    MESSAGE("  Some optional dependency was not found.")
    MESSAGE("  ${CMAKE_CURRENT_SOURCE_DIR}/doc/dependencies.txt")
    MESSAGE("  may help identify the right packages")
    MESSAGE("************************************")
    MESSAGE("")
ENDIF(LINTEL_FIND_ALL_NOTFOUND)

