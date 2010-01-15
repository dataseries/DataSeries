#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# Main cmake control file

# ELSEIF came with 2.4.4
CMAKE_MINIMUM_REQUIRED(VERSION 2.4.4 FATAL_ERROR)

IF(COMMAND CMAKE_POLICY)
    CMAKE_POLICY(SET CMP0005 OLD) # we escape a string definition in src/CMakeLists.txt
    # once we are cmake 2.6 and above only we can set this to new and remove the escaping
ENDIF(COMMAND CMAKE_POLICY)

PROJECT(DataSeries)

SET(DATASERIES_VERSION 0.0)
SET(DATASERIES_ABI_VERSION 0)

SET(CMAKE_MODULE_PATH "${DataSeries_SOURCE_DIR}" "${CMAKE_INSTALL_PREFIX}/share/cmake-modules" ${CMAKE_MODULE_PATH})

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

