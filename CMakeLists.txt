#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# Main cmake control file

# ELSEIF came with 2.4.4
CMAKE_MINIMUM_REQUIRED(VERSION 2.4.4 FATAL_ERROR)

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
