# Find the DataSeries includes and library
#
#  DATASERIES_INCLUDE_DIR - where to find DataSeries/*.hpp
#  DATASERIES_LIBRARIES   - List of libraries when using DataSeries
#  DATASERIES_FOUND       - True if DataSeries found.

INCLUDE(LintelFind)

LINTEL_FIND_LIBRARY_CMAKE_INCLUDE_FILE(DATASERIES DataSeries/Extent.hpp DataSeries)
