#
# (c) Copyright 2007-2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake description for the doc directory

INCLUDE(LintelDocs)

FILE(GLOB DOXYGEN_EXAMPLES ${DataSeries_SOURCE_DIR}/src/example/*.cpp)
SET(DOXYGEN_DEP_EXTRAS ${DOXYGEN_EXAMPLES} mainpage.hpp)
SET(DOXYGEN_EXTRA_INPUTS ${CMAKE_HOME_DIRECTORY}/doc/mainpage.hpp)

LINTEL_DOCS_BUILD()

ADD_SUBDIRECTORY(tr)
ADD_SUBDIRECTORY(os-review-2008)
ADD_SUBDIRECTORY(fast2009-nfs-analysis)

LINTEL_POD2MAN(dataseries-utils.pod 7 ${DATASERIES_POD2MAN_RELEASE} "Misc." dataseries-utils.7)
