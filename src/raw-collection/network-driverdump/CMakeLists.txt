#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/raw-collection/network-driverdump directory

IF(PCAP_ENABLED AND "${HOST_TYPE}" STREQUAL "Linux")
    ADD_EXECUTABLE(network-driverdump driverdump.cpp)
    TARGET_LINK_LIBRARIES(network-driverdump ${LINTELPTHREAD_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
    INSTALL(TARGETS network-driverdump DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
    LINTEL_POD2MAN(driverdump.cpp 1 ${DATASERIES_POD2MAN_RELEASE} "-" network-driverdump.1)
ENDIF(PCAP_ENABLED AND "${HOST_TYPE}" STREQUAL "Linux")

