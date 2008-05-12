#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/raw-collection/network-driverdump directory

IF(PCAP_ENABLED)
    ADD_EXECUTABLE(network-driverdump driverdump.cpp)
    TARGET_LINK_LIBRARIES(network-driverdump ${LINTELPTHREAD_LIBRARIES})
    INSTALL(TARGETS network-driverdump DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
ENDIF(PCAP_ENABLED)

