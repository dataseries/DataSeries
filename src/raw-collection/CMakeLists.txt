#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/raw-collection directory

ADD_SUBDIRECTORY(lsf)
ADD_SUBDIRECTORY(network-driverdump)

IF(LINUX_COMPILER_ENABLED AND PCAP_ENABLED)
    ADD_EXECUTABLE(lindump-mmap lindump-mmap.c)
    INSTALL(TARGETS lindump-mmap DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
ENDIF(LINUX_COMPILER_ENABLED AND PCAP_ENABLED)

