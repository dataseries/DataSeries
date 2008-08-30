#
# (c) Copyright 2008, Hewlett-Packard Development Company, LP
#
#  See the file named COPYING for license details
#
# cmake rules for the src/analysis/nfs directory

DATASERIES_PROGRAM(nfssubset)

DATASERIES_PROGRAM(nfsdsanalysis 
	common.cpp mod1.cpp mod2.cpp mod3.cpp mod4.cpp 
	AttrOpsCommonJoin.cpp
	CommonAttrRWJoin.cpp
	HostInfo.cpp 
	ReadWriteExtent.cpp
	ServerLatency.cpp 
        ../../process/sourcebyrange.cpp)


