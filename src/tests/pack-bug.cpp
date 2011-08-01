// -*-C++-*-
/*
   (c) Copyright 2010, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    test program for DataSeries
*/

#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/ExtentField.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/DStoTextModule.hpp>
#include <DataSeries/commonargs.hpp>
#include <DataSeries/GeneralField.hpp>

int main(int argc, char *argv[]) {
    SINVARIANT(argc == 2);

    TypeIndexModule source("Trace::NFS::common");
    source.addSource(argv[1]);

    Extent *extent = source.getExtent();
    SINVARIANT(extent != NULL);
    Extent::ByteArray packeddata;
    if (extent) {
        extent->packData(packeddata, Extent::compress_lzf, 9, NULL, NULL, NULL);
	std::cout << "packed: " << packeddata.size() << " bytes\n";
    }
    source.close();
    return 0;
}
