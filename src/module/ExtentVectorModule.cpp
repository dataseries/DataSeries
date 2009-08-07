// -*-C++-*-
/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation of ExtentVectorModule
*/

#include <Lintel/AssertBoost.hpp>

#include <DataSeries/ExtentVectorModule.hpp>

ExtentVectorModule::ExtentVectorModule(const std::vector<SortedIndex::IndexEntry*>* extents,
				       const std::string &index_type)
    : IndexSourceModule(), cur_extent(0), extents(extents), index_type(index_type)
{
}

ExtentVectorModule::~ExtentVectorModule() {
}

void ExtentVectorModule::lockedResetModule() {
    cur_extent = 0;
}

IndexSourceModule::PrefetchExtent *ExtentVectorModule::lockedGetCompressedExtent() {
    // while there are more extents, read them. Return NULL if no more
    if (cur_extent == extents->size()) {
	return NULL;
    }
    SINVARIANT(cur_extent < extents->size());
    PrefetchExtent *ret = readCompressed(extents->at(cur_extent)->source.get(), 
					 extents->at(cur_extent)->offset,
					 index_type);
    
    ++cur_extent;
    return ret;
}
