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

ExtentVectorModule::ExtentVectorModule(
	const std::vector<std::string>& file_names,
	const std::vector<SortedIndex::IndexEntry*>* extents, 
	const std::string &index_type) 
    : IndexSourceModule(), cur_extent(0), index_type(index_type), 
      extents(extents), file_names(file_names)
{
    if (extents->size() != 0) {
	cur_source = new DataSeriesSource(file_names[extents->at(0)->source], false);
    }
}

ExtentVectorModule::~ExtentVectorModule() {
    if (cur_source) {
	delete cur_source;
	cur_source = NULL;
    }
}

void ExtentVectorModule::lockedResetModule() {
    cur_extent = 0;
    if (cur_source) {
	delete cur_source;
	cur_source = NULL;
    }
    if (extents->size() != 0) {
	cur_source = new DataSeriesSource(file_names[extents->at(0)->source], false);
    }
}

IndexSourceModule::PrefetchExtent *ExtentVectorModule::lockedGetCompressedExtent() {
    // while there are more extents, read them. Return NULL if no more
    if (cur_extent == extents->size()) {
	return NULL;
    }
    SINVARIANT(cur_extent < extents->size());
    PrefetchExtent *ret = readCompressed(cur_source, 
	    extents->at(cur_extent)->offset,
		    index_type);
    
    ++cur_extent;
    if (cur_extent < extents->size() && extents->at(cur_extent)->source != extents->at(cur_extent-1)->source) {
	delete cur_source;
	cur_source = NULL;
	cur_source = new DataSeriesSource(file_names[extents->at(cur_extent)->source], false);
    }
    return ret;

	    

}
