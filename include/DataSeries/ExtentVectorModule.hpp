/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file 

    A module which is intended to be used as the destination
    module for searches done using the SortedIndex.  It takes a vector
    of filenames, a vector of extents, and an index_type and allows
    for sequential processing of the extents in that vector.
*/

#ifndef DATASERIES_EXTENT_VECTOR_MODULE_HPP
#define DATASERIES_EXTENT_VECTOR_MODULE_HPP

#include <DataSeries/GeneralField.hpp>
#include <DataSeries/SortedIndex.hpp>
#include <DataSeries/IndexSourceModule.hpp>

class ExtentVectorModule : public IndexSourceModule {
public:
    /** Create a new ExtentVectorModule
	@param extents vector of extents to read
	@param index_type index type to pass on to prefetching for debugging.
     */
    ExtentVectorModule(const std::vector<SortedIndex::IndexEntry *>* extents,
		       const std::string &index_type);
    /** Destructor */
    virtual ~ExtentVectorModule();

protected:
    virtual PrefetchExtent *lockedGetCompressedExtent();
    virtual void lockedResetModule();

private:
    size_t cur_extent; // current extent being processed, indexes into extents
    const std::vector<SortedIndex::IndexEntry *>* extents;	// extents that contain searched value
    const std::string index_type;
};

#endif
