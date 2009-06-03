// -*-C++-*-
/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation of SortedIndexModule - builds an index for keys of
    totally sorted DS file
*/

#include <DataSeries/SortedIndexModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>


SortedIndexModule::SortedIndexModule(const std::string &index_filename,
				     const std::string &index_type,
				     const std::string &fieldname) :
    source_(NULL),
    cur_extent_(0),
    index_type_(index_type),
    firstTime(true) {
    TypeIndexModule tim("DSIndex::Extent::MinMax::" + index_type);
    tim.addSource(index_filename);
    ExtentSeries s;
    Int64Field extent_offset(s,"extent_offset");
    GeneralField *min_field = NULL;
    GeneralField *max_field = NULL;
    while(true) {
	Extent *e = tim.getExtent();
	if (e == NULL) {
	    break;
	}
	s.setExtent(e);
	if (min_field == NULL) {
	    min_field = GeneralField::create(NULL, s, "min:" + fieldname);
	    max_field = GeneralField::create(NULL, s, "max:" + fieldname);
	}
	for (; s.pos.morerecords(); ++s.pos) {
	    addIndexEntry(GeneralValue(min_field), GeneralValue(max_field),
			  extent_offset.val());
	    if (source_ == NULL) {
		Variable32Field filename(s,"filename");
		sourceFilename =  filename.stringval();
		source_ = new DataSeriesSource(filename.stringval(), false);
	    }
	}
	delete e;
    }
    delete min_field;
    delete max_field;
}

void
SortedIndexModule::lookup(int64_t value) {
    if (!firstTime) {
	lockedResetModule();
	source_ = new DataSeriesSource(sourceFilename, false);
    } else {
	firstTime = false;
    }
    for (std::vector<IndexEntry>::iterator i = 
	     std::lower_bound(index_.begin(), index_.end(), value);
	 i->inRange(value); ++i) {
	extents_.push_back(i->offset_);
    }
}

void SortedIndexModule::lockedResetModule() {
    extents_.clear();
    cur_extent_ = 0;
}

IndexSourceModule::PrefetchExtent *
SortedIndexModule::lockedGetCompressedExtent() {
    if (cur_extent_ >= extents_.size()) {
	return NULL;
    }
    PrefetchExtent *ret = readCompressed(source_, extents_[cur_extent_],
					 index_type_);
    ++cur_extent_;
    return ret;
}
