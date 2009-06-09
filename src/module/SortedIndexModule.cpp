// -*-C++-*-
/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation of SortedIndexModule
*/

#include <boost/scoped_ptr.hpp>
#include <boost/foreach.hpp>

#include <Lintel/AssertBoost.hpp>

#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/SortedIndexModule.hpp>

SortedIndexModule::SortedIndexModule(const std::string &index_filename,
				     const std::string &index_type,
				     const std::string &fieldname) :
    cur_extent(0),
    index_type(index_type),
    first_time(true) {
    // we are going to read all index entries for the fieldname specified,
    // set up series and relevant fields to read from it
    TypeIndexModule tim("DSIndex::Extent::MinMax::" + index_type);
    tim.addSource(index_filename);
    ExtentSeries s;
    Int64Field extent_offset(s, "extent_offset");
    Variable32Field filename(s, "filename");
    boost::scoped_ptr<GeneralField> min_field;
    boost::scoped_ptr<GeneralField> max_field;
    // keep track of current filename and source being processed
    // along with the index for that filename
    std::string cur_fname("");
    boost::shared_ptr<DataSeriesSource> cur_source;
    IndexEntryVector *cur_index = NULL;
    // these variables are used to check if input file is sorted
    bool check = true;
    GeneralValue last_max;
    while(true) {
	boost::scoped_ptr<Extent> e(tim.getExtent());
	if (!e) {
	    break;
	}
	s.setExtent(e.get());
	if (!min_field) {
	    min_field.reset(GeneralField::create(s, "min:" + fieldname));
	    max_field.reset(GeneralField::create(s, "max:" + fieldname));
	}
	for (; s.pos.morerecords(); ++s.pos) {
	    // check to see if this is a new set of per-file entries
	    if (cur_fname != filename.stringval()) {
		cur_fname = filename.stringval();
		cur_source.reset(new DataSeriesSource(cur_fname, false));
		index.push_back(IndexEntryVector());
		cur_index = &index[index.size()-1];
		check = false;
	    }
	    cur_index->push_back(IndexEntry(cur_source,
					    min_field->val(), max_field->val(),
					    extent_offset.val()));
	    if (check && (last_max > min_field->val())) {
		// TODO: throw exception instead
		FATAL_ERROR("unsorted data file");
	    }
	    last_max = max_field->val();
	    check = true;
	}
    }
}

SortedIndexModule::~SortedIndexModule() {
    // nothing to be done
}

void
SortedIndexModule::search(const GeneralValue &value) {
    // clear old results
    extents.clear();
    cur_extent = 0;
    // search each index for relevant extents
    BOOST_FOREACH(IndexEntryVector &iev, index) {
	// lower bound is used to find the first entry that may contain the
	// value being looked up. This is ensured by defining an < operator
	// that compares against max (rather than min). 
	for(std::vector<IndexEntry>::iterator i = 
		std::lower_bound(iev.begin(), iev.end(), value);
	    i != iev.end() && i->inRange(value); ++i) {
	    extents.push_back(&(*i));
	}
    }
    // if this isn't the first time we have done a search, reset prefetching
    // N.B. must do this *after* extents have been built
    // TODO: check if possible to rewrite resetPos to work without first_time
    // check
    if (first_time) {
	first_time = false;
    }
    else {
	resetPos();  // N.B. calls lockedResetModule()
    }
}

void SortedIndexModule::lockedResetModule() {
    // nothing to be done
}

IndexSourceModule::PrefetchExtent *
SortedIndexModule::lockedGetCompressedExtent() {
    // while there are more extents, read them. Return NULL if no more
    if (cur_extent >= extents.size()) {
	return NULL;
    }
    PrefetchExtent *ret = readCompressed(extents[cur_extent]->source.get(), 
					 extents[cur_extent]->offset,
					 index_type);
    ++cur_extent;
    return ret;
}
