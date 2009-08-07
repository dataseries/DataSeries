// -*-C++-*-
/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation of SortedIndex
*/

#include <boost/foreach.hpp>
#include <boost/scoped_ptr.hpp>

#include <Lintel/AssertBoost.hpp>

#include <DataSeries/SortedIndex.hpp>
#include <DataSeries/TypeIndexModule.hpp>

SortedIndex::SortedIndex(const std::string &index_filename,
			 const std::string &index_type,
			 const std::string &fieldname) 
// : index_type(index_type)
{
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
	    }
	    if (!cur_index->empty()) {
		INVARIANT(last_max <= min_field->val(),
			  boost::format("file %s is not sorted, %s > %s") 
			  % cur_fname % last_max % min_field->val());
	    }
	    last_max = max_field->val();

	    cur_index->push_back(IndexEntry(cur_source,
					    min_field->val(), max_field->val(),
					    extent_offset.val()));
	}
    }
}

SortedIndex::~SortedIndex() { }

std::vector<SortedIndex::IndexEntry*>* SortedIndex::search(const GeneralValue &value) {
    // extents that contain searched value
    std::vector<SortedIndex::IndexEntry *> *extents = new std::vector<IndexEntry*>;
    // search each index for relevant extents
    BOOST_FOREACH(IndexEntryVector &iev, index) {
	// See comment in header for use of lower bound and < operator.
	for(std::vector<IndexEntry>::iterator i = 
		std::lower_bound(iev.begin(), iev.end(), value);
	    i != iev.end() && i->inRange(value); ++i) {
	    extents->push_back(&(*i));
	}
    }
    return extents;
}

