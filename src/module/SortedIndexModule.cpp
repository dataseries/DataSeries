// -*-C++-*-
/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation of SortedIndexModule
*/

#include <boost/foreach.hpp>
#include <boost/scoped_ptr.hpp>

#include <Lintel/AssertBoost.hpp>

#include <DataSeries/SortedIndexModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

SortedIndexModule::SortedIndexModule(const std::string &index_filename,
				     const std::string &index_type,
				     const std::string &fieldname) 
    : need_reset(false), cur_extent(0), index_type(index_type)
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

SortedIndexModule::~SortedIndexModule() { }

void SortedIndexModule::search(const GeneralValue &value) {
    INVARIANT(extents.size() == cur_extent,
	      boost::format("did not finish reading all extents before search"));
    extents.clear();
    cur_extent = 0;
    // search each index for relevant extents
    BOOST_FOREACH(IndexEntryVector &iev, index) {
	// See comment in header for use of lower bound and < operator.
	for(std::vector<IndexEntry>::iterator i = 
		std::lower_bound(iev.begin(), iev.end(), value);
	    i != iev.end() && i->inRange(value); ++i) {
	    extents.push_back(&(*i));
	}
    }

    // Currently invalid to call resetPos unless we have already tried
    // to get some extents out.  Must not call resetPos until after we
    // prepare the extents or the prefetching code may incorrectly
    // determine there is nothing to prefetch.
    if (need_reset) {
	resetPos();  // N.B. calls lockedResetModule()
    }
}

void SortedIndexModule::searchSet(const std::vector<GeneralValue> &values) {
    INVARIANT(extents.size() == cur_extent,
	      boost::format("did not finish reading all extents before search"));
    extents.clear();
    cur_extent = 0;

    BOOST_FOREACH(const GeneralValue &value, values) {
        // search each index for relevant extents
        BOOST_FOREACH(IndexEntryVector &iev, index) {
            // See comment in header for use of lower bound and < operator.
            for(std::vector<IndexEntry>::iterator i = 
                    std::lower_bound(iev.begin(), iev.end(), value);
                i != iev.end() && i->inRange(value); ++i) {
                extents.push_back(&(*i));
            }
        }
    }

    // sort the extents co-located by file (source pointer sorted) and
    // then ordered by location
    std::sort(extents.begin(), extents.end(), entrySorter);
}

void SortedIndexModule::lockedResetModule() { }

IndexSourceModule::PrefetchExtent *SortedIndexModule::lockedGetCompressedExtent() {
    // while there are more extents, read them. Return NULL if no more
    if (cur_extent == extents.size()) {
        need_reset = true;
	return NULL;
    }

    // skip duplicate extents
    while (cur_extent + 1 != extents.size() &&
           entryEqual(extents[cur_extent], extents[cur_extent + 1])) {
        ++cur_extent;
    }

    SINVARIANT(cur_extent < extents.size());
    PrefetchExtent *ret = readCompressed(extents[cur_extent]->source.get(), 
					 extents[cur_extent]->offset,
					 index_type);
    ++cur_extent;
    return ret;
}
