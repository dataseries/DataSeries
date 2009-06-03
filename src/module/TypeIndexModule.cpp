// -*-C++-*-
/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <DataSeries/TypeIndexModule.hpp>

using namespace std;

TypeIndexModule::TypeIndexModule(const string &_type_match)
    : IndexSourceModule(),
      type_match(_type_match),
      second_type_match(""),
      indexSeries(ExtentSeries::typeExact),
      extentOffset(indexSeries,"offset"),
      extentType(indexSeries,"extenttype"),
      cur_file(0), cur_source(NULL),
      my_type(NULL)
{ }

TypeIndexModule::~TypeIndexModule()
{ }

void TypeIndexModule::setMatch(const string &_type_match) {
    INVARIANT(startedPrefetching() == false,
	      "invalid to set prefix after we start prefetching; just doesn't make sense to make a change like this -- would have different results pop out");
    type_match = _type_match;
}

void TypeIndexModule::setSecondMatch(const std::string &_type_match) {
    INVARIANT(startedPrefetching() == false,
	      "invalid to set prefix after we start prefetching; just doesn't make sense to make a change like this -- would have different results pop out");
    second_type_match = _type_match;
}


void TypeIndexModule::addSource(const std::string &filename) {
    INVARIANT(startedPrefetching() == false,
	      "can't add sources safely after starting prefetching -- could get confused about the end of the entries.");
    inputFiles.push_back(filename);
}

void TypeIndexModule::lockedResetModule() {
    indexSeries.clearExtent();
    cur_file = 0;
}

TypeIndexModule::PrefetchExtent *TypeIndexModule::lockedGetCompressedExtent() {
    while(true) {
	if (indexSeries.curExtent() == NULL) { 
	    // advance to next file.
	    if (cur_file == inputFiles.size()) {
		INVARIANT(!inputFiles.empty(), "type index module had no input files??");
		return NULL;
	    }

	    cur_source = new DataSeriesSource(inputFiles[cur_file]);
	    INVARIANT(cur_source->indexExtent != NULL,
		      "can't handle source with null index extent\n");

	    if (type_match.empty()) {
		// nothing to do, match all types
	    } else if (my_type == NULL) {
		my_type = matchType();
	    } else {
		const ExtentType *tmp = matchType();
		// TODO: figure out what we should allow, should the series typematching rules be imported here?
		INVARIANT(my_type == tmp,
			  boost::format("two different types were matched; this is currently invalid\nFile with mismatch was %s\nType 1:\n%s\nType 2:\n%s\n")
			  % inputFiles[cur_file]
			  % my_type->getXmlDescriptionString()
			  % tmp->getXmlDescriptionString());
	    }

	    // index extent is exactly one extent/file
	    indexSeries.setExtent(cur_source->indexExtent);
	}


	// each row refers to a single extent in the file
	for(;indexSeries.pos.morerecords(); ++indexSeries.pos) {
	    if (type_match.empty() || 
		(my_type != NULL && extentType.stringval() == my_type->getName())) {
		off64_t v = extentOffset.val(); 

		PrefetchExtent *ret
		    = readCompressed(cur_source, v, extentType.stringval());

		++indexSeries.pos;

		return ret; 
	    }
	}

	SINVARIANT(indexSeries.pos.morerecords() == false);
	// we're done with the current DS file (no more records in the index)

	// TODO-tomer: if the above invariant passes regression tests remove the next check
	if (indexSeries.pos.morerecords() == false) { 
	    indexSeries.clearExtent();
	    delete cur_source; 
	    cur_source = NULL;
	    ++cur_file; 
	}
    }
}

const ExtentType *TypeIndexModule::matchType() {
    SINVARIANT(cur_source != NULL);
    const ExtentType *t = cur_source->getLibrary().getTypeMatch(type_match, true);
    const ExtentType *u = NULL;
    if (!second_type_match.empty()) {
	u = cur_source->getLibrary().getTypeMatch(second_type_match, true);
    }
    INVARIANT(t == NULL || u == NULL || t == u,
	      boost::format("both %s and %s matched different types %s and %s")
	      % type_match % second_type_match
	      % t->getName() % u->getName());
    return t != NULL ? t : u;
}
