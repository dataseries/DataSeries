// -*-C++-*-
/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <DataSeries/TypeIndexModule.H>

#ifndef __HP_aCC
using namespace std;
#endif

TypeIndexModule::TypeIndexModule(const string &_type_match)
    : IndexSourceModule(), 
      type_match(_type_match), 
      second_type_match(""),
      indexSeries(ExtentSeries::typeExact),
      extentOffset(indexSeries,"offset"), 
      extentType(indexSeries,"extenttype"),
      cur_file(0), cur_source(NULL),
      my_type(NULL)
{
}

TypeIndexModule::~TypeIndexModule()
{
}

void
TypeIndexModule::setMatch(const string &_type_match)
{
    AssertAlways(startedPrefetching() == false,
		 ("invalid to set prefix after we start prefetching; just doesn't make sense to make a change like this -- would have different results pop out"));
    type_match = _type_match;
}

void
TypeIndexModule::setSecondMatch(const std::string &_type_match)
{
    AssertAlways(startedPrefetching() == false,
		 ("invalid to set prefix after we start prefetching; just doesn't make sense to make a change like this -- would have different results pop out"));
    second_type_match = _type_match;
}


void
TypeIndexModule::addSource(const std::string &filename) 
{
    AssertAlways(startedPrefetching() == false, 
		 ("can't add sources safely after starting prefetching -- could get confused about the end of the entries.\n"));
    inputFiles.push_back(filename);
}

void
TypeIndexModule::lockedResetModule()
{
    indexSeries.clearExtent();
    cur_file = 0;
}

TypeIndexModule::compressedPrefetch *
TypeIndexModule::lockedGetCompressedExtent()
{
    while(true) {
	if (indexSeries.curExtent() == NULL) {
	    if (cur_file == inputFiles.size()) {
		return NULL;
	    }
	    cur_source = new DataSeriesSource(inputFiles[cur_file]);
	    INVARIANT(cur_source->indexExtent != NULL,
		      "can't handle source with null index extent\n");
	    if (type_match.empty()) {
		// nothing to do
	    } else if (my_type == NULL) {
		my_type = matchType();
	    } else {
		ExtentType *tmp = matchType();
		INVARIANT(my_type == tmp, "two different types were matched; this is currently invalid"); // TODO: figure out what we should allow, should the series typematching rules be imported here?
	    }

	    indexSeries.setExtent(cur_source->indexExtent);
	}
	for(;indexSeries.pos.morerecords();++indexSeries.pos) {
	    if (type_match.empty() ||
		(my_type != NULL &&
		 extentType.stringval() == my_type->getName())) {
		off64_t v = extentOffset.val();
		compressedPrefetch *ret 
		    = getCompressed(cur_source, v, extentType.stringval());
		++indexSeries.pos;
		return ret;
	    }
	}
	if (indexSeries.pos.morerecords() == false) {
	    indexSeries.clearExtent();
	    delete cur_source;
	    cur_source = NULL;
	    ++cur_file;
	}
    }
}

ExtentType *
TypeIndexModule::matchType()
{
    INVARIANT(cur_source != NULL, "bad");
    ExtentType *t = cur_source->getLibrary().getTypeMatch(type_match, true);
    ExtentType *u = NULL;
    if (!second_type_match.empty()) {
	u = cur_source->getLibrary().getTypeMatch(second_type_match, true);
    }
    INVARIANT(t == NULL || u == NULL || t == u,
	      boost::format("both %s and %s matched different types %s and %s")
	      % type_match % second_type_match
	      % t->getName() % u->getName());
    return t != NULL ? t : u;
}
