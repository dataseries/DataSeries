// -*-C++-*-
/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <TypeIndexModule.H>

#ifndef __HP_aCC
using namespace std;
#endif

TypeIndexModule::TypeIndexModule(const string &_type_prefix)
    : IndexSourceModule(), 
      type_prefix(_type_prefix), 
      indexSeries(ExtentSeries::typeXMLIdentical),
      extentOffset(indexSeries,"offset"), 
      extentType(indexSeries,"extenttype"),
      cur_file(0),
      cur_source(NULL)
{
}

TypeIndexModule::~TypeIndexModule()
{
}

void
TypeIndexModule::setPrefix(const string &_type_prefix)
{
    AssertAlways(startedPrefetching() == false,
		 ("invalid to set prefix after we start prefetching; just doesn't make sense to make a change like this -- would have different results pop out"));
    type_prefix = _type_prefix;
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
	    AssertAlways(cur_source->indexExtent != NULL,
			 ("can't handle source with null index extent\n"));
	    indexSeries.setExtent(cur_source->indexExtent);
	}
	for(;indexSeries.pos.morerecords();++indexSeries.pos) {
	    if (ExtentType::prefixmatch(extentType.stringval(),type_prefix)) {
		off64_t v = extentOffset.val();
		compressedPrefetch *ret = getCompressed(cur_source,
							v, extentType.stringval());
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

