/* -*-C++-*-
*******************************************************************************
*
* File:         TypeIndexModule.C
* RCS:          $Header: /mount/cello/cvs/DataSeries/src/module/TypeIndexModule.C,v 1.3 2005/02/15 01:18:36 anderse Exp $
* Description:  implementation
* Author:       Eric Anderson
* Created:      Sat May 29 18:17:15 2004
* Modified:     Wed Jan 19 19:03:45 2005 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2004, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#include <TypeIndexModule.H>

#ifndef __HP_aCC
using namespace std;
#endif

TypeIndexModule::TypeIndexModule(const string &_type_prefix, 
				 SourceList *sources)
    : IndexSourceModule(sources), 
      type_prefix(_type_prefix), 
      indexSeries(ExtentSeries::typeXMLIdentical),
      extentOffset(indexSeries,"offset"), 
      extentType(indexSeries,"extenttype")
{
}

TypeIndexModule::~TypeIndexModule()
{
}

void
TypeIndexModule::lockedResetModule()
{
    indexSeries.clearExtent();
    cur_source = 0;
}

TypeIndexModule::compressedPrefetch *
TypeIndexModule::lockedGetCompressedExtent()
{
    while(true) {
	if (indexSeries.curExtent() == NULL) {
	    if (cur_source == (int)sourcelist->sources.size()) {
		return NULL;
	    }
	    AssertAlways(sourcelist->sources[cur_source].dss->indexExtent != NULL,
			 ("can't handle source with null index extent\n"));
	    indexSeries.setExtent(sourcelist->sources[cur_source].dss->indexExtent);
	}
	for(;indexSeries.pos.morerecords();++indexSeries.pos) {
	    if (ExtentType::prefixmatch(extentType.stringval(),type_prefix)) {
		sourcelist->useSource(cur_source);
		off64_t v = extentOffset.val();
		compressedPrefetch *ret = getCompressed(sourcelist->sources[cur_source].dss,
							v, extentType.stringval());
		sourcelist->unuseSource(cur_source);
		++indexSeries.pos;
		return ret;
	    }
	}
	if (indexSeries.pos.morerecords() == false) {
	    ++cur_source;
	    indexSeries.clearExtent();
	}
    }
}

void
TypeIndexModule::setPrefix(const string &_type_prefix)
{
    AssertAlways(startedPrefetching() == false,
		 ("invalid to set prefix after we start prefetching"));
    type_prefix = _type_prefix;
}


