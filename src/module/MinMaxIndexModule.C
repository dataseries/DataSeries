/* -*-C++-*-
*******************************************************************************
*
* File:         MinMaxIndexModule.C
* RCS:          $Header: /mount/cello/cvs/DataSeries/src/module/MinMaxIndexModule.C,v 1.2 2005/02/15 01:18:36 anderse Exp $
* Description:  implementation
* Author:       Eric Anderson
* Created:      Mon Jun  7 08:37:15 2004
* Modified:     Wed Jan 19 19:01:06 2005 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2004, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#include <algorithm>

#include <MinMaxIndexModule.H>

#include <TypeIndexModule.H>
#include <HashMap.H>

#ifndef __HP_aCC
using namespace std;
#endif

static bool inrange(const GeneralValue &v, const GeneralValue &minrange,
		    const GeneralValue &maxrange)
{
    return minrange <= v && v <= maxrange;
}

static bool intervalOverlap(const GeneralValue &a_min, const GeneralValue &a_max,
			    const GeneralValue &b_min, const GeneralValue &b_max)
{
    return inrange(a_min,b_min,b_max) ||
	inrange(a_max,b_min,b_max) ||
	inrange(b_min, a_min, a_max) ||
	inrange(b_max, a_min, a_max);
}

struct xinfo {
    string filename;
    ExtentType::int64 extent_offset;
    GeneralValue sortvalue;
    xinfo(const string &a,ExtentType::int64 b,GeneralValue &c)
	: filename(a), extent_offset(b), sortvalue(c) { }
};

class xinfo_bysortvalue {
public:
    bool operator() (const xinfo &a, const xinfo &b) {
	return a.sortvalue < b.sortvalue;
    }
};

void
MinMaxIndexModule::init(const std::string &index_filename,
			std::vector<selector> &intersection_list,
			const std::string &sort_fieldname)
{
    vector<xinfo> kept_extents;
    TypeIndexModule tim("DSIndex::Extent::MinMax::" + index_type);
    tim.addSource(index_filename);

    ExtentSeries s;
    Variable32Field filename(s,"filename");
    Int64Field extent_offset(s,"extent_offset");
    GeneralField *sort_val = NULL;
    while(true) {
	Extent *e = tim.getExtent();
	if (e == NULL) {
	    break;
	}
	s.setExtent(e);
	if (sort_val == NULL) {
	    sort_val = GeneralField::create(NULL,s,sort_fieldname);
	    // can't create generalfields until we know the type, but
	    // can't know the type until we read in the extent.  We could
	    // modify generalfields so that their type can be determined
	    // based on a generalvalue type, but that would be limiting.
	    for(unsigned i=0;i<intersection_list.size();++i) {
		selector &sel = intersection_list[i];
		sel.minf = GeneralField::create(NULL,s,"min:" + sel.min_fieldname);
		sel.maxf = GeneralField::create(NULL,s,"max:" + sel.max_fieldname);
	    }
	}
	for(;s.pos.morerecords();++s.pos) {
	    bool all_overlap = true;
	    GeneralValue extent_sort(sort_val);
	    for(unsigned i=0;i<intersection_list.size();++i) {
		selector &sel = intersection_list[i];
		GeneralValue extent_min(sel.minf);
		GeneralValue extent_max(sel.maxf);
		if (false == intervalOverlap(extent_min,extent_max,sel.minv,sel.maxv)) {
		    all_overlap = false;
		    break;
		}
	    }
	    if (all_overlap) {
		if (false)
		    printf("keep %s @ %lld\n",filename.stringval().c_str(),
			   extent_offset.val());
		kept_extents.push_back(xinfo(filename.stringval(),
					     extent_offset.val(),
					     extent_sort));
	    } else {
		if (false)
		    printf("skip %s @ %lld\n",filename.stringval().c_str(),
			   extent_offset.val());
	    }
	}
	delete e;
    }
    for(unsigned i=0;i<intersection_list.size();++i) {
	selector &sel = intersection_list[i];
	delete sel.minf;
	delete sel.maxf;
    }
    delete sort_val;
    unsigned sourcecount = 0;
    HashMap<string,int> fn2sourcenum;
    sort(kept_extents.begin(),kept_extents.end(),xinfo_bysortvalue());
    for(vector<xinfo>::iterator i=kept_extents.begin();
	i != kept_extents.end(); ++i) {
	int *sourcenum = fn2sourcenum.lookup(i->filename);
	if (sourcenum == NULL) {
	    fn2sourcenum[i->filename] = sourcecount;
	    AssertAlways(sourcelist->sources.size() == sourcecount,
			 ("internal"));
	    sourcenum = fn2sourcenum.lookup(i->filename);
	    ++sourcecount;
	    addSource(i->filename);
	}
	AssertAlways(sourcenum != NULL,("internal"));
	extentList.push_back(extentorder(*sourcenum,i->extent_offset));
	if (false)
	    printf("add %s -- %d @ %lld\n",
		   i->filename.c_str(),*sourcenum,i->extent_offset);
    }
}


MinMaxIndexModule::MinMaxIndexModule(const string &index_filename,
				     const string &_index_type,
				     const GeneralValue minv, 
				     const GeneralValue maxv,
				     const string &min_fieldname,
				     const string &max_fieldname,
				     const string &sort_fieldname)
    : IndexSourceModule(NULL), index_type(_index_type), curextent(0)
{
    vector<selector> tmp;
    selector foo(minv,maxv,min_fieldname,max_fieldname);
    tmp.push_back(foo);
    init(index_filename,tmp,sort_fieldname);
}

MinMaxIndexModule::MinMaxIndexModule(const std::string &index_filename,
				     const std::string &_index_type,
				     std::vector<selector> intersection_list,
				     const std::string &sort_fieldname)
    : IndexSourceModule(NULL), index_type(_index_type), curextent(0)
{
    init(index_filename,intersection_list,sort_fieldname);
}


void
MinMaxIndexModule::lockedResetModule()
{
    AssertFatal(("unimplemented"));
}

IndexSourceModule::compressedPrefetch *
MinMaxIndexModule::lockedGetCompressedExtent()
{
    if (curextent >= extentList.size())
	return NULL;
    int sourcenum = extentList[curextent].sourcenum;
    sourcelist->useSource(sourcenum);
    compressedPrefetch *ret = getCompressed(sourcelist->sources[sourcenum].dss,
					    extentList[curextent].offset,
					    index_type);
    sourcelist->unuseSource(sourcenum);
    ++curextent;
    return ret;
}

