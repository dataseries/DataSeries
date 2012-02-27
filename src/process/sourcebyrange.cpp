/* -*-C++-*-
   (c) Copyright 2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    sourcebyrange implementation, not this is obsolete, don't do this
*/

#include <DataSeries/IndexSourceModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

using namespace std;
using boost::format;

bool isNumber(char *v) {
    while(true) {
	if (*v == '\0') {
	    return true;
	} else if (!isdigit(*v)) {
	    return false;
	} else {
	    ++v;
	}
    }
}

static bool 
inrange(ExtentType::int64 val, ExtentType::int64 minrange, ExtentType::int64 maxrange)
{
    return val >= minrange && val <= maxrange;
}

void
sourceByIndex(TypeIndexModule *source,char *index_filename,int start_secs, int end_secs)
{
    ExtentType::int64 start_ns = (ExtentType::int64)start_secs * 1000000000;
    if (end_secs == 0) {
	end_secs = 2147483647;
    }
    ExtentType::int64 end_ns = (ExtentType::int64)end_secs * 1000000000;
    TypeIndexModule src("NFS trace: common index");
    src.addSource(index_filename);
    Extent::Ptr e = src.getSharedExtent();
    INVARIANT(e->type.getName() == "NFS trace: common index",
	      format("whoa, extent type %s bad") % e->type.getName());

    char *start_add = (char *)sbrk(0);
    ExtentSeries s(e);
    Int64Field start_id(s,"start-id"), end_id(s,"end-id"), 
	start_time(s,"start-time"), end_time(s,"end-time");
    Variable32Field filename(s,"filename");
    int nfiles = 0;
    cout << format("start ns %d; end ns %d\n") % start_ns % end_ns;
    for(;s.morerecords();++s) {
	if (inrange(start_time.val(),start_ns,end_ns) ||
	    inrange(end_time.val(),start_ns,end_ns) ||
	    inrange(start_ns,start_time.val(),end_time.val()) ||
	    inrange(end_ns,start_time.val(),end_time.val())) { 
	    ++nfiles;
	    if (false) {
		cerr << format("addsource %s %d %d\n")
		    % filename.stringval() % start_time.val() % end_time.val();
	    }
	    source->addSource(filename.stringval());
	}
    }
    INVARIANT(nfiles > 0,format("didn't find any files for range [%d .. %d]")
	      % start_secs % end_secs);
    
    e = src.getSharedExtent(); 
    SINVARIANT(e == NULL);
    INVARIANT(src.getSharedExtent() == NULL,"whoa, index had incorrect extents");
    char *end_add = (char *)sbrk(0);
    if (false) {
	cout << format("%d bytes used for %d files, or %d bytes/file\n")
	    % (end_add - start_add) % nfiles % ((end_add - start_add)/nfiles);
    }
}

