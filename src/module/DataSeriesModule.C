/* -*-C++-*-
*******************************************************************************
*
* File:         DataSeriesModule.C
* RCS:          $Header: /mount/cello/cvs/DataSeries/cpp/module/DataSeriesModule.C,v 1.6 2004/09/28 05:08:32 anderse Exp $
* Description:  implementation
* Author:       Eric Anderson
* Created:      Mon Aug  4 23:00:20 2003
* Modified:     Tue Jan 24 13:40:03 2006 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>

#include <DataSeriesModule.H>

DataSeriesModule::~DataSeriesModule()
{
}

void
DataSeriesModule::getAndDelete(DataSeriesModule &from)
{
    while(true) {
	Extent *e = from.getExtent();
	if (e == NULL) return;
	delete e;
    }
}


SourceModule::SourceModule(SourceList *_sources)
    : total_uncompressed_bytes(0), 
    total_compressed_bytes(0), decode_time(0),
    sourcelist(_sources), cur_source(0)
{
    if (sourcelist == NULL) {
	sourcelist = new SourceList();
    }
    sourcelist->addReference();
}

SourceModule::~SourceModule()
{
    sourcelist->removeReference();
}

DataSeriesSource *
SourceModule::curSource()
{
    if (cur_source == (int)sourcelist->sources.size())
	return NULL;
    return sourcelist->sources[cur_source].dss;
}

static inline double 
timediff(struct timeval &end,struct timeval &start)
{
    return end.tv_sec - start.tv_sec + (end.tv_usec - start.tv_usec) * 1e-6;
}

Extent *
SourceModule::getExtent() 
{
    while(true) {
	if (cur_source == (int)sourcelist->sources.size())
	    return NULL;
	sourcelist->useSource(cur_source);
	struct rusage rusage_start;
	AssertAlways(getrusage(RUSAGE_SELF,&rusage_start)==0,
		     ("getrusage failed: %s\n",strerror(errno)));
	Extent *e = sourcelist->sources[cur_source].dss->readExtent();
	struct rusage rusage_end;
	AssertAlways(getrusage(RUSAGE_SELF,&rusage_end)==0,
		     ("getrusage failed: %s\n",strerror(errno)));
	sourcelist->unuseSource(cur_source);
	if (e != NULL) {
	    total_uncompressed_bytes += e->extentsize();
	    decode_time += timediff(rusage_end.ru_utime,rusage_start.ru_utime) +
		timediff(rusage_end.ru_stime,rusage_start.ru_stime);

	    return e;
	}
	++cur_source;
    }
}

bool
SourceModule::haveSources()
{ 
    return sourcelist->sources.empty() == false; 
}

void
SourceModule::SourceList::addSource(DataSeriesSource *source) 
{
    mutex.lock();
    if (nactive >= target_active) {
	lockedCloseInactive();
    }
    sources.push_back(source);
    if (source->isactive()) {
	++nactive;
    }
    mutex.unlock();
}

void
SourceModule::SourceList::addSource(const std::string &filename) 
{
   addSource(new DataSeriesSource(filename));
}

void
SourceModule::SourceList::addReference()
{
    mutex.lock(); 
    AssertAlways(refcount >= 0,("bad"));
    ++refcount;
    mutex.unlock();
}

void
SourceModule::SourceList::removeReference()
{
    mutex.lock();
    AssertAlways(refcount > 0,("bad"));
    --refcount;
    bool dodelete = refcount == 0;
    mutex.unlock();
    if (dodelete) {
	delete this;
    }
}

SourceModule::SourceList::~SourceList()
{
    AssertAlways(refcount == 0,("bad"));
    refcount = -1;
    for(unsigned i = 0;i<sources.size();++i) {
	delete sources[i].dss;
	sources[i].dss = NULL;
    }
}

void
SourceModule::SourceList::useSource(unsigned sourcenum)
{
    mutex.lock();
    if (nactive >= target_active) {
	lockedCloseInactive();
    }
    AssertAlways(sourcenum < sources.size(),("bad"));
    AssertAlways(sources[sourcenum].use_count >= 0,("bad"));
    if (sources[sourcenum].use_count == 0) {
	if (sources[sourcenum].dss->isactive() == false) {
	    sources[sourcenum].dss->reopenfile();
	    ++nactive;
	}
    } else {
	AssertAlways(sources[sourcenum].dss->isactive(),("bad"));
    }
    ++sources[sourcenum].use_count;
    mutex.unlock();
}

void
SourceModule::SourceList::unuseSource(unsigned sourcenum)
{
    mutex.lock();
    AssertAlways(sourcenum < sources.size(),("bad"));
    AssertAlways(sources[sourcenum].use_count > 0,
		 ("unuse(%d) at %d",sourcenum,sources[sourcenum].use_count));
    --sources[sourcenum].use_count;
    mutex.unlock();
}

void
SourceModule::SourceList::lockedCloseInactive()
{
    for(unsigned i=0;i<sources.size();++i) {
	if (sources[i].use_count == 0 &&
	    sources[i].dss->isactive()) {
	    sources[i].dss->closefile();
	    --nactive;
	}
    }
}

    

FilterModule::FilterModule(DataSeriesModule &_from, 
			   const std::string &_type_prefix)
    : from(_from), type_prefix(_type_prefix)
{
}

FilterModule::~FilterModule()
{
}

Extent *
FilterModule::getExtent()
{
    while(true) {
	Extent *e = from.getExtent();
	if (e == NULL)
	    return NULL;
	if (ExtentType::prefixmatch(e->type->name,type_prefix))
	    return e;
	delete e;
    }
}

OutputModule::OutputModule(DataSeriesSink &_sink, ExtentSeries &_series,
			   const ExtentType *_outputtype, int _target_extent_size)
    : sink(_sink), series(_series), outputtype(_outputtype),
      target_extent_size(_target_extent_size)
{
    AssertAlways(series.curExtent() == NULL,
		 ("usage error, outputmodule series extent started with an extent\n"));
    cur_extent = new Extent(outputtype);
    series.setExtent(cur_extent);
}

OutputModule::~OutputModule()
{
    flushExtent();
}


void
OutputModule::newRecord()
{
    AssertAlways(series.curExtent() == cur_extent,
		 ("usage error, someone else changed the series extent\n"));
    if ((int)(cur_extent->extentsize() + outputtype->fixedrecordsize()) > target_extent_size) {
	sink.writeExtent(cur_extent);
	cur_extent->clear();
    }
    series.newRecord();
}

void
OutputModule::flushExtent()
{
    if (cur_extent->fixeddata.size() > 0) {
	sink.writeExtent(cur_extent);
	cur_extent->clear();
    }
}

