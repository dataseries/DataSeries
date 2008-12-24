/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <Lintel/HashTable.hpp>
#include <Lintel/PointerUtil.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/SequenceModule.hpp>

#include "analysis/nfs/mod2.hpp"

using namespace std;
using boost::format;

// Two possible implementation of the large size * analysis:
// 1. Build a hash table over the attr-ops extents, and hash-join with
//    the common extents in a single pass
// 2. Do a merge join over the two tables (both are sorted roughly by
//    record-id) and do the analysis over the merged data
//
// We take the second approach because it will take a lot less memory when the
// attr-ops gets too large

class LargeSizeFileHandle : public NFSDSModule {
public:
    LargeSizeFileHandle(DataSeriesModule &_source, int _nkeep)
	: source(_source),
	  operation(s,"operation"),
	  filehandle(s,"filehandle"),
	  filesize(s,"file-size"),
	  nkeep(_nkeep),min_file_size(0)
    { }
    virtual ~LargeSizeFileHandle() { }

    struct hteData {
	string filehandle,operation;
	ExtentType::int64 maxsize;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) const {
	    return lintel::hashBytes(k.filehandle.data(),k.filehandle.size());
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.filehandle == b.filehandle;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	SINVARIANT(e->type.getName() == "attr-ops-join");

	hteData k;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (filesize.val() < min_file_size)
		continue;
	    k.filehandle = filehandle.stringval();
	    hteData *v = stats_table.lookup(k);
	    if (v == NULL) {
		k.operation = operation.stringval();
		k.maxsize = filesize.val();
		stats_table.add(k);
	    } else if (v->maxsize < filesize.val()) {
		k.operation = operation.stringval();
		k.maxsize = filesize.val();
	    }
	    // should do a min file size prune from time to time...
	}
	return e;
    }
    
    class sortByMaxSize {
    public:
	bool operator()(hteData *a, hteData *b) const {
	    return a->maxsize > b->maxsize;
	}
    };
    
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	vector<hteData *> vals;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	}
	sort(vals.begin(),vals.end(),sortByMaxSize());
	for(unsigned int i=0;i<nkeep;++i) {
	    printf("%9s %s %lld\n",vals[i]->operation.c_str(),
		   hexstring(vals[i]->filehandle).c_str(),
		   vals[i]->maxsize);
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Variable32Field operation,filehandle;
    Int64Field filesize;
    unsigned int nkeep;
    ExtentType::int64 min_file_size;
};

NFSDSModule *
NFSDSAnalysisMod::newLargeSizeFileHandle(DataSeriesModule &prev,
					 int nkeep)
{
    return new LargeSizeFileHandle(prev,nkeep);
}

class LargeSizeFilename : public NFSDSModule {
public:
    LargeSizeFilename(DataSeriesModule &_source, int _nkeep)
	: source(_source),
	  operation(s,"operation"),
	  filehandle(s,"filehandle"),
	  filename(s,"filename",Field::flag_nullable),
	  filesize(s,"file-size"),
	  nkeep(_nkeep),min_file_size(0)
    { }
    virtual ~LargeSizeFilename() { }

    struct hteData {
	string filename,operation;
	ExtentType::int64 maxsize;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) const {
	    return lintel::hashBytes(k.filename.data(),k.filename.size());
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.filename == b.filename;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	SINVARIANT(e->type.getName() == "attr-ops-join");

	hteData k;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (filesize.val() < min_file_size)
		continue;
	    if (filename.isNull()) {
		string *tmp = fnByFileHandle(filehandle.stringval());
		if (tmp == NULL) {
		    continue;
		} else {
		    k.filename = *tmp;
		}
	    } else {
		k.filename = filename.stringval();
	    }
	    hteData *v = stats_table.lookup(k);
	    if (v == NULL) {
		k.operation = operation.stringval();
		k.maxsize = filesize.val();
		stats_table.add(k);
	    } else if (v->maxsize < filesize.val()) {
		v->operation = operation.stringval();
		v->maxsize = filesize.val();
	    }
	    // should do a min file size prune from time to time...
	}
	return e;
    }
    
    class sortByMaxSize {
    public:
	bool operator()(hteData *a, hteData *b) const {
	    return a->maxsize > b->maxsize;
	}
    };
    
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	vector<hteData *> vals;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	}
	sort(vals.begin(),vals.end(),sortByMaxSize());
	if (nkeep > vals.size()) 
	    nkeep = vals.size();
	for(unsigned int i=0;i<nkeep;++i) {
	    printf("%10s %10lld %s\n",vals[i]->operation.c_str(),
		   vals[i]->maxsize, maybehexstring(vals[i]->filename).c_str());
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Variable32Field operation,filehandle,filename;
    Int64Field filesize;
    unsigned int nkeep;
    ExtentType::int64 min_file_size;
};

NFSDSModule *
NFSDSAnalysisMod::newLargeSizeFilename(DataSeriesModule &prev,
				       int nkeep)
{
    return new LargeSizeFilename(prev,nkeep);
}

class LargeSizeFilenameWrite : public NFSDSModule {
public:
    LargeSizeFilenameWrite(DataSeriesModule &_source, int _nkeep)
	: source(_source),
	  operation(s,"operation"),
	  filehandle(s,"filehandle"),
	  filename(s,"filename",Field::flag_nullable),
	  filesize(s,"file-size"),
	  dest(s,"server"),
	  nkeep(_nkeep),min_file_size(0)
    { }
    virtual ~LargeSizeFilenameWrite() { }

    struct hteData {
	string filename;
	ExtentType::int64 maxsize;
	unsigned int dest;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) const {
	    return lintel::hashBytes(k.filename.data(),k.filename.size());
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.filename == b.filename;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	SINVARIANT(e->type.getName() == "attr-ops-join");

	hteData k;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (filesize.val() < min_file_size)
		continue;
	    if (operation.size() != 5 || operation.stringval() != "write") {
		continue;
	    }
	    if (filename.isNull()) {
		string *tmp = fnByFileHandle(filehandle.stringval());
		if (tmp == NULL) {
		    continue;
		} else {
		    k.filename = *tmp;
		}
	    } else {
		k.filename = filename.stringval();
	    }
	    hteData *v = stats_table.lookup(k);
	    if (v == NULL) {
		k.maxsize = filesize.val();
		k.dest = dest.val();
		stats_table.add(k);
	    } else if (v->maxsize < filesize.val()) {
		v->maxsize = filesize.val();
		v->dest = dest.val();
	    }
	    // should do a min file size prune from time to time...
	}
	return e;
    }
    
    class sortByMaxSize {
    public:
	bool operator()(hteData *a, hteData *b) const {
	    return a->maxsize > b->maxsize;
	}
    };
    
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	vector<hteData *> vals;
	unsigned long long sum_write = 0;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	    sum_write += i->maxsize;
	}
	printf("sum(max size seen for each written file with known name) = %.2f MB\n",
	       (double)sum_write/(1024.0*1024.0));
	sort(vals.begin(),vals.end(),sortByMaxSize());
	if (nkeep > vals.size()) 
	    nkeep = vals.size();
	for(unsigned int i=0;i<nkeep;++i) {
	    printf("%10lld %d.%d.%d.%d %s\n",
		   vals[i]->maxsize, 
		   (vals[i]->dest >> 24) & 0xFF,
		   (vals[i]->dest >> 16) & 0xFF,
		   (vals[i]->dest >> 8) & 0xFF,
		   (vals[i]->dest >> 0) & 0xFF,
		   maybehexstring(vals[i]->filename).c_str());
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Variable32Field operation,filehandle,filename;
    Int64Field filesize;
    Int32Field dest;
    unsigned int nkeep;
    ExtentType::int64 min_file_size;
};

NFSDSModule *
NFSDSAnalysisMod::newLargeSizeFilenameWrite(DataSeriesModule &prev,
					    int nkeep)
{
    return new LargeSizeFilenameWrite(prev,nkeep);
}

class LargeSizeFilehandleWrite : public NFSDSModule {
public:
    LargeSizeFilehandleWrite(DataSeriesModule &_source, int _nkeep)
	: source(_source),
	  operation(s,"operation"),
	  filehandle(s,"filehandle"),
	  filesize(s,"file-size"),
	  dest(s,"server"),
	  nkeep(_nkeep),min_file_size(0)
    { }
    virtual ~LargeSizeFilehandleWrite() { }

    struct hteData {
	string filehandle;
	ExtentType::int64 maxsize;
	unsigned int dest;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) const {
	    return lintel::hashBytes(k.filehandle.data(),k.filehandle.size());
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.filehandle == b.filehandle;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	SINVARIANT(e->type.getName() == "attr-ops-join");

	hteData k;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (filesize.val() < min_file_size)
		continue;
	    if (operation.size() != 5 || operation.stringval() != "write") {
		continue;
	    }
	    k.filehandle = filehandle.stringval();
	    hteData *v = stats_table.lookup(k);
	    if (v == NULL) {
		k.maxsize = filesize.val();
		k.dest = dest.val();
		stats_table.add(k);
	    } else if (v->maxsize < filesize.val()) {
		v->maxsize = filesize.val();
		v->dest = dest.val();
	    }
	    // should do a min file size prune from time to time...
	}
	return e;
    }
    
    class sortByMaxSize {
    public:
	bool operator()(hteData *a, hteData *b) const {
	    return a->maxsize > b->maxsize;
	}
    };
    
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	vector<hteData *> vals;
	unsigned long long sum_write = 0;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	    sum_write += i->maxsize;
	}
	printf("sum(max size seen for each written filehandle) = %.2f MB\n",
	       (double)sum_write/(1024.0*1024.0));
	sort(vals.begin(),vals.end(),sortByMaxSize());
	if (nkeep > vals.size()) 
	    nkeep = vals.size();
	for(unsigned int i=0;i<nkeep;++i) {
	    printf("%10lld %d.%d.%d.%d %s\n",
		   vals[i]->maxsize, 
		   (vals[i]->dest >> 24) & 0xFF,
		   (vals[i]->dest >> 16) & 0xFF,
		   (vals[i]->dest >> 8) & 0xFF,
		   (vals[i]->dest >> 0) & 0xFF,
		   hexstring(vals[i]->filehandle).c_str());
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Variable32Field operation,filehandle;
    Int64Field filesize;
    Int32Field dest;
    unsigned int nkeep;
    ExtentType::int64 min_file_size;
};

NFSDSModule *
NFSDSAnalysisMod::newLargeSizeFilehandleWrite(DataSeriesModule &prev,
					      int nkeep)
{
    return new LargeSizeFilehandleWrite(prev,nkeep);
}

class FileageByFilehandle : public NFSDSModule {
public:
    FileageByFilehandle(DataSeriesModule &_source, int _nkeep,
			int _recent_age_seconds)
	: source(_source),
	  server(s,"server"),
	  packet_at(s,"reply-at"),
	  modify_time(s,"modify-time"),
	  filehandle(s,"filehandle"),
	  filename(s,"filename",Field::flag_nullable),
	  type(s,"type"),
	  filesize(s,"file-size"),
	  nkeep(_nkeep),min_file_size(0),
	  recent_age_seconds(_recent_age_seconds)
    { }
    virtual ~FileageByFilehandle() { }

    // TODO: add option to include server in the "filehandle"; current
    // implementation assumes globally unique filehandles, which
    // happens to be true for NetApp filers, but does not have to be
    // true in general.

    struct hteData {
	ExtentType::int32 server;
	string filehandle, filename, type;
	ExtentType::int64 maxsize, file_age;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) const {
	    return lintel::hashBytes(k.filehandle.data(),k.filehandle.size(),k.server);
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) const {
	    return a.server == b.server && a.filehandle == b.filehandle;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    ExtentType::int64 llround(ExtentType::int64 v,int to) {
	if (v < 0) return -llround(-v,to);
	int vx = v % to;
	if (vx >= to/2) {
	    return v + to - vx;
	} else {
	    return v - vx;
	}
    }

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	SINVARIANT(e->type.getName() == "attr-ops-join");

	hteData k;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    k.filehandle = filehandle.stringval();
	    k.server = server.val();

	    hteData *v = stats_table.lookup(k);
	    ExtentType::int64 file_age = packet_at.val() - modify_time.val();
	    //	    file_age = llround(file_age,100000);

	    if (v == NULL) {
		k.type = type.stringval();
		k.maxsize = filesize.val();
		k.file_age = file_age;
		v = stats_table.add(k);
	    } else {
		if (filesize.val() > v->maxsize) {
		    v->maxsize = filesize.val();
		}
		if (file_age < v->file_age) {
		    v->file_age = file_age;
		}
	    }
	    if (v->filename.empty() == true && filename.isNull() == false) {
		v->filename = filename.stringval();
	    }
	}
	return e;
    }
    
    class sortByMinFileAge {
    public:
	bool operator()(hteData *a, hteData *b) const {
	    return a->file_age == b->file_age ? a->maxsize < b->maxsize : a->file_age < b->file_age;
	}
    };
    
    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	vector<hteData *> vals;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	}
	sort(vals.begin(),vals.end(),sortByMinFileAge());
	if (nkeep > vals.size()) 
	    nkeep = vals.size();
	for(unsigned int i=0;i<nkeep;++i) {
	    if (vals[i]->filename.empty()) {
		vals[i]->filename = "*unknown*";
	    }
	    printf("%10.3f secs %s %20s %8s %lld\n",
		   (double)vals[i]->file_age / (1.0e9),
		   hexstring(vals[i]->filehandle).c_str(),
		   maybehexstring(vals[i]->filename).c_str(),
		   vals[i]->type.c_str(),
		   vals[i]->maxsize);
	}
	double recent_mb = 0;
	double old_mb = 0;
	const double recent_age_boundary = recent_age_seconds;
	int nrecent = 0;
	for(unsigned int i=0;i<vals.size();++i) {
	    double age = vals[i]->file_age / 1.0e9;
	    if (age < recent_age_boundary) {
		++nrecent;
		//		printf("%.5f %lld\n",age,vals[i]->maxsize);
		recent_mb += vals[i]->maxsize / (1024.0*1024.0);
	    } else {
		old_mb += vals[i]->maxsize / (1024.0*1024.0);
	    }
	}
	printf("%d unique filehandles, %d recent (%d seconds): %.2f GB total files accessed; %.2f GB recent, or %.2f%%\n",
	       vals.size(), nrecent, recent_age_seconds, (recent_mb + old_mb)/1024.0, recent_mb/1024.0, 100.0 * recent_mb / (recent_mb+old_mb));
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Int32Field server;
    Int64Field packet_at, modify_time;
    Variable32Field filehandle,filename,type;
    Int64Field filesize;
    unsigned int nkeep;
    ExtentType::int64 min_file_size;
    const int recent_age_seconds;
};



NFSDSModule *
NFSDSAnalysisMod::newFileageByFilehandle(DataSeriesModule &prev,
					 int nkeep,
					 int recent_age_seconds)
{
    return new FileageByFilehandle(prev, nkeep, recent_age_seconds);
}

