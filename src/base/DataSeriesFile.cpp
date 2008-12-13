// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    DataSeriesFile implementation
*/

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/resource.h>
#include <sys/time.h>

#include <ostream>

#include <boost/static_assert.hpp>

#include <Lintel/Double.hpp>
#include <Lintel/HashTable.hpp>
#include <Lintel/LintelLog.hpp>

#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/ExtentField.hpp>

using namespace std;
using boost::format;

#if (_FILE_OFFSET_BITS == 64 && !defined(_LARGEFILE64_SOURCE)) || defined(__CYGWIN__)
#define _LARGEFILE64_SOURCE
#define lseek64 lseek
#endif

#ifndef _LARGEFILE64_SOURCE
#error "Must compile with -D_LARGEFILE64_SOURCE"
#endif

#ifdef __CYGWIN__
#define O_LARGEFILE 0
#endif

DataSeriesSource::DataSeriesSource(const string &_filename)
    : filename(_filename), fd(-1), cur_offset(0)
{
    reopenfile();
    mylibrary.registerType(ExtentType::getDataSeriesXMLType());
    mylibrary.registerType(ExtentType::getDataSeriesIndexTypeV0());
    INVARIANT(mylibrary.getTypeByName("DataSeries: XmlType") 
	      == &ExtentType::getDataSeriesXMLType(), "internal");
    Extent::ByteArray data;
    const int file_header_size = 2*4 + 4*8;
    data.resize(file_header_size);
    Extent::checkedPread(fd,0,data.begin(),file_header_size);
    cur_offset = file_header_size;
    INVARIANT(data[0] == 'D' && data[1] == 'S' &&
	      data[2] == 'v' && data[3] == '1',
	      "Invalid data series source, not DSv1");
    typedef ExtentType::int32 int32;
    typedef ExtentType::int64 int64;
    int32 check_int = *(int32 *)(data.begin() + 4);
    if (check_int == 0x12345678) {
	need_bitflip = false;
    } else if (check_int == 0x78563412) {
	need_bitflip = true;
    } else {
	FATAL_ERROR(boost::format("Unable to interpret check integer %x")
		    % check_int);
    }
    if (need_bitflip) {
	Extent::flip4bytes(data.begin()+4);
	Extent::flip8bytes(data.begin()+8);
	Extent::flip8bytes(data.begin()+16);
	Extent::flip8bytes(data.begin()+24);
	Extent::flip8bytes(data.begin()+32);
    }
    INVARIANT(*(int32 *)(data.begin() + 4) == 0x12345678,
	      "int32 check failed");
    INVARIANT(*(int64 *)(data.begin() + 8) == 0x123456789ABCDEF0LL,
	      "int64 check failed");
    INVARIANT(fabs(3.1415926535897932384 - *(double *)(data.begin() + 16)) 
	      < 1e-18, "fixed double check failed");
    INVARIANT(*(double *)(data.begin() + 24) == Double::Inf,
	      "infinity double check failed");
    INVARIANT(*(double *)(data.begin() + 32) != *(double *)(data.begin() + 32),
	      "NaN double check failed");
    Extent::ByteArray extentdata;
    INVARIANT(Extent::preadExtent(fd,cur_offset,extentdata,need_bitflip),
	      "Invalid file, must have a first extent");
    Extent *e = new Extent(mylibrary,extentdata,need_bitflip);
    INVARIANT(&e->type == &ExtentType::getDataSeriesXMLType(),
	      "First extent must be the type defining extent");

    ExtentSeries type_extent_series(e);
    Variable32Field typevar(type_extent_series,"xmltype");
    for(;type_extent_series.pos.morerecords();++type_extent_series.pos) {
	string v = typevar.stringval();
	mylibrary.registerType(v);
    }
    delete e;
    struct stat ds_file_stats;
    int ret_val = fstat(fd,&ds_file_stats);
    INVARIANT(ret_val == 0,
	      boost::format("fstat failed: %s")
	      % strerror(errno));
    BOOST_STATIC_ASSERT(sizeof(ds_file_stats.st_size) >= 8); // won't handle large files correctly unless this is true.
    off64_t tailoffset = ds_file_stats.st_size-7*4;
    INVARIANT(tailoffset > 0, "file is too small to be a dataseries file??");
    byte tail[7*4];
    Extent::checkedPread(fd,tailoffset,tail,7*4);
    DataSeriesSink::verifyTail(tail,need_bitflip,filename);
    if (need_bitflip) {
	Extent::flip4bytes(tail+4);
	Extent::flip8bytes(tail+16);
    }
    int32 packedsize = *(int32 *)(tail + 4);
    off64_t indexoffset = *(int64 *)(tail + 16);
    INVARIANT(tailoffset - packedsize == indexoffset,
	      boost::format("mismatch on index offset %d - %d != %d!")
	      % tailoffset % packedsize % indexoffset);
    indexExtent = preadExtent(indexoffset);
    INVARIANT(indexExtent != NULL, "index extent read failed");
}

DataSeriesSource::~DataSeriesSource()
{
    if (isactive()) {
	closefile();
    }
    delete indexExtent;
}

void
DataSeriesSource::closefile()
{
    CHECKED(close(fd) == 0,
	    boost::format("close failed: %s") % strerror(errno));
    fd = -1;
}

void
DataSeriesSource::reopenfile()
{
    INVARIANT(fd == -1, "trying to reopen non-closed source?!");
    fd = open(filename.c_str(), O_RDONLY | O_LARGEFILE);
    INVARIANT(fd >= 0,boost::format("error opening file '%s' for read: %s")
	      % filename % strerror(errno));
}

Extent *
DataSeriesSource::preadExtent(off64_t &offset, unsigned *compressedSize) {
    Extent::ByteArray extentdata;
    
    off64_t save_offset = offset;
    if (Extent::preadExtent(fd, offset, extentdata, need_bitflip) == false) {
	return NULL;
    }
    if (compressedSize) *compressedSize = extentdata.size();
    Extent *ret = new Extent(mylibrary,extentdata,need_bitflip);
    ret->extent_source = filename;
    ret->extent_source_offset = save_offset;
    INVARIANT(&ret->type != &ExtentType::getDataSeriesXMLType(),
	      "Invalid to have a type extent after the first extent.");
    return ret;
}

class DataSeriesSinkPThreadCompressor : public PThread {
public:
    DataSeriesSinkPThreadCompressor(DataSeriesSink *_mine)
	: mine(_mine) { }

    virtual ~DataSeriesSinkPThreadCompressor() { }

    virtual void *run() {
	mine->compressorThread();
	return NULL;
    }
    DataSeriesSink *mine;
};

class DataSeriesSinkPThreadWriter : public PThread {
public:
    DataSeriesSinkPThreadWriter(DataSeriesSink *_mine)
	: mine(_mine) { }

    virtual ~DataSeriesSinkPThreadWriter() { }

    virtual void *run() {
	mine->writerThread();
	return NULL;
    }
    DataSeriesSink *mine;
};

int DataSeriesSink::compressor_count = -1;

DataSeriesSink::DataSeriesSink(const string &_filename,
			       int _compression_modes,
			       int _compression_level)
    : index_series(ExtentType::getDataSeriesIndexTypeV0()),
      index_extent(index_series),
      field_extentOffset(index_series,"offset"),
      field_extentType(index_series,"extenttype"),
      wrote_library(false),
      compression_modes(_compression_modes),
      compression_level(_compression_level),
      chained_checksum(0), 
      bytes_in_progress(0), max_bytes_in_progress(256*1024*1024),
      shutdown_workers(false), filename(_filename)
{
    stats.packed_size += 2*4 + 4*8;

    if (filename == "-") {
	fd = fileno(stdout);
    } else {
	fd = open(filename.c_str(), 
		  O_WRONLY | O_LARGEFILE | O_CREAT | O_TRUNC, 0666);
    }
    INVARIANT(fd >= 0, boost::format("Error opening %s for write: %s")
	      % filename % strerror(errno));
    const string filetype = "DSv1";
    checkedWrite(filetype.data(),4);
    ExtentType::int32 int32check = 0x12345678;
    checkedWrite(&int32check,4);
    ExtentType::int64 int64check = 0x123456789ABCDEF0LL;
    checkedWrite(&int64check,8);
    double doublecheck = 3.1415926535897932384; 
    checkedWrite(&doublecheck,8);
    doublecheck = Double::Inf;
    checkedWrite(&doublecheck,8);
    doublecheck = Double::NaN;
    checkedWrite(&doublecheck,8);
    cur_offset = 2*4 + 4*8;
    int pthread_count = compressor_count;
    if (pthread_count == -1) {
	pthread_count = PThreadMisc::getNCpus();
    }
    
    for(int i=0; i < pthread_count; ++i) {
	DataSeriesSinkPThreadCompressor *t 
	    = new DataSeriesSinkPThreadCompressor(this);
	compressors.push_back(t);
	t->start();
    }
    if (compressors.empty()) {
	writer = NULL;
    } else {
	writer = new DataSeriesSinkPThreadWriter(this);
	writer->start();
    }
}

DataSeriesSink::~DataSeriesSink()
{
    if (cur_offset > 0) {
	close();
    }
}

void
DataSeriesSink::close()
{
    INVARIANT(wrote_library,
	      "error: never wrote the extent type library?!");
    INVARIANT(cur_offset >= 0, "error: close called twice?!");

    mutex.lock();
    shutdown_workers = true;
    available_work_cond.broadcast();
    available_write_cond.broadcast();
    mutex.unlock();

    for(vector<PThread *>::iterator i = compressors.begin();
	i != compressors.end(); ++i) {
	(**i).join();
    }
    writer->join();

    writeOutPending();
    INVARIANT(pending_work.empty() && bytes_in_progress == 0, "bad");
    ExtentType::int64 index_offset = cur_offset;
    
    // TODO: make a warning and/or test case for this?

    // Special case handling of record for index series; this will
    // present "difficulties" in the future when we want to put the
    // compression type into the index series since we don't know that
    // until after we've already compressed the data.
    index_series.newRecord(); 
    field_extentOffset.set(cur_offset);
    field_extentType.set(index_extent.type.name);

    mutex.lock();
    bytes_in_progress += index_extent.size();
    pending_work.push_back(new toCompress(index_extent, NULL));
    pending_work.front()->in_progress = true;
    lockedProcessToCompress(pending_work.front());

    INVARIANT(bytes_in_progress == pending_work.front()->compressed.size(), "bad");
    INVARIANT(pending_work.size() == 1, "bad");
    INVARIANT(pending_work.front()->readyToWrite(), "bad");
    uint32_t packed_size = pending_work.front()->compressed.size();
    mutex.unlock();
    writeOutPending();

    mutex.lock();
    INVARIANT(pending_work.empty() && bytes_in_progress == 0, 
	      boost::format("bad %d %d") % pending_work.empty()
	      % bytes_in_progress);

    char *tail = new char[7*4];
    INVARIANT((reinterpret_cast<unsigned long>(tail) % 8) == 0, 
	      "malloc alignment glitch?!");
    for(int i=0;i<4;i++) {
	tail[i] = 0xFF;
    }
    typedef ExtentType::int32 int32;
    *(int32 *)(tail + 4) = packed_size;
    *(int32 *)(tail + 8) = ~packed_size;
    *(int32 *)(tail + 12) = chained_checksum;
    *(ExtentType::int64 *)(tail + 16) = (ExtentType::int64)index_offset;
    *(int32 *)(tail + 24) = BobJenkinsHash(1776,tail,6*4);
    checkedWrite(tail,7*4);
    delete [] tail;
    int ret = ::close(fd);
    INVARIANT(ret == 0, boost::format("close failed: %s") % strerror(errno));
    fd = -1;
    cur_offset = -1;
    index_series.clearExtent();
    index_extent.clear();
    mutex.unlock();
}

void
DataSeriesSink::checkedWrite(const void *buf, int bufsize)
{
    ssize_t ret = write(fd,buf,bufsize);
    INVARIANT(ret != -1,
	      boost::format("Error on write of %d bytes: %s")
	      % bufsize % strerror(errno));
    INVARIANT(ret == bufsize,
	      boost::format("Partial write %d bytes out of %d bytes (disk full?): %s")
	      % ret % bufsize % strerror(errno));
}

void
DataSeriesSink::writeExtent(Extent &e, Stats *stats)
{
    INVARIANT(wrote_library,
	      "must write extent type library before writing extents!\n");
    INVARIANT(valid_types[&e.type],
	      boost::format("type %s (%p) wasn't in your type library")
	      % e.type.name % &e.type);
    INVARIANT(!shutdown_workers,
	      "must not call writeExtent after calling close()");
    queueWriteExtent(e, stats);
}

void
DataSeriesSink::writeExtentLibrary(ExtentTypeLibrary &lib)
{
    INVARIANT(!wrote_library, "Can only write extent library once");
    ExtentSeries type_extent_series(ExtentType::getDataSeriesXMLType());
    Extent type_extent(type_extent_series);

    Variable32Field typevar(type_extent_series,"xmltype");
    for(map<const string, const ExtentType *>::iterator i = lib.name_to_type.begin();
	i != lib.name_to_type.end();++i) {
	const ExtentType *et = i->second;
	if (et->name == "DataSeries: XmlType") {
	    continue; // no point of writing this out; can't use it.
	}

	type_extent_series.newRecord();
	INVARIANT(et->xmldesc.size() > 0, "whoa extenttype has no xml data?!");
	typevar.set(et->xmldesc.data(),et->xmldesc.size());
	valid_types[et] = true;
	if ((et->majorVersion() == 0 && et->minorVersion() == 0) ||
	    et->getNamespace().empty()) {
	    // Once we have a version of dsrepack/dsselect that can
	    // change the XML type, we can make this an error.
	    cerr << boost::format("Warning: type '%s' is missing either a version or a namespace")
		% et->name << endl;
	}
    }
    queueWriteExtent(type_extent, NULL);
    mutex.lock();
    INVARIANT(!wrote_library, "bad, two calls to writeExtentLibrary()");
    wrote_library = true; 
    mutex.unlock();
}

void
DataSeriesSink::removeStatsUpdate(Stats *would_update)
{
    // need this to keep stats updates in processToCompress from getting a half-written pointer
    PThreadAutoLocker lock1(Stats::getMutex()); 
    // need this to keep anyone else from changing pending_work while we fiddle with it.
    PThreadAutoLocker lock2(mutex);

    for(Deque<toCompress *>::iterator i = pending_work.begin();
	i != pending_work.end(); ++i) {
	if ((**i).to_update == would_update) {
	    INVARIANT(would_update->use_count > 0, "internal");
	    --would_update->use_count;
	    (**i).to_update = NULL;
	}
    }
}

void
DataSeriesSink::verifyTail(ExtentType::byte *tail,
			   bool need_bitflip,
			   const string &filename)
{
    // Only thing we can't check here is a match between the offset of
    // the tail and the offset stored in the tail.
    for(int i=0;i<4;i++) {
	INVARIANT(tail[i] == 0xFF,
		  boost::format("bad header for the tail of %s!") % filename);
    }
    typedef ExtentType::int32 int32;
    int32 packed_size = *(int32 *)(tail + 4);
    int32 tilde_packed_size = *(int32 *)(tail + 8);
    int32 bjhash = *(int32 *)(tail + 24);
    if (need_bitflip) {
	packed_size = Extent::flip4bytes(packed_size);
	tilde_packed_size = Extent::flip4bytes(tilde_packed_size);
	bjhash = Extent::flip4bytes(bjhash);
    }
    INVARIANT(packed_size == ~tilde_packed_size,
	      "bad packed size in the tail!");
    int32 check_bjhash = BobJenkinsHash(1776,tail,6*4);
    INVARIANT(bjhash == check_bjhash, "bad hash in the tail!");
}

void DataSeriesSink::setCompressorCount(int count) {
    INVARIANT(count >= -1, "?");
    compressor_count = count;
}

void DataSeriesSink::setMaxBytesInProgress(size_t nbytes) {
    PThreadScopedLock lock(mutex);
    if (nbytes > max_bytes_in_progress) {
	// May be able to get both more work and more things queued.
	available_work_cond.broadcast();
	available_queue_cond.broadcast();
    }
    max_bytes_in_progress = nbytes;
}

void DataSeriesSink::queueWriteExtent(Extent &e, Stats *to_update) {
    if (to_update) {
	PThreadAutoLocker lock(Stats::getMutex());
	++to_update->use_count;
    }
    mutex.lock();
    INVARIANT(!shutdown_workers, "got to qWE after call to close()??");
    INVARIANT(cur_offset > 0, "queueWriteExtent on closed file");
    LintelLogDebug("DataSeriesSink", format("queueWriteExtent(%d bytes)")
		   % e.size());
    bytes_in_progress += e.size(); // putting this into toCompress erases e
    pending_work.push_back(new toCompress(e, to_update));

    if (compressors.empty()) {
	SINVARIANT(pending_work.size() == 1 && bytes_in_progress == 0);
	pending_work.front()->in_progress = true;
	bytes_in_progress += e.size();
	lockedProcessToCompress(pending_work.front());
	pending_work.front()->in_progress = false;
	mutex.unlock();
	writeOutPending();
	SINVARIANT(bytes_in_progress == 0);
	return;
    } 
	
    available_work_cond.signal();
    if (false) cout << boost::format("qwe wait? %d %d\n") % bytes_in_progress % pending_work.size();
    while(!canQueueWork()) {
	LintelLogDebug("DataSeriesSink", 
		       format("after queueWriteExtent %d >= %d || %d >= %d")
		       % bytes_in_progress % max_bytes_in_progress 
		       % pending_work.size() % (2 * compressors.size()));
	available_queue_cond.wait(mutex);
    }
    mutex.unlock();
	
}

void
DataSeriesSink::flushPending()
{
    mutex.lock();
    while(bytes_in_progress > 0) {
	available_queue_cond.wait(mutex);
    }
    mutex.unlock();
}

DataSeriesSink::Stats DataSeriesSink::getStats() {
    // Make a copy so it's thread safe.
    PThreadScopedLock lock(Stats::getMutex());
    Stats ret = stats; 
    return ret;
}

void
DataSeriesSink::writeOutPending(bool have_lock)
{
    if (!have_lock) {
	mutex.lock();
    }
    Deque<toCompress *> to_write;
    while(!pending_work.empty() 
	  && pending_work.front()->readyToWrite()) {
	pending_work.front()->wipeExtent();
	to_write.push_back(pending_work.front());
	pending_work.pop_front();
    }
    mutex.unlock();

    size_t bytes_written = 0;
    while(!to_write.empty()) {
	toCompress *tc = to_write.front();
	to_write.pop_front();
	INVARIANT(cur_offset > 0,"Error: writeoutPending on closed file\n");
	index_series.newRecord();
	field_extentOffset.set(cur_offset);
	field_extentType.set(tc->extent.type.name);
	
	checkedWrite(tc->compressed.begin(), tc->compressed.size());
	cur_offset += tc->compressed.size();
	chained_checksum 
	    = BobJenkinsHashMix3(tc->checksum, chained_checksum, 1972);
	bytes_written += tc->compressed.size();
	delete tc;
    }

    mutex.lock();
    INVARIANT(bytes_in_progress >= bytes_written, 
	      boost::format("internal %d %d") 
	      % bytes_in_progress % bytes_written);
    bytes_in_progress -= bytes_written;
    if (false) cout << boost::format("qwe broadcast wop? %d %d\n") % bytes_in_progress % pending_work.size();
    if (canQueueWork()) {
	// Don't say there is free space until we actually finished writing.
	available_queue_cond.broadcast();
    }
    mutex.unlock();
}

static void get_thread_cputime(struct timespec &ts)
{
    ts.tv_sec = 0; ts.tv_nsec = 0;

    // getrusage combines all the threads together so results in
    // massive over-counting.  clock_gettime can go backwards on
    // RHEL4u4 on opteron2216HE's.  Probably the only option will be
    // to get this out of /proc on linux, and who knows what on other
    // platforms.

    return;
    // 
    // #include <sys/syscall.h>
//    long ret = syscall(__NR_clock_gettime, CLOCK_THREAD_CPUTIME_ID, &ts);
//
//    INVARIANT(ret == 0 && ts.tv_sec >= 0 && ts.tv_nsec >= 0, "??");
//    return ts.tv_sec + ts.tv_nsec*1.0e-9;
}

// This function assumes that bytes_in_progress was updated to the
// uncompressed size prior to calling the function.
void
DataSeriesSink::lockedProcessToCompress(toCompress *work)
{
    // TODO: consider switching all of the size updates over to
    // looking at the reserved space rather than the used space.
    INVARIANT(bytes_in_progress >= work->extent.size(), "internal");
    size_t uncompressed_size = work->extent.size();
    bytes_in_progress += uncompressed_size; // could temporarily be 2*e.size in worst case if we are trying multiple algorithms
    LintelLogDebug("DataSeriesSink", 
		   format("compress(%d bytes), in progress %d bytes")
		   % work->extent.size() % bytes_in_progress);

    INVARIANT(work->in_progress, "??");
    INVARIANT(cur_offset > 0,"Error: processToCompress on closed file\n");

    mutex.unlock();

    struct timespec pack_start, pack_end;
    get_thread_cputime(pack_start);
    
    int headersize, fixedsize, variablesize;
    work->checksum = work->extent.packData(work->compressed, compression_modes,
					   compression_level, &headersize,
					   &fixedsize, &variablesize);
    get_thread_cputime(pack_end);

    double pack_extent_time = (pack_end.tv_sec - pack_start.tv_sec) 
	+ (pack_end.tv_nsec - pack_start.tv_nsec)*1e-9;
    
    INVARIANT(pack_extent_time >= 0, 
	      boost::format("get_thread_cputime broken? %d.%d - %d.%d = %.9g")
	      % pack_end.tv_sec % pack_end.tv_nsec 
	      % pack_start.tv_sec % pack_start.tv_nsec % pack_extent_time);
    // Slightly less efficient than calling update on the two separate stats,
    // but easier to code.
    Stats tmp;
    tmp.update(headersize + fixedsize + variablesize, fixedsize,
	       work->extent.variabledata.size(), variablesize, 
	       work->compressed.size(), 
	       *reinterpret_cast<uint32_t *>(work->compressed.begin()+4), 
	       pack_extent_time, work->compressed[6*4], 
	       work->compressed[6*4+1]);

    INVARIANT(work->compressed.size() > 0, "??");

    {   // update stats, have to do this before we complete the extent
	// as otherwise the work pointer could vanish under us
	PThreadAutoLocker lock(Stats::getMutex());
	stats += tmp;
	if (work->to_update != NULL) {
	    *work->to_update += tmp;
	    INVARIANT(work->to_update->use_count > 0, "internal");
	    --work->to_update->use_count;
	    work->to_update = NULL;
	}
    }
    INVARIANT(work->extent.size() == uncompressed_size, "internal");
    work->extent.clear();
    mutex.lock();
    work->in_progress = false; 
    INVARIANT(!pending_work.empty(), "bad");
    
    INVARIANT(bytes_in_progress >= 2 * uncompressed_size, "internal");
    // subtract the temporary from above and the cleared extent.
    bytes_in_progress -= 2*uncompressed_size; 
    bytes_in_progress += work->compressed.size(); // add in the compressed bits
}

void 
DataSeriesSink::compressorThread() 
{
#if 0
    // This didn't seem to have any actual effect; it should have let the
    // copy thread run at 100%, but it didn't seem to have that effect.
    int policy = -1;
    struct sched_param param;

    INVARIANT(pthread_getschedparam(pthread_self(), &policy, &param) == 0, 
	      "bad");

    int minprio = sched_get_priority_max(policy);
    if (param.sched_priority > minprio) {
	param.sched_priority = minprio;
	INVARIANT(pthread_setschedparam(pthread_self(), policy, &param) == 0,
		  "bad");
    }
#endif
    mutex.lock();
    while(true) {
	toCompress *work = NULL;
	for(Deque<toCompress *>::iterator i = pending_work.begin();
	    i != pending_work.end(); ++i) {
	    if ((**i).compressed.size() == 0 &&
		(**i).in_progress == false) {
		work = *i;
		work->in_progress = true;
		break;
	    }
	}
	if (work == NULL) {
	    if (shutdown_workers) {
		break;
	    }
	    available_work_cond.wait(mutex);
	} else {
	    lockedProcessToCompress(work);
	    if (false) cout << boost::format("qwe broadcast compr? %d %d\n") % bytes_in_progress % pending_work.size();
    
	    if (canQueueWork()) { // may be able to queue work since we just freed up space.
		available_queue_cond.broadcast();
	    }
	    if (pending_work.front()->readyToWrite()) {
		available_write_cond.signal();
	    }
	}
    }
    mutex.unlock();
}

void
DataSeriesSink::writerThread()
{
    mutex.lock();
    while(!shutdown_workers) {
	if (!pending_work.empty() &&
	    pending_work.front()->readyToWrite()) {
	    writeOutPending(true);
	    mutex.lock();
	} else {
	    available_write_cond.wait(mutex);
	}
    }
    mutex.unlock();
}

void
DataSeriesSink::Stats::reset()
{
    use_count = 0;

    extents = compress_none = compress_lzo = compress_gzip 
	= compress_bz2 = compress_lzf = 0;
    unpacked_size = unpacked_fixed = unpacked_variable 
	= unpacked_variable_raw = packed_size = 0;
    pack_time = 0;
}

DataSeriesSink::Stats::~Stats()
{
    INVARIANT(use_count == 0, 
	      boost::format("deleting Stats %p before %d == use_count == 0\n"
			    "you need to have called sink.removeStatsUpdate()")
	      % this % use_count);
}

DataSeriesSink::Stats &
DataSeriesSink::Stats::operator+=(const DataSeriesSink::Stats &from)
{
    extents += from.extents;
    compress_none += from.compress_none;
    compress_lzo += from.compress_lzo;
    compress_gzip += from.compress_gzip;
    compress_bz2 += from.compress_bz2;
    compress_lzf += from.compress_lzf;
    unpacked_size += from.unpacked_size;
    unpacked_fixed += from.unpacked_fixed;
    unpacked_variable += from.unpacked_variable;
    unpacked_variable_raw += from.unpacked_variable_raw;
    packed_size += from.packed_size;
    INVARIANT(from.pack_time >= 0, 
	      boost::format("from.pack_time = %.6g < 0") % from.pack_time);
    pack_time += from.pack_time;
    return *this;
}

DataSeriesSink::Stats &
DataSeriesSink::Stats::operator-=(const DataSeriesSink::Stats &from)
{
    extents -= from.extents;
    compress_none -= from.compress_none;
    compress_lzo -= from.compress_lzo;
    compress_gzip -= from.compress_gzip;
    compress_bz2 -= from.compress_bz2;
    compress_lzf -= from.compress_lzf;
    unpacked_size -= from.unpacked_size;
    unpacked_fixed -= from.unpacked_fixed;
    unpacked_variable -= from.unpacked_variable;
    unpacked_variable_raw -= from.unpacked_variable_raw;
    packed_size -= from.packed_size;
    pack_time -= from.pack_time;

    return *this;
}

DataSeriesSink::Stats &
DataSeriesSink::Stats::operator =(const Stats &from)
{
    if (this == &from) {
	return *this;
    }
    memcpy(this, &from, sizeof(from));
    this->use_count = 0;
    return *this;
}

void
DataSeriesSink::Stats::update(uint32_t unp_size, uint32_t unp_fixed, 
			      uint32_t unp_var_raw, uint32_t unp_variable, 
			      uint32_t pkd_size, uint32_t pkd_var_size, 
			      double pkd_time, 
			      unsigned char fixed_compress_mode,
			      unsigned char variable_compress_mode)
{
    ++extents;
    unpacked_size += unp_size;
    unpacked_fixed += unp_fixed;
    unpacked_variable_raw += unp_var_raw;
    unpacked_variable += unp_variable;
    packed_size += pkd_size;
    INVARIANT(pkd_time >= 0, 
	      boost::format("update(pkd_time = %.6g < 0)") % pkd_time);
    pack_time += pkd_time;
    updateCompressMode(fixed_compress_mode);
    if (pkd_var_size > 0) {
	updateCompressMode(variable_compress_mode);
    }
}

void
DataSeriesSink::Stats::updateCompressMode(unsigned char compress_mode)
{
    switch(compress_mode) 
	{
	case 0: ++compress_none; break;
	case 1: ++compress_lzo; break;
	case 2: ++compress_gzip; break;
	case 3: ++compress_bz2; break;
	case 4: ++compress_lzf; break;
	default:
	    FATAL_ERROR(boost::format("whoa, unknown compress option %d\n") 
			% static_cast<unsigned>(compress_mode));
	}
}

// TODO: make this take a int64 records argument also, default -1, and
// print out record counts, etc, like logfu2ds.C
void
DataSeriesSink::Stats::printText(ostream &to, const string &extent_type)
{
    if (extent_type.empty()) {
	to << boost::format("  wrote %d extents\n")
	    % extents;
    } else {
	to << boost::format("  wrote %d extents of type %s\n")
	    % extents % extent_type;
    }

    to << boost::format("  compression (none,lzo,gzip,bz2,lzf): (%d,%d,%d,%d,%d)\n")
	% compress_none % compress_lzo % compress_gzip % compress_bz2 
	% compress_lzf;
    to << boost::format("  unpacked: %d = %d (fixed) + %d (variable, %d raw)\n")
	% unpacked_size % unpacked_fixed % unpacked_variable 
	% unpacked_variable_raw;
    to << boost::format("  packed size: %d; pack time: %.3f\n")
	% packed_size % pack_time;
}

PThreadMutex &
DataSeriesSink::Stats::getMutex()
{
    static PThreadMutex mutex;
    
    return mutex;
}
