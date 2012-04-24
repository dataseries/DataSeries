#include <DataSeries/DataSeriesSink.hpp>

#include <fcntl.h>

#include <Lintel/LintelLog.hpp>
#include <Lintel/HashFns.hpp>

dataseries::IExtentSink::~IExtentSink() { }

using namespace std;
using boost::format;

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

void DataSeriesSink::WorkerInfo::startThreads(PThreadScopedLock &lock, DataSeriesSink *sink) {
    int pthread_count = compressor_count;
    if (pthread_count == -1) {
	pthread_count = PThreadMisc::getNCpus();
    }
    
    for(int i=0; i < pthread_count; ++i) {
	compressors.push_back(new DataSeriesSinkPThreadCompressor(sink));
	compressors.back()->start();
    }
    if (compressors.empty()) {
        writer = NULL;
    } else {
        writer = new DataSeriesSinkPThreadWriter(sink);
        writer->start();
    }
}

void DataSeriesSink::WorkerInfo::stopThreads(PThreadScopedLock &lock) {
    keep_going = false;
    
    available_work_cond.broadcast();
    available_write_cond.broadcast();

    PThreadScopedUnlock unlock(lock);

    for(vector<PThread *>::iterator i = compressors.begin();
        i != compressors.end(); ++i) {
        (**i).join();
        delete *i;
    }
    compressors.clear();
    writer->join();
    delete writer;
    writer = NULL;
}

void DataSeriesSink::WorkerInfo::setMaxBytesInProgress(PThreadMutex &mutex, size_t nbytes) {
    PThreadScopedLock lock(mutex);
    if (nbytes > max_bytes_in_progress) {
	// May be able to get both more work and more things queued.
	available_work_cond.broadcast();
	available_queue_cond.broadcast();
    }
    max_bytes_in_progress = nbytes;
}    

void DataSeriesSink::WorkerInfo::flushPending(PThreadMutex &mutex) {
    PThreadScopedLock lock(mutex);
    while (bytes_in_progress > 0) {
	available_queue_cond.wait(mutex);
    }
}


DataSeriesSink::DataSeriesSink(int compression_modes, int compression_level)
    : stats(), mutex(), valid_types(), compression_modes(compression_modes),
      compression_level(compression_level), writer_info(), 
      worker_info(256*1024*1024), filename()
{ }

DataSeriesSink::DataSeriesSink(const string &filename, int compression_modes,
			       int compression_level)
    : stats(), mutex(), valid_types(), compression_modes(compression_modes),
      compression_level(compression_level), writer_info(),
      worker_info(256*1024*1024), filename()
{
    open(filename);
}

DataSeriesSink::~DataSeriesSink() {
    if (writer_info.cur_offset > 0) {
	close();
    }
}

void DataSeriesSink::setExtentWriteCallback(const ExtentWriteCallback &callback) {
    PThreadScopedLock lock(mutex);
    writer_info.extent_write_callback = callback;
}

void DataSeriesSink::open(const string &in_filename) {
    PThreadScopedLock lock(mutex);

    SINVARIANT(worker_info.isQuiesced() && writer_info.isQuiesced() 
               && stats.extents == 0 && stats.pack_time == 0);
    filename = in_filename;

    stats.packed_size += 2*4 + 4*8;

    INVARIANT(filename != "-", "opening stdout as a file isn't expected to work, and '-' as a filename makes little sense");
    writer_info.fd = ::open(filename.c_str(), O_WRONLY | O_LARGEFILE | O_CREAT | O_TRUNC, 0666);
    INVARIANT(writer_info.fd >= 0,
              format("Error opening %s for write: %s") % filename % strerror(errno));
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
    writer_info.index_series.newExtent();
    writer_info.cur_offset = 2*4 + 4*8;
    worker_info.keep_going = true;
    worker_info.startThreads(lock, this);
}

void DataSeriesSink::close(bool do_fsync, Stats *to_update) {
    PThreadScopedLock lock(mutex);

    INVARIANT(writer_info.wrote_library,
              "error: never wrote the extent type library?!");
    INVARIANT(writer_info.cur_offset >= 0, "error: close called twice?!");

    worker_info.stopThreads(lock);
    writer_info.writeOutPending(lock, worker_info);

    SINVARIANT(worker_info.pending_work.empty() && worker_info.bytes_in_progress == 0);
    ExtentType::int64 index_offset = writer_info.cur_offset;
    
    // Special case handling of record for index series; this will
    // present "difficulties" in the future when we want to put the
    // compression type into the index series since we don't know that
    // until after we've already compressed the data.
    writer_info.index_series.newRecord(); 
    writer_info.field_extentOffset.set(writer_info.cur_offset);
    writer_info.field_extentType.set(writer_info.index_series.getExtentRef().getType().getName());

    worker_info.bytes_in_progress += writer_info.index_series.getExtentRef().size();
    worker_info.pending_work.push_back
        (new ToCompress(writer_info.index_series.getSharedExtent(), NULL));
    worker_info.pending_work.front()->in_progress = true;
    lockedProcessToCompress(lock, worker_info.pending_work.front());

    SINVARIANT(worker_info.bytes_in_progress 
               == worker_info.pending_work.front()->compressed.size());
    SINVARIANT(worker_info.pending_work.size() == 1);
    SINVARIANT(worker_info.pending_work.front()->readyToWrite());
    uint32_t packed_size = worker_info.pending_work.front()->compressed.size();

    writer_info.writeOutPending(lock, worker_info);

    INVARIANT(worker_info.pending_work.empty() && worker_info.bytes_in_progress == 0, 
	      format("bad %d %d") % worker_info.pending_work.empty()
              % worker_info.bytes_in_progress);

    char *tail = new char[7*4];
    INVARIANT((reinterpret_cast<unsigned long>(tail) % 8) == 0, 
	      "malloc alignment glitch?!");
    for(int i=0;i<4;i++) {
	tail[i] = 0xFF;
    }
    typedef ExtentType::int32 int32;
    *(int32 *)(tail + 4) = packed_size;
    *(int32 *)(tail + 8) = ~packed_size;
    *(int32 *)(tail + 12) = writer_info.chained_checksum;
    *(ExtentType::int64 *)(tail + 16) = (ExtentType::int64)index_offset;
    *(int32 *)(tail + 24) = lintel::bobJenkinsHash(1776,tail,6*4);
    checkedWrite(tail,7*4);
    delete [] tail;
    if (do_fsync) {
        fsync(writer_info.fd);
    }
    int ret = ::close(writer_info.fd);
    INVARIANT(ret == 0, format("close failed: %s") % strerror(errno));
    writer_info.fd = -1;
    writer_info.wrote_library = false;
    writer_info.cur_offset = -1;
    writer_info.chained_checksum = 0;
    writer_info.index_series.clearExtent();
    if (to_update != NULL) {
        *to_update += stats;
    }
    stats.reset();
}

void DataSeriesSink::rotate(const string &new_filename, const ExtentTypeLibrary &library,
                            bool do_fsync, Stats *to_update) {
    FATAL_ERROR("unimplemented");
}

void DataSeriesSink::WriterInfo::checkedWrite(const void *buf, int bufsize) {
    ssize_t ret = write(fd, buf, bufsize);
    INVARIANT(ret != -1, format("Error on write of %d bytes: %s") % bufsize % strerror(errno));
    INVARIANT(ret == bufsize, format("Partial write %d bytes out of %d bytes (disk full?): %s")
	      % ret % bufsize % strerror(errno));
}

void DataSeriesSink::writeExtent(Extent &e, Stats *stats) {
    INVARIANT(writer_info.wrote_library,
	      "must write extent type library before writing extents!\n");
    INVARIANT(valid_types.exists(e.type), format("type %s (%p) wasn't in your type library")
	      % e.getType().getName() % &e.getType());
    INVARIANT(worker_info.keep_going, "must not call writeExtent after calling close()");
    
    Extent::Ptr we(new Extent(e.getType()));
    we->swap(e);
    
    queueWriteExtent(we, stats);
}

void DataSeriesSink::writeExtentLibrary(const ExtentTypeLibrary &lib) {
    INVARIANT(!writer_info.wrote_library, "Can only write extent library once");
    ExtentSeries type_extent_series(ExtentType::getDataSeriesXMLType());
    type_extent_series.newExtent();

    Variable32Field typevar(type_extent_series,"xmltype");
    for(ExtentTypeLibrary::NameToType::const_iterator i = lib.name_to_type.begin();
	i != lib.name_to_type.end();++i) {
	const ExtentType::Ptr et = i->second;
	if (et->getName() == "DataSeries: XmlType") {
	    continue; // no point of writing this out; can't use it.
	}

	type_extent_series.newRecord();
	const string &type_desc(et->getXmlDescriptionString());
	INVARIANT(!type_desc.empty(), "whoa extenttype has no xml data?!");
	typevar.set(type_desc);
	valid_types.add(et);
	if ((et->majorVersion() == 0 && et->minorVersion() == 0) ||
	    et->getNamespace().empty()) {
	    // Once we have a version of dsrepack/dsselect that can
	    // change the XML type, we can make this an error.
	    cerr << format("Warning: type '%s' is missing either a version or a namespace")
		% et->getName() << endl;
	}
    }
    queueWriteExtent(type_extent_series.getSharedExtent(), NULL);

    PThreadScopedLock lock(mutex);
    INVARIANT(!writer_info.wrote_library, "bad, two calls to writeExtentLibrary()");
    writer_info.wrote_library = true; 
}

void DataSeriesSink::removeStatsUpdate(Stats *would_update) {
    PThreadScopedLock lock(mutex);

    for(Deque<ToCompress *>::iterator i = worker_info.pending_work.begin();
	i != worker_info.pending_work.end(); ++i) {
	if ((**i).to_update == would_update) {
	    SINVARIANT(would_update->use_count > 0);
	    --would_update->use_count;
	    (**i).to_update = NULL;
	}
    }
}

void DataSeriesSink::verifyTail(ExtentType::byte *tail,
			   bool need_bitflip,
			   const string &filename) {
    // Only thing we can't check here is a match between the offset of
    // the tail and the offset stored in the tail.
    for(int i=0;i<4;i++) {
	INVARIANT(tail[i] == 0xFF, format("bad header for the tail of %s!") % filename);
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
    int32 check_bjhash = lintel::bobJenkinsHash(1776,tail,6*4);
    INVARIANT(bjhash == check_bjhash, "bad hash in the tail!");
}

void DataSeriesSink::setCompressorCount(int count) {
    INVARIANT(count >= -1, "?");
    compressor_count = count;
}

void DataSeriesSink::queueWriteExtent(Extent::Ptr e, Stats *to_update) {
    PThreadScopedLock lock(mutex);
    if (to_update) {
	++to_update->use_count;
    }
    INVARIANT(worker_info.keep_going, "got to qWE after call to close()??");
    INVARIANT(writer_info.cur_offset > 0, "queueWriteExtent on closed file");
    LintelLogDebug("DataSeriesSink", format("queueWriteExtent(%d bytes)") % e->size());
    worker_info.bytes_in_progress += e->size(); // putting this into ToCompress erases e
    worker_info.pending_work.push_back(new ToCompress(e, to_update));

    if (worker_info.compressors.empty()) {
	SINVARIANT(worker_info.pending_work.size() == 1 && worker_info.bytes_in_progress == 0);
	worker_info.pending_work.front()->in_progress = true;
	worker_info.bytes_in_progress += e->size();
	lockedProcessToCompress(lock, worker_info.pending_work.front());
	worker_info.pending_work.front()->in_progress = false;
	writer_info.writeOutPending(lock, worker_info);
	SINVARIANT(worker_info.bytes_in_progress == 0);
	return;
    } 
	
    worker_info.available_work_cond.signal();
    LintelLogDebug("DataSeriesSink", format("qwe wait? %d %d\n") % worker_info.bytes_in_progress
                   % worker_info.pending_work.size());
    while(!worker_info.canQueueWork()) {
        INVARIANT(worker_info.keep_going, "got to qWE after call to close()??");
        INVARIANT(writer_info.cur_offset > 0, "queueWriteExtent on closed file");
	LintelLogDebug("DataSeriesSink", format("after queueWriteExtent %d >= %d || %d >= %d")
		       % worker_info.bytes_in_progress % worker_info.max_bytes_in_progress 
		       % worker_info.pending_work.size() % (2 * worker_info.compressors.size()));
	worker_info.available_queue_cond.wait(mutex);
    }
}

DataSeriesSink::Stats DataSeriesSink::getStats(Stats *from) {
    // Make a copy so it's thread safe.
    PThreadScopedLock lock(mutex);
    Stats ret = from == NULL ? stats : *from; 
    return ret;
}

void DataSeriesSink::WriterInfo::writeOutPending(PThreadScopedLock &lock, WorkerInfo &worker_info) {
    Deque<ToCompress *> to_write;
    while (worker_info.frontReadyToWrite()) {
	to_write.push_back(worker_info.pending_work.front());
	worker_info.pending_work.pop_front();
    }
    
    size_t bytes_written = 0;
    {
        ExtentWriteCallback ewc(extent_write_callback);
        PThreadScopedUnlock unlock(lock);

        while(!to_write.empty()) {
            ToCompress *tc = to_write.front();
            to_write.pop_front();
            INVARIANT(cur_offset > 0,"Error: writeoutPending on closed file\n");
            
            if (ewc) {
                ewc(cur_offset, *tc->extent);
            }
            tc->wipeExtent();
            
            index_series.newRecord();
            field_extentOffset.set(cur_offset);
            field_extentType.set(tc->extent->getType().getName());
            
            checkedWrite(tc->compressed.begin(), tc->compressed.size());
            cur_offset += tc->compressed.size();
            chained_checksum = lintel::BobJenkinsHashMix3(tc->checksum, chained_checksum, 1972);
            bytes_written += tc->compressed.size();
            delete tc;
        }
    }

    INVARIANT(worker_info.bytes_in_progress >= bytes_written, format("internal %d %d") 
	      % worker_info.bytes_in_progress % bytes_written);
    worker_info.bytes_in_progress -= bytes_written;
    LintelLogDebug("DataSeriesSink", format("qwe broadcast wop? %d %d")
                   % worker_info.bytes_in_progress % worker_info.pending_work.size());
    if (worker_info.canQueueWork()) {
	// Don't say there is free space until we actually finished writing.
	worker_info.available_queue_cond.broadcast();
    }
}

static void get_thread_cputime(struct timespec &ts) {
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
void DataSeriesSink::lockedProcessToCompress(PThreadScopedLock &lock, ToCompress *work) {
    SINVARIANT(worker_info.bytes_in_progress >= work->extent->size());
    size_t uncompressed_size = work->extent->size();
    worker_info.bytes_in_progress += uncompressed_size; // could temporarily be 2*e.size in worst case if we are trying multiple algorithms
    LintelLogDebug("DataSeriesSink", format("compress(%d bytes), in progress %d bytes")
		   % work->extent->size() % worker_info.bytes_in_progress);

    INVARIANT(work->in_progress, "??");
    INVARIANT(writer_info.cur_offset > 0,"Error: processToCompress on closed file\n");

    Stats tmp;
    {
        PThreadScopedUnlock unlock(lock);

        size_t nrecords = work->extent->nRecords();
        struct timespec pack_start, pack_end;
        get_thread_cputime(pack_start);

        uint32_t headersize, fixedsize, variablesize;
        work->checksum = work->extent->packData(work->compressed, compression_modes,
                                               compression_level, &headersize,
                                               &fixedsize, &variablesize);
        get_thread_cputime(pack_end);

        double pack_extent_time = (pack_end.tv_sec - pack_start.tv_sec) 
            + (pack_end.tv_nsec - pack_start.tv_nsec)*1e-9;
    
        INVARIANT(pack_extent_time >= 0, format("get_thread_cputime broken? %d.%d - %d.%d = %.9g")
                  % pack_end.tv_sec % pack_end.tv_nsec 
                  % pack_start.tv_sec % pack_start.tv_nsec % pack_extent_time);
        // Slightly less efficient than calling update on the two separate stats,
        // but easier to code.
        tmp.update(headersize + fixedsize + variablesize, fixedsize,
                   work->extent->variabledata.size(), variablesize, 
                   work->compressed.size(), 
                   *reinterpret_cast<uint32_t *>(work->compressed.begin()+4), 
                   nrecords, pack_extent_time, work->compressed[6*4], 
                   work->compressed[6*4+1]);

        INVARIANT(work->compressed.size() > 0, "??");

        SINVARIANT(work->extent->size() == uncompressed_size);
    }
    // update stats, have to do this before we complete the extent
    // as otherwise the work pointer could vanish under us

    stats += tmp;
    if (work->to_update != NULL) {
        *work->to_update += tmp;
        SINVARIANT(work->to_update->use_count > 0);
        --work->to_update->use_count;
        work->to_update = NULL;
    }
    work->in_progress = false; 
    SINVARIANT(!worker_info.pending_work.empty());
    
    SINVARIANT(worker_info.bytes_in_progress >= 2 * uncompressed_size);
    // subtract the temporary from above and the cleared extent.
    worker_info.bytes_in_progress -= 2*uncompressed_size; 
    worker_info.bytes_in_progress += work->compressed.size(); // add in the compressed bits
}

void  DataSeriesSink::compressorThread()  {
#if 0
    // This didn't seem to have any actual effect; it should have let the
    // copy thread run at 100%, but it didn't seem to have that effect.
    int policy = -1;
    struct sched_param param;

    SINVARIANT(pthread_getschedparam(pthread_self(), &policy, &param) == 0);

    int minprio = sched_get_priority_max(policy);
    if (param.sched_priority > minprio) {
	param.sched_priority = minprio;
	SINVARIANT(pthread_setschedparam(pthread_self(), policy, &param) == 0);
    }
#endif
    PThreadScopedLock lock(mutex);
    while(true) {
	ToCompress *work = NULL;
	for(Deque<ToCompress *>::iterator i = worker_info.pending_work.begin();
	    i != worker_info.pending_work.end(); ++i) {
	    if ((**i).in_progress == false && (**i).compressed.size() == 0) {
		work = *i;
		work->in_progress = true;
		break;
	    }
	}
	if (work == NULL) {
            if (!worker_info.keep_going) { 
                break; // only stop if there is no work to do.
            }
	    worker_info.available_work_cond.wait(mutex);
	} else {
	    lockedProcessToCompress(lock, work);

	    LintelLogDebug("DataSeriesSink", format("qwe broadcast compr? %d %d\n")
                           % worker_info.bytes_in_progress % worker_info.pending_work.size());
    
	    if (worker_info.canQueueWork()) { // just freed up space.
		worker_info.available_queue_cond.broadcast();
	    }
            SINVARIANT(!worker_info.pending_work.empty());
	    if (worker_info.frontReadyToWrite()) {
		worker_info.available_write_cond.signal();
	    }
	}
    }
}

void DataSeriesSink::writerThread() {
    PThreadScopedLock lock(mutex);
    while(worker_info.keep_going) {
	if (worker_info.frontReadyToWrite()) {
	    writer_info.writeOutPending(lock, worker_info);
	} else {
	    worker_info.available_write_cond.wait(mutex);
	}
    }
}

void DataSeriesSink::Stats::reset() {
    use_count = 0;

    extents = compress_none = compress_lzo = compress_gzip = compress_bz2 = compress_lzf = 0;
    unpacked_size = unpacked_fixed = unpacked_variable = unpacked_variable_raw = packed_size 
	= nrecords = 0;
    pack_time = 0;
}

DataSeriesSink::Stats::~Stats() {
    INVARIANT(use_count == 0, 
	      format("deleting Stats %p before %d == use_count == 0\n"
                     "you need to have called sink.removeStatsUpdate()")
	      % this % use_count);
}

DataSeriesSink::Stats &DataSeriesSink::Stats::operator+=(const DataSeriesSink::Stats &from) {
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
    nrecords += from.nrecords;
    INVARIANT(from.pack_time >= 0, format("from.pack_time = %.6g < 0") % from.pack_time);
    pack_time += from.pack_time;
    return *this;
}

DataSeriesSink::Stats &DataSeriesSink::Stats::operator-=(const DataSeriesSink::Stats &from) {
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
    nrecords -= from.nrecords;
    pack_time -= from.pack_time;

    return *this;
}

DataSeriesSink::Stats &DataSeriesSink::Stats::operator =(const Stats &from) {
    if (this == &from) {
	return *this;
    }
    memcpy(this, &from, sizeof(from));
    this->use_count = 0;
    return *this;
}

void DataSeriesSink::Stats::update(uint32_t unp_size, uint32_t unp_fixed, 
				   uint32_t unp_var_raw, uint32_t unp_variable, 
				   uint32_t pkd_size, uint32_t pkd_var_size, 
				   size_t nrecs, double pkd_time, 
				   unsigned char fixed_compress_mode,
				   unsigned char variable_compress_mode) {
    ++extents;
    unpacked_size += unp_size;
    unpacked_fixed += unp_fixed;
    unpacked_variable_raw += unp_var_raw;
    unpacked_variable += unp_variable;
    packed_size += pkd_size;
    nrecords += nrecs;
    INVARIANT(pkd_time >= 0, format("update(pkd_time = %.6g < 0)") % pkd_time);
    pack_time += pkd_time;
    updateCompressMode(fixed_compress_mode);
    if (pkd_var_size > 0) {
	updateCompressMode(variable_compress_mode);
    }
}

void DataSeriesSink::Stats::updateCompressMode(unsigned char compress_mode) {
    switch(compress_mode) 
	{
	case 0: ++compress_none; break;
	case 1: ++compress_lzo; break;
	case 2: ++compress_gzip; break;
	case 3: ++compress_bz2; break;
	case 4: ++compress_lzf; break;
	default:
	    FATAL_ERROR(format("whoa, unknown compress option %d\n") 
			% static_cast<unsigned>(compress_mode));
	}
}

void DataSeriesSink::Stats::printText(ostream &to, const string &extent_type) {
    if (extent_type.empty()) {
	to << format("  wrote %d extents, %d records\n") % extents % nrecords;
    } else {
	to << format("  wrote %d extents, %d records of type %s\n")
            % extents % nrecords % extent_type;
    }

    to << format("  compression (none,lzo,gzip,bz2,lzf): (%d,%d,%d,%d,%d)\n")
	% compress_none % compress_lzo % compress_gzip % compress_bz2 % compress_lzf;
    to << format("  unpacked: %d = %d (fixed) + %d (variable, %d raw)\n")
	% unpacked_size % unpacked_fixed % unpacked_variable % unpacked_variable_raw;
    to << format("  packed size: %d; pack time: %.3f\n") % packed_size % pack_time;
}

