#include <boost/bind.hpp>

#include <Lintel/LintelLog.hpp>

#include <DataSeries/RotatingFileSink.hpp>

using namespace std;
using namespace dataseries;

// Not a sane path to try opening as a file, so use it as a sentinal.
const string RotatingFileSink::closed_filename("/");

RotatingFileSink::RotatingFileSink(uint32_t compression_modes, uint32_t compression_level) 
    : mutex(), cond(), compression_modes(compression_modes), compression_level(compression_level), 
      pthread_worker(), library(), pending(), current_sink(), callback(),
      worker_mutex(), worker_continue(true), new_filename()
{
    pthread_worker = new PThreadFunction(boost::bind(&RotatingFileSink::worker, this));
    pthread_worker->start();
}

RotatingFileSink::~RotatingFileSink()
{
    close();
    {
        PThreadScopedLock lock(worker_mutex);
        
        worker_continue = false;
        cond.broadcast();
    }
    pthread_worker->join();
    delete pthread_worker;
    pthread_worker = NULL;
    INVARIANT(current_sink == NULL, "someone called changeFile while destructor was running?");

    INVARIANT(pending.empty(), "did you write extents but never call changeFile?");
}

const ExtentType &RotatingFileSink::registerType(const string &xmldesc) {
    PThreadScopedLock lock(mutex);

    INVARIANT(worker_continue && current_sink == NULL && pending.empty() && new_filename.empty(),
              "invalid to register types if the sink is active");
    return library.registerTypeR(xmldesc);
}

void
RotatingFileSink::setExtentWriteCallback(const DataSeriesSink::ExtentWriteCallback &in_callback) {
    PThreadScopedLock lock(mutex);

    callback = in_callback;
    if (current_sink != NULL) {
        current_sink->setExtentWriteCallback(callback);
    }
}

void RotatingFileSink::close() {
    setExtentWriteCallback(DataSeriesSink::ExtentWriteCallback());
    while (!changeFile(closed_filename, true)) {
        waitForCanChange();
    }
    waitForCanChange();
}

void RotatingFileSink::writeExtent(Extent &e, Stats *to_update) {
    PThreadScopedLock lock(mutex);
    SINVARIANT(worker_continue); // opportunistic check (protected by worker_lock)

    if (current_sink != NULL) { // available to write
        current_sink->writeExtent(e, to_update);
    } else { // rotating or closed.
        pending.push_back(Pending(e, to_update));
        if (pending.size() > 49 && (pending.size() % 10) == 0) { // 1MB extents --> 50MB
            LintelLog::warn(boost::format("warning, you have %d pending extents, did you remember"
                                   " to RotatingFileSink::changeFile()?") % pending.size());
        }
    }
}

IExtentSink::Stats RotatingFileSink::getStats(Stats *from) {
    FATAL_ERROR("unimplemented, semantics unclear");
}

void RotatingFileSink::removeStatsUpdate(Stats *would_update) {
    FATAL_ERROR("unimplemented, semantics unclear");
}


bool RotatingFileSink::canChangeFile() {
    PThreadScopedLock lock(worker_mutex);

    return worker_continue && new_filename.empty();
}

string RotatingFileSink::getNewFilename() {
    PThreadScopedLock lock(worker_mutex);

    string ret(new_filename);
    return ret;
}

void RotatingFileSink::waitForCanChange() {
    PThreadScopedLock lock(worker_mutex);

    while (!new_filename.empty()) {
        SINVARIANT(worker_continue);
        cond.wait(worker_mutex);
    }
    SINVARIANT(worker_continue);
}


bool RotatingFileSink::changeFile(const string &in_new_filename, bool failure_ok) {
    PThreadScopedLock lock(worker_mutex);

    if (worker_continue && new_filename.empty()) {
        new_filename = in_new_filename;
        cond.broadcast();
        return true;
    } else {
        INVARIANT(failure_ok, "unable to change file, either the sink is being deleted"
                  " or a rotation is still in progress");
        return false;
    }
}

void RotatingFileSink::workerNullifyCurrent(PThreadScopedLock &worker_lock) {
    PThreadScopedUnlock unlock(worker_lock);

    DataSeriesSink *old_sink;
    {
        PThreadScopedLock lock(mutex);
        old_sink = current_sink;
        current_sink = NULL;
    }
    delete old_sink; 
}

void *RotatingFileSink::worker() {
    PThreadScopedLock worker_lock(worker_mutex);

    while (worker_continue) {
        if (new_filename.empty()) {
            cond.wait(worker_mutex);
        } else if (new_filename == closed_filename) {
            workerNullifyCurrent(worker_lock);
            new_filename.clear();
            cond.broadcast();
        } else {
            string to_filename(new_filename); // probably unnecessary
            workerNullifyCurrent(worker_lock);

            DataSeriesSink *new_sink;
            { // don't block anyone while creating the new one.
                PThreadScopedUnlock unlock(worker_lock);

                new_sink = new DataSeriesSink(to_filename, compression_modes, compression_level);
                // safe, all changes to library must have been made while there is no current sink,
                // and new_filename is empty.
                new_sink->writeExtentLibrary(library);

                PThreadScopedLock lock(mutex); // swap in the new one.

                new_sink->setExtentWriteCallback(callback);
                current_sink = new_sink;
                // Put all the pending things into the new sink.
                while (!pending.empty()) {
                    current_sink->writeExtent(*pending.front().e, pending.front().to_update);
                    delete pending.front().e; // pointer copy implies destructor delete fails.
                    pending.pop_front();
                }
            }
            new_filename.clear();
            cond.broadcast(); // wake up anyone waiting on change
        }
    }
    return NULL;
}

