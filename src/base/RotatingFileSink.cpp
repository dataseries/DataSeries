#include <boost/bind.hpp>

#include <DataSeries/RotatingFileSink.hpp>

using namespace std;
using namespace dataseries;

// Not a sane path to try opening as a file, so use it as a sentinal.
const string RotatingFileSink::closed_filename("/");

RotatingFileSink::RotatingFileSink(uint32_t compression_modes, uint32_t compression_level) 
    : compression_modes(compression_modes), compression_level(compression_level), mutex(), cond(),
      new_filename(), pending(), current_sink(), pthread_worker(), worker_continue(true)
{
    pthread_worker = new PThreadFunction(boost::bind(&RotatingFileSink::worker, this));
    pthread_worker->start();
}

RotatingFileSink::~RotatingFileSink()
{
    {
        PThreadScopedLock lock(mutex);
        
        worker_continue = false;
        cond.broadcast();
    }
    pthread_worker->join();
    delete pthread_worker;
    pthread_worker = NULL;
    delete current_sink;

    SINVARIANT(pending.empty());
}

const ExtentType &RotatingFileSink::registerType(const string &xmldesc) {
    PThreadScopedLock lock(mutex);

    INVARIANT(worker_continue && current_sink == NULL && pending.empty() && new_filename.empty(),
              "invalid to register types if the sink is active");
    return library.registerTypeR(xmldesc);
}

bool RotatingFileSink::canChangeFile() {
    PThreadScopedLock lock(mutex);

    return new_filename.empty();
}

void RotatingFileSink::waitForCanChange() {
    PThreadScopedLock lock(mutex);

    while (!new_filename.empty()) {
        SINVARIANT(worker_continue);
        cond.wait(mutex);
    }
}


void RotatingFileSink::changeFile(const string &in_new_filename) {
    PThreadScopedLock lock(mutex);

    SINVARIANT(worker_continue);
    SINVARIANT(new_filename.empty());
    new_filename = in_new_filename;
    cond.broadcast();
}

void RotatingFileSink::writeExtent(Extent &e, Stats *to_update) {
    PThreadScopedLock lock(mutex);
    SINVARIANT(worker_continue);

    if (new_filename.empty() && current_sink != NULL) { // not rotating, not closed
        current_sink->writeExtent(e, to_update);
    } else { // rotating or closed.
        pending.push_back(Pending(e, to_update));
    }
}

IExtentSink::Stats RotatingFileSink::getStats(Stats *from) {
    FATAL_ERROR("unimplemented, semantics unclear");
}

void RotatingFileSink::removeStatsUpdate(Stats *would_update) {
    FATAL_ERROR("unimplemented, semantics unclear");
}

void *RotatingFileSink::worker() {
    PThreadScopedLock lock(mutex);

    while (worker_continue) {
        if (new_filename.empty()) {
            cond.wait(mutex);
        } else {
            string to_filename(new_filename); // probably unnecessary
            DataSeriesSink *old_sink = current_sink;
            current_sink = NULL; // guarantee no more writes

            DataSeriesSink *new_sink;
            { // don't block up writers
                PThreadScopedUnlock unlock(lock);

                delete old_sink;
                new_sink = new DataSeriesSink(to_filename, compression_modes, compression_level);
                new_sink->writeExtentLibrary(library);
            }
            current_sink = new_sink;
            // Put all the pending things into the new sink.
            while (!pending.empty()) {
                current_sink->writeExtent(*pending.front().e, pending.front().to_update);
                delete pending.front().e; // pointer copy implies destructor delete fails.
                pending.pop_front();
            }
            new_filename.clear();
            cond.broadcast(); // wake up anyone waiting on change
        }
    }
    return NULL;
}

