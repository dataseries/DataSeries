#include <memory>

#include <boost/format.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/tss.hpp>

#include <Lintel/LintelLog.hpp>

#include <DataSeries/ParallelFilterModule.hpp>

class FilterThread : public PThread {
public:
    FilterThread(ParallelFilterModule *module)
        : module(module) {}

    virtual ~FilterThread() {}

    virtual void *run() {
        module->startFilterThread();
        return NULL;
    }

private:
    ParallelFilterModule *module;
};

ParallelFilterModule::ParallelFilterModule(DataSeriesModule &upstreamModule,
                                           size_t extentSizeLimit,
                                           int threadCount,
                                           size_t downstreamQueueLimit)
    : extentSizeLimit(extentSizeLimit),
      upstreamModule(upstreamModule),
      upstreamEmpty(false),
      threadCount(threadCount == 0 ? PThreadMisc::getNCpus() : threadCount),
      activeThreadCount(this->threadCount),
      downstreamQueueLimit(downstreamQueueLimit == 0 ?
                           this->threadCount * extentSizeLimit * 2 : downstreamQueueLimit),
      downstreamQueueSize(0) {
}

ParallelFilterModule::~ParallelFilterModule() {
    downstreamQueueMutex.lock();
    bool threadsCreated = filterThreads.size();
    downstreamQueueMutex.unlock();

    if (threadsCreated > 0) {
        BOOST_FOREACH(PThreadPtr filterThread, filterThreads) {
            filterThread->join();
        }
    }
}

Extent* ParallelFilterModule::getExtent() {
    PThreadScopedLock lock(downstreamQueueMutex);

    if (filterThreads.size() == 0) {
        createThreads();
    }

    while (downstreamQueue.empty() && activeThreadCount > 0) {
        downstreamQueueNotEmptyCond.wait(downstreamQueueMutex);
    }

    if (downstreamQueue.empty()) {
        return NULL;
    }

    Extent *extent = downstreamQueue.front();
    downstreamQueue.pop_front();
    downstreamQueueSize -= extent->size();
    downstreamQueueNotFullCond.broadcast();

    return extent;
}

void ParallelFilterModule::startFilterThread() {
    LintelLogDebug("parallelfiltermodule", "Started a filter thread.");

    tls.reset(createTls());

    while (!upstreamEmpty) {
        waitDownstreamQueueNotFull();

        Extent *extent = createNextExtent(tls.get());
        if (extent == NULL) {
            upstreamEmpty = true;
            break;
        }

        appendDownstreamQueue(extent);
    }

    downstreamQueueMutex.lock();
    --activeThreadCount;
    downstreamQueueNotEmptyCond.broadcast(); // This has nothing to do with the queue not being
                                             // empty, but it's a special case for the end.
    downstreamQueueMutex.unlock();
}

bool ParallelFilterModule::downstreamQueueFull() {
    return downstreamQueueSize + extentSizeLimit > downstreamQueueLimit;
}

void ParallelFilterModule::waitDownstreamQueueNotFull() {
    PThreadScopedLock lock(downstreamQueueMutex);

    while (downstreamQueueFull()) {
        downstreamQueueNotFullCond.wait(downstreamQueueMutex);
    }

    // It's not full at this point. Reserve the available space so nobody else takes it.
    downstreamQueueSize += extentSizeLimit;
}

void ParallelFilterModule::appendDownstreamQueue(Extent *extent) {
    PThreadScopedLock lock(downstreamQueueMutex);
    downstreamQueue.push_back(extent);

    // At this stage we know the extent's real size, so we can adjust the queue size so that
    // it reflects the actual size rather than what we previously estimated.
    downstreamQueueSize += extent->size() - extentSizeLimit;

    downstreamQueueNotEmptyCond.broadcast();
}

void ParallelFilterModule::createThreads() {
    // Create the threads. This must be done here, rather than in the constructor, in order
    // to avoid calling a virtual function before the object is fully constructed.
    LintelLogDebug("parallelfiltermodule", boost::format("Using %s filter threads.") % threadCount);
    for (int i = 0; i < threadCount; ++i) {
        PThreadPtr filterThread(new FilterThread(this));
        filterThreads.push_back(filterThread);
        filterThread->start();
    }
}

ParallelFilterModule::ThreadLocalStorage* ParallelFilterModule::createTls() {
    return NULL;
}
