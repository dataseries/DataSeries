// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A module which sorts DataSeries records in memory via multiple threads.
*/

#ifndef __DATASERIES_PARALLELMEMORYSORTMODULE_H
#define __DATASERIES_PARALLELMEMORYSORTMODULE_H

#include <deque>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/shared_ptr.hpp>

#include <Lintel/LintelLog.hpp>
#include <Lintel/PriorityQueue.hpp>
#include <Lintel/PThread.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/Field.hpp>
#include <DataSeries/GeneralField.hpp>

template <typename F, typename C, typename P>
class ParallelMemorySortModule : public DataSeriesModule {
public:
    ParallelMemorySortModule(DataSeriesModule &upstreamModule,
                             const std::string &fieldName,
                             const C &fieldComparator,
                             const P &fieldParitioner,
                             uint32_t threadCount = 0,
                             size_t extentSizeLimit = 1 * 1000000,
                             uint32_t mergeFactor = 2); // 1 MB

    virtual ~ParallelMemorySortModule();

    /** Returns the next @c Extent according to the sorted order. Note that the first call to
        getExtent will be significantly slower than the rest, because it triggers the sorting process. */
    virtual Extent *getExtent();

private:
    DataSeriesModule &upstreamModule;
    std::string fieldName;
    C fieldComparator;
    P fieldPartitioner;
    uint32_t threadCount;
    size_t extentSizeLimit;
    uint32_t mergeFactor;

    // Have we launched the threads?
    bool initialized;

    // How many threads are currently in the first phase?
    uint32_t sortThreadCount;

    // A buffer for extents that are ready to be provided to the downstream module.
    // This buffer enables us to use multiple threads for the merging phase.
    std::deque<Extent*> downstreamExtents;

    PThreadMutex mutex;
    PThreadCond cond;

    //
    // Partitioned extents
    //

    typedef const void* Position;
    typedef std::vector<Position> PositionVector;
    typedef boost::shared_ptr<Extent> ExtentPtr;

    /** A class for holding a sorted upstream extent with information to assist with the
       per-partition merging. */
    class PartitionedExtent {
    public:
       ExtentPtr extent;
       PositionVector positions;
       std::vector<PositionVector::iterator> partitionBegins;
       std::vector<PositionVector::iterator> partitionEnds;
       std::vector<PositionVector::iterator> partitionIterators;
    };

    typedef boost::shared_ptr<PartitionedExtent> PartitionedExtentPtr;
    std::vector<PartitionedExtentPtr> partitionedExtents;

    //
    // Partitions
    //

    /** A class for holding the final extents of a specific partition. The extents are
        allocated and must be deallocated by the downstream module. */
    class Partition {
    public:
        std::deque<Extent*> extents;
    };

    typedef boost::shared_ptr<Partition> PartitionPtr;
    typedef std::vector<PartitionPtr> PartitionVector;
    PartitionVector partitions;
    typename PartitionVector::iterator partitionIterator;

    //
    // Comparators
    //

    /** A data class to hold data that is needed by an AbstractComparator. This cannot
            be stored directly in AbstractComparator because std::sort copies the comparator
            (once for each item!), resulting in a performance hit. */
    class ComparatorData {
    public:
        ComparatorData(const std::string &fieldName, C &fieldComparator);
        C &fieldComparator;
        ExtentSeries seriesLhs;
        ExtentSeries seriesRhs;
        F fieldLhs;
        F fieldRhs;
    };

    /** A base class for comparators that wrap a user-specified FieldComparator. */
    class AbstractComparator {
    public:
        AbstractComparator(ComparatorData &data);
        void setExtent(Extent *extent); // Allows us to use relocate later.

    protected:
        ComparatorData &data;
    };

    /** An internal class for comparing fields based on pointers to their records. */
    class PositionComparator : public AbstractComparator {
    public:
        PositionComparator(ComparatorData &data);
        bool operator()(const void* positionLhs, const void* positionRhs);
    };

    /** An internal class for comparing partitioned extents based on pointers to their records. */
    class PartitionedExtentComparator : public AbstractComparator {
    public:
        PartitionedExtentComparator(ComparatorData &data, uint32_t partitionNum);
        bool operator()(PartitionedExtent *partitionedExtentLhs,
                        PartitionedExtent *partitionedExtentRhs);
    private:
        uint32_t partitionNum;
    };

    //
    // Threads
    //

    class WorkerThread: public PThread {
    public:
        WorkerThread(ParallelMemorySortModule<F, C, P> &module,
                     uint32_t partitionNum) : module(module), partitionNum(partitionNum) {}
        virtual ~WorkerThread() {}

        virtual void *run() {
            module.startThread(partitionNum);
            return NULL;
        }

    private:
        ParallelMemorySortModule<F, C, P> &module;
        uint32_t partitionNum;
    };

    typedef boost::shared_ptr<PThread> PThreadPtr;
    std::vector<PThreadPtr> threads;

    //
    // Methods
    //

    void createThreads();
    void startThread(uint32_t partitionNum);

    // Retrieve the next extent from the relevant partition.
    Extent *getNextExtent();

    void sortAndPartition();
    void sortAndPartition(PartitionedExtentPtr &partitionedExtent);
    void merge(uint32_t partitionNum);
};

template <typename F, typename C, typename P>
ParallelMemorySortModule<F, C, P>::
ParallelMemorySortModule(DataSeriesModule &upstreamModule,
                         const std::string &fieldName,
                         const C &fieldComparator,
                         const P &fieldPartitioner,
                         uint32_t threadCount,
                         size_t extentSizeLimit,
                         uint32_t mergeFactor)
    : upstreamModule(upstreamModule),
      fieldName(fieldName),
      fieldComparator(fieldComparator),
      fieldPartitioner(fieldPartitioner),
      threadCount(threadCount == 0 ? PThreadMisc::getNCpus() : threadCount),
      extentSizeLimit(extentSizeLimit),
      mergeFactor(mergeFactor),
      initialized(false),
      sortThreadCount(this->threadCount) {
    this->fieldPartitioner.initialize(this->threadCount);
}

template <typename F, typename C, typename P>
ParallelMemorySortModule<F, C, P>::
~ParallelMemorySortModule() {
}

template <typename F, typename C, typename P>
Extent *ParallelMemorySortModule<F, C, P>::
getExtent() {
    if (!initialized) {
        // Create the partitions but leave them empty.
        for (uint32_t i = 0; i < threadCount; ++i) {
            PartitionPtr partition(new Partition);
            partitions.push_back(partition);
        }
        partitionIterator = partitions.begin();
        createThreads(); // This doesn't block.
        BOOST_FOREACH(PThreadPtr &thread, threads) {
            thread->join();
        }
        initialized = true;
    }

    return getNextExtent();
}

template <typename F, typename C, typename P>
Extent *ParallelMemorySortModule<F, C, P>::
getNextExtent() {
    if (partitionIterator == partitions.end()) {
        return NULL;
    }

    // Skip over any empty partitions. It's unlikely that this loop will iterate more than
    // once, because partitions are rarely empty.
    PartitionPtr &partition = *partitionIterator;
    while (partition->extents.empty()) {
        ++partitionIterator;
        if (partitionIterator == partitions.end()) {
            return NULL;
        }
        partition = *partitionIterator;
    }

    // At this stage partition is a non-empty partition so we just need to return the first extent.
    Extent *extent = partition->extents.front();
    partition->extents.pop_front();
    return extent;
}

template <typename F, typename C, typename P>
void ParallelMemorySortModule<F, C, P>::
createThreads() {
    for (uint32_t i = 0; i < threadCount; ++i) {
        PThreadPtr thread(new WorkerThread(*this, i));
        threads.push_back(thread);
        thread->start();
    }
}

template <typename F, typename C, typename P>
void ParallelMemorySortModule<F, C, P>::
startThread(uint32_t partitionNum) {
    LintelLogDebug("parallelmemorysortmodule", boost::format("Started thread #%s.") % partitionNum);
    sortAndPartition();

    // Wait until all the threads have finished the first phase (sorting) before proceeding.
    mutex.lock();
    --sortThreadCount;
    while (sortThreadCount > 0) {
        cond.wait(mutex);
    }
    // At this point we know that sortThreadCount == 0.
    mutex.unlock();
    cond.broadcast();

    merge(partitionNum);
}

template <typename F, typename C, typename P>
void ParallelMemorySortModule<F, C, P>::
sortAndPartition() {
    while (true) {
        mutex.lock();
        ExtentPtr extent(upstreamModule.getExtent());
        mutex.unlock();

        if (extent.get() == NULL) {
            break; // All of the upstream extents have been read.
        }

        LintelLogDebug("parallelmemorysortmodule", "Retrieved an upstream extent.");

        PartitionedExtentPtr partitionedExtent(new PartitionedExtent);
        partitionedExtent->extent = extent;
        sortAndPartition(partitionedExtent);

        mutex.lock();
        partitionedExtents.push_back(partitionedExtent);
        mutex.unlock();
    }
}

template <typename F, typename C, typename P>
void ParallelMemorySortModule<F, C, P>::
sortAndPartition(PartitionedExtentPtr &partitionedExtent) {
    ComparatorData data(fieldName, fieldComparator);
    PositionComparator comparator(data);

    ExtentSeries series(partitionedExtent->extent.get());
    F field(series, fieldName);

    // Initialize the positions in the position vector.
    partitionedExtent->positions.reserve(partitionedExtent->extent->getRecordCount());
    for (; series.more(); series.next()) {
        partitionedExtent->positions.push_back(series.getCurPos());
    }

    comparator.setExtent(partitionedExtent->extent.get());
    std::sort(partitionedExtent->positions.begin(),
              partitionedExtent->positions.end(),
              comparator);

    // The positions are now in ascending order. Mark the first and after-last positions of each
    // vector. TODO: Consider changing this to a binary search.
    partitionedExtent->partitionBegins.push_back(partitionedExtent->positions.begin());
    partitionedExtent->partitionIterators.push_back(partitionedExtent->positions.begin());
    uint32_t currentPartitionNum = 0;
    for (PositionVector::iterator it = partitionedExtent->positions.begin();
         it != partitionedExtent->positions.end();
         ++it) {
        series.setCurPos(*it);
        uint32_t partitionNum = fieldPartitioner.getPartition(field);

        if (partitionNum != currentPartitionNum) {
            // This is the beginning of the next partition.
            currentPartitionNum = partitionNum;
            partitionedExtent->partitionEnds.push_back(it);
            partitionedExtent->partitionBegins.push_back(it);
            partitionedExtent->partitionIterators.push_back(it);
        }
    }

    // Add some empty partitions if necessary.
    while (partitionedExtent->partitionBegins.size() < threadCount) {
        partitionedExtent->partitionEnds.push_back(partitionedExtent->positions.end());
        partitionedExtent->partitionBegins.push_back(partitionedExtent->positions.end());
        partitionedExtent->partitionIterators.push_back(partitionedExtent->positions.end());
    }

    partitionedExtent->partitionEnds.push_back(partitionedExtent->positions.end());

}

template <typename F, typename C, typename P>
void ParallelMemorySortModule<F, C, P>::
merge(uint32_t partitionNum) {
    // We don't want to think about this special case anymore.
    if (partitionedExtents.size() == 0) {
        return;
    }

    ComparatorData data(fieldName, fieldComparator);
    PartitionedExtentComparator comparator(data, partitionNum);
    comparator.setExtent(partitionedExtents[0]->extent.get()); // Just so that we can use relocate later.
    PriorityQueue<PartitionedExtent*, PartitionedExtentComparator> queue(comparator);

    // Insert partitioned extents into the queue (if this partition is not empty in them).
    BOOST_FOREACH(PartitionedExtentPtr &partitionedExtent, partitionedExtents) {
        if (partitionedExtent->partitionIterators[partitionNum] !=
            partitionedExtent->partitionEnds[partitionNum]) {
            queue.push(partitionedExtent.get());
        }
    }

    Extent *destinationExtent = new Extent(partitionedExtents[0]->extent->getType());
    ExtentSeries destinationSeries(destinationExtent);
    ExtentSeries sourceSeries(destinationExtent);
    ExtentRecordCopy recordCopier(sourceSeries, destinationSeries);

    while (!queue.empty()) {
        PartitionedExtent *partitionedExtent = queue.top();
        typename PositionVector::iterator &it = partitionedExtent->partitionIterators[partitionNum];

        sourceSeries.setExtent(partitionedExtent->extent.get());
        sourceSeries.setCurPos(*it);
        destinationSeries.newRecord();
        recordCopier.copyRecord();

        if (extentSizeLimit != 0 && destinationExtent->size() >= extentSizeLimit) {
            partitions[partitionNum]->extents.push_back(destinationExtent);
            LintelLogDebug("parallelmemorysortmodule",
                           boost::format("Created a downstream extent for partition %s.") % partitionNum);
            destinationExtent = new Extent(destinationExtent->getType());
            destinationSeries.setExtent(destinationExtent);
            // TODO: Do we need to somehow reset the copier?
        }

        ++it; // This changes the actual iterator because it is a reference variable.
        if (it == partitionedExtent->partitionEnds[partitionNum]) {
            --it;
            queue.pop(); // Revert the change before popping!
        } else {
            queue.replaceTop(partitionedExtent);
        }
    }
}

template <typename F, typename C, typename P>
ParallelMemorySortModule<F, C, P>::
AbstractComparator::AbstractComparator(ComparatorData &data)
    : data(data) {
}

template <typename F, typename C, typename P>
void ParallelMemorySortModule<F, C, P>::
AbstractComparator::setExtent(Extent *extent) {
    data.seriesLhs.setExtent(extent);
    data.seriesRhs.setExtent(extent);
}

template <typename F, typename C, typename P>
ParallelMemorySortModule<F, C, P>::
ComparatorData::ComparatorData(const std::string &fieldName,
                               C &fieldComparator)
    : fieldComparator(fieldComparator),
      fieldLhs(seriesLhs, fieldName), fieldRhs(seriesRhs, fieldName) {
}

template <typename F, typename C, typename P>
ParallelMemorySortModule<F, C, P>::
PositionComparator::PositionComparator(ComparatorData &data)
    : AbstractComparator(data) {
}

template <typename F, typename C, typename P>
bool ParallelMemorySortModule<F, C, P>::
PositionComparator::operator()(Position positionLhs, Position positionRhs) {
    this->data.seriesLhs.setCurPos(positionLhs);
    this->data.seriesRhs.setCurPos(positionRhs);
    return this->data.fieldComparator(this->data.fieldLhs, this->data.fieldRhs);
}

template <typename F, typename C, typename P>
ParallelMemorySortModule<F, C, P>::
PartitionedExtentComparator::PartitionedExtentComparator(ComparatorData &data, uint32_t partitionNum)
    : AbstractComparator(data), partitionNum(partitionNum) {
}

template <typename F, typename C, typename P>
bool ParallelMemorySortModule<F, C, P>::
PartitionedExtentComparator::operator()(PartitionedExtent *partitionedExtentLhs,
                                        PartitionedExtent *partitionedExtentRhs) {
    DEBUG_SINVARIANT(partitionedExtentLhs->partitionIterators[partitionNum] !=
                     partitionedExtentLhs->partitionEnds[partitionNum]);
    DEBUG_SINVARIANT(partitionedExtentRhs->partitionIterators[partitionNum] !=
                     partitionedExtentRhs->partitionEnds[partitionNum]);
    this->data.seriesLhs.relocate(partitionedExtentLhs->extent.get(),
                                  *partitionedExtentLhs->partitionIterators[partitionNum]);
    this->data.seriesRhs.relocate(partitionedExtentRhs->extent.get(),
                                  *partitionedExtentRhs->partitionIterators[partitionNum]);

    return this->data.fieldComparator(this->data.fieldRhs, this->data.fieldLhs);
}

#endif
