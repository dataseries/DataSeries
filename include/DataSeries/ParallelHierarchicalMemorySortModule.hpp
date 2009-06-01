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

template <typename FieldType, typename FieldComparator>
class ParallelMemorySortModule : public DataSeriesModule {
public:
    ParallelMemorySortModule(DataSeriesModule &upstreamModule,
                             const std::string &fieldName,
                             const FieldComparator &fieldComparator,
                             uint32_t threadCount = 0,
                             size_t extentSizeLimit = 1 * 1000000,
                             uint32_t mergeFactor = 2); // 1 MB

    virtual ~ParallelMemorySortModule();

    /** Returns the next @c Extent according to the sorted order. Note that the first call to
        getExtent will be significantly slower than the rest, because it triggers the sorting process. */
    virtual Extent *getExtent();

private:
    //
    // Sorted extent vectors (SEVs)
    //

    typedef boost::shared_ptr<Extent> ExtentPtr;
    typedef std::vector<ExtentPtr> ExtentVector;

    class Sev;
    class ThreadLocalStorage;
    typedef boost::shared_ptr<Sev> SevPtr;

    class AbsolutePosition {
    public:
        const void *position;
        Extent *extent;
    };
    typedef std::vector<AbsolutePosition> AbsolutePositionVector;

    class Sev {
    public:
        Sev(ExtentPtr &extent, ThreadLocalStorage &tls);
        Sev(std::vector<SevPtr> &sevs, ThreadLocalStorage &tls);

        ExtentVector extents;
        AbsolutePositionVector positions;
        typename AbsolutePositionVector::iterator iterator;
    };

    /** A data class to hold data that is needed by an AbstractComparator. This cannot
        be stored directly in AbstractComparator because std::sort copies the comparator
        (once for each item!), resulting in a performance hit. */
    class ComparatorData {
    public:
        ComparatorData(const std::string &fieldName, FieldComparator &fieldComparator);
        FieldComparator &fieldComparator;
        ExtentSeries seriesLhs;
        ExtentSeries seriesRhs;
        FieldType fieldLhs;
        FieldType fieldRhs;
    };

    /** A base class for comparators that wrap a user-specified FieldComparator. */
    class AbstractComparator {
    public:
        AbstractComparator(ComparatorData &data);
        void setExtent(Extent *extent); // so we can use relocate later

    protected:
        ComparatorData &data;
    };

    /** An internal class for comparing fields based on pointers to their records. */
    class PositionComparator : public AbstractComparator {
    public:
        PositionComparator(ComparatorData &data);
        bool operator()(const AbsolutePosition &positionLhs, const AbsolutePosition &positionRhs);
    };

    /** An internal class for comparing SEVs based on pointers to their records. */
    class SevComparator : public AbstractComparator {
    public:
        SevComparator(ComparatorData &data);
        bool operator()(Sev *sevLhs, Sev *sevRhs);
    };

    typedef std::deque<SevPtr> SevList;

    SevPtr finalSev; // the SEV with all the data (only populated after readAndSort finishes)

    //
    // Merge levels
    //

    /** A class representing a single merge level. Each merge level stores a collection of
        similarly-sized SEVs. The SEVs on level i each consist of 2^i upstream extents. */
    class MergeLevel {
    public:
        MergeLevel(uint32_t level) : level(level) {}
        uint32_t level;
        SevList sevs;
    };
    typedef boost::shared_ptr<MergeLevel> MergeLevelPtr;

    typedef std::vector<MergeLevelPtr> MergeLevelVector;
    MergeLevelVector mergeLevels;
    PThreadMutex mergeLevelsMutex; // Protects both mergeLevels and mergeLevelQueue.

    //
    // Threads
    //

    typedef boost::shared_ptr<PThread> PThreadPtr;
    class MergeThread: public PThread {
    public:
        MergeThread(ParallelMemorySortModule<FieldType, FieldComparator> *module) : module(module) {}
        virtual ~MergeThread() {}

        virtual void *run() {
            module->startMergeThread();
            return NULL;
        }

    private:
        ParallelMemorySortModule<FieldType, FieldComparator> *module;
    };

    class ThreadLocalStorage {
    public:
        boost::shared_ptr<ComparatorData> positionComparatorData;
        boost::shared_ptr<ComparatorData> sevComparatorData;
        boost::shared_ptr<PositionComparator> positionComparator;
        boost::shared_ptr<SevComparator> sevComparator;
    };

    std::vector<PThreadPtr> mergeThreads;
    boost::thread_specific_ptr<ThreadLocalStorage> tls;

    //
    // Various functions
    //

    void createMergeThreads();
    Extent* createNextExtent();
    void startMergeThread();
    ThreadLocalStorage* createTls();

    bool mergeData(bool allowDifferentLevels = false);
    bool retrieveData();

    //
    // Various variables
    //

    DataSeriesModule &upstreamModule;
    std::string fieldName;
    FieldComparator fieldComparator;
    uint32_t threadCount;
    size_t extentSizeLimit;
    uint32_t mergeFactor;
    uint32_t totalSevCount;
    PThreadMutex upstreamModuleMutex;
};

template <typename FieldType, typename FieldComparator>
ParallelMemorySortModule<FieldType, FieldComparator>::
ParallelMemorySortModule(DataSeriesModule &upstreamModule,
                         const std::string &fieldName,
                         const FieldComparator &fieldComparator,
                         uint32_t threadCount,
                         size_t extentSizeLimit,
                         uint32_t mergeFactor)
    : upstreamModule(upstreamModule),
      fieldName(fieldName),
      fieldComparator(fieldComparator),
      threadCount(threadCount == 0 ? PThreadMisc::getNCpus() : threadCount),
      extentSizeLimit(extentSizeLimit),
      mergeFactor(mergeFactor),
      totalSevCount(0) {
}

template <typename FieldType, typename FieldComparator>
ParallelMemorySortModule<FieldType, FieldComparator>::
~ParallelMemorySortModule() {
}

template <typename FieldType, typename FieldComparator>
Extent *ParallelMemorySortModule<FieldType, FieldComparator>::
getExtent() {
    if (!finalSev) {
        createMergeThreads();
        BOOST_FOREACH(PThreadPtr &mergeThread, mergeThreads) {
            mergeThread->join();
        }
        INVARIANT(mergeLevels.back()->sevs.size() == 1, boost::format("There are %s SEVs in the final level.") % mergeLevels.back()->sevs.size());
        finalSev = mergeLevels.back()->sevs[0];

        // Check that we didn't loose any SEVs along the way.
        mergeLevels.pop_back();

        for (typename MergeLevelVector::reverse_iterator it = mergeLevels.rbegin();
             it != mergeLevels.rend();
             ++it) {
            MergeLevelPtr &mergeLevel = *it;
            INVARIANT(mergeLevel->sevs.size() == 0,
                      boost::format("We are leaving behind %s SEVs at level %s/%s!") % mergeLevel->sevs.size() % mergeLevel->level % mergeLevels.size());
        }
    }

    // pull some more records from the priority queue and constructs an extent
    return createNextExtent();
}

template <typename FieldType, typename FieldComparator>
void ParallelMemorySortModule<FieldType, FieldComparator>::
createMergeThreads() {
    MergeLevelPtr mergeLevel0(new MergeLevel(0));
    mergeLevels.push_back(mergeLevel0);

    // All we need to do is create the threads. They do all the work.
    LintelLogDebug("parallelmemorysortmodule", boost::format("Using %s merge threads.") % threadCount);
    for (uint32_t i = 0; i < threadCount; ++i) {
        PThreadPtr mergeThread(new MergeThread(this));
        mergeThreads.push_back(mergeThread);
        mergeThread->start();
    }
}

template <typename FieldType, typename FieldComparator>
bool ParallelMemorySortModule<FieldType, FieldComparator>::
mergeData(bool allowDifferentLevels) {
    std::vector<SevPtr> sevs;
    uint32_t level = 0;

    if (totalSevCount < (allowDifferentLevels ? 2 : mergeFactor)) {
        return false;
    }

    {
        PThreadScopedLock lock(mergeLevelsMutex);
        BOOST_FOREACH(MergeLevelPtr &mergeLevel, mergeLevels) {
            if (!allowDifferentLevels) {
                if (mergeLevel->sevs.size() >= mergeFactor) {
                    for (uint32_t i = 0; i < mergeFactor; ++i) {
                        sevs.push_back(mergeLevel->sevs.front());
                        mergeLevel->sevs.pop_front();
                    }
                    level = mergeLevel->level;
                    break;
                }
            } else { // This deals with the leftovers (one SEV at some levels) at the end.
                while (!mergeLevel->sevs.empty()) {
                    level = mergeLevel->level;
                    sevs.push_back(mergeLevel->sevs.front());
                    mergeLevel->sevs.pop_front();
                    if (sevs.size() == 2) { // Limit merge factor to two at the end for parallelism.
                        break;
                    }
                }
            }
        }

        if (sevs.empty()) {
            SINVARIANT(!allowDifferentLevels);
            return false;
        }

        INVARIANT(sevs.size() > 1, boost::format("%s") % allowDifferentLevels);

        totalSevCount -= sevs.size();

        uint32_t verifySevCount= 0;
        BOOST_FOREACH(MergeLevelPtr &l, mergeLevels) {
            verifySevCount += l->sevs.size();
        }
        SINVARIANT(verifySevCount == totalSevCount);

    }

    LintelLogDebug("parallelmemorysortmodule",
                   boost::format("Merging %s level-%s SEVs.") % mergeFactor % level);

    // Check that we're not losing any data.
    uint32_t count =  0;
    BOOST_FOREACH(SevPtr &sev, sevs) {
        count += sev->positions.size();
    }

    SevPtr mergedSev(new Sev(sevs, *tls)); // merge!
    uint32_t upperLevel = level + 1;

    INVARIANT(mergedSev->positions.size() == count, "We lost some records!");

    {
        PThreadScopedLock lock(mergeLevelsMutex);
        if (mergeLevels.size() <= upperLevel) {
            // This is the first time we've hit this level, so add a new level.
            MergeLevelPtr mergeLevel(new MergeLevel(upperLevel));
            mergeLevels.push_back(mergeLevel);
        }
        mergeLevels[upperLevel]->sevs.push_back(mergedSev);
        ++totalSevCount;
    }

    return true;
}

template <typename FieldType, typename FieldComparator>
bool ParallelMemorySortModule<FieldType, FieldComparator>::
retrieveData() {
    ExtentPtr extent;
    {
        // Read an extent from the upstream module and create an SEV out of it.
        PThreadScopedLock lock(upstreamModuleMutex);
        extent.reset(upstreamModule.getExtent());
        if (!extent) {
            return false;
        }
    }

    SevPtr sev(new Sev(extent, *tls)); // sort an extent!

    LintelLogDebug("parallelmemorysortmodule", "Created a level-0 SEV.");

    {
        // Add the SEV to the lowest merge level.
        PThreadScopedLock lock(mergeLevelsMutex);
        mergeLevels[0]->sevs.push_back(sev);

        ++totalSevCount;

        uint32_t verifySevCount= 0;
        BOOST_FOREACH(MergeLevelPtr &l, mergeLevels) {
            verifySevCount += l->sevs.size();
        }
        SINVARIANT(verifySevCount == totalSevCount);
    }

    return true;
}

template <typename FieldType, typename FieldComparator>
void ParallelMemorySortModule<FieldType, FieldComparator>::
startMergeThread() {
    LintelLogDebug("parallelmemorysortmodule", "Creating thread local storage.");
    tls.reset(createTls());

    // If there is stuff to merge then do so. Otherwise, fetch more data from upstream.

    // Phase 1: Prefer same-level merging over retrieving.
    while (mergeData() || retrieveData());
    while (mergeData(true));
}

template <typename
FieldType, typename FieldComparator>
typename ParallelMemorySortModule<FieldType, FieldComparator>::ThreadLocalStorage*
ParallelMemorySortModule<FieldType, FieldComparator>::
createTls() {
    ThreadLocalStorage *tls = new ThreadLocalStorage();

    tls->positionComparatorData.reset(new ComparatorData(fieldName, fieldComparator));
    tls->sevComparatorData.reset(new ComparatorData(fieldName, fieldComparator));

    tls->positionComparator.reset(new PositionComparator(*tls->positionComparatorData));
    tls->sevComparator.reset(new SevComparator(*tls->sevComparatorData));

    return tls;
}

template <typename FieldType, typename FieldComparator>
ParallelMemorySortModule<FieldType, FieldComparator>::Sev::
Sev(ExtentPtr &extent, ThreadLocalStorage &tls) {
    extents.push_back(extent);
    positions.reserve(extent->getRecordCount());

    ExtentSeries series(extent.get());
    for (; series.more(); series.next()) {
        AbsolutePosition absolute;
        absolute.position = series.getCurPos();
        absolute.extent = extent.get();
        positions.push_back(absolute);
    }

    tls.positionComparator->setExtent(extent.get());
    std::sort(positions.begin(), positions.end(), *tls.positionComparator);

    iterator = positions.begin();
}

template <typename FieldType, typename FieldComparator>
ParallelMemorySortModule<FieldType, FieldComparator>::Sev::
Sev(std::vector<SevPtr> &sevs, ThreadLocalStorage &tls) {
    DEBUG_SINVARIANT(!sevs.empty());
    DEBUG_SINVARIANT(!sevs[0]->extents.empty());

    tls.sevComparator->setExtent(sevs[0]->extents[0].get());

    PriorityQueue<Sev*, SevComparator> queue(*tls.sevComparator);
    size_t count = 0;
    size_t size = 0;
    BOOST_FOREACH(SevPtr &sev, sevs) {
        size += sev->positions.size();
        count += sev->extents.size();
        extents.insert(extents.end(), sev->extents.begin(), sev->extents.end());
        queue.push(sev.get());
    }
    SINVARIANT(extents.size() == count);

    positions.reserve(size);

    while (!queue.empty()) {
        Sev *sev = queue.top();
        positions.push_back(*sev->iterator);
        ++(sev->iterator);
        if (sev->iterator == sev->positions.end()) {
            --(sev->iterator); // Revert the change before popping!
            queue.pop();
        } else {
            queue.replaceTop(sev);
        }
    }

    INVARIANT(positions.size() == size, boost::format("We have %s records but expected %s.") % positions.size() % size);

    iterator = positions.begin();
}

template <typename FieldType, typename FieldComparator>
ParallelMemorySortModule<FieldType, FieldComparator>::
AbstractComparator::AbstractComparator(ComparatorData &data)
    : data(data) {
}

template <typename FieldType, typename FieldComparator>
void ParallelMemorySortModule<FieldType, FieldComparator>::
AbstractComparator::setExtent(Extent *extent) {
    data.seriesLhs.setExtent(extent);
    data.seriesRhs.setExtent(extent);
}

template <typename FieldType, typename FieldComparator>
ParallelMemorySortModule<FieldType, FieldComparator>::
ComparatorData::ComparatorData(const std::string &fieldName,
                               FieldComparator &fieldComparator)
    : fieldComparator(fieldComparator),
      fieldLhs(seriesLhs, fieldName), fieldRhs(seriesRhs, fieldName) {
}

template <typename FieldType, typename FieldComparator>
ParallelMemorySortModule<FieldType, FieldComparator>::
PositionComparator::PositionComparator(ComparatorData &data)
    : AbstractComparator(data) {
}

template <typename FieldType, typename FieldComparator>
bool ParallelMemorySortModule<FieldType, FieldComparator>::
PositionComparator::operator()(const AbsolutePosition &positionLhs, const AbsolutePosition &positionRhs) {
    this->data.seriesLhs.setCurPos(positionLhs.position);
    this->data.seriesRhs.setCurPos(positionRhs.position);
    return this->data.fieldComparator(this->data.fieldLhs, this->data.fieldRhs);
}

template <typename FieldType, typename FieldComparator>
ParallelMemorySortModule<FieldType, FieldComparator>::
SevComparator::SevComparator(ComparatorData &data)
    : AbstractComparator(data) {
}

template <typename FieldType, typename FieldComparator>
bool ParallelMemorySortModule<FieldType, FieldComparator>::
SevComparator::operator()(Sev *sevLhs, Sev *sevRhs) {
    // sevLhs->iterator and sevLhs->iterator are valid entries
    DEBUG_SINVARIANT(sevLhs->iterator != sevLhs->positions.end());
    DEBUG_SINVARIANT(sevRhs->iterator != sevRhs->positions.end());
    DEBUG_SINVARIANT(sevLhs->iterator->extent != NULL);
    DEBUG_SINVARIANT(sevRhs->iterator->extent != NULL);
    this->data.seriesLhs.relocate(sevLhs->iterator->extent, sevLhs->iterator->position);
    this->data.seriesRhs.relocate(sevRhs->iterator->extent, sevRhs->iterator->position);

    return this->data.fieldComparator(this->data.fieldRhs, this->data.fieldLhs);
}

template <typename FieldType, typename FieldComparator>
Extent* ParallelMemorySortModule<FieldType, FieldComparator>::
createNextExtent() {
    if (finalSev->iterator == finalSev->positions.end()) {
        return NULL;
    }

    Extent *destinationExtent = new Extent(finalSev->iterator->extent->getType());

    ExtentSeries destinationSeries(destinationExtent);
    ExtentSeries sourceSeries(destinationExtent);
    ExtentRecordCopy recordCopier(sourceSeries, destinationSeries);

    for (; finalSev->iterator != finalSev->positions.end(); ++finalSev->iterator) {
        sourceSeries.relocate(finalSev->iterator->extent, finalSev->iterator->position);
        destinationSeries.newRecord();
        recordCopier.copyRecord();
        if (extentSizeLimit != 0 && destinationExtent->size() >= extentSizeLimit) {
            break;
        }
    }

    return destinationExtent;
}

#endif
