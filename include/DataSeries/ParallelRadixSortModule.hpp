// -*-C++-*-
/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A module which buffers all the extents from the upstream module
    and sorts all the records in memory. getExtent returns new extents.
*/

#ifndef __DATASERIES_PARALLELRADIXSORTMODULE_H
#define __DATASERIES_PARALLELRADIXSORTMODULE_H

#include <netinet/in.h>

#include <vector>
#include <algorithm>
#include <memory>

#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/format.hpp>

#include <Lintel/LintelLog.hpp>
#include <Lintel/Clock.hpp>
#include <Lintel/PriorityQueue.hpp>
#include <Lintel/PThread.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/Field.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/Extent.hpp>

class ParallelRadixSortModule : public DataSeriesModule {
public:
    /** Constructs a new @c ParallelRadixSortModule that will sort all the records based on the field
        named @param field_name\. A sorting functor must also be provided.
        \param upstream_module The upstream @c DataSeriesModule from which this module requests
                               extents.
        \param field_name      The name of the field on which the records will be sorted.
        \param extent_size_limit The maximum size of the extents returned by getExtent. A value
                               of 0 indicates that a single extent should be returned.
        \param thread_count    The number of threads to use. -1 indicates that the number of threads
                               should be equal to the number of CPU cores.*/
    ParallelRadixSortModule(DataSeriesModule &upstream_module,
                            const std::string &field_name,
                            size_t extent_size_limit = 1 << 20,
                            int32_t thread_count = -1);

    virtual ~ParallelRadixSortModule();

    /** Returns the next @c Extent according to the sorted order. Note that the first call to
        getExtent will be significantly slower than the rest, because it triggers the sorting process. */
    virtual Extent *getExtent();

private:
    class Position {
    public:
        Position(Extent *extent, const void* position, uint32_t cache)
            : extent(extent), position(position), cache(cache) { }
        Position() { }
        Extent *extent;
        const void* position;
        uint32_t cache;
    };
    typedef std::vector<Position> PositionVector;

    class Bucket {
    public:
        Bucket() : count(0) { }
        PositionVector positions;
        size_t count;
        PositionVector::iterator forward_iterator;
        PositionVector::iterator backward_iterator;
    };

    /** A data class to hold data that is needed by an AbstractComparator. This cannot
        be stored directly in AbstractComparator because std::sort copies the comparator
        (once for each item!), resulting in a performance hit. */
    class ComparatorData {
    public:
        ComparatorData(const std::string &field_name);
        ExtentSeries series_lhs;
        ExtentSeries series_rhs;
        FixedWidthField field_lhs;
        FixedWidthField field_rhs;
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
        bool operator()(const Position &position_lhs, const Position &position_rhs);
    };

    void retrieveExtents();
    void prepareBuckets();
    void sortBuckets();
    void startCreatingExtents();
    //Extent* createNextExtent();
    Extent* copyNextExtent();
    void startSortThread(uint32_t thread_index);
    void startPrepareThread(uint32_t thread_index);
    void startCopyThread(uint32_t thread_index);

    typedef std::vector<Extent*> ExtentVector;
    typedef std::deque<Extent*> ExtentQueue;
    typedef std::vector<Bucket> BucketVector;

    ExtentVector extents;
    BucketVector buckets;

    bool initialized; // starts as false, becomes true after first call to upstream_module.getExtent
    DataSeriesModule &upstream_module;
    std::string field_name;
    size_t extent_size_limit;
    uint32_t thread_count;

    ExtentSeries series;
    FixedWidthField field;

    size_t total_record_count;
    size_t total_size;
    size_t average_record_size;
    size_t records_per_destination_extent;

    ExtentQueue downstream_extents;
    PThreadMutex downstream_lock;
    PThreadCond downstream_not_full_cond;
    PThreadCond downstream_not_empty_cond;
    BucketVector::iterator current_bucket;
    size_t current_position;
    size_t current_extent_before_copy;
    size_t current_extent_after_copy;

    Clock::Tfrac start_clock;
    Clock::Tfrac stop_clock;
    Clock::Tfrac copy_start_clock;
    Clock::Tfrac copy_stop_clock;

    typedef boost::shared_ptr<PThread> PThreadPtr;
    std::vector<PThreadPtr> copy_threads;

    class PrepareThread : public PThread {
    public:
        PrepareThread(ParallelRadixSortModule &module, uint32_t thread_index)
            : module(module), thread_index(thread_index) {}
        virtual ~PrepareThread() {}

        virtual void *run() {
            module.startPrepareThread(thread_index);
            return NULL;
        }

        ParallelRadixSortModule &module;
        uint32_t thread_index;
    };

    class SortThread : public PThread {
    public:
        SortThread(ParallelRadixSortModule &module, uint32_t thread_index)
            : module(module), thread_index(thread_index) {}
        virtual ~SortThread() {}

        virtual void *run() {
            module.startSortThread(thread_index);
            return NULL;
        }

        ParallelRadixSortModule &module;
        uint32_t thread_index;
    };

    class CopyThread : public PThread {
    public:
        CopyThread(ParallelRadixSortModule &module, uint32_t thread_index)
            : module(module), thread_index(thread_index) {
        }
        virtual ~CopyThread() {}

        virtual void *run() {
            module.startCopyThread(thread_index);
            return NULL;
        }

        ParallelRadixSortModule &module;
        uint32_t thread_index;
    };
};


ParallelRadixSortModule::
ParallelRadixSortModule(DataSeriesModule &upstream_module,
                        const std::string &field_name,
                        size_t extent_size_limit,
                        int32_t thread_count)
    : initialized(false),
      upstream_module(upstream_module), field_name(field_name),
      extent_size_limit(extent_size_limit),
      thread_count(thread_count == -1 ? PThreadMisc::getNCpus() : thread_count),
      field(series, field_name),
      total_record_count(0), total_size(0), current_position(0),
      current_extent_before_copy(0), current_extent_after_copy(0) {
    LintelLogDebug("ParallelRadixSortModule", boost::format("Using %s threads.") % this->thread_count);
    buckets.resize(1 << 16);

    extents.reserve(20000 /*(1 << 30) / (64 << 10)*/);
    BOOST_FOREACH(Bucket &bucket, buckets) {
        bucket.positions.reserve(200);
    }
}

ParallelRadixSortModule::~ParallelRadixSortModule() {
    BOOST_FOREACH(Extent *extent, extents) {
        delete extent;
    }
    extents.clear();
}

Extent *ParallelRadixSortModule::getExtent() {
    if (!initialized) {
        retrieveExtents();
        prepareBuckets();
        sortBuckets();
        startCreatingExtents();
        initialized = true;
    }

    return copyNextExtent();
}

void ParallelRadixSortModule::retrieveExtents() {


    ExtentSeries series;
        FixedWidthField field(series, field_name);
    start_clock = Clock::todTfrac();
    Extent *extent = NULL;
    while ((extent = upstream_module.getExtent()) != NULL) {
        /*for (series.start(extent); series.more(); series.next()) {
            uint16_t bucket_index = htons(*reinterpret_cast<uint16_t*>(field.val()));
            ++buckets[bucket_index].count;
        }*/
        total_record_count += extent->getRecordCount();
        total_size += extent->size();
        extents.push_back(extent);



                        for (series.start(extent); series.more(); series.next()) {
                            uint16_t bucket_index = htons(*reinterpret_cast<uint16_t*>(field.val()));
                            uint32_t cache = htonl(*reinterpret_cast<uint32_t*>(field.val() + 2));
                            buckets[bucket_index].positions.push_back(Position(extent, series.getCurPos(), cache));

                        }




    }

    average_record_size = total_size / total_record_count;
    records_per_destination_extent = extent_size_limit / average_record_size + 1;

    stop_clock = Clock::todTfrac();
    LintelLogDebug("ParallelRadixSortModule", boost::format("retrieveExtents ran in %s seconds") % Clock::TfracToDouble(stop_clock - start_clock));
}

// TODO: parallelizing bucketing (divide buckets among threads) might be worth further exploration
void ParallelRadixSortModule::startPrepareThread(uint32_t thread_index) {
    ExtentSeries series;
    FixedWidthField field(series, field_name);

    // Place the elements in the buckets.
    if (thread_index == 0) {
        for (uint32_t i = 0; i < extents.size(); i += 2) {
            Extent *extent = extents[i];
            for (series.start(extent); series.more(); series.next()) {
                uint16_t bucket_index = htons(*reinterpret_cast<uint16_t*>(field.val()));
                uint32_t cache = htonl(*reinterpret_cast<uint32_t*>(field.val() + 2));
                *buckets[bucket_index].forward_iterator = Position(extent, series.getCurPos(), cache);
                ++buckets[bucket_index].forward_iterator;
            }
        }
    } else {
        for (uint32_t i = 1; i < extents.size(); i += 2) {
            Extent *extent = extents[i];
            for (series.start(extent); series.more(); series.next()) {
                uint16_t bucket_index = htons(*reinterpret_cast<uint16_t*>(field.val()));
                uint32_t cache = htonl(*reinterpret_cast<uint32_t*>(field.val() + 2));
                *buckets[bucket_index].backward_iterator = Position(extent, series.getCurPos(), cache);
                --buckets[bucket_index].backward_iterator;
            }
        }
    }
}

void ParallelRadixSortModule::prepareBuckets() {
    start_clock = Clock::todTfrac();

    /*// Allocate enough memory in each bucket for the positions.
    BOOST_FOREACH(Bucket &bucket, buckets) {
        bucket.positions.resize(bucket.count);
        bucket.forward_iterator = bucket.positions.begin();
        bucket.backward_iterator = bucket.positions.end();
        bucket.backward_iterator--;
    }



    BOOST_FOREACH(Bucket &bucket, buckets) {
            bucket.positions.reserve(200);
        }
    ExtentSeries series;
        FixedWidthField field(series, field_name);

        BOOST_FOREACH(Extent *extent, extents) {
                for (series.start(extent); series.more(); series.next()) {
                    uint16_t bucket_index = htons(*reinterpret_cast<uint16_t*>(field.val()));
                    uint32_t cache = htonl(*reinterpret_cast<uint32_t*>(field.val() + 2));
                    buckets[bucket_index].positions.push_back(Position(extent, series.getCurPos(), cache));

                }
        }



    stop_clock = Clock::todTfrac();
    LintelLogDebug("ParallelRadixSortModule", boost::format("prepareBuckets/0 ran in %s seconds") % Clock::TfracToDouble(stop_clock - start_clock));
    if (thread_count == 0) {
        startPrepareThread(0); // Don't create the thread.
        startPrepareThread(1);
    } else {
        std::vector<PThreadPtr> prepare_threads;
        for (uint32_t i = 0; i < 2; ++i) {
            PThreadPtr prepare_thread(new PrepareThread(*this, i));
            prepare_threads.push_back(prepare_thread);
            prepare_thread->start();
        }

        BOOST_FOREACH(PThreadPtr &prepare_thread, prepare_threads) {
            prepare_thread->join();
        }
    }

#if LINTEL_ASSERT_BOOST_DEBUG
    BOOST_FOREACH(Bucket &bucket, buckets) {
        --bucket.forward_iterator;
        SINVARIANT(bucket.forward_iterator == bucket.backward_iterator);
        ++bucket.forward_iterator;
    }
#endif

    stop_clock = Clock::todTfrac();
    LintelLogDebug("ParallelRadixSortModule", boost::format("prepareBuckets/1 ran in %s seconds") % Clock::TfracToDouble(stop_clock - start_clock));
    */
}


void ParallelRadixSortModule::startSortThread(uint32_t thread_index) {
    ComparatorData comparator_data(field_name);
    PositionComparator position_comparator(comparator_data);
    position_comparator.setExtent(extents[0]); // Initialize the comparator.

    uint32_t actual_thread_count = (thread_count == 0) ? 1 : thread_count;

    for (uint32_t i = thread_index; i < buckets.size(); i += actual_thread_count) {
        Bucket &bucket = buckets[i];
        std::sort(bucket.positions.begin(), bucket.positions.end(), position_comparator);
    }
}

void ParallelRadixSortModule::sortBuckets() {
    if (extents.empty()) {
        return;
    }

    start_clock = Clock::todTfrac();;

    if (thread_count == 0) {
        startSortThread(0); // Don't create the thread.
    } else {
        std::vector<PThreadPtr> sort_threads;
        for (uint32_t i = 0; i < thread_count; ++i) {
            PThreadPtr sort_thread(new SortThread(*this, i));
            sort_threads.push_back(sort_thread);
            sort_thread->start();
        }

        BOOST_FOREACH(PThreadPtr &sort_thread, sort_threads) {
            sort_thread->join();
        }
    }

    stop_clock = Clock::todTfrac();
    LintelLogDebug("ParallelRadixSortModule", boost::format("sortBuckets ran in %s seconds") % Clock::TfracToDouble(stop_clock - start_clock));
}

void ParallelRadixSortModule::startCopyThread(uint32_t thread_index) {
    uint32_t actual_thread_count = (thread_count == 0) ? 1 : thread_count;
    while (true) {
        //Clock::Tfrac extent_copy_start_clock = Clock::todTfrac();

        // Step 1: Find out what extents/records to copy to a destination extent.
        downstream_lock.lock();

        // Wait until there's space for the new extent.
        while (downstream_extents.size() == actual_thread_count) {
            downstream_not_full_cond.wait(downstream_lock);
        }

        // Take a number in line.
        size_t current_extent = current_extent_before_copy;


        BucketVector::iterator first_bucket = current_bucket;
        size_t first_position = current_position;

        size_t record_count = 0;

        while (record_count < records_per_destination_extent && current_bucket != buckets.end()) {
            //SINVARIANT(current_bucket->count == current_bucket->positions.size());
            size_t remaining_positions = current_bucket->positions.size() - current_position;
            if (record_count + remaining_positions <= records_per_destination_extent) {
                // Take all the remaining positions in this bucket.
                current_position = 0;
                ++current_bucket;
                record_count += remaining_positions;
            } else {
                // Take only some of the remaining positions in this bucket.
                current_position += records_per_destination_extent - record_count;
                record_count = records_per_destination_extent;
            }
        }

        BucketVector::iterator last_bucket = current_bucket;
        size_t end_position = current_position;

        if (record_count == 0) {
            SINVARIANT(current_bucket == last_bucket && current_position == end_position);
            downstream_lock.unlock();
            return;
        }

        ++current_extent_before_copy; // Take a number so that we can add the new extent in order.

        downstream_lock.unlock();

        if (record_count == 0) {
            SINVARIANT(current_bucket == last_bucket && current_position == end_position);
            return;
        }

        Extent *destination_extent = new Extent(extents[0]->getType());

        ExtentSeries destination_series(destination_extent);
        ExtentSeries source_series(destination_extent); // initialize so we can use relocate later
        ExtentRecordCopy record_copier(source_series, destination_series);
        destination_series.newRecords(record_count, false);

        //LintelLogDebug("ParallelRadixSortModule", boost::format("Creating an extent with %s records.") % record_count);

        // Step 2: Copy the extents/records to the destination extent.
        while (first_bucket != last_bucket) {
            while (first_position < first_bucket->positions.size()) {
                Position &position = first_bucket->positions[first_position];
                source_series.relocate(position.extent, position.position);
                record_copier.copyRecord();
                destination_series.next();
                ++first_position;
            }
            ++first_bucket;
            first_position = 0;
        }
        SINVARIANT(first_bucket == last_bucket);
        while (first_position < end_position) {
            Position &position = first_bucket->positions[first_position];
            source_series.relocate(position.extent, position.position);
            record_copier.copyRecord();
            destination_series.next();
            ++first_position;
        }

        // Step 3: Add the destination extent to the downstream queue.
        downstream_lock.lock();

        while (current_extent != current_extent_after_copy) {
            // Wait for another thread to finish copying an extent. Then maybe it will be our turn.
            downstream_not_empty_cond.wait(downstream_lock);
        }

        downstream_extents.push_back(destination_extent);
        SINVARIANT(current_extent_after_copy < current_extent_before_copy);
        ++current_extent_after_copy;

        downstream_not_empty_cond.broadcast();

        downstream_lock.unlock();

        //Clock::Tfrac extent_copy_stop_clock = Clock::todTfrac();

        //LintelLogDebug("ParallelRadixSortModule", boost::format("Copied extent in %s seconds") %
                       //Clock::TfracToDouble(extent_copy_stop_clock - extent_copy_start_clock));
    }
}

void ParallelRadixSortModule::startCreatingExtents() {
    copy_start_clock = Clock::todTfrac();

    if (extents.empty()) {
        return;
    }

    current_bucket = buckets.begin();
    current_position = 0;

    uint32_t actual_thread_count = (thread_count == 0) ? 1 : thread_count;

    for (uint32_t i = 0; i < actual_thread_count; ++i) {
        PThreadPtr copy_thread(new CopyThread(*this, i));
        copy_threads.push_back(copy_thread);
        copy_thread->start();
    }
}

Extent* ParallelRadixSortModule::copyNextExtent() {
    PThreadScopedLock lock(downstream_lock);

    while (downstream_extents.empty() && (current_bucket != buckets.end() ||
           current_extent_after_copy != current_extent_before_copy)) {
        downstream_not_empty_cond.wait(downstream_lock);
    }

    if (downstream_extents.empty()) {
        INVARIANT(total_record_count == 0, boost::format("%s records were left over.") % total_record_count);

        copy_stop_clock = Clock::todTfrac();
        LintelLogDebug("ParallelRadixSortModule", boost::format("Copying ran in %s seconds") % Clock::TfracToDouble(copy_stop_clock - copy_start_clock));
        return NULL;
    }

    Extent *destination_extent = downstream_extents.front();
    downstream_extents.pop_front();
    downstream_not_full_cond.signal();

    total_record_count -= destination_extent->getRecordCount();

    return destination_extent;
}
/*
Extent* ParallelRadixSortModule::createNextExtent() {
    start_clock = Clock::todTfrac();

    if (total_record_count == 0) {
        stop_clock = Clock::todTfrac();
        copy_clock += (stop_clock - start_clock);
        LintelLogDebug("ParallelRadixSortModule", boost::format("Copying ran in %s seconds") % Clock::TfracToDouble(copy_clock));
        return NULL;
    }

    Extent *destination_extent = new Extent(extents[0]->getType());

    ExtentSeries destination_series(destination_extent);
    ExtentSeries source_series(destination_extent); // initialize so we can use relocate later
    ExtentRecordCopy record_copier(source_series, destination_series);

    size_t record_count = std::min(total_record_count, records_per_destination_extent);
    total_record_count -= record_count;
    destination_series.newRecords(record_count, false);

    while (true) {
        while (position_iterator != bucket_iterator->positions.end()) {
            source_series.relocate(position_iterator->extent, position_iterator->position);
            record_copier.copyRecord();
            ++position_iterator;
            --record_count;
            destination_series.next();
            if (record_count == 0) {
                stop_clock = Clock::todTfrac();
                copy_clock += (stop_clock - start_clock);
                return destination_extent;
            }
        }
        ++bucket_iterator;
        position_iterator = bucket_iterator->positions.begin();
    }

    SINVARIANT(false);
    return NULL;
}
*/
ParallelRadixSortModule::
PositionComparator::PositionComparator(ComparatorData &data)
    : AbstractComparator(data) {
}

bool ParallelRadixSortModule::PositionComparator::operator()(const Position &position_lhs,
                                                           const Position &position_rhs) {
    if (position_lhs.cache < position_rhs.cache) {
        return true;
    }
    if (position_lhs.cache > position_rhs.cache) {
        return false;
    }
    this->data.series_lhs.relocate(position_lhs.extent, position_lhs.position);
    this->data.series_rhs.relocate(position_rhs.extent, position_rhs.position);
    return memcmp(this->data.field_lhs.val(), this->data.field_rhs.val(), this->data.field_lhs.size()) <= 0;
}

ParallelRadixSortModule::
AbstractComparator::AbstractComparator(ComparatorData &data)
    : data(data) {
}

void ParallelRadixSortModule::
AbstractComparator::setExtent(Extent *extent) {
    data.series_lhs.setExtent(extent);
    data.series_rhs.setExtent(extent);
}

ParallelRadixSortModule::
ComparatorData::ComparatorData(const std::string &field_name)
    : field_lhs(series_lhs, field_name), field_rhs(series_rhs, field_name) {
}

#endif
