// -*-C++-*-
/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A module which buffers all the extents from the upstream module
    and sorts all the records in memory. getExtent returns new extents.
*/

#ifndef __DATASERIES_MEMORYRADIXSORTMODULE_H
#define __DATASERIES_MEMORYRADIXSORTMODULE_H

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

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/Field.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/Extent.hpp>

class MemoryRadixSortModule : public DataSeriesModule {
public:
    /** Constructs a new @c MemoryRadixSortModule that will sort all the records based on the field
        named @param fieldName\. A sorting functor must also be provided.
        \param upstreamModule  The upstream @c DataSeriesModule from which this module requests
                               extents.
        \param fieldName       The name of the field on which the records will be sorted.
        \param extent_size_limit The maximum size of the extents returned by getExtent. A value
                               of 0 indicates that a single extent should be returned. */
    MemoryRadixSortModule(DataSeriesModule &upstreamModule,
                          const std::string &fieldName,
                          size_t extent_size_limit = 1 << 20);

    virtual ~MemoryRadixSortModule();

    /** Returns the next @c Extent according to the sorted order. Note that the first call to
        getExtent will be significantly slower than the rest, because it triggers the sorting process. */
    virtual Extent *getExtent();

private:
    class Position {
    public:
        Position(Extent *extent, const void* position, uint32_t cache)
            : extent(extent), position(position), cache(cache) { }
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
    Extent* createNextExtent();

    typedef std::vector<Extent*> ExtentVector;
    typedef std::vector<Bucket> BucketVector;

    ExtentVector extents;
    BucketVector buckets;
    BucketVector::iterator bucket_iterator;
    PositionVector::iterator position_iterator;

    bool initialized; // starts as false, becomes true after first call to upstreamModule.getExtent
    DataSeriesModule &upstreamModule;
    std::string field_name;
    size_t extent_size_limit;

    ComparatorData comparatorData;
    PositionComparator positionComparator;

    ExtentSeries series;
    FixedWidthField field;

    size_t total_record_count;
    size_t total_size;
    size_t average_record_size;
    size_t records_per_destination_extent;

    Clock::Tfrac start_clock;
    Clock::Tfrac stop_clock;
    Clock::Tfrac copy_clock;
};


MemoryRadixSortModule::
MemoryRadixSortModule(DataSeriesModule &upstreamModule,
                      const std::string &field_name,
                      size_t extent_size_limit)
    : initialized(false),
      upstreamModule(upstreamModule), field_name(field_name),
      extent_size_limit(extent_size_limit),
      comparatorData(field_name),
      positionComparator(comparatorData),
      field(series, field_name),
      total_record_count(0),
      total_size(0) {
}

MemoryRadixSortModule::~MemoryRadixSortModule() {
    BOOST_FOREACH(Extent *extent, extents) {
        delete extent;
    }
    extents.clear();
}

Extent *MemoryRadixSortModule::getExtent() {
    if (!initialized) {
        retrieveExtents();
        prepareBuckets();
        sortBuckets();

        bucket_iterator = buckets.begin();
        position_iterator = buckets[0].positions.begin();

        initialized = true;
    }

    return createNextExtent();
}

void MemoryRadixSortModule::retrieveExtents() {
    start_clock = Clock::todTfrac();
    buckets.resize(1 << 16);
    Extent *extent = NULL;
    while ((extent = upstreamModule.getExtent()) != NULL) {
        for (series.start(extent); series.more(); series.next()) {
            uint16_t bucket_index = htons(*reinterpret_cast<uint16_t*>(field.val()));
            ++buckets[bucket_index].count;
            ++total_record_count;
        }
        total_size += extent->size();
        extents.push_back(extent);
    }

    average_record_size = total_size / total_record_count;
    records_per_destination_extent = extent_size_limit / average_record_size + 1;

    stop_clock = Clock::todTfrac();
    LintelLogDebug("MemoryRadixSortModule", boost::format("retrieveExtents ran in %s seconds") % Clock::TfracToDouble(stop_clock - start_clock));
}

void MemoryRadixSortModule::prepareBuckets() {
    //const uint8_t bucket_power = 16;
    //const uint16_t bucket_mask = 0xFF;
    start_clock = Clock::todTfrac();

    // Count the number of elements in each bucket.
    /*BOOST_FOREACH(Extent *extent, extents) {
        for (series.start(extent); series.more(); series.next()) {
            //uint16_t bucket_index = bucket_mask & htons(*reinterpret_cast<uint16_t*>(field.val()));
            uint16_t bucket_index = htons(*reinterpret_cast<uint16_t*>(field.val()));
            ++buckets[bucket_index].count;
        }
    }*/

    // Allocate enough memory in each bucket for the positions.
    BOOST_FOREACH(Bucket &bucket, buckets) {
        bucket.positions.reserve(bucket.count);
    }

    // Place the elements in the buckets.
    BOOST_FOREACH(Extent *extent, extents) {
        for (series.start(extent); series.more(); series.next()) {
            //uint16_t bucket_index = bucket_mask & htons(*reinterpret_cast<uint16_t*>(field.val()));
            uint16_t bucket_index = htons(*reinterpret_cast<uint16_t*>(field.val()));
            uint32_t cache = htonl(*reinterpret_cast<uint32_t*>(field.val() + 2));
            buckets[bucket_index].positions.push_back(Position(extent, series.getCurPos(), cache));
        }
    }
    stop_clock = Clock::todTfrac();
    LintelLogDebug("MemoryRadixSortModule", boost::format("prepareBuckets ran in %s seconds") % Clock::TfracToDouble(stop_clock - start_clock));
}

void MemoryRadixSortModule::sortBuckets() {
    start_clock = Clock::todTfrac();;
    if (extents.empty()) {
        return;
    }
    positionComparator.setExtent(extents[0]); // Initialize the comparator.

    BOOST_FOREACH(Bucket &bucket, buckets) {
        std::sort(bucket.positions.begin(), bucket.positions.end(), positionComparator);
    }
    stop_clock = Clock::todTfrac();
    LintelLogDebug("MemoryRadixSortModule", boost::format("sortBuckets ran in %s seconds") % Clock::TfracToDouble(stop_clock - start_clock));
}

Extent* MemoryRadixSortModule::createNextExtent() {
    start_clock = Clock::todTfrac();

    if (total_record_count == 0) {
        stop_clock = Clock::todTfrac();
        copy_clock += (stop_clock - start_clock);
        LintelLogDebug("MemoryRadixSortModule", boost::format("Copying ran in %s seconds") % Clock::TfracToDouble(copy_clock));
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

MemoryRadixSortModule::
PositionComparator::PositionComparator(ComparatorData &data)
    : AbstractComparator(data) {
}

bool MemoryRadixSortModule::PositionComparator::operator()(const Position &position_lhs,
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

MemoryRadixSortModule::
AbstractComparator::AbstractComparator(ComparatorData &data)
    : data(data) {
}

void MemoryRadixSortModule::
AbstractComparator::setExtent(Extent *extent) {
    data.series_lhs.setExtent(extent);
    data.series_rhs.setExtent(extent);
}

MemoryRadixSortModule::
ComparatorData::ComparatorData(const std::string &field_name)
    : field_lhs(series_lhs, field_name), field_rhs(series_rhs, field_name) {
}

#endif
