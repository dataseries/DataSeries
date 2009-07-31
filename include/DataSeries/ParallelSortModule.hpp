// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A module which sorts all the extents from the upstream module. If the sorting cannot be completed
    in memory, an external sort is used.
*/

#ifndef __DATASERIES_PARALLELSORTMODULE_H
#define __DATASERIES_PARALLELSORTMODULE_H

#include <stdlib.h>

#include <string>
#include <vector>
#include <deque>
#include <iostream>

#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/format.hpp>
#include <boost/foreach.hpp>

#include <Lintel/PriorityQueue.hpp>

#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/ExtentReader.hpp>
#include <DataSeries/ExtentWriter.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/ParallelRadixSortModule.hpp>
#include <DataSeries/ThrottleModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/ThreadSafeBuffer.hpp>


/** A class that sorts input of any size. It uses an external (ie, two-phase) sort when there
    if too much data to do it in memory (the amount of available memory is an input parameter). */
class ParallelSortModule : public DataSeriesModule {
public:
    /** Constructs a new @c ParallelSortModule that will sort all the records based on the field
        named @param field_name\. A sorting functor must also be provided.
        \param upstream_module  The upstream @c DataSeriesModule from which this module requests
                               extents.
        \param field_name       The name of the field on which the records will be sorted.
        \param extent_size_limit The maximum size of the extents returned by getExtent. A value
                               of 0 indicates that a single extent should be returned.
        \param memory_limit     The maximum amount of memory to use for the upstream buffers. Note
                               that some overhead should be assumed.
        \param compress_temp    Whether or not LZF compression should be used on the merge files.
        \param temp_file_prefix  In case an external (two-phase) sort is required, @c ParallelSortModule will
                               create temporary DataSeries files. The files will be named by appending
                               an incrementing integer to the specified @param temp_file_prefix\. */
    ParallelSortModule(DataSeriesModule &upstream_module,
               const std::string &field_name,
               size_t extent_size_limit = 1 << 20, // 1 MB
               int32_t thread_count = -1,
               size_t memory_limit = 1 << 30, // 1 GB
               bool compress_temp = false,
               const std::string &temp_file_prefix = "");

    virtual ~ParallelSortModule();

    /** Returns the next @c Extent according to the sorted order. Note that the first call to
        getExtent will be significantly slower than the rest, because it triggers the sorting process. */
    virtual Extent *getExtent();

private:
    typedef boost::shared_ptr<ParallelRadixSortModule> ParallelRadixSortModulePtr;
    struct SortedMergeFile {
        SortedMergeFile(const std::string &file, const ExtentType &extent_type);
        void open();

        std::string file_name;
        const ExtentType &extent_type;

        boost::shared_ptr<ExtentReader> input_module;
        boost::shared_ptr<Extent> extent; // the extent that we're currently reading from
        const void *position; // where are we in the current extent?
    };
    typedef boost::function <bool (SortedMergeFile*, SortedMergeFile*)> SortedMergeFileComparator;

    struct BatchWorkItem {
    	BatchWorkItem(Extent *first_extent, ParallelRadixSortModulePtr module) :
				first_extent(first_extent), module(module) {}

    	Extent *first_extent;
    	ParallelRadixSortModulePtr module;
    };

    typedef boost::shared_ptr<BatchWorkItem> BatchWorkItemPtr;

    class WriteThread : public PThread {
	public:
		WriteThread(ParallelSortModule *module) :
			module(module) {}
		virtual ~WriteThread() {}

		virtual void* run() {
			module->startWriteThread();
			return NULL;
		}

	private:
		ParallelSortModule *module;
    };

    void createSortedFiles(Extent *first_extent);
    void prepareSortedMergeFiles();
    Extent *createNextExtent();
    bool compareSortedMergeFiles(SortedMergeFile *sorted_merge_file_lhs,
                                 SortedMergeFile *sorted_merge_file_rhs);
    void resetMemorySortModule();
    void createTemporaryDir();
    void startWriteThread();

    bool initialized;
    bool external; // automatically set to true when upstream module provides more data than
                   // we can store in memory

    DataSeriesModule &upstream_module;
    std::string field_name;
    size_t extent_size_limit;
    uint32_t thread_count;
    size_t memory_limit;
    bool compress_temp;
    std::string temp_file_dir;
    std::string temp_file_prefix;

    ThrottleModule throttle_module;
    ParallelRadixSortModulePtr memory_sort_module;
    ThreadSafeBuffer<BatchWorkItemPtr> batch_sort_buffer;

    std::vector<boost::shared_ptr<SortedMergeFile> > sorted_merge_files;
    PriorityQueue<SortedMergeFile*, SortedMergeFileComparator> sorted_merge_file_queue;

    ExtentSeries series_lhs;
    ExtentSeries series_rhs;
    FixedWidthField field_lhs;
    FixedWidthField field_rhs;

    Clock::Tfrac phase_start_clock;
    Clock::Tfrac phase_stop_clock;

    WriteThread write_thread;
};

ParallelSortModule::ParallelSortModule(DataSeriesModule &upstream_module,
                                       const std::string &field_name,
                                       size_t extent_size_limit,
                                       int32_t thread_count,
                                       size_t memory_limit,
                                       bool compress_temp,
                                       const std::string &temp_file_prefix)
    : initialized(false), external(false),
      upstream_module(upstream_module), field_name(field_name),
      extent_size_limit(extent_size_limit), thread_count(thread_count), memory_limit(memory_limit),
      compress_temp(compress_temp), temp_file_prefix(temp_file_prefix),
      throttle_module(this->upstream_module, memory_limit / 2),
      batch_sort_buffer(1),
      sorted_merge_file_queue(boost::bind(&ParallelSortModule::compareSortedMergeFiles, this, _1, _2)),
      field_lhs(series_lhs, field_name), field_rhs(series_rhs, field_name), write_thread(this) {
    resetMemorySortModule();
}

ParallelSortModule::~ParallelSortModule() {
    if (!temp_file_dir.empty()) {
        LintelLogDebug("ParallelSortModule", boost::format("Removing the temporary directory %s") % temp_file_dir);
        rmdir(temp_file_dir.c_str());
    }
}

Extent *ParallelSortModule::getExtent() {
    if (!initialized) {
        initialized = true;
        phase_start_clock = Clock::todTfrac();
        Extent *first_extent = memory_sort_module->getExtent();
        external = throttle_module.limitReached();
        if (!external) {
            return first_extent;
        }
        write_thread.start();
        createTemporaryDir();
        createSortedFiles(first_extent);
        phase_stop_clock = Clock::todTfrac();
        LintelLogDebug("ParallelSortModule",
                       boost::format("Completed phase 1 of two-phase sort in %s seconds.") %
                       Clock::TfracToDouble(phase_stop_clock - phase_start_clock));
        phase_start_clock = phase_stop_clock;
        prepareSortedMergeFiles();
    }

    // for external sort we need to merge from the files; for memory sort we just return an extent
    return external ? createNextExtent() : memory_sort_module->getExtent();
}

void ParallelSortModule::startWriteThread() {
	uint32_t i = 0;
	BatchWorkItemPtr work_item;
	while (batch_sort_buffer.remove(&work_item)) {
		Extent *extent = work_item->first_extent;
		INVARIANT(extent != NULL, "Why are we making an empty file?");

		// create a new input file entry
		boost::shared_ptr<SortedMergeFile> sorted_merge_file(new SortedMergeFile(
				temp_file_prefix + (boost::format("%d") % i++).str(),
				extent->getType()));
		sorted_merge_files.push_back(sorted_merge_file);

		// create the sink
		ExtentWriter sink(sorted_merge_file->file_name, compress_temp, false);

		Clock::Tfrac start_clock = Clock::todTfrac();

		LintelLogDebug("ParallelSortModule",
					   boost::format("Created a temporary file for the external sort: '%s'") %
					   sorted_merge_file->file_name);

		uint32_t extent_count = 0;
		do {
			//LintelLogDebug("ParallelSortModule", boost::format("Writing extent #%s to the temporary file.") % extent_count);
			++extent_count;
			sink.writeExtent(extent);
			delete extent;
			extent = work_item->module->getExtent();
		} while (extent != NULL);

		// close the sink
		sink.close();

		LintelLogDebug("ParallelSortModule", "Finished creating the temporary file.");

		Clock::Tfrac stop_clock = Clock::todTfrac();

		LintelLogDebug("ParallelSortModule",
					   boost::format("Wrote the file '%s' in %s seconds.") %
					   sorted_merge_file->file_name % Clock::TfracToDouble(stop_clock - start_clock));
	}
}

void ParallelSortModule::createSortedFiles(Extent *first_extent) {
    bool last_batch = !throttle_module.limitReached();
    SINVARIANT(!last_batch); // We already know this is an external sort.

    Extent *extent = first_extent;
    while (true) {
    	// Take memory_sort_module and start writing its extents to a temporary file.
    	BatchWorkItemPtr work_item(new BatchWorkItem(extent, memory_sort_module));
    	batch_sort_buffer.add(work_item);

    	if (last_batch) {
    		break;
    	}

    	resetMemorySortModule();
    	extent = memory_sort_module->getExtent(); // This will cause extents to start flowing through our throttle module.
    	last_batch = !throttle_module.limitReached();
    };

    batch_sort_buffer.signalDone();
    write_thread.join();
}

void ParallelSortModule::resetMemorySortModule() {
    memory_sort_module.reset(new ParallelRadixSortModule(throttle_module, field_name, extent_size_limit, thread_count));
    throttle_module.reset();
}

void ParallelSortModule::prepareSortedMergeFiles() {
    // create the input modules and read/store the first extent from each one
    BOOST_FOREACH(boost::shared_ptr<SortedMergeFile> &sorted_merge_file, sorted_merge_files) {
        sorted_merge_file->open();
        sorted_merge_file_queue.push(sorted_merge_file.get()); // add the file to our priority queue
    }
}

Extent *ParallelSortModule::createNextExtent() {
    if (sorted_merge_file_queue.empty()) {
        phase_stop_clock = Clock::todTfrac();
        LintelLogDebug("ParallelSortModule",
                       boost::format("Completed phase 2 of two-phase sort in %s seconds.") %
                       Clock::TfracToDouble(phase_stop_clock - phase_start_clock));
        return NULL;
    }

    SortedMergeFile *sorted_merge_file = sorted_merge_file_queue.top();
    INVARIANT(sorted_merge_file->extent != NULL, "each file must have at least one extent"
            " and we are never returning finished input files to the queue");

    Extent *destinationExtent = new Extent(sorted_merge_file->extent->getType());

    ExtentSeries destination_series(destinationExtent);
    ExtentSeries source_series(destinationExtent); // just so we can use relocate later
    ExtentRecordCopy recordCopier(source_series, destination_series);

    size_t recordCount = 0;

    // each iteration of this loop adds a single record to the destination extent
    do {
        sorted_merge_file = sorted_merge_file_queue.top();

        source_series.relocate(sorted_merge_file->extent.get(), sorted_merge_file->position);
        destination_series.newRecord();
        recordCopier.copyRecord();
        ++recordCount;

        source_series.next();

        if (source_series.more()) { // more records available in the current extent
            sorted_merge_file->position = source_series.getCurPos();
            sorted_merge_file_queue.replaceTop(sorted_merge_file);
        } else {
            Extent *nextExtent = NULL;

            do { // skip over any empty extents
                nextExtent = sorted_merge_file->input_module->getExtent();
                if (nextExtent == NULL) {
                    break; // this input file is done
                }
                source_series.setExtent(nextExtent);
            } while (!source_series.more());

            if (nextExtent == NULL) { // no more records so pop it out
                sorted_merge_file_queue.pop();
                sorted_merge_file->extent.reset(); // be nice and clean up!
                sorted_merge_file->position = NULL;
                unlink(sorted_merge_file->file_name.c_str()); // delete this temporary file
            } else { // more records available in a new extent
                sorted_merge_file->extent.reset(nextExtent);
                sorted_merge_file->position = source_series.getCurPos();
                sorted_merge_file_queue.replaceTop(sorted_merge_file);
            }
        }
    } while (!sorted_merge_file_queue.empty() &&
             (extent_size_limit == 0 || destinationExtent->size() < extent_size_limit));

    return destinationExtent;
}

bool ParallelSortModule::compareSortedMergeFiles(SortedMergeFile *sorted_merge_file_lhs,
                                                 SortedMergeFile *sorted_merge_file_rhs) {
    series_lhs.setExtent(sorted_merge_file_lhs->extent.get());
    series_lhs.setCurPos(sorted_merge_file_lhs->position);

    series_rhs.setExtent(sorted_merge_file_rhs->extent.get());
    series_rhs.setCurPos(sorted_merge_file_rhs->position);

    // swap field_rhs and field_lhs because compareSortedExtents == "less important" and
    // fieldComparator == "less than"
    return memcmp(field_rhs.val(), field_lhs.val(), field_lhs.size()) <= 0;
}

void ParallelSortModule::createTemporaryDir() {
    if (temp_file_prefix.empty()) {
        char path[50];
        strcpy(path, "/tmp/sortXXXXXX");
        CHECKED(mkdtemp(path) != NULL,
                boost::format("Unable to create the temporary directory '%s'") % path);
        this->temp_file_dir = path;
        this->temp_file_prefix = path;
        this->temp_file_prefix += "/batch";
    }
    LintelLogDebug("ParallelSortModule", boost::format("Temporary files will have the prefix '%s'") %
                   this->temp_file_prefix);
}

ParallelSortModule::SortedMergeFile::SortedMergeFile(const std::string &file_name,
                                                     const ExtentType &extent_type)
    : file_name(file_name), extent_type(extent_type) {
}

void ParallelSortModule::SortedMergeFile::open() {
    input_module.reset(new ExtentReader(file_name, extent_type));
    extent.reset(input_module->getExtent());
    INVARIANT(extent.get() != NULL, boost::format("Why do we have an empty file (%s)?") % file_name);

    ExtentSeries series(extent.get());
    position = series.getCurPos();
}

#endif
