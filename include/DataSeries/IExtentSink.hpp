#ifndef DATASERIES_IEXTENTSINK_HPP
#define DATASERIES_IEXTENTSINK_HPP

/*
   (c) Copyright 2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Interface for DataSeriesSink
*/

namespace dataseries {
    /** \brief Interface to sinks sufficient to support OutputModule */
    // TODO: see whether this interface is also sufficient for network sinks on the tomer branch.
    class IExtentSink {
    public:
        /** \brief Statistics on extents written via this sink.  Note that each implementation
            of the interface needs to maintain thread safety, and to provide implementations
            of getStats/removeStatsUpdate that are properly thread safe for callers */

        // TODO-sprint: We need mutex in Stats to avoid it being updated by multiple sinks, since
        // now it would be passed to multiple since through RotateFileSink.
        class Stats {
        public:
            Stats(const Stats &from) {
                *this = from;
            }
            Stats & operator=(const Stats &from);
            // If anything other than plain old data gets put in here, 
            // then the assignment operator needs to be fixed.
            /** The total number of Extents written */
            uint32_t extents;

            uint32_t
            /** The number of Extents that were not compressed at all */
            compress_none,
            /** The number of Extents that were compressed using lzo */
                compress_lzo,
            /** The number of Extents that were compressed using gzip */
                compress_gzip, 
            /** The number of Extents that were compressed using bzip2 */
                compress_bz2,
            /** The number of Extents that were compressed using lzf */
                compress_lzf;

            uint64_t
            /** The total number of bytes in the Extents before compression. */
            unpacked_size,
            /** The number of bytes in fixed size records before
                compression. */
                unpacked_fixed,
            /** The number of bytes in the Extents' string pools
                before compression. */
                unpacked_variable,
            /** The number of bytes in the Extents' string pools
                before any processing.  In particular it will
                include duplicate fields marked with pack_unique. */
                unpacked_variable_raw,
            /** The total number of bytes after compression. */
                packed_size,
	    /** The number of records that were written */
                nrecords;

            /** The time spent packing/compressing Extents.  This is
                supposed to be the thread time, rather than wall clock time.
                The pack_time statistic has been disabled pending figuring
                out how to make it accurate.  For mor discussion, see
                DataSeriesFile.C:get_thread_cputime() */
            double pack_time; 

            /** Initializes all statistics to 0. */
            Stats() {
                reset();
            }
            ~Stats();
            /** Reset all statistics to 0. */
            void reset();
            Stats & operator+=(const Stats &from);
            Stats & operator-=(const Stats &from);

            /// specify an extent type if this statistic was entirely for one
            /// type.
            void printText(std::ostream &to, const std::string &extent_type = "");

            void update(uint32_t unpacked_size, uint32_t unpacked_fixed,
                        uint32_t unpacked_variable_raw,
                        uint32_t unpacked_variable, uint32_t packed_size,
                        uint32_t packed_variable_size, size_t nrecords, double pack_time, 
                        unsigned char fixed_compress_mode,
                        unsigned char variable_compress_mode);

            uint32_t use_count; // how many updates are pending to this stats?
            void updateCompressMode(unsigned char compress_mode);
        };

        IExtentSink() { }
        virtual ~IExtentSink(); 

        /** Add an @c Extent to the write queue.

            writeExtent is thread safe, although obviously the precise
            order of writing the extents can't be guarenteed if two
            writeExtent calls are executing at the same time.  The to_update
            stats allows for separate statistics for different extent types.
            
            \arg e writeExtent is destructive, the contents of e will be
                moved (not copied) to the queue, so e will be empty on return.
            
            \arg toUpdate If toUpdate is not NULL, it will be updated
                with statistics about writing e, when e is actually written.
            
            \warning A copy of the pointer toUpdate is saved, so the pointee must
                not be destroyed, without either closing the file or calling
                \link DataSeriesSink::removeStatsUpdate removeStatsUpdate \endlink
                first.  Also the stats are updated under a per sink lock, so should
                not be shared across multiple sinks.
            
           \pre \link DataSeriesSink::writeExtentLibrary writeExtentLibrary
               \endlink must have been called and the @c ExtentTypeLibrary passed
               to it must contain the type of e. */
        virtual void writeExtent(Extent &e, Stats *to_update) = 0;

        /** Returns combined stats for all the Extents that have been written so far.  (Meaning
            actually written, not just queued).  You are only guaranteed to have all the statistics
            for writing this file after you call close().  If you have passed a stats object into
            the sink when writing extents, you can get a copy of it by specifying that pointer, and
            using the same lock a copy of the stats will be made.  This allows you to make a safe
            copy of the stats for a subset of the extents rather than all of the ones for the
            file. */
        virtual Stats getStats(Stats *from = NULL) = 0;

        /** If you are going to delete a Stats that you previously used to
            write to a DataSeriesFile before you close the file, you need
            to call this. */
        virtual void removeStatsUpdate(Stats *would_update) = 0;
    };
};

#endif
