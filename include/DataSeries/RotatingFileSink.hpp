#ifndef DATASERIES_ROTATING_FILE_SINK_HPP
#define DATASERIES_ROTATING_FILE_SINK_HPP

/*
   (c) Copyright 2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Interface for changing between IExtentSinks on demand.
*/

#include <boost/shared_ptr.hpp>

#include <Lintel/Deque.hpp>
#include <Lintel/PThread.hpp>

#include <DataSeries/DataSeriesSink.hpp>

namespace dataseries {
    /** \brief Class for changing between DataSeriesSink's on demand.  Most of the work is done by
      a separate thread so that the actual calls into RotatingFileSink should all be relatively
      fast. This implementation is probably slightly less efficient than one directly in
      DataSeriesSink, but it is much less invasive. */
    class RotatingFileSink : public IExtentSink {
    public:
        RotatingFileSink(uint32_t compression_modes = Extent::compress_all, 
                         uint32_t compression_level = 9);
        virtual ~RotatingFileSink();

        typedef boost::shared_ptr<DataSeriesSink> SinkPtr;
        
        /** Register a type on the rotating sink */
        const ExtentType &registerType(const std::string &xmldesc);

        /** Return true if we have completed starting to use a new file, so are free to change
            again. */
        bool canChangeFile();

        /** Blocking call to wait until canChangeFile is true */
        void waitForCanChange();

        /** Open a new file and start using it as the primary output.  sink hasn't finished
            closing.  If you change the filename to closed_filename, then the current output will
            be closed, but the sink will still accept extents.  Precondition: canChangeFile() */
        void changeFile(const std::string &new_filename);

        virtual void writeExtent(Extent &e, Stats *to_update);

        /** Errors out for now */
        virtual Stats getStats(Stats *from = NULL);
        virtual void removeStatsUpdate(Stats *would_update);

        static const std::string closed_filename;
    private:
        struct Pending {
            Extent *e;
            Stats *to_update;
            Pending(Extent &in_e, Stats *to_update) 
                : e(new Extent(in_e.getType())), to_update(to_update) 
            {
                e->swap(in_e);
            }
        };

        uint32_t compression_modes, compression_level;
        PThreadMutex mutex;
        PThreadCond cond;

        std::string new_filename;
        Deque<Pending> pending;

        DataSeriesSink *current_sink;

        PThreadFunction *pthread_worker;
        bool worker_continue;

        ExtentTypeLibrary library;
        void *worker();
    };
};

#endif
