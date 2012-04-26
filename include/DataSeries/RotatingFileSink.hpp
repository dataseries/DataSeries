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
        
        /** Register a type on the rotating sink; only valid to call up to the first call to
            changeFile. */
        const ExtentType::Ptr registerType(const std::string &xmldesc);

        /** Set the extent write callback; will set the current callback, but the old callback may
            continue to be used until a rotation is completed, i.e. a call to
            changeFile(something); waitForCanChange(); Note, it is safe to call the canChangeFile,
            getNewFilename, and changeFile methods while in the callback, but it is not safe to
            call the other operations because that could block up the writer thread which would
            result in blocking up the queue in the data series file. Also note that the callback
            may be called on extents written to a file that is no longer current since rotation has
            already occurred.  Therefore, if you are going to rotate based on extent position, then
            you should also limit rotation to some frequency, and/or check that the current file is
            large before re-rotating. */
        void setExtentWriteCallback(const DataSeriesSink::ExtentWriteCallback &callback);

        /** Complete the transition to a new sink (if any), and flush out the current data series
            sink.  If you change the file during a flush, this function may exit with a change in
            progress.  However, setExtentWriteCallback() followed by a flush will guarantee that
            after the flush completes the new callback will be called and any old one will not. */

        void flush();

        /** Close a RotatingFileSink, after this call, no callback will be called, although the
            RotatingFileSink will still continue to buffer extents */
        void close();

        virtual void writeExtent(Extent &e, Stats *to_update);

        /** Errors out for now */
        virtual Stats getStats(Stats *from = NULL);
        virtual void removeStatsUpdate(Stats *would_update);

        static const std::string closed_filename;

        /** Return true if we have completed starting to use a new file, so are free to change
            again. */
        bool canChangeFile();

        /** Return the name of the file we are changing to (if any) */
        std::string getNewFilename();

        /** Blocking call to wait until canChangeFile is true */
        void waitForCanChange();

        /** Open a new file and start using it as the primary output.  sink hasn't finished
            closing.  If you change the filename to closed_filename, then the current output will
            be closed, but the sink will still accept extents.  Precondition: canChangeFile() or
            failure_ok set to true.  Returns true on successful change or false otherwise */
        bool changeFile(const std::string &new_filename, bool failure_ok = false);
    private:
        struct Pending {
            Extent *e;
            Stats *to_update;
            Pending(Extent &in_e, Stats *to_update) 
                : e(new Extent(in_e.getTypePtr())), to_update(to_update) 
            {
                e->swap(in_e);
            }
        };

        PThreadMutex mutex; 
        PThreadCond cond;

        // Mostly fixed values
        uint32_t compression_modes, compression_level;
        PThreadFunction *pthread_worker;
        ExtentTypeLibrary library;
        
        // Variable values
        Deque<Pending> pending;
        DataSeriesSink *current_sink;
        DataSeriesSink::ExtentWriteCallback callback;

        PThreadMutex worker_mutex; // order is worker_mutex before mutex
        bool worker_continue;
        std::string new_filename;

        void *worker();
        void workerNullifyCurrent(PThreadScopedLock &worker_lock);
    };
};

#endif
