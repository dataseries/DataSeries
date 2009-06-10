// -*-C++-*-
/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#ifndef DATASERIES_PUSHMODULE_H
#define DATASERIES_PUSHMODULE_H

#include <deque>

#include <Lintel/PThread.hpp>

#include <DataSeries/DataSeriesModule.hpp>

/** A module that allows you to push in extents and read them out simultaneously. getExtent
    should not be called in the same thread as addExtent, because getExtent will block if
    there are no extents in the buffer and close has not been called. */
class PushModule : public DataSeriesModule {
public:
    PushModule() : size(0), closed(false) {}
    virtual ~PushModule() {}

    /** Add an extent to the internal buffer. */
    void addExtent(Extent *extent);

    /** Returns the total amount of data (in bytes) that is currently buffered. */
    size_t getSize();

    /** Indicate that no more extents will be added. After this is called, getExtent will
        no longer block, and will return NULL when it is out of data. */
    void close();

    /** Returns the first buffered extent, if available. Otherwise, it blocks until an
        extent is added via addExtent, or close is called (in which case NULL is returned).
        Note that the user of this module must ensure that all of the extents that were
        added were later retrieved. (Otherwise, there will be a memory leak.) */
    Extent* getExtent();

private:
    size_t size;
    bool closed;
    std::deque<Extent*> extents;
    PThreadMutex mutex;
    PThreadCond cond;
};

#endif
