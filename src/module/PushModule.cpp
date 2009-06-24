// -*-C++-*-
/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <DataSeries/PushModule.hpp>

void PushModule::addExtent(Extent *extent) {
    PThreadScopedLock lock(mutex);
    size += extent->size();
    extents.push_back(extent);
}

Extent* PushModule::getExtent() {
    PThreadScopedLock lock(mutex);
    while (extents.empty()) {
        if (closed) {
            return NULL;
        }
        cond.wait(mutex);
    }
    Extent *extent = extents.front();
    extents.pop_front();
    size -= extent->size();
    return extent;
}

void PushModule::close() {
    PThreadScopedLock lock(mutex);
    closed = true;
    cond.broadcast();
}

size_t PushModule::getSize() {
    PThreadScopedLock lock(mutex);
    return size;
}
