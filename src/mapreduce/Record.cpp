/*
 * Record.cpp
 *
 *  Created on: Apr 24, 2009
 *      Author: shirant
 */

#include <boost/format.hpp>

#include <Lintel/LintelLog.hpp>

#include <DataSeries/TypeIndexModule.hpp>

#include "Record.h"

using namespace std;
using boost::format;

Record::Record() :
    started(false), sort(false), index(0) {
}

Record::~Record() {
    close();
}

void Record::close() {
    outputModule.reset(NULL);
    sink.reset(NULL);
    inputModule.reset(NULL);
}

ExtentSeries& Record::getSeries() {
    return series;
}

//
// Methods for input records.
//

void Record::attachInput(const string &file, size_t bytesPerFetch, bool sort) {
    inputModule.reset(new TypeIndexModule(getTypeName()));
    inputModule->addSource(file);

    this->bytesPerFetch = bytesPerFetch;
    this->sort = sort;
    extentsPerFetch = 0; // will be calculated the first time fetchExtents is called
    started = false;
}

bool Record::read() {
    if (!started) {
        started = true;
        fetchExtents();
    } else {
        ++index;
    }

    if (extents.size() == 0) return false;

    // we have one or more extents

    // find the next non-empty extent
    while (index == series.getRecordCount()) {
        fetchExtents(); // also sets index to 0
        if (extents.size() == 0) return false;
    }

    series.setRecordIndex(index);
    return true;
}

void Record::fetchExtents() {
    extents.resize(0);
    index = 0;

    if (extentsPerFetch == 0) {
        Extent *extent = inputModule->getExtent(); // TODO: someone needs to release all these extents!!
        if (extent == NULL) return;
        extents.push_back(extent);

        extentsPerFetch = bytesPerFetch / extent->size();
        if (extentsPerFetch == 0) extentsPerFetch = 1;
        LintelLogDebug("mapreduce", format("Fetching %d extent(s) at a time") % extentsPerFetch);
    }

    while (extents.size() < extentsPerFetch) {
        Extent *extent = inputModule->getExtent();
        if (extent == NULL) break;
        extents.push_back(extent);
    }

    series.setExtents(extents);
    if (extents.size() == 0) return;
    if (sort) series.sortRecords(this);
}

//
// Methods for output records.
//

void Record::attachOutput(const string &file, int extentSize) {
    sink.reset(new DataSeriesSink(file, Extent::compress_none, 0));

    // create type library and write to file
    ExtentTypeLibrary library;
    const ExtentType *extentType = library.registerType(getTypeXml());
    series.setType(*extentType);
    outputModule.reset(new OutputModule(*sink, series, extentType, extentSize));
    sink->writeExtentLibrary(library);
}

void Record::create() {
    outputModule->newRecord();
}
