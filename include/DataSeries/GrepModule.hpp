// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A module that filters records based on a given matching function.
*/

#ifndef __DATASERIES_GREPMODULE_H
#define __DATASERIES_GREPMODULE_H

#include <string>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>
#include <DataSeries/GeneralField.hpp>

class GrepModule : public DataSeriesModule {
public:
    typedef boost::function<bool (const Variable32Field&)> FieldMatcher;

    GrepModule(DataSeriesModule &upstreamModule,
               const std::string &fieldName,
               const FieldMatcher &fieldMatcher);

    Extent *getExtent();

private:
    DataSeriesModule &upstreamModule;
    std::string fieldName;
    FieldMatcher fieldMatcher;

    ExtentSeries sourceSeries;
    Variable32Field field;

    ExtentSeries destinationSeries;

    ExtentRecordCopy recordCopier;
};

GrepModule::GrepModule(DataSeriesModule &upstreamModule,
                       const std::string &fieldName,
                       const FieldMatcher &fieldMatcher)
    : upstreamModule(upstreamModule), fieldName(fieldName), fieldMatcher(fieldMatcher),
      field(sourceSeries, fieldName), recordCopier(sourceSeries, destinationSeries) {
}

Extent* GrepModule::getExtent() {
    std::auto_ptr<Extent> sourceExtent(upstreamModule.getExtent());
    sourceSeries.setExtent(sourceExtent.get());

    Extent *destinationExtent = NULL;
    bool matchFound = false;

    while (true) {
        for (; sourceSeries.more(); sourceSeries.next()) {
            if (fieldMatcher(field)) {
                if (!matchFound) { // the first match for the user's call to getExtent
                    destinationExtent = new Extent(sourceExtent->getType());
                    destinationSeries.setExtent(destinationExtent);
                    matchFound = true;
                }
                recordCopier.copyRecord();
            }
        }

        if (matchFound) break;
        sourceExtent.reset(upstreamModule.getExtent()); // the next extent
        if (sourceExtent.get() == NULL) break;
        sourceSeries.setExtent(sourceExtent.get());
    }

    return destinationExtent;
}

#endif
