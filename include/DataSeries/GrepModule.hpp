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

template <typename FieldType, typename FieldMatcher>
class GrepModule : public DataSeriesModule {
public:
    GrepModule(DataSeriesModule &upstreamModule,
               const std::string &fieldName,
               const FieldMatcher &fieldMatcher);

    /// Currently returns exactly one extent. If your grep is non-selective,
    /// this extent could be very large.
    Extent *getExtent();
private:
    DataSeriesModule &upstreamModule;
    std::string fieldName;
    FieldMatcher fieldMatcher;

    ExtentSeries sourceSeries;
    FieldType field;

    ExtentSeries destinationSeries;

    ExtentRecordCopy recordCopier;
};

template <typename FieldType, typename FieldMatcher>
GrepModule<FieldType, FieldMatcher>::GrepModule(DataSeriesModule &upstreamModule,
                       const std::string &fieldName,
                       const FieldMatcher &fieldMatcher)
    : upstreamModule(upstreamModule), fieldName(fieldName), fieldMatcher(fieldMatcher),
      field(sourceSeries, fieldName), recordCopier(sourceSeries, destinationSeries) {
}

template <typename FieldType, typename FieldMatcher>
Extent* GrepModule<FieldType, FieldMatcher>::getExtent() {
    Extent *destinationExtent = NULL;
    bool matchFound = false;

    std::auto_ptr<Extent> sourceExtent(upstreamModule.getExtent());
    while (sourceExtent.get() != NULL) {
        sourceSeries.setExtent(sourceExtent.get());

        for (; sourceSeries.more(); sourceSeries.next()) {
            if (fieldMatcher(field)) {
                if (!matchFound) { // the first match for the user's call to getExtent
                    destinationExtent = new Extent(sourceExtent->getType());
                    destinationSeries.setExtent(destinationExtent);
                    matchFound = true;
                }
                destinationSeries.newRecord();
                recordCopier.copyRecord();
            }
        }

        sourceExtent.reset(upstreamModule.getExtent()); // the next extent
    }

    return destinationExtent;
}

#endif
