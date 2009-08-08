// -*-C++-*-
/*
  // TODO-tomer: probably 2008-2009, and others
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

// TODO-tomer: namespace dataseries {
// TODO-tomer: shiny new documentation to be added all over.
template <typename FieldType, typename FieldMatcher>
class GrepModule : public DataSeriesModule {
public:
    GrepModule(DataSeriesModule &upstreamModule,
               const std::string &fieldName,
               const FieldMatcher &fieldMatcher) 
	: upstreamModule(upstreamModule), fieldName(fieldName), fieldMatcher(fieldMatcher),
	  field(sourceSeries, fieldName), recordCopier(sourceSeries, destinationSeries) 
    { }

    /// Currently returns exactly one extent. If your grep is non-selective,
    /// this extent could overflow available memory.
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
Extent *GrepModule<FieldType, FieldMatcher>::getExtent() {
    // TODO-tomer: I think there's a boost scoped_ptr
    std::auto_ptr<Extent> sourceExtent(upstreamModule.getExtent());
    while (sourceExtent.get() != NULL) {
        for (sourceSeries.start(sourceExtent.get()); sourceSeries.more(); sourceSeries.next()) {
            if (fieldMatcher(field)) {
                if (destinationSeries.getExtent() == NULL) { // the first match found
                    destinationSeries.setExtent(new Extent(sourceExtent->getType()));
                }
                destinationSeries.newRecord();
                recordCopier.copyRecord();
            }
        }

        sourceExtent.reset(upstreamModule.getExtent()); // the next extent
    }

    return destinationSeries.getExtent();
}
// }
#endif
