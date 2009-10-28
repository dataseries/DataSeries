// -*-C++-*-
/*
   (c) Copyright 2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Data Series Module for calculating a statistic over an expression
    grouped by another field.
*/

#ifndef __DATASERIES_DSSTATGROUPBY_H
#define __DATASERIES_DSSTATGROUPBY_H

#include <Lintel/HashMap.hpp>

#include <DataSeries/DSExpr.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/RowAnalysisModule.hpp>

class DSStatGroupByModule : public RowAnalysisModule {
public:
    DSStatGroupByModule(DataSeriesModule &source,
			const std::string &expression,
			const std::string &groupby,
			const std::string &stattype = "basic",
			const std::string &whereexpr = "",
			ExtentSeries::typeCompatibilityT tc = ExtentSeries::typeExact);

    typedef HashMap<GeneralValue, Stats *> mytableT;

    virtual ~DSStatGroupByModule();
    
    virtual void prepareForProcessing();
    virtual void processRow();
    virtual void printResult();

    /// return true if the specified stat_type is valid for constructing a
    /// DSStatGroupByModule.
    static bool validStatType(const std::string &stat_type);
private:
    mytableT mystats;
    std::string expression, groupby_name, stattype;
    GeneralField *groupby;
    DSExpr *expr;
};

#endif
