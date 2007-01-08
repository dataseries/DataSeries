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

#include <Lintel/HashMap.H>

#include <DataSeries/RowAnalysisModule.H>
#include <DataSeries/GeneralField.H>

namespace DSStatGroupByExpr {
    class Expr;
}

class DSStatGroupBy : public RowAnalysisModule {
public:
    DSStatGroupBy(DataSeriesModule &source,
		  const std::string &_expression,
		  const std::string &_groupby,
		  const std::string &_stattype,
		  ExtentSeries::typeCompatibilityT tc = ExtentSeries::typeXMLIdentical);

    typedef HashMap<GeneralValue, Stats *> mytableT;

    virtual ~DSStatGroupBy();
    
    virtual void prepareForProcessing();

    virtual void processRow();

    virtual void printResult();

    mytableT mystats;
    std::string expression, groupby_name, stattype;
    GeneralField *groupby;
    DSStatGroupByExpr::Expr *expr;
};

#endif
