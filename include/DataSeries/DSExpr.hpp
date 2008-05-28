// -*-C++-*-
/*
   (c) Copyright 2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Expressions over dataseries fields and constants
*/

#ifndef __DATASERIES_DSEXPR_H
#define __DATASERIES_DSEXPR_H

#include <string>

#include <DataSeries/ExtentSeries.hpp>

class DSExpr {
public:
    virtual ~DSExpr();
    virtual double valDouble() = 0;
    virtual int64_t valInt64() = 0;
    virtual bool valBool() = 0;

    static DSExpr *make(ExtentSeries &series, std::string &expr);
};


#endif
