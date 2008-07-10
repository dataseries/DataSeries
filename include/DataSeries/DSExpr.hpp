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

    // t_Numeric is double or int64 or bool
    typedef enum { t_Unknown, t_Numeric, t_Bool, t_String } expr_type_t;
    virtual expr_type_t getType() = 0;

    virtual double valDouble() = 0;
    virtual int64_t valInt64() = 0;
    virtual bool valBool() = 0;
    virtual const std::string valString() = 0;

    static DSExpr *make(ExtentSeries &series, std::string &expr);

    static const std::string usage;
};


#endif
