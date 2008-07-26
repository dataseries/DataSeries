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

#include <iosfwd>
#include <string>
#include <vector>

#include <boost/utility.hpp>

#include <DataSeries/ExtentSeries.hpp>

class DSExpr;
class DSExprParser;

class DSExprFunction : boost::noncopyable {
    // name
    // min required args
    // max required args
    // extra args ok
    // type
    // eval
};

class DSExprParserFactory : boost::noncopyable {
public:
    virtual ~DSExprParserFactory() {}

    virtual DSExprParser *make() = 0;

    static DSExprParserFactory &GetDefaultParserFactory();

    // later, if/as needed ...
    // static DSExprParserFactory &GetParserFactory(const std::string &name);
    // static void RegisterParserFactory(const std::string &name,
    //                                   DSExprParserFactory &factory);

protected:
    DSExprParserFactory() {}
};

class DSExprParser : boost::noncopyable {
public:
    virtual ~DSExprParser() {}
    
    virtual const std::string getUsage() const = 0;

    virtual DSExpr *parse(ExtentSeries &series, std::string &expr) = 0;

    virtual void registerFunction(DSExprFunction &) = 0;

    static DSExprParser *MakeDefaultParser() {
	DSExprParserFactory &factory = 
	    DSExprParserFactory::GetDefaultParserFactory();
	return factory.make();
    }
    
protected:
    DSExprParser() {}
};

class DSExpr : boost::noncopyable {
public:
    typedef std::vector<DSExpr *> List;

    virtual ~DSExpr() {}

    // t_Numeric is double or int64 or bool
    typedef enum { t_Unknown, t_Numeric, t_Bool, t_String } expr_type_t;
    virtual expr_type_t getType() = 0;

    virtual double valDouble() = 0;
    virtual int64_t valInt64() = 0;
    virtual bool valBool() = 0;
    virtual const std::string valString() = 0;

    virtual void dump(std::ostream &) = 0;

    // deprecated
    static DSExpr *make(ExtentSeries &series, std::string &expr_string);
    // deprecated
    static std::string usage();
};

#endif
