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

#include <boost/function.hpp>
#include <boost/smart_ptr.hpp>
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
    /// Function is allowed to return NULL if it is unable to map field_name to an appropriate
    /// series.
    typedef std::pair<ExtentSeries *, std::string> Selector;

    typedef boost::function<Selector (const std::string &field_name)> FieldNameToSelector;

    virtual ~DSExprParser() {}
    
    /// Get the usage for this parser.
    virtual const std::string getUsage() const = 0;

    /// Parse an expression over a single series.
    virtual DSExpr *parse(ExtentSeries &series, const std::string &expr) = 0;

    /// Parse an expression where the series to use for each of the different fields is determined
    /// by the specified function.  This functionality allows for a single expression to combine
    /// the values in multiple extents.
    virtual DSExpr *parse(const FieldNameToSelector &field_name_to_selector,
                          const std::string &expr) = 0;
      

    /// Register a function for use in calculating an expression
    virtual void registerFunction(DSExprFunction &) = 0;

    static DSExprParser *MakeDefaultParser() {
	return DSExprParserFactory::GetDefaultParserFactory().make();
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

    /// Make an expression over a single series.
    static DSExpr *make(ExtentSeries &series, const std::string &expr_string) {
        boost::scoped_ptr<DSExprParser> parser(DSExprParser::MakeDefaultParser());
        return parser->parse(series, expr_string);
    }

    /// Make an expression where the series to use for each of the different fields is determined
    /// by the specified function.  This functionality allows for a single expression to combine
    /// the values in multiple extents.
    static DSExpr *make(const DSExprParser::FieldNameToSelector &field_name_to_selector, 
                        const std::string &expr_string) {
        boost::scoped_ptr<DSExprParser> parser(DSExprParser::MakeDefaultParser());
        return parser->parse(field_name_to_selector, expr_string);
    }

    /// Get the current usage description for expressions.
    static std::string usage() {
        boost::scoped_ptr<DSExprParser> parser(DSExprParser::MakeDefaultParser());
        return parser->getUsage();
    }

};

#endif
