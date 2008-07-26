#include <DataSeries/DSExpr.hpp>

#include <boost/smart_ptr.hpp>

#include "DSExprImpl.hpp"

using namespace std;

// deprecated
DSExpr *
DSExpr::make(ExtentSeries &series, string &expr_string)
{
    DSExprParserFactory &defaultParserFactory =
	DSExprParserFactory::GetDefaultParserFactory();
    boost::scoped_ptr<DSExprParser> parser(defaultParserFactory.make());
    return parser->parse(series, expr_string);
}

// deprecated
string 
DSExpr::usage()
{
    DSExprParserFactory &defaultParserFactory =
	DSExprParserFactory::GetDefaultParserFactory();
    boost::scoped_ptr<DSExprParser> parser(defaultParserFactory.make());
    return parser->getUsage();
}

//////////////////////////////////////////////////////////////////////

class DefaultParser : public DSExprParser
{
    const string getUsage() const
    {
	return string(
"  expressions include:\n"
"    field names, numeric (double) constants, string literals,\n"
"    functions, +, -, *, /, ()\n"
"  functions include:\n"
"    fn.TfracToSeconds\n"
"  boolean expressions include:\n"
"    <, <=, >, >=, ==, !=, ||, &&, !\n"
"  for fields with non-alpha-numeric or _ in the name, escape with \\\n"
		      );
    }

    DSExpr *parse(ExtentSeries &series, string &expr)
    {
	// TODO: DSExprImpl::Driver and the defined factory interface
	// have a poor impedance match.  One or the other needs to
	// change.
	DSExprImpl::Driver driver(series);
	driver.doit(expr);
	return driver.expr;
    }

    void registerFunction(DSExprFunction &)
    {
	FATAL_ERROR("not implemented");
    }
};

//////////////////////////////////////////////////////////////////////

class DefaultParserFactory : public DSExprParserFactory 
{
    DSExprParser *make() {
	return new DefaultParser();
    }
} DefaultParserFactorySingleton;

DSExprParserFactory &
DSExprParserFactory::GetDefaultParserFactory()
{
    return DefaultParserFactorySingleton;
}
