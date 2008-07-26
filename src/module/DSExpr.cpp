#include <DataSeries/DSExpr.hpp>

#include "DSExprImpl.hpp"

DSExpr::~DSExpr()
{
}

DSExpr *
DSExpr::make(ExtentSeries &series, std::string &expression) 
{
    DSExprImpl::Driver driver(series);

    driver.doit(expression);
    return driver.expr;
}

// TODO: get from the implementation.
const std::string DSExpr::usage(
"  expressions include:\n"
"    field names, numeric (double) constants, string literals,\n"
"    functions, +, -, *, /, ()\n"
"  functions include:\n"
"    fn.TfracToSeconds\n"
"  boolean expressions include:\n"
"    <, <=, >, >=, ==, !=, ||, &&, !\n"
"  for fields with non-alpha-numeric or _ in the name, escape with \\\n"
);

