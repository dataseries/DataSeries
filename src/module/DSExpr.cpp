#include <Lintel/AssertBoost.hpp>

#include <DataSeries/DSExpr.hpp>
#include <module/DSExprParse.hpp>

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

void DSExprImpl::Driver::doit(const std::string &str)
{
    startScanning(str);
    
    DSExprImpl::Parser parser(*this, scanner_state);
    int ret = parser.parse();
    INVARIANT(ret == 0 && expr != NULL, "parse failed");
    finishScanning();
}

DSExprImpl::Driver::~Driver()
{
    INVARIANT(scanner_state == NULL, "bad");
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

