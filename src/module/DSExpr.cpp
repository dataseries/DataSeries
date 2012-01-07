#include <DataSeries/DSExpr.hpp>

#include "DSExprImpl.hpp"

using namespace std;

//////////////////////////////////////////////////////////////////////

class DefaultParser : public DSExprParser {
    DSExpr *parse(ExtentSeries &series, const string &expr) {
	// TODO: DSExprImpl::Driver and the defined factory interface
	// have a poor impedance match.  One or the other needs to
	// change.
	DSExprImpl::Driver driver(series);
	driver.doit(expr);
	return driver.expr;
    }

    DSExpr *parse(const FieldNameToSeries &field_name_to_series, const string &expr) {
	DSExprImpl::Driver driver(field_name_to_series);
	driver.doit(expr);
	return driver.expr;
    }

    const string getUsage() const {
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

    void registerFunction(DSExprFunction &) {
	FATAL_ERROR("not implemented");
    }
};

//////////////////////////////////////////////////////////////////////

class DefaultParserFactory : public DSExprParserFactory {
    DSExprParser *make() {
	return new DefaultParser();
    }
} DefaultParserFactorySingleton;

DSExprParserFactory &DSExprParserFactory::GetDefaultParserFactory() {
    return DefaultParserFactorySingleton;
}
