// -*-C++-*-
/*
   (c) Copyright 2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation
*/

#include <Lintel/AssertBoost.hpp>
#include <Lintel/StatsQuantile.hpp>

#include <DataSeries/DSStatGroupByModule.hpp>

using namespace std;

DSStatGroupByModule::DSStatGroupByModule(DataSeriesModule &source,
					 const string &_expression,
					 const string &_groupby,
					 const string &_stattype,
					 const string &where_expr,
					 ExtentSeries::typeCompatibilityT tc)
    : RowAnalysisModule(source, tc), expression(_expression), 
      groupby_name(_groupby), stattype(_stattype), groupby(NULL),
      expr(NULL)
{
    if (!where_expr.empty()) {
	setWhereExpr(where_expr);
    }
}

DSStatGroupByModule::~DSStatGroupByModule()
{
    delete expr;
    expr = NULL;
    delete groupby;
    groupby = NULL;
}

void
DSStatGroupByModule::prepareForProcessing()
{
    // Have to do this here rather than constructor as we need the XML
    // from the first extent in order to build the generalfields

    expr = DSExpr::make(series, expression);

    groupby = GeneralField::create(NULL, series, groupby_name);
}

void 
DSStatGroupByModule::processRow() 
{
    GeneralValue groupby_val(groupby);
    Stats *stat = mystats[groupby_val];
    if (stat == NULL) {
	if (stattype == "basic") {
	    stat = new Stats();
	} else if (stattype == "quantile") {
	    stat = new StatsQuantile();
	} else {
	    FATAL_ERROR(boost::format("unknown stattype %s") % stattype);
	}
	mystats[groupby->val()] = stat;
    }
    stat->add(expr->valDouble());
}

void
DSStatGroupByModule::printResult()
{
    cout << "# Begin DSStatGroupByModule" << endl;
    cout << boost::format("# processed %d rows, where clause eliminated %d rows\n") 
	% processed_rows % ignored_rows;
    if (stattype == "basic") {
	cout << boost::format("# %s, count(*), mean(%s), stddev, min, max\n")
	    % groupby_name % expression;
	for(mytableT::iterator i = mystats.begin(); 
	    i != mystats.end(); ++i) {
	    cout << boost::format("%1%, %2%, %3$.6g, %4$.6g, %5$.6g, %6$.6g\n")
		% i->first % i->second->count() % i->second->mean() % i->second->stddev()
		% i->second->min() % i->second->max();
	}
    } else {
	cout << boost::format("# %s(%s) group by %s\n")
	    % stattype % expression % groupby_name;
	for(mytableT::iterator i = mystats.begin(); 
	    i != mystats.end(); ++i) {
	    cout << boost::format("# group %1%\n")
		% i->first;
	    i->second->printText(cout);
	}
    }
    cout << "# End DSStatGroupByModule" << endl;
}

