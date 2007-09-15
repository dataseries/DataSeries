// -*-C++-*-
/*
   (c) Copyright 2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation
*/

#include <Lintel/AssertBoost.H>
#include <Lintel/StatsQuantile.H>

#include <DataSeries/DSStatGroupByModule.H>

#include "DSStatGroupByParse.hpp"

using namespace std;

DSStatGroupByModule::DSStatGroupByModule(DataSeriesModule &source,
					 const string &_expression,
					 const string &_groupby,
					 const string &_stattype,
					 ExtentSeries::typeCompatibilityT tc)
    : RowAnalysisModule(source, tc), expression(_expression), 
      groupby_name(_groupby), stattype(_stattype), groupby(NULL),
      expr(NULL)
{
}

DSStatGroupByModule::~DSStatGroupByModule()
{
    delete expr;
}

void
DSStatGroupByModule::prepareForProcessing()
{
    // Have to do this here rather than constructor as we need the XML
    // from the first extent in order to build the generalfield

    setInputString(expression);
    
    expr = NULL;
    DSStatGroupBy::Parser parser(*this);
    int ret = parser.parse();
    INVARIANT(ret == 0 && expr != NULL, "parse failed");
    
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
    stat->add(expr->value());
}

void
DSStatGroupByModule::printResult()
{
    cout << "# Begin DSStatGroupByModule" << endl;
    cout << boost::format("# %s, count(*), mean(%s), stddev, min, max")
	% groupby_name % expression << endl;
    for(mytableT::iterator i = mystats.begin(); 
	i != mystats.end(); ++i) {
	cout << boost::format("%1%, %2%, %3$.6g, %4$.6g, %5$.6g, %6$.6g")
	    % i->first % i->second->count() % i->second->mean() % i->second->stddev()
	    % i->second->min() % i->second->max() 
	     << endl;
    }
    cout << "# End DSStatGroupByModule" << endl;
}

