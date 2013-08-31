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

namespace {
    const string str_basic("basic");
    const string str_quantile("quantile");
}

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
    SINVARIANT(validStatType(stattype));
    if (!where_expr.empty()) {
        setWhereExpr(where_expr);
    }
}

DSStatGroupByModule::~DSStatGroupByModule() {
    delete expr;
    expr = NULL;
    delete groupby;
    groupby = NULL;
}

void DSStatGroupByModule::prepareForProcessing() {
    // Have to do this here rather than constructor as we need the XML
    // from the first extent in order to build the generalfields

    expr = DSExpr::make(series, expression);
    if (!groupby_name.empty()) {
        groupby = GeneralField::create(NULL, series, groupby_name);
    }
}

void DSStatGroupByModule::processRow() {
    GeneralValue groupby_val;
    if (groupby != NULL) {
        groupby_val.set(groupby);
    } else {
        groupby_val.setInt32(1);
    }
    Stats *stat = mystats[groupby_val];
    if (stat == NULL) {
        if (stattype == str_basic) {
            stat = new Stats();
        } else if (stattype == str_quantile) {
            stat = new StatsQuantile();
        } else {
            FATAL_ERROR(boost::format("unknown stattype %s") % stattype);
        }
        mystats[groupby_val] = stat;
    }
    stat->add(expr->valDouble());
}

void DSStatGroupByModule::printResult() {
    cout << "# Begin DSStatGroupByModule\n";
    cout << boost::format("# processed %d rows, where clause eliminated %d rows\n") 
            % processed_rows % ignored_rows;

    // Someone might call printResult on an interim basis so we can't sort 
    // the underlying hashtable.
    vector<GeneralValue> keys = mystats.keys();
    sort(keys.begin(), keys.end());

    if (stattype == str_basic) {
        if (groupby_name.empty()) {
            cout << boost::format("# count(*), mean(%s), stddev, min, max\n") % expression;
        } else {
            cout << boost::format("# %s, count(*), mean(%s), stddev, min, max\n")
                    % groupby_name % expression;
        }
        for (vector<GeneralValue>::iterator i = keys.begin(); i != keys.end(); ++i) {
            GeneralValue &k(*i);
            Stats *v = mystats[k];

            if (!groupby_name.empty()) {
                cout << k << ", ";
            }
            cout << boost::format("%1%, %2$.6g, %3$.6g, %4$.6g, %5$.6g\n")
                    % v->count() % v->mean() % v->stddev() % v->min() % v->max();
        }
    } else if (stattype == str_quantile) {
        if (groupby_name.empty()) {
            cout << boost::format("# %s(%s)\n") % stattype % expression;
        } else {
            cout << boost::format("# %s(%s) group by %s\n") % stattype % expression % groupby_name;
        }
        for (vector<GeneralValue>::iterator i = keys.begin(); i != keys.end(); ++i) {
            GeneralValue &k(*i);
            Stats *v = mystats[k];
            if (!groupby_name.empty()) {
                cout << boost::format("# group %1%\n") % k;
            }
            v->printText(cout);
        }
    } else {
        FATAL_ERROR("wasn't stat type already checked?");
    }
    cout << "# End DSStatGroupByModule\n";
}

bool DSStatGroupByModule::validStatType(const string &stat_type) {
    return stat_type == str_basic || stat_type == str_quantile;
}
