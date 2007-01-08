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

#include <DataSeries/DSStatGroupBy.H>

#include "DSStatGroupByDefs.h"

extern "C" {
    extern int DSStatGroupBy_yylex();
    void DSStatGroupByLex_setInputString(const char *str, int len);
}

YYSTYPE DSStatGroupBy_yylval;

namespace DSStatGroupByExpr {
    class Expr {
    public:
	Expr() { };
	virtual ~Expr() { };
	virtual double val() = 0;
    };

    class ExprField : public Expr {
    public:
	ExprField(ExtentSeries &series, const std::string &fieldname)
	{ 
	    field = GeneralField::create(NULL, series, fieldname);
	}
	
	virtual ~ExprField() { };

	virtual double val() {
	    return field->valDouble();
	}
	
	static Expr *parse(ExtentSeries &series) {
	    int t = DSStatGroupBy_yylex();
	    INVARIANT(t == FIELD, "bad");
	    return new ExprField(series, DSStatGroupBy_yylval.text);
	}
    private:
	GeneralField *field;
    };
    
    class Subtract : public Expr {
    public:
	Subtract(Expr *_left, Expr *_right)
	    : left(_left), right(_right)
	{ }
	
	virtual ~Subtract() { delete left; delete right; }
	virtual double val() {
	    return left->val() - right->val();
	}
    private:
	Expr *left, *right;
    };
}

using namespace DSStatGroupByExpr;
using namespace std;

DSStatGroupBy::DSStatGroupBy(DataSeriesModule &source,
			     const string &_expression,
			     const string &_groupby,
			     const string &_stattype,
			     ExtentSeries::typeCompatibilityT tc)
    : RowAnalysisModule(source, tc), expression(_expression), 
      groupby_name(_groupby), stattype(_stattype), groupby(NULL),
      expr(NULL)
{
}

DSStatGroupBy::~DSStatGroupBy()
{
    delete expr;
}

void
DSStatGroupBy::prepareForProcessing()
{
    // Have to do this here rather than constructor as we need the XML
    // from the first extent in order to build the generalfield

    DSStatGroupByLex_setInputString(expression.c_str(), expression.size());
    
    // TODO: redo this with bison.

    Expr *left = ExprField::parse(series);
    
    int t = DSStatGroupBy_yylex();
    INVARIANT(t == '-' || t == END_OF_STRING, "bad");
    if (t == '-') {
	Expr *right = ExprField::parse(series);
	t = DSStatGroupBy_yylex();
	INVARIANT(t == END_OF_STRING, "bad");
	expr = new Subtract(left, right);
    } else {
	expr = left;
    }

    groupby = GeneralField::create(NULL, series, groupby_name);
}

void 
DSStatGroupBy::processRow() 
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
    stat->add(expr->val());
}

void
DSStatGroupBy::printResult()
{
    cout << "# Begin DSStatGroupBy" << endl;
    cout << boost::format("# %s, count(*), mean(%s), stddev, min, max")
	% groupby_name % expression << endl;
    for(mytableT::iterator i = mystats.begin(); 
	i != mystats.end(); ++i) {
	cout << boost::format("%1%, %2%, %3$.6g, %4$.6g, %5$.6g, %6$.6g")
	    % i->first % i->second->count() % i->second->mean() % i->second->stddev()
	    % i->second->min() % i->second->max() 
	     << endl;
    }
    cout << "# End DSStatGroupBy" << endl;
}

