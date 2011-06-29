// -*-C++-*-
/*
   (c) Copyright 2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation
*/

#include <DataSeries/DSExpr.hpp>
#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SequenceModule.hpp>

RowAnalysisModule::RowAnalysisModule(DataSeriesModule &_source,
				     ExtentSeries::typeCompatibilityT _tc)
    : processed_rows(0), ignored_rows(0), 
      series(_tc), source(_source), prepared(false), where_expr(NULL)
{
    SINVARIANT(&source != NULL);
}

RowAnalysisModule::~RowAnalysisModule() {
    delete where_expr;
    where_expr = NULL;
}

void RowAnalysisModule::newExtentHook(const Extent &e) { }

void RowAnalysisModule::firstExtent(const Extent &e) { }

void RowAnalysisModule::prepareForProcessing() { }

Extent *RowAnalysisModule::getExtent() {
    Extent *e = source.getExtent();
    if (e == NULL) {
	completeProcessing();
	return NULL;
    }
    if (!prepared) {
	firstExtent(*e);
    }
    newExtentHook(*e);
    series.setExtent(e);
    if (!prepared) {
	prepareForProcessing();
	prepared = true;
	if (!where_expr_str.empty()) {
	    where_expr = DSExpr::make(series, where_expr_str);
	}
    }
    for(;series.pos.morerecords();++series.pos) {
	if (!where_expr || where_expr->valBool()) {
	    ++processed_rows;
	    processRow();
	} else {
	    ++ignored_rows;
	}
    }
    series.setExtent(NULL);
    return e;
}

void RowAnalysisModule::completeProcessing() { }

void RowAnalysisModule::printResult() { }

void RowAnalysisModule::setWhereExpr(const std::string &expr) {
    where_expr_str = expr;
    INVARIANT(!prepared, "can't set where expr after prepare");
}

int RowAnalysisModule::printAllResults(SequenceModule &sequence, int expected_nonprintable) {
    int non_rowmods = 0;
    bool printed_any = false;
    for(SequenceModule::iterator i = sequence.begin();
	i != sequence.end();++i) {
	RowAnalysisModule *ram = dynamic_cast<RowAnalysisModule *>(i->get());
	if (ram == NULL) {
	    non_rowmods += 1;
	} else {
	    if (printed_any) {
		printf("\n");
	    } else {
		printed_any = true;
	    }
	    ram->printResult();
	}
    }
    INVARIANT(expected_nonprintable < 0 || non_rowmods == expected_nonprintable,
	      boost::format("mismatch on number of non-printable"
			    " modules %d != %d (expected)\n")
	      % non_rowmods % expected_nonprintable);

    return non_rowmods;
}
