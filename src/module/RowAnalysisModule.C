/* -*-C++-*-
*******************************************************************************
*
* File:         RowAnalysisModule.C
* RCS:          $Header: /mount/cello/cvs/DataSeries/src/module/RowAnalysisModule.C,v 1.2 2005/02/15 01:18:36 anderse Exp $
* Description:  implementation
* Author:       Eric Anderson
* Created:      Mon Apr 26 22:49:05 2004
* Modified:     Sat Oct  2 19:04:13 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2004, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#include "RowAnalysisModule.H"
#include "SequenceModule.H"

RowAnalysisModule::RowAnalysisModule(DataSeriesModule &_source,
				     ExtentSeries::typeCompatibilityT _tc)
    : series(_tc), source(_source)
{
}

RowAnalysisModule::~RowAnalysisModule()
{
}

Extent *
RowAnalysisModule::getExtent()
{
    Extent *e = source.getExtent();
    if (e == NULL) {
	completeProcessing();
	return NULL;
    }
    for(series.setExtent(e);series.pos.morerecords();++series.pos) {
	processRow();
    }
    return e;
}

void
RowAnalysisModule::completeProcessing()
{
}

void
RowAnalysisModule::printResult()
{
}

int
RowAnalysisModule::printAllResults(SequenceModule &sequence,
				   int expected_nonprintable)
{
    int non_rowmods = 0;
    bool printed_any = false;
    for(SequenceModule::iterator i = sequence.begin();
	i != sequence.end();++i) {
	RowAnalysisModule *ram = dynamic_cast<RowAnalysisModule *>(*i);
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
    AssertAlways(expected_nonprintable < 0 ||
		 non_rowmods == expected_nonprintable,
		 ("mismatch on number of non-printable modules %d != %d (expected)\n",
		  non_rowmods,expected_nonprintable));
     return non_rowmods;
}
