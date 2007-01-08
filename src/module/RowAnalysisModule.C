// -*-C++-*-
/*
   (c) Copyright 2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    implementation
*/

#include <DataSeries/RowAnalysisModule.H>
#include <DataSeries/SequenceModule.H>

RowAnalysisModule::RowAnalysisModule(DataSeriesModule &_source,
				     ExtentSeries::typeCompatibilityT _tc)
    : series(_tc), source(_source), prepared(false)
{
}

RowAnalysisModule::~RowAnalysisModule()
{
}

void
RowAnalysisModule::prepareForProcessing()
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
    series.setExtent(e);
    if (!prepared) {
	prepareForProcessing();
	prepared = true;
    }
    for(;series.pos.morerecords();++series.pos) {
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
