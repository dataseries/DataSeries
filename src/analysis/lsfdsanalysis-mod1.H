/* -*-C++-*-
*******************************************************************************
*
* File:         lsfdsanalysis-mod1.H
* RCS:          $Header: /mount/cello/cvs/Grizzly/cpp/analysis/lsfdsanalysis-mod1.H,v 1.2 2004/04/30 18:26:03 anderse Exp $
* Description:  One batch of modules for lsfdsanalysis
* Author:       Eric Anderson
* Created:      Mon Apr 19 14:34:24 2004
* Modified:     Wed Apr 28 21:12:44 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2004, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#ifndef __LSFDSANALYSIS_MOD1_H
#define __LSFDSANALYSIS_MOD1_H

#include <DataSeries/SequenceModule.H>

#include "lsfdsanalysis-common.H"

namespace LSFDSAnalysisMod {
    // in lsfdsanalysis-mod1.C
    void initFarmLoadArgs();
    void handleFarmLoadArgs(const char *arg);
    RowAnalysisModule *newFarmLoad(DataSeriesModule &tail);
    
    void handleTraceMetaIdArgs(const char *arg);
    void addTraceMetaIdModules(SequenceModule &sequence);

    void addProductionReportModules(SequenceModule &sequence);

    //   void handleRenderRequestLookupArgs(const char *arg);
    //    void addRRLookupModules(SequenceModule &rr_sequence, SequenceModule &rj_sequence);
}

#endif

