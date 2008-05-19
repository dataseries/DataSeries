/* -*-C++-*-
   (c) Copyright 2004-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details

   Description:  Mostly finished modules split out for faster compilation.
*/

#ifndef __LSFDSANALYSIS_MOD1_H
#define __LSFDSANALYSIS_MOD1_H

#include <DataSeries/SequenceModule.hpp>

#include "lsfdsanalysis-common.hpp"

namespace LSFDSAnalysisMod {
    // in lsfdsanalysis-mod1.C
    struct FarmLoadArgs;
    FarmLoadArgs *handleFarmLoadArgs(const std::string &arg);
    RowAnalysisModule *newFarmLoad(DataSeriesModule &tail, FarmLoadArgs *args);
    
    void handleTraceMetaIdArgs(const char *arg);
    void addTraceMetaIdModules(SequenceModule &sequence);

    void addProductionReportModules(SequenceModule &sequence);

    //   void handleRenderRequestLookupArgs(const char *arg);
    //    void addRRLookupModules(SequenceModule &rr_sequence, SequenceModule &rj_sequence);
}

#endif

