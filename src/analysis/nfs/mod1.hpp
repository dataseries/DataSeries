/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#ifndef __NFSDSANALYSIS_MOD1_H
#define __NFSDSANALYSIS_MOD1_H

#include "analysis/nfs/common.hpp"

namespace NFSDSAnalysisMod {
    // in nfsdsanalysis-mod1:
    NFSDSModule *newNFSOpPayload(DataSeriesModule &prev);
    NFSDSModule *newClientServerPairInfo(DataSeriesModule &prev);
    NFSDSModule *newUnbalancedOps(DataSeriesModule &prev);
    NFSDSModule *newPayloadInfo(DataSeriesModule &prev);
    NFSDSModule *newNFSTimeGaps(DataSeriesModule &prev);

    extern double gap_parm;
}

#endif
