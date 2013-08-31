/*
  (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

  See the file named COPYING for license details
*/

#ifndef __NFSDSANALYSIS_MOD4_H
#define __NFSDSANALYSIS_MOD4_H

#include "analysis/nfs/common.hpp"

namespace NFSDSAnalysisMod {
    // nfsdsanalysis-mod4:
    NFSDSModule *newServersPerFilehandle(DataSeriesModule &prev);
    NFSDSModule *newTransactions(DataSeriesModule &prev);
    NFSDSModule *newOutstandingRequests(DataSeriesModule &prev, 
                                        int latency_offset);
}

#endif

