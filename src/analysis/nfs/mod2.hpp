/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#ifndef __NFSDSANALYSIS_MOD2_H
#define __NFSDSANALYSIS_MOD2_H

#include "analysis/nfs/common.hpp"

namespace NFSDSAnalysisMod {
    // nfsdsanalysis-mod2:
    NFSDSModule *newAttrOpsCommonJoin();
    void setAttrOpsSources(DataSeriesModule *join, SequenceModule &common_seq,
			   SequenceModule &attrops_seq);
    NFSDSModule *newLargeSizeFileHandle(DataSeriesModule &prev,int nkeept);
    NFSDSModule *newLargeSizeFilename(DataSeriesModule &prev, int nkeep);
    NFSDSModule *newLargeSizeFilenameWrite(DataSeriesModule &prev, int nkeep);
    NFSDSModule *newLargeSizeFilehandleWrite(DataSeriesModule &prev, int nkeep);
    NFSDSModule *newFileageByFilehandle(DataSeriesModule &prev, int nkeep, int recent_age_seconds);
}

#endif

