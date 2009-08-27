/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#ifndef __NFSDSANALYSIS_MOD3_H
#define __NFSDSANALYSIS_MOD3_H

#include "analysis/nfs/common.hpp"

namespace NFSDSAnalysisMod {
    NFSDSModule *newFileSizeByType(DataSeriesModule &prev); 
    NFSDSModule *newUniqueBytesInFilehandles(DataSeriesModule &prev);
    NFSDSModule *newCommonBytesInFilehandles(DataSeriesModule &prev);
    NFSDSModule *newFilesRead(DataSeriesModule &prev);
    extern double read_sampling;
};

#endif
