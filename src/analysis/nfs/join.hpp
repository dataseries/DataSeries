/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#ifndef NFSDSANALYSIS_JOIN_HPP
#define NFSDSANALYSIS_JOIN_HPP

#include <DataSeries/SequenceModule.hpp>

#include <analysis/nfs/common.hpp>

namespace NFSDSAnalysisMod {
    NFSDSModule *newAttrOpsCommonJoin();
    void setAttrOpsSources(DataSeriesModule *join, SequenceModule &common_seq,
			   SequenceModule &attrops_seq);
    NFSDSModule *newCommonAttrRWJoin();
    void setCommonAttrRWSources(DataSeriesModule *join, SequenceModule &commonattr_seq,
				SequenceModule &rw_seq);

}

#endif
