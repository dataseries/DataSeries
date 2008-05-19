// -*-C++-*-
/*
   (c) Copyright 2004-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Common lsf analysis bits.
*/

#ifndef __LSFDSANALYSIS_COMMON_H
#define __LSFDSANALYSIS_COMMON_H

#include <string>
#include <vector>

#include <Lintel/HashMap.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/RowAnalysisModule.hpp>

// g++ was rebuilding inline static strings every time when measured
// at one point in 2003, and still was doing so in a later test in
// 2007.

extern const std::string group_dedicated_bear, group_ers, group_misc, 
    group_rwc, def_unknown, str_all, str_production, str_sequence,
    str_team, str_queue, str_hostgroup, str_team_group, str_cluster, 
    str_username, str_misc, str_unclassified, str_na, str_colon, 
    str_unknown, str_squnknown, empty_string;

// extern std::string str_ok_neq_rr_rj_1, str_ok_neq_rr_rj_2, str_ok_neq_rr_rj_3, str_ok_neq_rr_rj_4;

extern HashMap<std::string,std::string> team_remap;

void prepareCommonValues();

bool ersTestJob(int uid);

#endif
