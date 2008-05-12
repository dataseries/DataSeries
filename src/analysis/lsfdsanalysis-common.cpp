// -*-C++-*-
/*
   (c) Copyright 2004-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Common lsf analysis bits.
*/

#include "analysis/lsfdsanalysis-common.H"

using namespace std;

const string group_dedicated_bear("dedicated-bear-");
const string group_ers("ers");
const string group_misc("misc");
const string group_rwc("rwc");
    
const string def_unknown("unknown");

const string str_all("all");
const string str_production("production");
const string str_sequence("sequence");
const string str_team("team");
const string str_queue("queue");
const string str_hostgroup("hostgroup");
const string str_team_group("team_group");
const string str_cluster("cluster");
const string str_username("username");

const string str_misc("misc");
const string str_unclassified("unclassified");
const string str_na("n/a");
const string str_colon(":");
const string str_unknown("unknown");
const string str_squnknown("squnknown");

const string empty_string("");

// one and two are generics that we want to overwrite.
// three is the ers testing sequence, whereas we want to keep the "real" information
// four is a renamed sequence in molasses, and we want the rename
string str_ok_neq_rr_rj_1, str_ok_neq_rr_rj_2, str_ok_neq_rr_rj_3, str_ok_neq_rr_rj_4;

HashMap<string,string> team_remap;

struct team_remap_ent {
    string from, to;
};

struct team_remap_ent team_remap_list[] = {
    { "molasses-rough-layout", "molasses:anim-layout" },
    { "molasses-final-layout", "molasses:anim-layout" },
    { "molasses-animation", "molasses:anim-layout" },
    { "molasses-effects", "molasses:effects" },
    { "molasses-lighting-1", "molasses:lighting" },
    { "molasses-lighting-2", "molasses:lighting" },
    { "molasses-high-priority", "molasses:high-priority" },
    { "molasses-surfacing", "molasses:misc" },
    { "molasses-finaling", "molasses:misc" },
    { "X:201865012569a826681f3db32d929e399f865a8fdace5229a1b339b9457c1095", "molasses:misc" },

    { "honey-layout", "honey:anim-layout" },
    { "honey-animation", "honey:anim-layout" },
    { "honey-effects", "honey:effects" },
    { "honey-effects-2", "honey:effects" },
    { "honey-lighting-1", "honey:lighting" },
    { "honey-lighting-2", "honey:lighting" },
    { "honey-lighting-2a", "honey:lighting" },
    { "honey-lighting-3", "honey:lighting" },
    { "honey-lighting-4", "honey:lighting" },
    { "honey-lighting-5", "honey:lighting" },
    { "honey-lighting-6", "honey:lighting" },
    { "honey-lighting-7", "honey:lighting" },
    { "honey-high-priority", "honey:high-priority" },
    { "honey-finaling", "honey:misc" },
    { "honey-surfacing", "honey:misc" },
    { "X:6ea83ba12a18e0ea58d94db20f47e467e847c05f27c35edd28c4242425fb73f8", "honey:consumer-products" },
    { "honey-consumer-products", "honey:consumer-products" },

    { "unaccounted", "misc" },
    { "X:0bc9b5037d8bb261700047f150665011735214bf7761b7c052cb761b2b45ea43", "misc" },
    { "X:85693fcd436a9fd741671592ee2da603", "misc" },
    { "X:f93af39bf2bf37c38aace89a8f7899fb", "misc" },
    { "X:dbf771d8ea938c06b1d3ea1e08580655", "misc" },
    { "X:698cf3e186b0f70c12f8a57d709d9c3315e02776c13dcb40e1aea9be839938d1", "misc" },
    { "X:da538a0135364ecbd0a33836e72f881dc6e8b7f2bbb22381ef1adfe402625938", "misc" },
    { "X:4f741190ec761cc594b76a5a089c2c2169ea8967715c254f3e4b3eecb50bbaff", "misc" },
};
    
void
prepareCommonValues()
{
    for(unsigned i=0;i<sizeof(team_remap_list)/sizeof(team_remap_ent);++i) {
	if (team_remap_list[i].from[0] == 'X' &&
	    team_remap_list[i].from[1] == ':') {
	    string &tmp = team_remap_list[i].from;
	    string unhexed = hex2raw(tmp.substr(2,tmp.size()-2));
	    team_remap[unhexed] = team_remap_list[i].to;
	} else {
	    team_remap[team_remap_list[i].from] = team_remap_list[i].to;
	}
    }
    str_ok_neq_rr_rj_1 = hex2raw("4c3aba05d7ab6232dc6a21b899466c63");
    str_ok_neq_rr_rj_2 = hex2raw("a725515bc6272f6707684a76b3cbe6ca");
    str_ok_neq_rr_rj_3 = hex2raw("a2e3386cad3c9872ca212d3c7572fcb5");
    str_ok_neq_rr_rj_4 = hex2raw("2d0668134867ddcc712e2ad0cb14959d");
}

bool
ersTestJob(int uid)
{
    // eanderson, jwiener, fpopvici, ewu
    return uid == 12271 || uid == 12285 || uid == 12295 || uid == 12213;
}
