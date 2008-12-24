/* -*-C++-*-
   (c) Copyright 2004-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details

   Description:  Mostly finished modules
*/

#include <Lintel/HashUnique.hpp>

#include "analysis/lsfdsanalysis-mod1.hpp"

using namespace std;
using boost::format;

namespace LSFDSAnalysisMod {

enum FarmLoad_groups { // code assumes GroupAll is first
    FarmLoad_All = 0, FarmLoad_Production, FarmLoad_Sequence, FarmLoad_Team,
    FarmLoad_Queue, FarmLoad_Hostgroup, FarmLoad_TeamGroup, FarmLoad_Cluster,
    FarmLoad_Username, FarmLoad_ExecHost, FarmLoad_Ngroups
};

// update usage when adding groupname
static const string FarmLoad_groupNames[] = {
    "all", "production", "sequence", "team", "queue", "hostgroup",
    "team_group", "cluster", "username", "exechost"
};

struct FarmLoadArgs {
    bool enable_group[FarmLoad_Ngroups], noerstest;
    unsigned rollup_granularity, rollup_start, rollup_end; 
    string where_expr;
    FarmLoadArgs() {
	for(unsigned i = 0; i < FarmLoad_Ngroups; ++i) {
	    enable_group[i] = false;
	}
	noerstest = false;
	rollup_granularity = rollup_start = rollup_end = 0;
    }
};

unsigned roundDown(unsigned v, unsigned granularity) {
    unsigned ret = v - (v % granularity);
    INVARIANT(ret <= v && ((ret % granularity) == 0),
	      boost::format("bad %d %d") % ret % v);
    return ret;
}

unsigned roundUp(unsigned v, unsigned granularity) {
    unsigned ret = v + ((granularity - (v % granularity)) % granularity);
    INVARIANT(ret >= v && ((ret % granularity) == 0),
	      boost::format("bad %d %d") % ret % v);
    return ret;
}

class FarmLoad : public RowAnalysisModule {
public:
    static const int cap_user_per_second = 4;

    struct rollupData {
	double pend_count, run_count, user_time, system_time, idle_time;
	rollupData() : pend_count(0), run_count(0), user_time(0), 
	    system_time(0), idle_time(0) { }
    };

    struct hteData {
	string group;
	vector<rollupData> data;
	~hteData() {
	}
    };

    struct hteHash {
	unsigned operator()(const hteData *k) const {
	    return lintel::hashBytes(k->group.data(),k->group.size());
	}
    };

    struct hteEqual {
	bool operator()(const hteData *a, const hteData *b) const {
	    return a->group == b->group;
	}
    };

    typedef HashTable<hteData *, hteHash, hteEqual> FarmLoadHash;

    int rounddown(unsigned v) {
	return roundDown(v, args.rollup_granularity);
    }
    int roundup(unsigned v) {
	return roundUp(v, args.rollup_granularity);
    }

    FarmLoad(DataSeriesModule &_source, FarmLoadArgs *_args)
	: RowAnalysisModule(_source), 
	  args(*_args),
	  cluster(series,"cluster_name"),
	  production(series,"production", Field::flag_nullable, def_unknown), 
	  sequence(series,"sequence", Field::flag_nullable, def_unknown),
	  event_time(series,"event_time"), submit_time(series,"submit_time"), 
	  start_time(series,"start_time", Field::flag_nullable),
	  end_time(series,"end_time", Field::flag_nullable), 
	  queue(series,"queue"), team(series,"team"),
	  user_time(series,"user_time"), system_time(series,"system_time"),
	  exec_host(series,"exec_host", Field::flag_nullable), 
	  username(series,"username"),
	  exec_host_group(series, "exec_host_group", Field::flag_nullable),
	  user_id(series,"user_id"),
	  minsubmit(2000000000), maxend(0), nrecords(0), 
	  nrecordsinwindow(0), negative_idle_records(0),
	  capped_rate_records(0)
    {
	allRollup = new hteData;
	allRollup->group = str_all;
	rollups.resize(FarmLoad_Ngroups);
	for(unsigned i = 1;i<rollups.size();++i) {
	    rollups[i] = new FarmLoadHash;
	}
	setWhereExpr(args.where_expr);
    }
    virtual ~FarmLoad() { }
    
    hteData getEntTmp;
    void addEnt(vector<hteData *> &ents,
		FarmLoad_groups group, 
		const string &key) {
	FarmLoadHash *ht = rollups[group];
	
	getEntTmp.group = key;
	hteData **ret = ht->lookup(&getEntTmp);
	if (ret == NULL) {
	    hteData *empty = new hteData;
	    empty->group = key;
	    ret = ht->add(empty);
	}
	ents.push_back(*ret);
    }

    static const string teamGroup(const string &production, const string &in) {
	const string *foo = team_remap.lookup(in);
	if (foo == NULL) {
	    fprintf(stderr,
		    "missing team group for production '%s' team '%s'\n",
		    maybehexstring(production).c_str(),
		    maybehexstring(in).c_str());
	    string outtmp = production;
	    outtmp.append(str_colon);
	    outtmp.append(str_unclassified);

	    team_remap[in] = outtmp;
	    foo = &str_misc;
	}
	return *foo;
    }

    // TODO: consider making this faster by having addPend, addRun
    // take the bucket to update, and do the bucket calculation
    // in addTimes.

    void addPend(int time, vector<hteData *> &ents,double frac)
    {
	DEBUG_INVARIANT(frac > 0, "should have pruned earlier");
	int offset = time - args.rollup_start;
	INVARIANT(offset >= 0, format("bad; time start = %d") % time);
	offset = offset / args.rollup_granularity;
	INVARIANT(offset < maxRollupChunks, format("bad (%d - %d)/%d = %d ")
		  % time % args.rollup_start % args.rollup_granularity 
		  % offset);
	for(unsigned i = 0;i<ents.size();++i) {
	    if(ents[i]->data.size() <= (unsigned)offset) {
		ents[i]->data.resize(offset+1);
	    }
	    ents[i]->data[offset].pend_count += frac;
	}
    }

    void addRun(int time, vector<hteData *> &ents, double user_per_sec,
		double system_per_sec, double frac)
    {
	DEBUG_INVARIANT(frac > 0, "should have pruned earlier");
	int offset = time - args.rollup_start;
	INVARIANT(offset >= 0,boost::format("bad %d %d") % time % args.rollup_start);
	offset = offset / args.rollup_granularity;
	SINVARIANT(offset < maxRollupChunks);
	for(unsigned i = 0;i<ents.size();++i) {
	    if(ents[i]->data.size() <= (unsigned)offset) {
		ents[i]->data.resize(offset+1);
	    }
	    rollupData &d = ents[i]->data[offset];
	    d.run_count += frac;
	    d.user_time += user_per_sec * frac;
	    d.system_time += system_per_sec * frac;
	    if ((user_per_sec + system_per_sec) < 1) {
		// if the job ran multi-cpu but we don't know how much it 
		// could have consumed, assume no idleness
		d.idle_time += (1-(user_per_sec + system_per_sec)) * frac;
	    }
	}
    }	

    double partialStartFrac(unsigned rounddown, unsigned exact) 
    {
	SINVARIANT(rounddown < exact 
		   && exact < rounddown + args.rollup_granularity);
	return (double)(args.rollup_granularity - (exact - rounddown))/(double)args.rollup_granularity;
    }

    double partialEndFrac(unsigned rounddown, unsigned exact)
    {
	SINVARIANT(rounddown < exact 
		   && exact < rounddown + args.rollup_granularity);
	return (double)(exact - rounddown)/(double)args.rollup_granularity;
    }

    void addTimes(vector<hteData *> &ents, unsigned exact_submit, 
		  unsigned exact_start, unsigned exact_end,
		  double user_per_sec, double system_per_sec) {
	unsigned align_submit = rounddown(exact_submit);
	unsigned align_start = rounddown(exact_start);
	unsigned align_end = rounddown(exact_end);
	INVARIANT(align_submit <= align_start && align_start <= align_end,
		  boost::format("bad %d %d %d") 
		  % align_submit % align_start % align_end);
	// TODO: see if there is a clean way to make these to parts
	// the same; they are essentially the same algorithm, but they
	// call the add pending function with different arguments.
	// boost::function/lambda perhaps?
	if (exact_start > exact_submit) {  // have pending time to account for
	    if (align_start == align_submit) { // all in one bucket
		INVARIANT(exact_start < align_submit + args.rollup_granularity, "bad");
		addPend(align_submit, ents, (exact_start - exact_submit)/(double)args.rollup_granularity);
	    } else { // exact_submit .. exact_start crosses a bucket boundary
		if (align_submit < exact_submit) { // partial pending in first bucket
		    addPend(align_submit,ents,
			    partialStartFrac(align_submit,exact_submit));
		    align_submit += args.rollup_granularity;
		} else {
		    INVARIANT(align_submit == exact_submit, "bad");
		}
		// bulk pending in middle
		for(;align_submit < align_start;align_submit += args.rollup_granularity) {
		    addPend(align_submit,ents,1);
		}
		if (align_submit < exact_start) { // partial pend at end
		    addPend(align_submit, ents,
			    partialEndFrac(align_submit,exact_start));
		}
	    }
	    INVARIANT(align_submit == align_start,"bad");
	} else {
	    INVARIANT(exact_start == exact_submit, "bad");
	}
	if (exact_end > exact_start) { // have running time to account for
	    if (align_end == align_start) { // all in one bucket
		INVARIANT(exact_end < align_start + args.rollup_granularity, "bad");
		addRun(align_start, ents, user_per_sec, system_per_sec,
		       (double)(exact_end - exact_start)/(double)args.rollup_granularity);
	    } else {
		if (align_start < exact_start) { // partial run in first bucket
		    addRun(align_start, ents, user_per_sec, system_per_sec,
			   partialStartFrac(align_start, exact_start));
		    align_start += args.rollup_granularity;
		}
		// bulk of run in middle
		for(;align_start < align_end; align_start += args.rollup_granularity) {
		    addRun(align_start, ents, user_per_sec, system_per_sec, 1);
		}
		INVARIANT(align_start == align_end,
			  boost::format("bad %d %d ;; %d %d") 
			  % align_start % align_end % exact_start % exact_end);
		if (align_start < exact_end) { // partial run at end
		    addRun(align_start, ents, user_per_sec, system_per_sec,
			   partialEndFrac(align_start,exact_end));
		}
	    }
	}
    }
 
    void setGroupsToUpdate() {
	groups_to_update.clear();
	if (args.enable_group[FarmLoad_All]) 
	    groups_to_update.push_back(allRollup);
	if (args.enable_group[FarmLoad_Production]) 
	    addEnt(groups_to_update,FarmLoad_Production, production.stringval());
	if (args.enable_group[FarmLoad_Sequence]) 
	    addEnt(groups_to_update,FarmLoad_Sequence, 
		   maybehexstring(production.stringval()).append(str_colon).append(maybehexstring(sequence.stringval())));
	if (args.enable_group[FarmLoad_Team]) 
	    addEnt(groups_to_update,FarmLoad_Team, team.stringval());
	if (args.enable_group[FarmLoad_Queue]) 
	    addEnt(groups_to_update,FarmLoad_Queue, queue.stringval());
	if (args.enable_group[FarmLoad_Cluster]) 
	    addEnt(groups_to_update,FarmLoad_Cluster, cluster.stringval());
	if (args.enable_group[FarmLoad_Username]) 
	    addEnt(groups_to_update,FarmLoad_Username, username.stringval());
	if (args.enable_group[FarmLoad_ExecHost] &&
	    exec_host.isNull() == false) 
	    addEnt(groups_to_update, FarmLoad_ExecHost, exec_host.stringval());
	if (exec_host.isNull() == false && args.enable_group[FarmLoad_Hostgroup]) {
	    string hostgroup;
	    if (exec_host_group.isNull()) {
		hostgroup = group_misc;
	    } else {
		hostgroup = exec_host_group.stringval();
	    }
	    addEnt(groups_to_update,FarmLoad_Hostgroup, hostgroup);
	}
	if (args.enable_group[FarmLoad_TeamGroup]) 
	    addEnt(groups_to_update, FarmLoad_TeamGroup, teamGroup(production.stringval(),team.stringval()));
    }
	
    virtual void processRow() { 
	// variables for exact to the second times, as used in
	// this calculations, so cropped to the start and end
	// windows.
	if (args.noerstest && ersTestJob(user_id.val())) { 
	    // ERS testing, ignore
	    return;
	}
	unsigned exact_submit, exact_start, exact_end;
	    
	INVARIANT(submit_time.val() >= 0 && start_time.val() >= 0 
		  && end_time.val() >= 0 && event_time.val() >= 0,
		  "bad");
	exact_submit = submit_time.val();
	// Next two decisions handle the case where we are analyzing
	// from jobs that are still running; in that case the event
	// time is the time the record was generated, which is either
	// the current time or the time the job was cancelled.
	exact_start = start_time.isNull() ? event_time.val() : start_time.val();
	exact_end = end_time.isNull() ? event_time.val() : end_time.val();

	minsubmit = min(minsubmit,exact_submit);
	maxend = max(maxend,exact_end);
	++nrecords;
	if (exact_end < args.rollup_start || exact_submit > args.rollup_end) {
	    return;
	}

	// Calculate these before we crop to the rollup window
	double user_per_sec = 0, system_per_sec = 0;
	    
	if (start_time.isNull() == false && exact_end > exact_start) {
	    unsigned wall_time = exact_end - exact_start;
	    user_per_sec = user_time.val() / wall_time;
	    system_per_sec = system_time.val() / wall_time;
	    if (wall_time > 0) {
		// This used to show up mostly because if a job was
		// restarted, it appeared the entire cpu time got
		// counted but the start and stop was of the last run.
		// Now we have multi-threaded programs, so this can be
		// legitimate.  Ought to be tracking nexechosts also,
		// but that doesn't seem to be in the dataseries
		// translation.  Similarly, ought to have a secondary
		// table that tells us for each dedicated host type
		// how many CPUs it has so we can cap user per second
		// on a per-host basis.
		if ((user_per_sec + system_per_sec) > 1) {
		    negative_idle_records += 1;
		    if (user_per_sec > cap_user_per_second) {
			++capped_rate_records;
			fprintf(stderr,
				"unexpectedly high user time %.6f %.6f ;; %.6f %.6f ;;  %d - %d = %d ;; %s %s\n",
				user_per_sec, system_per_sec,
				user_time.val(), system_time.val(), 
				end_time.val(),start_time.val(),end_time.val() - start_time.val(),
				cluster.stringval().c_str(),maybehexstring(exec_host.stringval()).c_str());
			user_per_sec = cap_user_per_second; // hardcode fixup...
		    }
		}
	    }
	}
	// crop to windows
	exact_submit = max(exact_submit,args.rollup_start);
	exact_start = max(exact_start,args.rollup_start);
	exact_start = min(exact_start,args.rollup_end);
	exact_end = min(exact_end,args.rollup_end);

	if (exact_submit > exact_start || exact_start > exact_end) {
	    fprintf(stderr,"weird %d %d %d -> %d %d %d\n",
		    submit_time.val(), start_time.val(), end_time.val(),
		    exact_submit,exact_start,exact_end);
	    if (exact_submit > exact_start) {
		exact_start = exact_submit;
	    }
	    if (exact_start > exact_end) {
		exact_end = exact_start;
	    }
	}
	INVARIANT(exact_submit <= exact_start && exact_start <= exact_end,
		  format("internal %d %d %d") 
		  % exact_submit % exact_start % exact_end);
	++nrecordsinwindow;

	setGroupsToUpdate();

	addTimes(groups_to_update, exact_submit, exact_start, exact_end,
		 user_per_sec, system_per_sec);
    }

    void printResultOne(hteData &x, const string &ptype) {
	printf("Begin Group %s; subent %s\n",ptype.c_str(),maybehexstring(x.group).c_str());
	for(unsigned j=0;j<x.data.size();++j) {
	    printf("  %d %.2f %.2f %.2f %.2f %.2f\n",
		   j * args.rollup_granularity + args.rollup_start,
		   x.data[j].pend_count, 
		   x.data[j].run_count, 
		   x.data[j].user_time, 
		   x.data[j].system_time, 
		   x.data[j].idle_time); 
	}
	printf("End Group %s; subent %s\n",ptype.c_str(),maybehexstring(x.group).c_str());

    }

    void printResultHT(FarmLoadHash *ht, const string &ptype)
    {
	for(FarmLoadHash::iterator i = ht->begin(); i != ht->end();++i) {
	    printResultOne(**i,ptype);
	}
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);

	printf("total time range %d .. %d; %d total records, %d records in window %d .. %d\n",
	       minsubmit,maxend,nrecords,nrecordsinwindow,args.rollup_start,args.rollup_end);
	printf("%d jobs over 100%% cpu utilization, %d capped at %d%% utilization\n", negative_idle_records,
	       capped_rate_records, 100 * cap_user_per_second);
	printf("columns are: time, pend_count, run_count, user_time, system_time, idle_time\n");
	printf("values are normalized to %d seconds\n",args.rollup_granularity);
	printResultOne(*allRollup,str_all);
	for(unsigned i = 1;i<rollups.size();++i) {
	    printResultHT(rollups[i],FarmLoad_groupNames[i]);
	}

	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    static const int maxRollupChunks = 100000;
    const FarmLoadArgs args;

    Variable32Field cluster,production,sequence;
    Int32Field event_time, submit_time, start_time, end_time;
    Variable32Field queue, team;
    DoubleField user_time, system_time;
    Variable32Field exec_host, username, exec_host_group;
    Int32Field user_id;
    unsigned minsubmit, maxend, nrecords, nrecordsinwindow, negative_idle_records, capped_rate_records;

    hteData *allRollup; // special case this one, avoid hash table lookups
    vector<FarmLoadHash *> rollups;
    vector<hteData *> groups_to_update; // we update the same number of groups lots of times, this avoids allocating the vector every time.
};

FarmLoadArgs *
handleFarmLoadArgs(const string &arg)
{
    FarmLoadArgs *ret = new FarmLoadArgs();
    static const string str_erstest("erstest");

    const string usage("invalid option to -a, expect  <granularity>:<start-secs>:<end-secs>:<rollupgroups...>:<where-clause>\n   use ? for the group to see valid rollupgroups, where-clause may be empty");

    vector<string> args;
    split(arg, ":", args);
    INVARIANT(args.size() == 5, usage);

    if (args[4] == "?") {
	cout << "Valid rollup groups: " << FarmLoad_groupNames[0];
	for(unsigned i = 1; i < FarmLoad_Ngroups; ++i) {
	    cout << ", " << FarmLoad_groupNames[i];
	}
	cout << "\n";
	exit(0);
    }
    ret->rollup_granularity = stringToInt32(args[0]);
    unsigned start = stringToUInt32(args[1]);
    unsigned end = stringToUInt32(args[2]);
    
    ret->rollup_start = roundDown(start, ret->rollup_granularity);
    ret->rollup_end = roundUp(end, ret->rollup_granularity);

    if (start != ret->rollup_start) {
	cout << boost::format("Warning, rounding start time %d to %d because of granularity %d\n")
	    % start % ret->rollup_start % ret->rollup_granularity;
    }

    if (end != ret->rollup_end) {
	cout << boost::format("Warning, rounding end time %d to %d because of granularity %d\n")
	    % end % ret->rollup_end % ret->rollup_granularity;
    }

    if (args[3] == "*") {
	for(int i=0;i<FarmLoad_Ngroups;++i) {
	    ret->enable_group[i] = true;
	}
    } else {
	vector<string> bits;
	split(args[3],",",bits);

	for(unsigned i = 0;i<bits.size();++i) {
	    bool found = false;
	    for(unsigned j = 0;j<FarmLoad_Ngroups;++j) {
		if (FarmLoad_groupNames[j] == bits[i]) {
		    ret->enable_group[j] = true;
		    found = true;
		    break;
		}
	    }
	    if (bits[i] == str_erstest) {
		found = true;
		ret->noerstest = false;
	    }
	    INVARIANT(found, boost::format("didn't find farmload rollup type '%s'") % bits[i]);
	}
    }

    ret->where_expr = args[4];
    INVARIANT(ret->rollup_granularity > 0,"bad");
    INVARIANT(ret->rollup_start < ret->rollup_end,
	      boost::format("bad %d %d") % ret->rollup_start % ret->rollup_end);
    return ret;
}

RowAnalysisModule *
newFarmLoad(DataSeriesModule &tail, FarmLoadArgs *args)
{
    return new FarmLoad(tail,args);
}

////////////////////////////////////////////////////////////////////////

static vector<int> trace_meta_id_list;

class TraceMetaId : public RowAnalysisModule {
public:
    TraceMetaId(DataSeriesModule &_source, int _meta_id) 
	: RowAnalysisModule(_source), target_meta_id(_meta_id),
	  meta_id(series,"meta_id", Field::flag_nullable), 
	  lsf_job_id(series,"job_id"), lsf_job_idx(series,"job_idx", Field::flag_nullable),
	  event_time(series,"event_time"), submit_time(series,"submit_time"),
	  start_time(series,"start_time", Field::flag_nullable),
	  end_time(series,"end_time", Field::flag_nullable),
	  user_time(series,"user_time"), system_time(series,"system_time"),
	  command(series,"command"), task(series,"task", Field::flag_nullable),
	  object(series,"object", Field::flag_nullable),
	  subtask(series,"subtask", Field::flag_nullable),
	  exec_host(series,"exec_host", Field::flag_nullable)
    {
    }
    virtual ~TraceMetaId() { }

    struct traceEnt {
	int lsf_job_id, lsf_job_idx;
	int submit, start, end, ev_time;
	bool is_pending, is_running;
	double cpu_time;
	string command, task, subthing, exec_host;
    };

    virtual void processRow() { 
	if (meta_id.val() == target_meta_id) {
	    traceEnt v;
	    v.lsf_job_id = lsf_job_id.val();
	    v.lsf_job_idx = lsf_job_idx.val();
	    v.submit = submit_time.val();
	    v.start = start_time.val();
	    v.end = end_time.val();
	    v.ev_time = event_time.val();
	    v.is_pending = start_time.isNull();
	    v.is_running = end_time.isNull();
	    v.cpu_time = user_time.val() + system_time.val();
	    v.command = command.stringval();
	    if (task.isNull()) {
		v.task = str_na;
	    } else {
		v.task = task.stringval();
	    }
	    if (!object.isNull()) {
		v.subthing = object.stringval();
	    } else if (!subtask.isNull()) {
		v.subthing = subtask.stringval();
	    } else {
		v.subthing = str_na;
	    }
	    v.exec_host = exec_host.stringval();
	    trace_log.push_back(v);
	}
    }
    
    struct traceEntOrder {
	static bool traceOrder(const traceEnt &a, const traceEnt &b) { // return a < b
	    if (a.lsf_job_id != b.lsf_job_id) {
		return a.lsf_job_id < b.lsf_job_id;
	    } else {
		return a.lsf_job_idx < b.lsf_job_idx;
	    }
	}
	bool operator() (const traceEnt &a, const traceEnt &b) const { // return a < b
	    // deal with pending order
	    if (a.is_pending && ! b.is_pending) {
		return false;
	    } else if (!a.is_pending && b.is_pending) {
		return true;
	    } else if (a.is_pending && b.is_pending) {
		if (a.submit != b.submit) {
		    return a.submit < b.submit;
		} else {
		    return traceOrder(a,b);
		}
	    }
	    // deal with running order
	    if (a.start != b.start) {
		return a.start < b.start;
	    } else { 
		return traceOrder(a,b);
	    }
	}
    };

    virtual void printResult() 
    {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	sort(trace_log.begin(),trace_log.end(),traceEntOrder());
	for(vector<traceEnt>::iterator i = trace_log.begin();
	    i != trace_log.end(); ++i) {
	    if (i->command == empty_string) {
		i->command = str_unknown;
	    }
	    printf("%d %d[%d]: %d..%d..%d %d; %.2f; %s %s %s %s\n",
		   target_meta_id, i->lsf_job_id,i->lsf_job_idx,
		   i->submit,i->is_pending ? -1 : i->start,
		   i->is_running ? -1 : i->end,
		   i->ev_time, i->cpu_time, 
		   i->exec_host == empty_string ? str_na.c_str() : i->exec_host.c_str(),
		   maybehexstring(i->command).c_str(),
		   maybehexstring(i->task).c_str(),
		   maybehexstring(i->subthing).c_str());
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

    const int target_meta_id;
    Int32Field meta_id, lsf_job_id, lsf_job_idx, event_time, 
	submit_time, start_time, end_time;
    DoubleField user_time, system_time;
    Variable32Field command, task, object, subtask, exec_host;

    vector<traceEnt> trace_log;
};

void
handleTraceMetaIdArgs(const char *arg)
{
    int meta_id = atoi(optarg);
    trace_meta_id_list.push_back(meta_id);
}

void
addTraceMetaIdModules(SequenceModule &sequence)
{
    for(vector<int>::iterator i = trace_meta_id_list.begin();
	i != trace_meta_id_list.end();++i) {
	sequence.addModule(new TraceMetaId(sequence.tail(),*i));
    }
}

////////////////////////////////////////////////////////////////////////

class ProductionReport : public RowAnalysisModule {
public:
    ProductionReport(DataSeriesModule &_source) :
	RowAnalysisModule(_source, ExtentSeries::typeLoose), 
	production(series,"production", Field::flag_nullable),
	sequence(series,"sequence", Field::flag_nullable),
	shot(series,"shot", Field::flag_nullable),
	user_id(series,"user_id"),
	nframes(series,"nframes", Field::flag_nullable),
	start_time(series,"start_time", Field::flag_nullable), 
	end_time(series,"end_time", Field::flag_nullable),
	resolution(series,"job_resolution", Field::flag_nullable), 
	wall_sum(0), frame_sum(0), noframes(0),
	unstarted(0), unfinished(0), job_count(0),
	ers_testing_job(0)
    {
    }
    virtual ~ProductionReport() { }
    
    struct rollup_data {
	double wall_sum;
	int frame_count, noframes;
	int r1count, r3count, rXcount, norcount;
	int job_count, unstarted, unfinished, ers_testing;
	rollup_data() : wall_sum(0), frame_count(0), noframes(0),
			r1count(0), r3count(0), rXcount(0), norcount(0),
			job_count(0), unstarted(0), unfinished(0), ers_testing(0) { }
    };
    struct prod_data {
	rollup_data rollup;
	HashMap<string,rollup_data> sequence_rollup;
	prod_data() { };
    };

    HashMap<string,prod_data> production_rollup;

    void updatedata(rollup_data &v) {
	// job counts
	if (user_id.val() == 12271) {
	    ++v.ers_testing;
	    return;
	}
	if (end_time.isNull()) {
	    ++v.unfinished;
	    return;
	}
	if (start_time.isNull()) {
	    ++v.unstarted;
	    return;
	}
	++v.job_count;

	// wall sum
	if (end_time.val() < start_time.val()) {
	    fprintf(stderr,"warning, bad start/end %d %d\n",start_time.val(),end_time.val());
	    return;
	}
	INVARIANT(end_time.val() >= start_time.val(),
		  format("bad %d %d") % end_time.val() % start_time.val());
	v.wall_sum += end_time.val() - start_time.val();
	    
	// frame count
	if (nframes.isNull()) {
	    ++v.noframes;
	} else {
	    ++v.frame_count;
	}
	// rX count
	if (resolution.isNull()) {
	    ++v.norcount;
	} else if (resolution.val() == 1.0) {
	    ++v.r1count;
	} else if (resolution.val() == 3.0) {
	    ++v.r3count;
	} else {
	    ++v.rXcount;
	}
    }

    virtual void processRow() { 
	prod_data *v;
	if (production.isNull()) {
	    v = &(production_rollup[def_unknown]);
	} else {
	    v = &(production_rollup[production.stringval()]);
	}
	updatedata(v->rollup);
	rollup_data *s;
	if (sequence.isNull()) {
	    s = &(v->sequence_rollup[str_squnknown]);
	} else {
	    s = &(v->sequence_rollup[sequence.stringval()]);
	}
	updatedata(*s);
    }

    void printRollup(rollup_data &v,const string &production,const string &sequence) {
	printf("  Production %s Sequence %s:\n",maybehexstring(production).c_str(),
	       maybehexstring(sequence).c_str());
	printf("    %d jobs, %d unstarted, %d unfinished, %d ers-testing\n",
	       v.job_count, v.unstarted, v.unfinished, v.ers_testing);
	printf("    %.4f wall years; %.2f hrs/job\n",
	       v.wall_sum / (365.0 * 24 * 60 * 60), 
	       v.wall_sum / (3600.0 * v.job_count));
	printf("    %d jobs w/o frame count; %d jobs with frame count\n",
	       v.noframes, v.frame_count);
	printf("    %d jobs@R1, %d jobs@R3, %d jobs@Rother, %d jobs w/o res\n",
	       v.r1count, v.r3count, v.rXcount, v.norcount);
    }

    virtual void printResult()
    {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	for(HashMap<string,prod_data>::iterator i = production_rollup.begin();
	    i != production_rollup.end(); ++i) {
	    printRollup(i->second.rollup,i->first,"sq*");
	    for(HashMap<string,rollup_data>::iterator j = i->second.sequence_rollup.begin();
		j != i->second.sequence_rollup.end();++j) {
		printRollup(j->second,i->first,j->first);
	    }
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

    const string target_production;
    Variable32Field production, sequence, shot;
    Int32Field user_id, nframes, start_time, end_time;
    DoubleField resolution;

    double wall_sum;
    int frame_sum, noframes;
    int unstarted, unfinished, job_count;
    int ers_testing_job;
};

void
addProductionReportModules(SequenceModule &sequence)
{
    sequence.addModule(new ProductionReport(sequence.tail()));    
}

/////////////////////////////////////////////////////////////////////

// static vector<int> find_rr_id_list, find_rr_userid_list;
// static vector<string> find_rr_team_list;
// static int find_rr_start, find_rr_end;
// static bool find_rr_any;
// 
// class RRLookup : public RowAnalysisModule {
// public:
//     RRLookup(DataSeriesModule &_source, vector<int> &rr_ids,
// 	     vector<string> &rr_team_list, vector<int> &rr_userid_list,
// 	     int _find_start, int _find_end, bool _find_any) :
// 	RowAnalysisModule(_source), 
// 	find_start(_find_start), find_end(_find_end), find_any(_find_any),
// 	rr_id(series,"request_id"), opened(series,"opened"), closed(series,"closed"), 
// 	user_id(series,"user_id"), status_id(series,"status_id"), frame_count(series,"frame_count"), 
// 	step(series,"step"), 
// 	production(series,"production"), sequence(series,"sequence"), shot(series,"shot"),
// 	team(series,"team"), batch(series,"batch"),
// 	frame_time_est(series,"frame_time"), priority(series,"priority"),
// 	resolution(series,"resolution",Field::flag_nullable)
//     {
// 	for(vector<int>::iterator i = rr_ids.begin(); i != rr_ids.end();++i) {
// 	    target_ids.add(*i);
// 	}
// 	have_user_or_team = !rr_team_list.empty() || !rr_userid_list.empty();
// 	for(vector<string>::iterator i = rr_team_list.begin(); i != rr_team_list.end(); ++i) {
// 	    target_teams.add(*i);
// 	}
// 	for(vector<int>::iterator i = rr_userid_list.begin(); i != rr_userid_list.end(); ++i) {
// 	    printf("add target users %d\n",*i);
// 	    target_users.add(*i);
// 	}
//     }
// 
//     struct data {
// 	int rr_id, opened, closed, user_id, status_id, frame_count, step;
// 	string prod, seq, shot, team, batch;
// 	double frame_time_est, priority, resolution; 
//     };
// 
//     HashMap<int,data> rr_found;
//     typedef HashMap<int,data>::iterator iterator;
// 
//     void addfound() {
// 	data d;
// 	d.rr_id = rr_id.val();
// 	d.opened = opened.val();
// 	d.closed = closed.val();
// 	d.user_id = user_id.val();
// 	d.status_id = status_id.val();
// 	d.frame_count = frame_count.val();
// 	d.step = step.val();
// 	d.prod = production.stringval();
// 	d.seq = sequence.stringval();
// 	d.shot = shot.stringval();
// 	d.team = team.stringval();
// 	d.frame_time_est = frame_time_est.val();
// 	d.priority = priority.val();
// 	d.resolution = resolution.val();
// 	d.batch = batch.stringval();
// 	AssertAlways(rr_found.lookup(d.rr_id) == NULL,
// 		     ("rr id %d is duplicated\n",d.rr_id));
// 	rr_found[d.rr_id] = d;
//     }
//    
//     static bool inrange(int a, int b_start, int b_end) {
// 	return a >= b_start && a <= b_end;
//     }
// 
//     static bool intervalOverlap(int a_start,int a_end, int b_start, int b_end) {
// 	return inrange(a_start,b_start,b_end) ||
// 	    inrange(a_end,b_start,b_end) ||
// 	    inrange(b_start, a_start, a_end) ||
// 	    inrange(b_end, a_start, a_end);
//     }
// 
//     virtual void processRow() { 
// 	if (target_ids.exists(rr_id.val())) {
// 	    addfound();
// 	} else {
// 	    int closed_time = closed.val();
// 	    if (closed_time == 0) {
// 		closed_time = time(NULL);
// 	    }
// 	    if (intervalOverlap(find_start,find_end,opened.val(),closed_time)) {
// 		if (find_any) {
// 		    addfound();
// 		} else if (target_users.exists(user_id.val())) {
// 		    addfound();
// 		} else if (target_teams.exists(team.stringval())) {
// 		    addfound();
// 		} 
// 	    }
// 	}
//     }
// 
//     virtual void printResult() {
// 	printf("Begin-%s\n",__PRETTY_FUNCTION__);
// 	for(iterator i = rr_found.begin();
// 	    i != rr_found.end(); ++i) {
// 	    printf("  rrid %d: %d .. %d; %s %s %s %s; %d %d; %d %.2f %.2f %.2f %d %s\n",
// 		   i->second.rr_id, i->second.opened, i->second.closed, 
// 		   maybehexstring(i->second.prod).c_str(),
// 		   maybehexstring(i->second.seq).c_str(),
// 		   maybehexstring(i->second.shot).c_str(),
// 		   maybehexstring(i->second.team).c_str(),
// 		   i->second.user_id, i->second.status_id,
// 		   i->second.frame_count, i->second.frame_time_est,
// 		   i->second.priority, i->second.resolution, 
// 		   i->second.step,
// 		   maybehexstring(i->second.batch).c_str());
// 	}
// 	printf("End-%s\n",__PRETTY_FUNCTION__);
//     }
// 
//     HashUnique<int> target_ids;
//     HashUnique<string> target_teams;
//     HashUnique<int> target_users;
//     const int find_start, find_end;
//     const bool find_any;
//     bool have_user_or_team;
//     Int32Field rr_id, opened, closed, user_id, status_id, frame_count, step;
//     Variable32Field production, sequence, shot, team, batch;
//     DoubleField frame_time_est, priority, resolution;
// };

// void 
// handleRenderRequestLookupArgs(const char *arg)
// {
//     if (isdigit(*arg)) {
// 	int rr_id = atoi(arg);
// 	find_rr_id_list.push_back(rr_id);
//     } else {
// 	AssertAlways((*arg == 'T' || *arg == 'U' ||
// 		      *arg == 'W' || *arg == 'A') && arg[1] == ':',
// 		     ("invalid argument to -d"));
// 	if (*arg == 'T') {
// 	    string foo(arg+2);
// 	    find_rr_team_list.push_back(maybehex2raw(foo));
// 	} else if (*arg == 'U') {
// 	    for(const char *tmp = arg+2;*tmp;++tmp) {
// 		AssertAlways(isdigit(*tmp),
// 			     ("invalid U: argument %s\n",arg+2));
// 	    }
// 	    find_rr_userid_list.push_back(atoi(arg+2));
// 	    printf("added userid entry %d\n",atoi(arg+2));
// 	} else if (*arg == 'W' || *arg == 'A') {
// 	    find_rr_start = atoi(arg+2);
// 	    char *pt2 = index(arg+2,':');
// 	    INVARIANT(pt2 != NULL && *pt2 == ':');
// 	    find_rr_end = atoi(pt2+1);
// 	    AssertAlways(find_rr_start > 0 && find_rr_end > find_rr_start,
// 			 ("invalid range %d %d\n",find_rr_start,find_rr_end));
// 	    if (*arg == 'A') {
// 		find_rr_any = true;
// 	    }
// 	} else {
// 	    AssertFatal(("internal"));
// 	}
//     }
// }

// class RR2MetaIdLookup : public RowAnalysisModule {
// public:
//     RR2MetaIdLookup(DataSeriesModule &_source, RRLookup &_lookuplist)
// 	: RowAnalysisModule(_source), lookuplist(_lookuplist),
// 	  meta_id(series,"meta_id"),
// 	  rr_id(series,"request_id"), production(series,"production"),
// 	  sequence(series,"sequence"), shot(series,"shot")
//     {
//     }
//     struct data {
// 	RRLookup::data rr;
// 	vector<int> meta_ids;
//     };
// 
//     HashMap<ExtentType::int32,data> rr_map;
// 
//     typedef HashMap<ExtentType::int32,data>::iterator iterator;
// 
//     virtual void processRow() { 
// 	RRLookup::data *v = lookuplist.rr_found.lookup(rr_id.val());
// 	if (v != NULL) {
// 	    SINVARIANT(production.equal(empty_string) == false);
// 	    if (!(production.equal(v->prod) && sequence.equal(v->seq) && 
// 		  shot.equal(v->shot))) {
// 		if ((v->seq == str_ok_neq_rr_rj_1 && (v->shot == str_ok_neq_rr_rj_1 ||
// 						      v->shot == str_ok_neq_rr_rj_2))
// 		    || v->seq == str_ok_neq_rr_rj_3 
// 		    || (v->seq == str_ok_neq_rr_rj_4 && shot.equal(v->shot))) {
// 		    // special case where the entry in rr and in rj is different for the
// 		    // sequence and the shot, and in this case the value in rj is the one
// 		    // we want, whereas the one in rr is a generic
// 		    // also special case from testing, where we want to overwrite with the
// 		    // "real" value
// 		    v->seq = sequence.stringval();
// 		    v->shot = shot.stringval();
// 		} else if ((sequence.equal(str_ok_neq_rr_rj_1) && 
// 			    (shot.equal(str_ok_neq_rr_rj_1) || shot.equal(str_ok_neq_rr_rj_2)))
// 			   || sequence.equal(str_ok_neq_rr_rj_3) 
// 			   || (sequence.equal(str_ok_neq_rr_rj_4) && shot.equal(v->shot))) {
// 		    // special case where we want to keep the values we already have
// 		} else {
// 		    fprintf(stderr,"WARNING: rr %d not unique %s/%s; %s/%s; %s/%s\n",
// 			    rr_id.val(),
// 			    maybehexstring(production.stringval()).c_str(),
// 			    maybehexstring(v->prod).c_str(),
// 			    maybehexstring(sequence.stringval()).c_str(),
// 			    maybehexstring(v->seq).c_str(),
// 			    maybehexstring(shot.stringval()).c_str(),
// 			    maybehexstring(v->shot).c_str());
// 		    v->prod = production.stringval();
// 		    v->seq = sequence.stringval();
// 		    v->shot = shot.stringval();
// 		}
// 	    }
// 	    data &v2 = rr_map[rr_id.val()];
// 	    if (v2.rr.prod == empty_string) {
// 		v2.rr = *v;
// 	    } 
// 	    v2.meta_ids.push_back(meta_id.val());
// 	}
//     }
// 
//     virtual void printResult() {
// 	printf("Begin-%s\n",__PRETTY_FUNCTION__);
// 	for(iterator i = rr_map.begin();
// 	    i != rr_map.end(); ++i) {
// 	    RRLookup::data &rr = i->second.rr;
// 	    printf("  rrid_meta %d: %d .. %d %s %s %s: ",i->first,rr.opened,rr.closed,
// 		   maybehexstring(rr.prod).c_str(),
// 		   maybehexstring(rr.seq).c_str(),
// 		   maybehexstring(rr.shot).c_str());
// 	    for(vector<int>::iterator j = i->second.meta_ids.begin();
// 		j != i->second.meta_ids.end();++j) {
// 		printf("%d ",*j);
// 	    }
// 	    printf("\n");
// 	}
// 
// 	printf("End-%s\n",__PRETTY_FUNCTION__);
//     }
// 
//     RRLookup &lookuplist;
//     Int32Field meta_id, rr_id;
//     Variable32Field production, sequence, shot;
// };

// void
// addRRLookupModules(SequenceModule &rrSequence, SequenceModule &rjSequence)
// {
//     if (find_rr_id_list.empty() == false || 
// 	find_rr_team_list.empty() == false ||
// 	find_rr_userid_list.empty() == false || find_rr_any) {
// 	if (find_rr_start == 0 && find_rr_end == 0) {
// 	    find_rr_end = time(NULL);
// 	    find_rr_start = find_rr_end - 86400;
// 	}
// 		
// 	RRLookup *rrlookup = new RRLookup(rrSequence.tail(), find_rr_id_list, 
// 					  find_rr_team_list, find_rr_userid_list, 
// 					  find_rr_start, find_rr_end, find_rr_any);
// 	rrSequence.addModule(rrlookup);
// 	rjSequence.addModule(new RR2MetaIdLookup(rjSequence.tail(),*rrlookup));
//     } 
// }

}
