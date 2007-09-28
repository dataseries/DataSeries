/* -*-C++-*-
*******************************************************************************
*
* File:         lsfdsanalysis-mod1.C
* RCS:          $Header: /mount/cello/cvs/Grizzly/cpp/analysis/lsfdsanalysis-mod1.C,v 1.5 2004/09/21 01:57:23 anderse Exp $
* Description:  Mostly finished modules
* Author:       Eric Anderson
* Created:      Mon Apr 19 14:33:24 2004
* Modified:     Sat Sep 11 23:29:07 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2004, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#include <Lintel/HashUnique.H>

#include "analysis/lsfdsanalysis-mod1.H"

using namespace std;

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

static bool FarmLoad_enableGroup[FarmLoad_Ngroups], FarmLoad_noerstest;
static int FarmLoad_granularity, FarmLoad_start, FarmLoad_end; 

class FarmLoad : public RowAnalysisModule {
public:
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
	unsigned operator()(const hteData *k) {
	    return HashTable_hashbytes(k->group.data(),k->group.size());
	}
    };

    struct hteEqual {
	bool operator()(const hteData *a, const hteData *b) {
	    return a->group == b->group;
	}
    };

    typedef HashTable<hteData *, hteHash, hteEqual> FarmLoadHash;

    int rounddown(int v) {
	int ret = v - (v % rollupGranularity);
	AssertAlways(ret <= v,("bad %d %d\n",ret,v));
	return ret;
    }
    int roundup(int v) {
	int ret = v + ((rollupGranularity -  (v % rollupGranularity)) % rollupGranularity);
	AssertAlways(ret >= v,("bad %d %d\n",ret,v));
	return ret;
    }

    FarmLoad(DataSeriesModule &_source, int _rollupGranularity, int _rollup_start, int _rollup_end)
	: RowAnalysisModule(_source), 
	  rollupGranularity(_rollupGranularity),
	  rollup_start(rounddown(_rollup_start)),
	  rollup_end(roundup(_rollup_end)),
	  cluster(series,"cluster_name"),
	  production(series,"production", Field::flag_nullable, def_unknown), 
	  sequence(series,"sequence", Field::flag_nullable, def_unknown),
	  event_time(series,"event_time"), submit_time(series,"submit_time"), 
	  start_time(series,"start_time", Field::flag_nullable),
	  end_time(series,"end_time", Field::flag_nullable), 
	  queue(series,"queue"), team(series,"team"),
	  user_time(series,"user_time"), system_time(series,"system_time"),
	  exec_host(series,"exec_host", Field::flag_nullable), 
	  exec_host_group(series, "exec_host_group", Field::flag_nullable),
	  username(series,"username"),
	  user_id(series,"user_id"),
	  minsubmit(2000000000), maxend(0), nrecords(0), nrecordsinwindow(0), negative_idle_records(0)
    {
	allRollup = new hteData;
	allRollup->group = str_all;
	rollups.resize(FarmLoad_Ngroups);
	for(unsigned i = 1;i<rollups.size();++i) {
	    rollups[i] = new FarmLoadHash;
	}
    }
    virtual ~FarmLoad() { }
    
    hteData getEntTmp;
    void addEnt(vector<hteData *> &ents,
		FarmLoad_groups group,const string &key) {
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

//    const string hostGroup(const string &in) {
//	if (in.size() <= 2) {
//	    return group_misc;
//	}
//	if (prefixequal(in, 
//	if (in[0] == 'c' && in[1] == 'q' && isdigit(in[2])) {
//	    return group_cq;
//	} else if (in[0] == 'h' && in[1] == 'p' && isdigit(in[2])) {
//	    return group_hp;
//	} else if (in[0] == 's' && in[1] == 'g' && isdigit(in[2])) {
//	    return group_sg;
//	} else if (in.size() > 3 && in[0] == 'e' && in[1] == 'r' && in[2] == 's' && isdigit(in[3])) {
//	    return group_ers;
//	}
//	if (false) { // fails for erstest5 :(
//	    for(unsigned i=0;i<in.size();i++) {
//		AssertAlways(!isdigit(in[i]),("bad %s\n",in.c_str()));
//	    }
//	}
//	return group_misc;
//    }

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

    void addPend(int time, vector<hteData *> &ents,double frac)
    {
	int offset = time - rollup_start;
	AssertAlways(offset >= 0,("bad; time start = %d",time));
	offset = offset / rollupGranularity;
	AssertAlways(offset < maxRollupChunks,("bad (%d - %d)/%d = %d ",time,rollup_start,rollupGranularity,offset));
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
	int offset = time - rollup_start;
	AssertAlways(offset >= 0,("bad"));
	offset = offset / rollupGranularity;
	AssertAlways(offset < maxRollupChunks,("bad"));
	for(unsigned i = 0;i<ents.size();++i) {
	    if(ents[i]->data.size() <= (unsigned)offset) {
		ents[i]->data.resize(offset+1);
	    }
	    rollupData &d = ents[i]->data[offset];
	    d.run_count += frac;
	    d.user_time += user_per_sec * frac;
	    d.system_time += system_per_sec * frac;
	    d.idle_time += (1-(user_per_sec + system_per_sec)) * frac;
	}
    }	

    double partialStartFrac(int rounddown,int exact) 
    {
	AssertAlways(rounddown < exact,("bad"));
	return (double)(rollupGranularity - (exact - rounddown))/(double)rollupGranularity;
    }

    double partialEndFrac(int rounddown,int exact)
    {
	AssertAlways(rounddown < exact,("bad"));
	return (double)(exact - rounddown)/(double)rollupGranularity;
    }

    void addTimes(vector<hteData *> &ents, ExtentType::int32 exact_submit, 
		  ExtentType::int32 exact_start, ExtentType::int32 exact_end,
		  double wall_time, double user_per_sec, double system_per_sec) {
	int submit = rounddown(exact_submit);
	int start = rounddown(exact_start);
	int end = rounddown(exact_end);
	AssertAlways(submit <= start && start <= end,
		     ("bad %d %d %d\n",submit,start,end));
	if (exact_start < submit + rollupGranularity) { // submit in tiny window before bucket boundary
	    if (exact_submit > exact_start) { // any pending?
		addPend(submit,ents,(exact_submit - exact_start)/(double)rollupGranularity);
	    }
	} else { // submit .. start crosses a bucket boundary
	    if (submit < exact_submit) { // partial pend at start
		addPend(submit,ents,
			partialStartFrac(submit,exact_submit));
		submit += rollupGranularity;
	    }
	    // bulk pend in middle
	    for(;submit < start;submit += rollupGranularity) {
		addPend(submit,ents,1);
	    }
	    if (submit < exact_start) { // partial pend at end
		addPend(submit,ents,
			partialEndFrac(submit,exact_start));
	    }
	}
	AssertAlways(submit == start,("bad"));
	if (wall_time > 0) {
	    if (exact_end < start + rollupGranularity) { // start in tiny window before bucket boundary
		if (exact_end > exact_start) { // any start?
		    addRun(start,ents,user_per_sec,system_per_sec,
			   (double)(exact_end - exact_start)/(double)rollupGranularity);
		} 
	    } else {
		if (start < exact_start) { // partial start at start
		    addRun(start,ents,user_per_sec,system_per_sec,
			   partialStartFrac(start,exact_start));
		    start += rollupGranularity;
		}
		// bulk of run in middle
		for(;start < end; start += rollupGranularity) {
		    addRun(start,ents,user_per_sec,system_per_sec,1);
		}
		AssertAlways(start == end,("bad %d %d ;; %d %d",start,end,exact_start,exact_end));
		if (start < exact_end) { // partial run at end
		    addRun(start,ents,user_per_sec,system_per_sec,
			   partialEndFrac(start,exact_end));
		}
	    }
	}
    }
 
    virtual void processRow() { 
	vector<hteData *> ents;
	// variables for exact to the second times, as used in
	// this calculations, so cropped to the start and end
	// windows.
	if (FarmLoad_noerstest && ersTestJob(user_id.val())) { 
	    // ERS testing, ignore
	    return;
	}
	ExtentType::int32 exact_submit, exact_start, exact_end;
	    
	exact_submit = submit_time.val();
	exact_start = start_time.isNull() ? event_time.val() : start_time.val();
	exact_end = end_time.isNull() ? event_time.val() : end_time.val();

	minsubmit = min(minsubmit,exact_submit);
	maxend = max(maxend,exact_end);
	++nrecords;
	if (exact_end < rollup_start || exact_submit > rollup_end) {
	    return;
	}
	double wall_time = 0, user_per_sec = 0, system_per_sec = 0;
	    
	if (start_time.isNull() == false) {
	    wall_time = exact_end - exact_start;
	    if (wall_time > 0) {
		user_per_sec = user_time.val() / wall_time;
		system_per_sec = system_time.val() / wall_time;
		if ((user_per_sec + system_per_sec) > 1) {
		    negative_idle_records += 1;
		    if (user_per_sec > 2) {
			fprintf(stderr,"bad %.6f %.6f ;; %.6f %.6f ;;  %d - %d = %d ;; %s %s\n",user_per_sec, system_per_sec,
				user_time.val(), system_time.val(), 
				end_time.val(),start_time.val(),end_time.val() - start_time.val(),
				cluster.stringval().c_str(),exec_host.stringval().c_str());
			user_per_sec = 2; // hardcode fixup...
		    }
		}
	    }
	}
	// crop to windows
	exact_submit = max(exact_submit,rollup_start);
	exact_start = max(exact_start,rollup_start);
	exact_start = min(exact_start,rollup_end);
	exact_end = min(exact_end,rollup_end);

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
	AssertAlways(exact_submit <= exact_start && exact_start <= exact_end,
		     ("internal %d %d %d\n",exact_submit,exact_start,exact_end));
	++nrecordsinwindow;
	ents.clear();
	if (FarmLoad_enableGroup[FarmLoad_All]) 
	    ents.push_back(allRollup);
	if (FarmLoad_enableGroup[FarmLoad_Production]) 
	    addEnt(ents,FarmLoad_Production, production.stringval());
	if (FarmLoad_enableGroup[FarmLoad_Sequence]) 
	    addEnt(ents,FarmLoad_Sequence, maybehexstring(production.stringval()).append(str_colon).append(maybehexstring(sequence.stringval())));
	if (FarmLoad_enableGroup[FarmLoad_Team]) 
	    addEnt(ents,FarmLoad_Team, team.stringval());
	if (FarmLoad_enableGroup[FarmLoad_Queue]) 
	    addEnt(ents,FarmLoad_Queue, queue.stringval());
	if (FarmLoad_enableGroup[FarmLoad_Cluster]) 
	    addEnt(ents,FarmLoad_Cluster, cluster.stringval());
	if (FarmLoad_enableGroup[FarmLoad_Username]) 
	    addEnt(ents,FarmLoad_Username, username.stringval());
	if (FarmLoad_enableGroup[FarmLoad_ExecHost] &&
	    exec_host.isNull() == false) 
	    addEnt(ents, FarmLoad_ExecHost, exec_host.stringval());
	if (exec_host.isNull() == false && FarmLoad_enableGroup[FarmLoad_Hostgroup]) {
	    string hostgroup;
	    if (exec_host_group.isNull()) {
		hostgroup = group_misc;
	    } else {
		hostgroup = exec_host_group.stringval();
	    }
	    addEnt(ents,FarmLoad_Hostgroup, hostgroup);
	}
	if (FarmLoad_enableGroup[FarmLoad_TeamGroup]) 
	    addEnt(ents, FarmLoad_TeamGroup, teamGroup(production.stringval(),team.stringval()));
	addTimes(ents, exact_submit, exact_start, exact_end,
		 wall_time, user_per_sec, system_per_sec);
    }

    void printResultOne(hteData &x, const string &ptype) {
	printf("Begin Group %s; subent %s\n",ptype.c_str(),maybehexstring(x.group).c_str());
	for(unsigned j=0;j<x.data.size();++j) {
	    printf("  %d %.2f %.2f %.2f %.2f %.2f\n",
		   j * rollupGranularity + rollup_start,
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
	       minsubmit,maxend,nrecords,nrecordsinwindow,rollup_start,rollup_end);
	printf("columns are: time, pend_count, run_count, user_time, system_time, idle_time\n");
	printf("values are normalized to %d seconds\n",rollupGranularity);
	printResultOne(*allRollup,str_all);
	for(unsigned i = 1;i<rollups.size();++i) {
	    printResultHT(rollups[i],FarmLoad_groupNames[i]);
	}

	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    static const int maxRollupChunks = 100000;
    const int rollupGranularity, rollup_start, rollup_end;

    Variable32Field cluster,production,sequence;
    Int32Field event_time, submit_time, start_time, end_time;
    Variable32Field queue, team;
    DoubleField user_time, system_time;
    Variable32Field exec_host, username, exec_host_group;
    Int32Field user_id;
    int minsubmit, maxend, nrecords, nrecordsinwindow, negative_idle_records;

    hteData *allRollup; // special case this one, avoid hash table lookups
    vector<FarmLoadHash *> rollups;
};

void
LSFDSAnalysisMod::initFarmLoadArgs()
{
    FarmLoad_noerstest = true;
    FarmLoad_granularity = 60;
    FarmLoad_end = time(NULL) - 5*60;
    FarmLoad_start = FarmLoad_start - 24*3600;
    for(int i=0;i<FarmLoad_Ngroups;++i) {
	FarmLoad_enableGroup[i] = true;
    }
    // these are fairly slow, so off by default
    FarmLoad_enableGroup[FarmLoad_Username] = false; 
    FarmLoad_enableGroup[FarmLoad_ExecHost] = false; 
}

void
LSFDSAnalysisMod::handleFarmLoadArgs(const char *arg)
{
    static const string str_erstest("erstest");

    const char *granularity = arg;
    const string usage("invalid option to -a, expect  <granularity>:<start-secs>:<end-secs>:<rollupgroups...>; use no args to see valid rollupgroups");
    char *startsecs = index(granularity,':');
    AssertAlways(startsecs != NULL,("%s",usage.c_str()));
    ++startsecs;
    char *endsecs = index(startsecs,':');
    AssertAlways(endsecs != NULL,("%s",usage.c_str()));
    ++endsecs;
    char *rollupgroups = index(endsecs,':');
    AssertAlways(rollupgroups != NULL && rollupgroups[1] != '\0',
		 ("%s",usage.c_str()));
    ++rollupgroups;
    FarmLoad_granularity = atoi(granularity);
    FarmLoad_start = atoi(startsecs);
    FarmLoad_end = atoi(endsecs);
    if (*rollupgroups == '*') {
	for(int i=0;i<FarmLoad_Ngroups;++i) {
	    FarmLoad_enableGroup[i] = true;
	}
    } else {
	string tmp = rollupgroups;
	vector<string> bits;
	for(unsigned i = 0; i<FarmLoad_Ngroups;++i) {
	    FarmLoad_enableGroup[i] = false;
	}
	split(tmp,",",bits);
	if (false) printf("%s split into: ",tmp.c_str());
	for(unsigned i = 0;i<bits.size();++i) {
	    if (false) printf("'%s', ",bits[i].c_str());
	    bool found = false;
	    for(unsigned j = 0;j<FarmLoad_Ngroups;++j) {
		if (FarmLoad_groupNames[j] == bits[i]) {
		    FarmLoad_enableGroup[j] = true;
		    found = true;
		    break;
		}
	    }
	    if (bits[i] == str_erstest) {
		found = true;
		FarmLoad_noerstest = false;
	    }
	    AssertAlways(found,
			 ("didn't find farmload rollup type '%s'\n",
			  bits[i].c_str()));
	}
	if (false) printf("\n");
    }

    AssertAlways(FarmLoad_granularity > 0,("bad"));
    AssertAlways(FarmLoad_start < FarmLoad_end,
		 ("bad %d %d",FarmLoad_start,FarmLoad_end));
}

RowAnalysisModule *
LSFDSAnalysisMod::newFarmLoad(DataSeriesModule &tail)
{
    return new FarmLoad(tail,FarmLoad_granularity,
			FarmLoad_start,FarmLoad_end); 
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
	bool operator() (const traceEnt &a, const traceEnt &b) { // return a < b
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
LSFDSAnalysisMod::handleTraceMetaIdArgs(const char *arg)
{
    int meta_id = atoi(optarg);
    trace_meta_id_list.push_back(meta_id);
}

void
LSFDSAnalysisMod::addTraceMetaIdModules(SequenceModule &sequence)
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
	AssertAlways(end_time.val() >= start_time.val(),("bad %d %d",end_time.val(),start_time.val()));
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
LSFDSAnalysisMod::addProductionReportModules(SequenceModule &sequence)
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
// LSFDSAnalysisMod::handleRenderRequestLookupArgs(const char *arg)
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
// 	    AssertAlways(pt2 != NULL && *pt2 == ':',("internal"));
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
// 	    AssertAlways(production.equal(empty_string) == false,("bad"));
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
// LSFDSAnalysisMod::addRRLookupModules(SequenceModule &rrSequence, SequenceModule &rjSequence)
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

