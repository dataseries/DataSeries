// -*-C++-*-
/*
   (c) Copyright 2004-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Analysis for LSF dataseries files
*/

// Note: the code in here which dealt with the render request and
// render jobs tables has been commented out since that data is not
// currently available.  Those tables identifed meta data about groups
// of lsf jobs.  The code has not been removed since it could be
// useful in the future.

using namespace std;

#include <getopt.h>
#include <ctype.h>
#include <time.h>
#include <limits.h>

#include <list>
#include <ostream>
#include <algorithm>

#include <Lintel/StatsQuantile.hpp>
#include <Lintel/HashTable.hpp>
#include <Lintel/HashMap.hpp>
#include <Lintel/HashUnique.hpp>
#include <Lintel/PriorityQueue.hpp>

#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/DStoTextModule.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/PrefetchBufferModule.hpp>
#include <DataSeries/MinMaxIndexModule.hpp>

#include "analysis/lsfdsanalysis-common.hpp"
#include "analysis/lsfdsanalysis-mod1.hpp"

using boost::format;
// needed to make g++-3.3 not suck.
extern int printf (__const char *__restrict __format, ...) 
   __attribute__ ((format (printf, 1, 2)));

enum optionsEnum {
    // LSF/grizzly rollups
    optFarmLoad = 0, // load on the farm
    optTraceMetaId, // trace meta id's 
    optJobReport, // an approximation of the Bear Job Report 
    optProductionReport, 
    optLSFJobTimePredict, 
    optHostAvailabilityReport,
    optComplexityAnalysis,
    optLSFJobStats,
    optLSFSlowJobs,
    optHostCalibration, 
    optBestEffort,
    optLastOption
};

static bool options[optLastOption];

class JobReport : public RowAnalysisModule {
public:
    JobReport(DataSeriesModule &_source, 
	      int _rollup_start, int _rollup_end, bool _noerstest)
	: RowAnalysisModule(_source), rollup_start(_rollup_start),
	  rollup_end(_rollup_end), noerstest(_noerstest), 
	  event_time(series,"event_time"),
	  submit_time(series,"submit_time"),
	  start_time(series,"start_time", Field::flag_nullable), 
	  end_time(series,"end_time", Field::flag_nullable),
	  team(series,"team"), 
	  production(series,"production", Field::flag_nullable, str_unknown), 
	  sequence(series,"sequence", Field::flag_nullable, str_unknown),
	  shot(series,"shot", Field::flag_nullable, str_unknown),
	  username(series,"username"),
	  meta_id(series,"meta_id", Field::flag_nullable, -1),
	  user_id(series,"user_id"), 
	  minsubmit(numeric_limits<int32_t>::max()), maxend(0), 
	  minwindow(numeric_limits<int32_t>::max()),
	  maxwindow(0), nrecords(0),
	  delivered_window(0), delivered_total(0),
	  early_submit(0), early_start(0), running_at_end(0)
    {
    }
    virtual ~JobReport() { }
    
    struct hte {
	int32_t metaid;
	string team, production, sequence, shot, username;
	double delivered_window, delivered_total;
	uint32_t finished_jobs, running_jobs, pending_jobs, cancelled_jobs;
	int32_t mintime, maxtime;
	hte() : metaid(0), delivered_window(0), delivered_total(0),
		finished_jobs(0), running_jobs(0), pending_jobs(0),
		cancelled_jobs(0), mintime(numeric_limits<int32_t>::max()), 
		maxtime(0) { }
    };

    struct hteHash {
	unsigned operator()(const hte &a) const {
	    unsigned tmp = lintel::hashBytes(a.team.data(),a.team.size(),a.metaid);
	    tmp = lintel::hashBytes(a.production.data(),a.production.size(),tmp);
	    tmp = lintel::hashBytes(a.sequence.data(),a.sequence.size(),tmp);
	    return lintel::hashBytes(a.shot.data(),a.shot.size(),tmp);
	}
    };

    struct hteEqual {
	bool operator()(const hte &a, const hte &b) const {
	    return a.metaid == b.metaid && a.shot == b.shot 
		&& a.sequence == b.sequence && a.team == b.team &&
		a.production == b.production;
	}
    };

    typedef HashTable<hte, hteHash, hteEqual> rollupT;

    rollupT rollup;

    virtual void  processRow() { 
	if (noerstest && ersTestJob(user_id.val())) {
	    // ERS testing, ignore
	    return;
	}
	// variables for exact to the second times, as used in this
	// calculations, so cropped to the start and end windows.
	int32_t exact_submit, exact_start, exact_end;
	    
	exact_submit = submit_time.val();
	exact_start = start_time.isNull() ? event_time.val() : start_time.val();
	exact_end = end_time.isNull() ? event_time.val() : end_time.val();

	// can't prune on exact start/end because we can have jobs which 
	// entirely finished before the window, but the job was not closed
	// in the render request database until after the window, and so we
	// want to report on that request in the report

	minsubmit = min(minsubmit,exact_submit);
	maxend = max(maxend,exact_end);

	int32_t window_submit, window_start, window_end;
	window_submit = max(exact_submit, rollup_start);
	window_submit = min(window_submit, rollup_end);
	window_start = max(exact_start, rollup_start);
	window_start = min(window_start, rollup_end);
	window_end = max(exact_end, rollup_start);
	window_end = min(window_end, rollup_end);
	INVARIANT(window_submit <= window_start && window_start <= window_end &&
		  window_start >= rollup_start && window_end <= rollup_end,
		  format("?! %d %d %d ; %d %d") % window_submit % window_start
		  % window_end % rollup_start % rollup_end);
	++nrecords;
	minwindow = min(window_submit,minwindow);
	maxwindow = max(window_end,maxwindow);
	delivered_total += exact_end - exact_start;
	delivered_window += window_end - window_start;
	if (exact_submit < rollup_start) {
	    ++early_submit;
	}
	if (exact_start < rollup_start) {
	    ++early_start;
	}
	if (exact_end > rollup_end || 
	    (end_time.isNull() && !start_time.isNull())) {
	    ++running_at_end;
	}
	hte k;
	if (false && meta_id.val() == 271329) {
	    printf("XYZZY-E %d %d %d\n",exact_submit, exact_start, exact_end);
	    printf("XYZZY-W %d %d %d\n",window_submit, window_start, window_end);
	}
	k.metaid = meta_id.val();
	k.team = team.stringval();
	k.production = production.stringval();
	k.sequence = sequence.stringval();
	k.shot = shot.stringval();

	hte *v = rollup.lookup(k);
	if (v == NULL) {
	    k.username = username.stringval();
	    v = rollup.add(k);
	}
	v->delivered_window += window_end - window_start;
	v->delivered_total += exact_end - exact_start;
	if (start_time.isNull()) {
	    if (end_time.isNull()) {
		++v->pending_jobs;
	    } else {
		++v->cancelled_jobs;
	    }
	} else {
            v->mintime = min(v->mintime,start_time.val());
	    if (end_time.isNull()) {
		v->maxtime = max(v->maxtime,event_time.val());
		++v->running_jobs;
	    } else {
		v->maxtime = max(v->maxtime,end_time.val());
		++v->finished_jobs;
	    }
	}
    }

    virtual void printResult()
    {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	printf("Window occupied seconds: %.0f; total seconds: %.0f\n",
	       delivered_window, delivered_total);
	printf("submit-range: %d %d; window-range: %d %d\n",
	       minsubmit, maxend, minwindow, maxwindow);
	for(rollupT::iterator i = rollup.begin(); i != rollup.end();++i) {
	    printf("meta %d, team %s, prod %s, seq %s, shot %s: window %.0f secs, total %.0f secs; frpc %d %d %d %d; username %s; %d .. %d\n",
		   i->metaid,maybehexstring(i->team).c_str(),
		   maybehexstring(i->production).c_str(),
		   maybehexstring(i->sequence).c_str(),
		   maybehexstring(i->shot).c_str(),
		   i->delivered_window, i->delivered_total,
		   i->finished_jobs, i->running_jobs, i->pending_jobs,
		   i->cancelled_jobs, maybehexstring(i->username).c_str(),
		   i->mintime, i->maxtime);
	}
 	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

    const int32_t rollup_start, rollup_end;
    const bool noerstest;
    Int32Field event_time, submit_time, start_time, end_time;
    Variable32Field team, production, sequence, shot, username;
    Int32Field meta_id, user_id;

    int32_t minsubmit, maxend, minwindow, maxwindow;
    uint64_t nrecords;
    double delivered_window, delivered_total;
    uint32_t early_submit, early_start, running_at_end;
};

class MetaId2InfoLookup : public RowAnalysisModule {
public:
    MetaId2InfoLookup(DataSeriesModule &_source, vector<int> &lookuplist)
	: RowAnalysisModule(_source), 
	  meta_id(series,"meta_id"), rr_id(series,"request_id"), 
	  production(series,"production"), sequence(series,"sequence"), 
          shot(series,"shot"), type(series,"type"),
	  owner(series,"owner"), priority(series,"priority")
    {
	for(vector<int>::iterator i = lookuplist.begin();
	    i != lookuplist.end();++i) {
	    lookupents.add(*i);
	}
    }
    struct data {
	int rr_id;
	string production, sequence, shot;
	double priority;
	string type, owner;
    };

    HashUnique<int> lookupents;
    HashMap<int32_t, data> meta_map;

    typedef HashMap<int32_t,data>::iterator iterator;

    virtual void processRow() { 
	if (lookupents.exists(meta_id.val())) {
	    SINVARIANT(production.equal(empty_string) == false);
	    data &v = meta_map[meta_id.val()];
	    INVARIANT(v.production == empty_string, "dup metaid?!");
	    v.rr_id = rr_id.val();
	    v.production = production.stringval();
	    v.sequence = sequence.stringval();
	    v.shot = shot.stringval();
	    v.priority = priority.val();
	    v.type = type.stringval();
	    v.owner = owner.stringval();
	}
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	for(iterator i = meta_map.begin();
	    i != meta_map.end();++i) {
	    data &d = i->second;
	    printf("  metaid %d: rr %d; %s %s %s; %.2f; %s %s\n",
		   i->first, d.rr_id, maybehexstring(d.production).c_str(),
		   maybehexstring(d.sequence).c_str(),
		   maybehexstring(d.shot).c_str(),
		   d.priority,
		   maybehexstring(d.type).c_str(),
		   maybehexstring(d.owner).c_str());
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

    Int32Field meta_id, rr_id;
    Variable32Field production, sequence, shot, type, owner;
    DoubleField priority;
};

class LSFJobTimePredict : public RowAnalysisModule {
public:
  LSFJobTimePredict(DataSeriesModule &_source )
    : RowAnalysisModule(_source),
      sequence(series,"sequence", Field::flag_nullable), 
    shot(series,"shot", Field::flag_nullable), 
    start_time(series, "start_time", Field::flag_nullable), 
    end_time(series, "end_time", Field::flag_nullable) 
  {
    printf("LSFJobTimePredict: Peasant predicts: 5\n") ; 
  }
 
    HashMap<string,int> sequence2seconds_map;

    virtual void processRow() {
	if (start_time.isNull()) return ;  
	int predicted = sequence2seconds_map[sequence.stringval()] ;
	int actual = end_time.val() - start_time.val() ; 
	sequence2seconds_map[sequence.stringval()] = actual ; 
	int error = actual - predicted ;
	double relerror = error / (double)actual ; 
	if ((actual > minCare) || (predicted > minCare)) {
	    accError.add(relerror) ; 
	    if ( false ) {
		printf("Running Time: %d Predicted Time: %d Error: %d Rel Error: %f\n",  actual, predicted, error, relerror)  ; 
	    }
	}
    } 

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	printf("Mean: %f Median: %f Min: %f Max: %f stdev: %f\n", accError.mean(), accError.getQuantile(0.5), accError.min(), accError.max(), accError.stddev()); 
	accError.printFile(stdout, 10); 
	printf("End-%s\n",__PRETTY_FUNCTION__);
	
    }

  Variable32Field sequence, shot; 
  Int32Field start_time, end_time ; 
  StatsQuantile accError ; 
  static const int minCare = 600 ; 
}; 

class HostAvailabilityReport : public RowAnalysisModule {
public:
    HostAvailabilityReport(DataSeriesModule &source)
	: RowAnalysisModule(source, ExtentSeries::typeLoose), 
	total_seconds(0), 
	start_time(series,"start_time", Field::flag_nullable),
	end_time(series,"end_time", Field::flag_nullable), 
	exec_host(series,"exec_host", Field::flag_nullable)
    {
    }

    struct availability {
	vector<int> weekdayHourAvailability; 
	vector<int> weekdayPrevSeconds;
	double weeks_available, total_seconds;
	int prev_interval_start, max_seen_time;
	availability() : weeks_available(0), total_seconds(0),
	    prev_interval_start(0), max_seen_time(0) {
	    weekdayHourAvailability.resize(7*24);
	    weekdayPrevSeconds.resize(7*24);
	}
	int hourused(int day, int hour) {
	    unsigned index = 24*day + hour;
	    SINVARIANT(index >= 0 && index < weekdayHourAvailability.size());
	    return weekdayHourAvailability[index];
	}
	static const int max_separation_seconds = 3*24*3600 + 12*3600; // 3.5 days of no jobs ==> down
	void addUsage(int seconds, int weekday, int hour) { 
	    if (seconds >= prev_interval_start + max_separation_seconds) {
		SINVARIANT(max_seen_time >= prev_interval_start);
		// new interval of availability starting
		if ((max_seen_time + max_separation_seconds) >= seconds) {
		    // continuous intervals
		    max_seen_time = prev_interval_start + max_separation_seconds;
		}
		weeks_available += (max_seen_time - prev_interval_start)/(7.0*24*3600);
		prev_interval_start += max_separation_seconds;
		if (prev_interval_start + max_separation_seconds < seconds) {
		    prev_interval_start = seconds; // skip to new start interval
		}
	    }
	    unsigned offset = weekday * 24 + hour;
	    SINVARIANT(offset >= 0 && offset 
		       <= weekdayHourAvailability.size());
		       
	    if ((weekdayPrevSeconds[offset] + 7200) < seconds) {
		// don't duplicate count hours
		weekdayHourAvailability[offset] += 1;
		weekdayPrevSeconds[offset] = seconds;
	    }
	    if (seconds > max_seen_time) {
		max_seen_time = seconds;
	    }
	}
    };

    typedef HashMap<string, availability> availabilityMapT;
    availabilityMapT host2availability;

    virtual void processRow() {
	if (exec_host.isNull() || start_time.isNull() || end_time.isNull())
	    return;
	availability &d = host2availability[exec_host.stringval()];
	d.total_seconds += end_time.val() - start_time.val();
	SINVARIANT(d.weeks_available >= 0);
	
	struct tm ltime;
	time_t start = 3600 * (start_time.val() / 3600);
	SINVARIANT(localtime_r(&start,&ltime) == &ltime);
	int cur_weekday = ltime.tm_wday;
	int cur_hour = ltime.tm_hour;
	SINVARIANT(ltime.tm_min == 0 && ltime.tm_sec == 0);
	for(;start <= end_time.val(); start += 3600) {
	    d.addUsage(start,cur_weekday,cur_hour);
	    cur_hour += 1;
	    if (cur_hour == 24) {
		cur_hour = 0;
		cur_weekday += 1;
		if (cur_weekday == 7)
		    cur_weekday = 0;
	    }
	}
	SINVARIANT(d.weeks_available >= 0 && d.weeks_available < 1000);
    }

    void resultHourDetail(availability &d) {
	double min_available_weeks = floor(d.weeks_available * 0.8);
	int hours_available = 0;
	for(int i=0;i<7*24;++i) {
	    if ((i % 24) == 0) {
		if (i > 0) {
		    printf(" (%d ha)",hours_available);
		    hours_available = 0;
		}
		printf("\n  ");
	    }
	    if (d.weekdayHourAvailability[i] >= min_available_weeks) {
		++hours_available;
	    }
	    printf("%3d ",d.weekdayHourAvailability[i]);
	}
	printf(" (%d ha)\n",hours_available);
    }

    static const bool host_verbose = true;

    int m2fdisabled9to6(availability &d) {
	int sumhour12to6 = 0;
	for(int day=1;day<=5;++day) { // m-f
	    for(int hour=12;hour < 18; ++hour) {
		sumhour12to6 += d.hourused(day,hour);
	    }
	}
	if (sumhour12to6/(5*6.0) < 0.1 * d.weeks_available) {
	    int latepart = 0;
	    for(int day = 1;day <= 5; ++day) {
		for(int i=18;i<24;++i) {
		    latepart += d.hourused(day,i);
		}
	    }
	    
	    // try to separate the m-f region into two pieces; range for off is 
	    // separationbyhour .. 18
	    int offsum[18];
	    int onsum[18];
	    for(int i=0;i<18;++i) {
		offsum[i] = latepart;
		onsum[i] = 0;
	    }
	    for(int day = 1; day <= 5; ++day) {
		for(int sephour = 0; sephour < 18; ++sephour) {
		    int hoursum = 0;
		    for(int hour=0;hour < sephour;++hour) {
			hoursum += d.hourused(day,hour);
		    }
		    offsum[sephour] += hoursum;
		    hoursum = 0;
		    for(int hour = sephour;hour < 18; ++hour) {
			hoursum += d.hourused(day,hour);
		    }
		    onsum[sephour] += hoursum;
		}
	    }
	    double maxval = 0; int maxat = 0;
	    for(int i=0;i<18;++i) {
		double on = onsum[i]/(double)(18-i);
		double off = offsum[i]/(double)(i+6);
		if ((off - on) > maxval) {
		    maxval = off - on;
		    maxat = i;
		}
	    }
	    if (maxat >= 15 || maxat <= 5) {
		for(int i=10;i<18;++i) {
		    double on = onsum[i]/(double)(18-i);
		    double off = offsum[i]/(double)(i+6);
		    if (host_verbose) 
			printf("(%d %.2f - %.2f = %.2f) ",i,off,on,off-on);
		}
		return -1;
	    }
		
	    INVARIANT(maxat > 5 && maxat < 15, 
		      format("invalid maxat %d") % maxat);
	    if (maxat >= 6 && maxat <= 9)
		return maxat;

	    for(int i=maxat-1;i<=maxat+1;++i) {
		double on = onsum[i]/(double)(18-i);
		double off = offsum[i]/(double)(i+6);
		if (host_verbose)
		    printf("(%d %.2f - %.2f = %.2f) ",i,off,on,off-on);
	    }
	    return maxat;
	}
	return -1;
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	StatsQuantile week_avail;
	for(availabilityMapT::iterator i = host2availability.begin();
	    i != host2availability.end();++i) {
	    availability &d = i->second;
	    SINVARIANT(d.weeks_available >= 0 && d.weeks_available < 1000);
	    d.weeks_available += (d.max_seen_time - d.prev_interval_start)/(7.0*24*3600);
	    SINVARIANT(d.weeks_available >= 0 && d.weeks_available < 1000);
	    SINVARIANT(d.max_seen_time >= d.prev_interval_start);
	    week_avail.add(d.weeks_available);
	}

	double low_avail = week_avail.getQuantile(0.05);
	//	double high_avail = week_avail.getQuantile(0.50);
	int host_count = 0;
	double weeks_always_available = 0;
	int hosts_always_available = 0;
	double weeks_partial = 0;
	int hosts_partial = 0;
	int hosts_unavail = 0;
	double weeks_unknown_25 = 0;
	double weeks_unknown_50 = 0;
	int hosts_unknown = 0;
	double total_seconds = 0;
	double total_seconds_always_available = 0;
	

	for(availabilityMapT::iterator i = host2availability.begin();
	    i != host2availability.end();++i) {
	    ++host_count;
	    availability &d = i->second;
	    total_seconds += d.total_seconds;
	    if (host_verbose) 
		printf("host %s %.2f weeks %.2f week-seconds:",i->first.c_str(),d.weeks_available,
		       d.total_seconds / (7*24*3600.0));
	    if (d.weeks_available < low_avail) {
		++hosts_unavail;
		if (host_verbose) printf(" *not-available*\n");
	    } else if (d.weeks_available >= low_avail) {
		int min_hours_avail = 24 * 3/4;
		int days_min_hours_avail = 0;
		double min_available_weeks = floor(d.weeks_available * 0.8);
		StatsQuantile used_quantile(0.001,200);
		for(int day = 0;day<7;++day) {
		    int hours_available = 0;
		    for(int hour = 0;hour < 24;++hour) {
			int index = day * 24 + hour;
			used_quantile.add(d.weekdayHourAvailability[index]);
			if (d.weekdayHourAvailability[index] >= min_available_weeks) {
			    ++hours_available;
			}
		    }
		    if (hours_available >= min_hours_avail) {
			days_min_hours_avail += 1;
		    }
		}
		if (days_min_hours_avail >= 4) {
		    if (host_verbose)
			printf(" *always-available* %.2f 25th %.2f 50th\n",
			       used_quantile.getQuantile(0.25),used_quantile.getQuantile(0.5));
		    ++hosts_always_available;
		    weeks_always_available += d.weeks_available;
		    total_seconds_always_available += d.total_seconds;
		} else if (used_quantile.getQuantile(0.75) < min_available_weeks / 3.0) {
		    if (host_verbose) printf(" *not-available*\n");
		    ++hosts_unavail;
		} else {
		    int startdisable = m2fdisabled9to6(d);
		    if (startdisable >= 6) {
			weeks_partial += d.weeks_available;
			++hosts_partial;
			if (host_verbose)
			    printf(" *m-f disabled %d .. %d\n",startdisable,18);
		    } else if (used_quantile.getQuantile(0.5) >= min_available_weeks &&
			       used_quantile.getQuantile(0.25) >= min_available_weeks * 0.75) {
			if (host_verbose) printf(" *always-available-enough*\n");
			++hosts_always_available;
			weeks_always_available += d.weeks_available;
			total_seconds_always_available += d.total_seconds;
		    } else {
			++hosts_unknown;
			weeks_unknown_25 += used_quantile.getQuantile(0.25);
			weeks_unknown_50 += used_quantile.getQuantile(0.50);
			if (host_verbose) {
			    printf(" unknown-host (%d do75 %.2f 25th %.2f 50th %.2f 75th, %.2f maw)",days_min_hours_avail,
				   used_quantile.getQuantile(0.25),used_quantile.getQuantile(0.5),
				   used_quantile.getQuantile(0.75),
				   min_available_weeks);
			    resultHourDetail(d);
			}
		    }
		}
	    } else {
		FATAL_ERROR("bad");
	    }
	}
	printf("summary: %d hosts; %d always, %d partial, %d unavail, %d unknown\n",
	       host_count, hosts_always_available, hosts_partial, hosts_unavail, hosts_unknown);
	// partial weeks are assumed to be 12 hours of computation available, e.g. 6am..6pm is 
	// unavailable
	double weeks_partial_effective = weeks_partial * (2*24 + 5 * 12)/(7.0*24);
	printf("summary: %.2f weeks_always, %.2f weeks_partial, %.2f effective_weeks_partial\n",
	       weeks_always_available, weeks_partial, weeks_partial_effective);
	printf("summary: %.2f/%.2f weeks_unknown_25/50, \n",
	       weeks_unknown_25, weeks_unknown_50);
	double cpu_years_total = 2.0 * (weeks_always_available + weeks_partial_effective + weeks_unknown_25) * 7 / 365.25;
	double cpu_years_actual = total_seconds / (365.25 * 24 * 60 * 60.0);
	printf("summary: %.2f cpu years available; %.2f cpu years acutal; %.2f%% used\n",
	       cpu_years_total, cpu_years_actual, 100.0 * cpu_years_actual / cpu_years_total);
	double cpu_years_total_always_available = 2.0 * weeks_always_available * 7 / 365.25;
	double cpu_years_actual_always_available = total_seconds_always_available / (365.25 * 24 * 60 * 60.0);
	printf("summary: always-available: %.2f cpu years available; %.2f cpu years acutal; %.2f%% used\n",
	       cpu_years_total_always_available, cpu_years_actual_always_available, 
	       100.0 * cpu_years_actual_always_available / cpu_years_total_always_available);

	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

    double total_seconds;
    Int32Field start_time, end_time;
    Variable32Field exec_host;
};

class ComplexityAnalysis : public RowAnalysisModule {
public:
  ComplexityAnalysis(DataSeriesModule &_source )
    : RowAnalysisModule(_source, ExtentSeries::typeLoose),
      production(series,"production", Field::flag_nullable),
      sequence(series,"sequence", Field::flag_nullable), 
      shot(series,"shot", Field::flag_nullable), 
      command(series, "command"),
      job_resolution(series, "job_resolution", Field::flag_nullable),
      start_time(series, "start_time", Field::flag_nullable), 
      end_time(series, "end_time", Field::flag_nullable) ,
      user_id(series, "user_id", Field::flag_nullable)
  {
  
  }
 
  struct hte {
	
    string production, sequence, shot;
    StatsQuantile *frametime;
	
  };

  struct hteHash {
    unsigned operator()(const hte &a) const {
      unsigned tmp = lintel::hashBytes(a.production.data(),a.production.size(),1972);
      tmp = lintel::hashBytes(a.sequence.data(),a.sequence.size());
      return lintel::hashBytes(a.shot.data(),a.shot.size(),tmp);
    }
  };

  struct hteEqual {
    bool operator()(const hte &a, const hte &b) const {
      return  a.shot == b.shot 
	&& a.sequence == b.sequence && a.production == b.production ;
    }
  };

  typedef HashTable<hte, hteHash, hteEqual> rollupT;

  rollupT rollup;
    
  virtual void processRow() {
    if (start_time.isNull() || production.isNull() || sequence.isNull() || shot.isNull() || end_time.isNull() || user_id.val() == 12271) return ;
    
    if (job_resolution.val() != 1) return;
   
    int actual = end_time.val() - start_time.val() ;  
    hte k;
    k.production = production.stringval();
    k.sequence = sequence.stringval();
    k.shot = shot.stringval();
    hte *v = rollup.lookup(k);
    if (v == NULL) {
      k.frametime = new StatsQuantile(0.01,10000000);
      v = rollup.add(k);
    };
    v -> frametime -> add(actual);
    if (false){
      printf("Count %ld \t Median: %f\n",v->frametime -> count(), v-> frametime -> getQuantile(0.5));
    }
  }

 

    virtual void printResult() {
        rollupT seqrollup;
        
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	printf("Production Sequence Shot FrameCount 10p 20p 30p 40p 50p 60p 70p 80p 90p\n");
	for (rollupT::iterator i = rollup.begin() ; i!= rollup.end(); ++i){
	  if (true){
	    printf("%s %s %s %ld %f %f %f %f %f %f %f %f %f\n",
		   maybehexstring(i -> production).c_str(),
		   maybehexstring(i -> sequence).c_str(),
		   maybehexstring(i -> shot).c_str(),
		   i->frametime -> count(), 
		   i-> frametime -> getQuantile(0.1),
		   i-> frametime -> getQuantile(0.2),
		   i-> frametime -> getQuantile(0.3),
		   i-> frametime -> getQuantile(0.4),
		   i-> frametime -> getQuantile(0.5),
		   i-> frametime -> getQuantile(0.6),
		   i-> frametime -> getQuantile(0.7),
		   i-> frametime -> getQuantile(0.8),
		   i-> frametime -> getQuantile(0.9)
		   );
	 }
	  if (false){
	    hte k = *i;
	    k.shot.clear();
	    k.frametime = NULL;
	    hte *v = seqrollup.lookup(k);
	    if (v == NULL) {
	      k.frametime = new StatsQuantile;
	      v = seqrollup.add(k);
	    };
	    v -> frametime -> add(i -> frametime -> getQuantile(0.9));
	  }
	}

	printf("------------------------------\n");
	if (false){
	  for (rollupT::iterator i = seqrollup.begin() ; i!= seqrollup.end(); ++i){
	    if (false){
	      printf(" %s / %s: %ld \t  %f\n",
		     maybehexstring(i -> production).c_str(),
		     maybehexstring(i -> sequence).c_str(),
		     i->frametime -> count(), i-> frametime -> mean());
	    }
	  }
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
	
    }

  Variable32Field production, sequence, shot, command; 
  DoubleField job_resolution;
  Int32Field start_time, end_time, user_id; 
}; 


static const string str_done("done");
static const string str_DONE("DONE"); 
class LSFJobStats : public RowAnalysisModule {
public:
    LSFJobStats(DataSeriesModule &_source)
	: RowAnalysisModule(_source),
	  wall_time(0.00001), // make wall_time more precise at cost of memory
	  status(series, "status"), 
	  submit_time(series, "submit_time"),
	  start_time(series, "start_time", Field::flag_nullable), 
	  end_time(series, "end_time", Field::flag_nullable),
	  cpu_time(series, "cpu_time"),
	  max_memory(series, "max_memory", Field::flag_nullable),
	  bumped_wall_time_count(0), skipped_short_job_count(0),
	  negative_cpu_time_count(0), high_cpu_frac_count(0)
    {
    }

    static const int max_cpu_frac = 2;
    static const bool sujoy_implementation = false;
    static const bool eric_implementation = true;
    virtual void processRow() {
	if (sujoy_implementation) {
	    if (start_time.isNull()) return;  
	    if (end_time.isNull()) return;  
	    if (status.equal(str_done) || status.equal(str_DONE)) {
		int wtime = end_time.val() - start_time.val(); 
		if  (wtime > 7 * 24 * 3600) 
		    return ; //at most 10 min,  at most 7 days 
		wall_time.add(wtime);
	    }
	}
	if (eric_implementation) {
	    if (start_time.isNull()) return;  
	    submit_delay.add(start_time.val() - submit_time.val());
	    if (end_time.isNull()) return;
	    wall_time.add(end_time.val() - start_time.val());
	    double wall_elapsed = end_time.val() - start_time.val();
	    INVARIANT(wall_elapsed >= 0, format
		      ("error end_time <= start_time! %.2f") % wall_elapsed);
	    if (cpu_time.val() < 0) {
		++negative_cpu_time_count;
	    } else {
		if (wall_elapsed == 0) {
		    if (cpu_time.val() < 0.01) {
			++skipped_short_job_count;
			return;
		    }
		    INVARIANT(cpu_time.val() < 2, format
			      ("whoa, zero wall, %.2f cpu") % cpu_time.val());
		    wall_elapsed = cpu_time.val();
		    ++bumped_wall_time_count;
		}

		if (max_memory.isNull() == false) {
		    memory_used.add(max_memory.val()/(1024.0*1024.0));
		}
		cpu_used.add(cpu_time.val());
		double x_frac = cpu_time.val() / wall_elapsed;
		if (x_frac > max_cpu_frac) {
		    x_frac = max_cpu_frac;
		    ++high_cpu_frac_count;
		}
		cpu_frac.add(x_frac);
		if (wall_elapsed >= 10*60) {
		    cpu_frac_10minplusjobs.add(x_frac);
		}
	    }
	}
    } 

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	if (eric_implementation) {
	    printf("submit_delay: ");
	    submit_delay.printFile(stdout,20);
	    submit_delay.printTail(stdout);
	    printf("cpu_frac (%d tiny skipped, %d bump wall time, %d high cpu): ",
		   skipped_short_job_count,bumped_wall_time_count, high_cpu_frac_count);
	    cpu_frac.printFile(stdout,20);
	    cpu_frac.printTail(stdout);
	    printf("cpu_frac for jobs with more than 10 minutes of wall time: ");
	    cpu_frac_10minplusjobs.printFile(stdout,20);
	    cpu_frac_10minplusjobs.printTail(stdout);
	    printf("cpu_used (%d negative skipped): ",negative_cpu_time_count);
	    cpu_used.printFile(stdout,20);
	    cpu_used.printTail(stdout);
	    printf("memory_used in MB: ");
	    memory_used.printFile(stdout,20);
	    memory_used.printTail(stdout);
	}
	if (eric_implementation || sujoy_implementation) {
	    printf("wall_time: ");
	    wall_time.printFile(stdout,10);
	    wall_time.printTail(stdout);
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

    StatsQuantile submit_delay, wall_time, cpu_frac, cpu_used, cpu_frac_10minplusjobs, memory_used;
    
    Variable32Field status;
    Int32Field submit_time, start_time, end_time;
    DoubleField cpu_time; 
    Int64Field max_memory;
    int bumped_wall_time_count, skipped_short_job_count;
    int negative_cpu_time_count, high_cpu_frac_count;
}; 

class LSFSlowJobs : public RowAnalysisModule {
public:
    LSFSlowJobs(DataSeriesModule &_source, bool _show_finished)
	: RowAnalysisModule(_source),
	  show_finished(_show_finished),
	  submit_time(series, "submit_time"),
	  start_time(series, "start_time", Field::flag_nullable), 
	  end_time(series, "end_time", Field::flag_nullable),
	  event_time(series, "event_time"),
	  job_id(series,"job_id"),
	  job_idx(series,"job_idx", Field::flag_nullable),
	  meta_id(series,"meta_id", Field::flag_nullable),
	  cpu_time(series, "cpu_time"),
	  system_time(series, "system_time"),
	  username(series, "username"),
	  exec_host(series, "exec_host", Field::flag_nullable),
	  production(series, "production", Field::flag_nullable),
	  sequence(series, "sequence", Field::flag_nullable),
	  shot(series, "shot", Field::flag_nullable),
	  jobs_examined(0), jobs_started(0), jobs_finished(0), jobs_slow(0)
    {
    }

    virtual void processRow() {
	++jobs_examined;
	if (start_time.isNull())
	    return; // job hasn't started, don't care
	++jobs_started;
	if (end_time.isNull() == false) {
	    ++jobs_finished;
	    if (show_finished == false)
	      return; // job is done, don't care
	}
	int wall_seconds = event_time.val() - start_time.val();
	if (wall_seconds < 15*60) 
	    return; // too short, don't care
	double cpu_used = cpu_time.val() + system_time.val();
	double efficiency = 100 * cpu_used / (double)wall_seconds;
	if (efficiency > 80) 
	    return; // running pretty good

	++jobs_slow;
	time_t curtime = event_time.val();
	char buf[50];
	ctime_r(&curtime,buf);
	buf[strlen(buf)-1] = '\0';
	printf("%d[%d] metaid %d on %s for user %s, %s/%s/%s running for %.2f minutes, %.2f cpu minutes, %.2f%% efficient; %s %s\n",
	       job_id.val(),job_idx.val(),meta_id.val(),exec_host.stringval().c_str(),
	       maybehexstring(username.stringval()).c_str(),
	       maybehexstring(production.stringval()).c_str(),
	       maybehexstring(sequence.stringval()).c_str(),
	       maybehexstring(shot.stringval()).c_str(),
	       (double)wall_seconds/60.0,cpu_used/60.0,
	       efficiency,
	       end_time.isNull() ? "last update" : "finished at",
	       buf);
    }

    virtual void printResult() {
	printf("%d jobs examined, %d jobs started, %d jobs finished, %d jobs slow\n",
	       jobs_examined, jobs_started, jobs_finished, jobs_slow);
    }
    
    const bool show_finished;
    Int32Field submit_time, start_time, end_time, event_time, job_id, job_idx, meta_id; 
    DoubleField cpu_time, system_time;
    Variable32Field username, exec_host, production, sequence, shot;
    int jobs_examined, jobs_started, jobs_finished, jobs_slow;
}; 

class HostCalibration : public RowAnalysisModule {
public:
    HostCalibration(DataSeriesModule &_source, const string &_reference_host)
	: RowAnalysisModule(_source),
	  reference_host(_reference_host),
	  start_time(series, "start_time", Field::flag_nullable), 
	  end_time(series, "end_time", Field::flag_nullable),
	  job_id(series, "job_id"),
	  job_index(series, "job_idx", Field::flag_nullable),
	  cluster_name(series, "cluster_name"),
	  exec_host(series, "exec_host")
    {
    }

    HashMap<string,int> cluster2id;
    HashMap<string,int> exec_host2id;

    struct jobinfo {
	int host_id, wall_seconds;
	jobinfo(int a, int b) : host_id(a), wall_seconds(b) { }
	jobinfo() : host_id(0), wall_seconds(0) { }
    };

    struct hte {
	int job_id, cluster_id, last_end;
	vector<jobinfo> jobs;
    };

    struct hteHash {
	unsigned operator()(const hte &ent) const {
	    unsigned a = ent.job_id;
	    unsigned b = ent.cluster_id;
	    unsigned ret = 1972;
	    lintel::BobJenkinsHashMix3(a,b,ret);
	    return ret;
	}
    };

    struct hteEqual {
	bool operator()(const hte &a, const hte &b) const {
	    return a.job_id == b.job_id && a.cluster_id == b.cluster_id;
	}
    };

    typedef HashTable<hte,hteHash,hteEqual> jobDataT;

    jobDataT jobData;

    int getId(HashMap<string,int> &map, const string &instr) {
	int ret = map[instr];
	if (ret == 0) {
	    ret = map.size() + 1;
	    map[instr] = ret;
	}
	return ret;
    }

    virtual void processRow() {
	if (start_time.isNull() || end_time.isNull() || job_index.isNull())
	    return; // job hasn't started or finished, or is a singleton, don't care

	int host_id = getId(exec_host2id,exec_host.stringval());
	int cluster_id = getId(cluster2id,cluster_name.stringval());
	
	hte k;
	k.job_id = job_id.val();
	k.cluster_id = cluster_id;
	
	hte *v = jobData.lookup(k);
	if (v == NULL) {
	    k.last_end = 0;
	    v = jobData.add(k);
	}
	if (end_time.val() > k.last_end) {
	    k.last_end = end_time.val();
	}
	if ((int)v->jobs.size() <= job_index.val()) {
	    v->jobs.resize(job_index.val()+1);
	}
	v->jobs[job_index.val()].host_id = host_id;
	v->jobs[job_index.val()].wall_seconds = end_time.val() - start_time.val();
    }

    virtual void printResult() {
    }
    
    const string reference_host;
    Int32Field start_time, end_time, job_id, job_index;
    Variable32Field cluster_name, exec_host;
}; 

class BestEffort : public RowAnalysisModule {
public:
    static const bool debug_pq = false;
    static const bool debug = false;
    struct BEInfo {
	int32_t starttime;
	int32_t reserved_cpus;
	int32_t best_effort_cpus;
    };

    BestEffort(DataSeriesModule &_source, 
	       int32_t _window_start, int32_t _window_end,
	       vector<BestEffort::BEInfo> &_reserved)
	: RowAnalysisModule(_source), 
	  window_start(_window_start), window_end(_window_end),
	  reserved(_reserved),
	  cluster_name(series, "cluster_name"),
	  event_time(series, "event_time"), 
	  start_time(series, "start_time", Field::flag_nullable), 
	  end_time(series, "end_time", Field::flag_nullable),
	  job_id(series,"job_id"), job_idx(series,"job_idx", Field::flag_nullable), 
	  user_id(series,"user_id"),
	  str_ers("ers"), ers_job_count(0), non_ers_job_count(0),
	  out_of_window_jobs(0), zero_length_jobs(0), ers_test_job(0)
    {
	SINVARIANT(reserved.empty() == false);
	jcTrans.push_back("Unknown");
	jcTrans.push_back("Reserved");
	jcTrans.push_back("BestEffort");
	jcTrans.push_back("Excess");
    }

    enum jobClass { jcUnknown = 0, jcReserved, jcBestEffort, jcExcess };

    struct jobEnt {
	int32_t starttime, endtime, job_id, job_idx;
	jobClass jobclass;

	jobEnt(int32_t a, int32_t b, int32_t c, int32_t d) 
	    : starttime(a), endtime(b), job_id(c), job_idx(d), jobclass(jcUnknown) { }
    };

    struct jobEntByStart {
	bool operator()(const jobEnt *a, const jobEnt *b) const {
	    return a->starttime >= b->starttime;
	}
    };

    struct jobEntByFinish {
	bool operator()(const jobEnt *a, const jobEnt *b) const {
	    return a->endtime >= b->endtime;
	}
    };

    static const int wrangler_uid = 403;

    virtual void processRow() {
	if (start_time.isNull())
	    return;
	if (ersTestJob(user_id.val())) {
	    ++ers_test_job;
	    return;
	}
	if (wrangler_uid != user_id.val()) {
	    fprintf(stderr,"warning, job %d[%d] is not a wrangler uid but %d\n",
		    job_id.val(), job_idx.val(), user_id.val());
	}
	if (cluster_name.equal(str_ers)) {
	    int32_t job_end_time = end_time.isNull() ? event_time.val() : end_time.val();
	    ++ers_job_count;
	    jobEnt *tmp = new jobEnt(start_time.val(),job_end_time,job_id.val(),job_idx.val());
	    if (tmp->endtime < window_start || tmp->starttime > window_end) {
		++out_of_window_jobs;
		delete tmp;
	    } else if (tmp->endtime == tmp->starttime) {
		++zero_length_jobs;
		delete tmp;
	    } else {
		// HACK: ignore the bits of the job that are before or
		// after the window boundary, better would be to fix
		// up new partial jobs made of splitting any running
		// jobs when we cross any of the time boundaries
		tmp->starttime = max(window_start,tmp->starttime);
		tmp->endtime = min(window_end,tmp->endtime);
		starts.push(tmp);
		finishes.push(tmp);
	    }
	} else {
	    ++non_ers_job_count;
	}
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	if (starts.size() == 0) {
	    printf("No ERS jobs found\n");
	} else {
	    int32_t cur_reserved_cap = 0, cur_best_effort_cap = 0;
	    int32_t cur_reserved_jobs = 0, cur_best_effort_jobs = 0;
	    
	    int64_t reserved_seconds = 0, best_effort_seconds = 0, excess_seconds = 0;
	    int32_t max_reserved = 0, max_best_effort = 0;
	    
	    int32_t base_time = starts.top()->starttime;
	    if (debug) {
		printf("base time %d\n",base_time);
	    }
	    while(starts.empty() == false || finishes.empty() == false) {
		if (reserved.empty() == false) {
		    int32_t mintime = min(starts.top()->starttime,
					   finishes.top()->endtime);
		    if (mintime >= reserved.front().starttime) {
			cur_reserved_cap = reserved.front().reserved_cpus;
			cur_best_effort_cap = reserved.front().best_effort_cpus;
			reserved.erase(reserved.begin());
			if (cur_reserved_jobs > cur_reserved_cap) {
			    fprintf(stderr,"warning, not correctly handling drop in reserved transitioning to best effort\n");
			}
			if (cur_best_effort_jobs > cur_best_effort_cap) {
			    fprintf(stderr,"warning, too many best effort jobs, incorrectly charging for them\n");
			}
		    }
		}
		
		if (debug_pq) {
		    printf("tops: %d %d\n",starts.empty() ? -1 : starts.top()->starttime - base_time,
			   finishes.top()->endtime - base_time);
		}
		if (starts.empty() == false &&
		    starts.top()->starttime < finishes.top()->endtime) {
		    // start is strictly first, prefer to end jobs
		    jobEnt *tmp = starts.top();
		    SINVARIANT(tmp->jobclass == jcUnknown);
		    if (cur_reserved_jobs < cur_reserved_cap) {
			tmp->jobclass = jcReserved;
			++cur_reserved_jobs;
			if (debug) {
			    printf("  start %d.%d at %d as reserved (%d total)\n",
				   tmp->job_id,tmp->job_idx,tmp->starttime - base_time,
				   cur_reserved_jobs);
			}
		    } else if (cur_best_effort_jobs < cur_best_effort_cap) {
			tmp->jobclass = jcBestEffort;
			++cur_best_effort_jobs;
			if (debug) {
			    printf("  start %d.%d at %d as best effort (%d total)\n",
				   tmp->job_id,tmp->job_idx,tmp->starttime - base_time,
				   cur_best_effort_jobs);
			}
		    } else {
			if (debug) {
			    printf("  start %d.%d at %d as excess (%d/%d)\n",
				   tmp->job_id,tmp->job_idx,tmp->starttime - base_time,
				   cur_reserved_jobs,cur_best_effort_jobs);
			}
			tmp->jobclass = jcExcess;
		    }
		    starts.pop();
		} else {
		    // finish is first
		    jobEnt *tmp = finishes.top();
		    SINVARIANT(tmp->jobclass != jcUnknown);
		    int32_t window_job_start = max(window_start,tmp->starttime);
		    int32_t window_job_end = min(window_end,tmp->endtime);
		    SINVARIANT(window_job_start <= window_job_end);
		    max_reserved = max(max_reserved, cur_reserved_jobs);
		    max_best_effort = max(max_best_effort, cur_best_effort_jobs);
		    if (tmp->jobclass == jcReserved) {
			--cur_reserved_jobs;
			reserved_seconds += window_job_end - window_job_start;
			if (debug) {
			    cout << format("  finish %d.%d at %d as reserved (%d total) %d cum secs\n")
				% tmp->job_id % tmp->job_idx % (tmp->endtime - base_time)
				% cur_reserved_jobs % reserved_seconds;
			}
		    } else if (tmp->jobclass == jcBestEffort) {
			--cur_best_effort_jobs;
			best_effort_seconds += window_job_end - window_job_start;
			if (debug) {
			    cout << format("  finish %d.%d at %d as best_effort (%d total) %lld cum secs\n")
				% tmp->job_id % tmp->job_idx % (tmp->endtime - base_time)
				% cur_best_effort_jobs % best_effort_seconds;
			}
		    } else if (tmp->jobclass == jcExcess) {
			excess_seconds += window_job_end - window_job_start;
			if (debug) {
			    cout << format("  finish %d.%d at %d as excess (%lld cum secs)\n")
				% tmp->job_id % tmp->job_idx 
				% (tmp->endtime - base_time) % excess_seconds;
			}
		    } else {
			FATAL_ERROR("internal");
		    }
		    finishes.pop();
		    delete tmp;
		}
	    }
	    SINVARIANT(cur_reserved_jobs == cur_best_effort_jobs &&
		       cur_best_effort_jobs == 0);
	    printf("window was %d .. %d\n",window_start,window_end);
	    printf("job counts: non-ers=%d, ers=%d, out-of-window-ers=%d, zero-length-ers=%d, ers-test=%d\n",
		   non_ers_job_count, ers_job_count, out_of_window_jobs, zero_length_jobs,ers_test_job);
	    cout << format("usage-seconds: reserved=%d, best-effort=%d, excess=%d\n")
		% reserved_seconds % best_effort_seconds % excess_seconds;
	}
			
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

    vector<string> jcTrans;
    int32_t window_start, window_end;
    vector<BEInfo> reserved;
    PriorityQueue<jobEnt *, jobEntByStart> starts;
    PriorityQueue<jobEnt *, jobEntByFinish> finishes;
    Variable32Field cluster_name;
    Int32Field event_time, start_time, end_time, job_id, job_idx, user_id;
    const string str_ers;
    int32_t ers_job_count, non_ers_job_count, out_of_window_jobs, zero_length_jobs, ers_test_job;
};

// static vector<int> find_metaid_list;
static int job_report_start, job_report_end;
static bool job_report_noerstest;
static bool lsfslowjobs_show_finished = false;
static string hostcalibration_refhost;
static int32_t best_effort_window_start, best_effort_window_end;
static vector<BestEffort::BEInfo> best_effort_reserved;

void
usage(char *argv[])
{
    cerr << "Usage: " << argv[0] << " [flags] (file-list+) or (index-file min_time max_time [cluster])\n";
    cerr << "   Note that the index form won't pick up any of the rr/rj ds files, so\n";
    cerr << "   those analysis won't work.\n";
    cerr << " flags: (default is to run all analysis)\n";
    cerr << " -a <granularity>:<start-secs>:<end-secs>:<rollup-groups> # FarmLoad analysis\n";
    cerr << "    rollup-groups is * or group,group,... where group is: all, production\n";
    cerr << "    sequence, team, queue, hostgroup, team_group, cluster, username, exechost\n";
    cerr << "    the group erstest will cause ers testing jobs to be included\n";
    cerr << " -b <metaid> # trace a meta-id; valid to specify multiple times\n";
    cerr << " -c <start-secs>:<end-secs> # Job report analysis\n";
//    cerr << " -d <rrid> | T:<team> | U:<user-id> | [AW]:<start-secs>:<end-secs> # render request lookup\n";
//    cerr << "    # will also find associated metaids, W spec constrains Team/User searches\n";
//    cerr << "    # options rrid, T:, U: are unioned together, and then intersected by W:\n";
//    cerr << "    # if only A: is given, all entries in the window are shown\n";
//    cerr << "    # default window is current-time - 1 day .. now\n";
    cerr << " -e # production report\n";
    cerr << " -f <meta-id> # meta-id to info lookup\n";
    cerr << " -g # execution time prediction \n" ; 
    cerr << " -h # help message\n";
    cerr << " -i # host availability report\n";
    cerr << " -j # Complexity analysis\n";
    cerr << " -k # job stats\n";
    cerr << " -l (yes|no -- show finished jobs) # slow lsf job report\n";
    cerr << " -m <reference-host> # Host calibration\n";
    cerr << " -n window_start:window_end:(purchase_start,reserved_cpus,best_effort_cpus)+\n";
    cerr << " -[b,d] operate on the r[rj].ds files, the rest operate on the lsf files\n";
    exit(1);
}

static vector<LSFDSAnalysisMod::FarmLoadArgs *> farm_load_args;

int
parseopts(int argc, char *argv[])
{
    bool any_selected;

    any_selected = false;
    while (1) {
	int opt = getopt(argc, argv, "a:b:c:d:ef:ghijkl:m:n:");
	if (opt == -1) break;
	switch(opt){
	case 'a': {
	    any_selected = true;
	    farm_load_args.push_back(LSFDSAnalysisMod::handleFarmLoadArgs(optarg));
	    break;
	}
	case 'b': {
	    FATAL_ERROR("disabled until tested and added to regression suite");
	    LSFDSAnalysisMod::handleTraceMetaIdArgs(optarg);
	    options[optTraceMetaId] = true;
	    break;
	}
	case 'c': {
	    FATAL_ERROR("disabled until tested and added to regression suite");
	    vector<string> opts;
	    split(optarg,":",opts);
	    INVARIANT(opts.size() == 3, "invalid option to -c,"
		      " expect <start-secs>:<end-secs>:[erstest]");
	    job_report_start = atoi(opts[0].c_str());
	    job_report_end = atoi(opts[1].c_str());
	    if (opts[2] == "erstest") {
		job_report_noerstest = false;
	    } else {
		INVARIANT(opts[2] == "",
			  format("unrecognized option '%s'") % opts[2]);
	    }
	    INVARIANT(job_report_end > job_report_start,
		      format("invalid option to -c, end(%d) <= start(%d)")
		      % job_report_end % job_report_start);
	    options[optJobReport] = true;
	    break;
	}
//	case 'd': {
//	    LSFDSAnalysisMod::handleRenderRequestLookupArgs(optarg);
//	    break;
//	}
	case 'e': {
	    FATAL_ERROR("disabled until tested and added to regression suite");
	    options[optProductionReport] = true;
	    break;
	}
//	case 'f': {
//	    int meta_id = atoi(optarg);
//	    INVARIANT(meta_id > 0, format("bad option %s") % optarg);
//	    find_metaid_list.push_back(meta_id);
//	    break;
//	}
	case 'g': {
	    FATAL_ERROR("disabled until tested and added to regression suite");
	    options[optLSFJobTimePredict] = true;
	    break;
	}
	case 'h': {
	    usage(argv);
	    exit(1);
	}
	case 'i': {
	    FATAL_ERROR("disabled until tested and added to regression suite");
	    options[optHostAvailabilityReport] = true;
	    break;
	}
	case 'j': {
	    FATAL_ERROR("disabled until tested and added to regression suite");
	    options[optComplexityAnalysis] = true;
	    break;
	}
	case 'k': {
	    FATAL_ERROR("disabled until tested and added to regression suite");
	    options[optLSFJobStats] = true;
	    break;
	}
	case 'l': {
	    FATAL_ERROR("disabled until tested and added to regression suite");
	    options[optLSFSlowJobs] = true;
	    vector<string> opts;
	    split(optarg,":",opts);
	    INVARIANT(opts.size() == 1, 
		      "invalid options to -l, expect [yes|no]");
	    if (opts[0] == "yes") {
		lsfslowjobs_show_finished = true;
	    } else if (opts[0] == "no") {
		lsfslowjobs_show_finished = false;
	    } else {
		FATAL_ERROR(format("expect [yes|no] to -l , not '%s'")
			    % opts[0]);
	    }
	    break;
	}
	case 'm': {
	    FATAL_ERROR("disabled until tested and added to regression suite");
	    options[optHostCalibration] = true;
	    hostcalibration_refhost = optarg;
	    break;
	}
	case 'n': {
	    FATAL_ERROR("disabled until tested and added to regression suite");
	    options[optBestEffort] = true;
	    vector<string> opts;
	    split(optarg,":",opts);
	    INVARIANT(opts.size() >= 3, "missing options to -n");
	    INVARIANT(opts[0].find(",") > opts[0].size(),
		      format("bad first opt %s, has ','; did you leave off"
			     " best-effort-starttime?") % opts[0]);
	    INVARIANT(opts[1].find(",") > opts[1].size(),
		      format("bad second opt %s, has ','; did you leave off"
			     " best-effort-endtime?") % opts[1]);
	    best_effort_window_start = stringToInteger<int32_t>(opts[0]);
	    best_effort_window_end = stringToInteger<int32_t>(opts[1]);
	    BestEffort::BEInfo tmp;
	    for(unsigned i = 2;i<opts.size();++i) {
		vector<string> beopts;
		split(opts[i],",",beopts);
		INVARIANT(beopts.size() == 3, 
			  format("purchase spec '%s' wrong\n") % opts[i]);
		tmp.starttime = stringToInteger<int32_t>(beopts[0]);
		tmp.reserved_cpus = stringToInteger<int32_t>(beopts[1]);
		tmp.best_effort_cpus = stringToInteger<int32_t>(beopts[2]);
		best_effort_reserved.push_back(tmp);
	    }
	    break;
	}
	case '?': FATAL_ERROR("invalid option");
	default:
	    FATAL_ERROR(format("getopt returned '%c'") % opt);
	}
    }
    INVARIANT(any_selected, "You need to have selected at least one option.\n"
	      "Try running with -h for options");

    return optind;
};

bool 
isuint(char *v)
{
    while (*v != '\0') {
	if (!isdigit(*v))
	    return false;
	++v;
    }
    return true;
}

int
main(int argc, char *argv[])
{
    job_report_end = time(NULL);
    job_report_start = job_report_end - 7*86400;
    job_report_noerstest = true;

    int first = parseopts(argc, argv);

    if (argc - first < 1) {
	usage(argv);
    }
    prepareCommonValues();

    IndexSourceModule *sourcea;

    if (((argc - first) == 3 || (argc - first) == 4) 
	&& isuint(argv[first+1]) &&
	isuint(argv[first+2])) {
	if ((argc - first) == 3) {
	    GeneralValue minv,maxv;
	    minv.setInt32(atoi(argv[first+1]));
	    maxv.setInt32(atoi(argv[first+2]));
	    sourcea = new MinMaxIndexModule(argv[first],
					    "Batch::LSF::Grizzly",
					    minv,maxv,
					    "submit_time",
					    "end_time",
					    "min:end_time");
	} else if ((argc - first) == 4) {
	    vector<MinMaxIndexModule::selector> tmp;
	    GeneralValue minv,maxv;
	    minv.setInt32(atoi(argv[first+1]));
	    maxv.setInt32(atoi(argv[first+2]));
	    tmp.push_back(MinMaxIndexModule::selector(minv,maxv,"submit_time","end_time"));
	    GeneralValue clustername;
	    clustername.setVariable32(argv[first+3]);
	    tmp.push_back(MinMaxIndexModule::selector(clustername,clustername,"cluster_name","cluster_name"));
	    sourcea = new MinMaxIndexModule(argv[first],
					    "Batch::LSF::Grizzly",
					    tmp,
					    "min:end_time");
	} else {
	    FATAL_ERROR("internal");
	}
    } else {
	TypeIndexModule *tmp = new TypeIndexModule("Batch::LSF::Grizzly");
	for(int i=first; i<argc; ++i) {
	    tmp->addSource(argv[i]); // only add to one, they share the sources
	}
	sourcea = tmp;
    }
    sourcea->startPrefetching();
    PrefetchBufferModule *prefetcha = new PrefetchBufferModule(*sourcea,32*1024*1024);

//     // Render Request, Render Jobs analysis...
// 
//     SequenceModule rrSequence(new TypeIndexModule("Bear: render requests",sourcea->getSourceList()));
//     SequenceModule rjSequence(new TypeIndexModule("Bear: render jobs",sourcea->getSourceList()));
// 
//     LSFDSAnalysisMod::addRRLookupModules(rrSequence,rjSequence);
// 
//     if (rrSequence.size() > 1) {
// 	DataSeriesModule::getAndDelete(rrSequence.tail());
// 	RowAnalysisModule::printAllResults(rrSequence,1);
//     }
// 
//     if (find_metaid_list.empty() == false) {
// 	rjSequence.addModule(new MetaId2InfoLookup(rjSequence.tail(), find_metaid_list));
//     }
// 
//     if (rjSequence.size() > 1) {
// 	DataSeriesModule::getAndDelete(rjSequence.tail());
// 	RowAnalysisModule::printAllResults(rjSequence,1);
//     }

    // LSF data analysis ...
    SequenceModule lsfSequence(prefetcha);

    for(vector<LSFDSAnalysisMod::FarmLoadArgs *>::iterator i = farm_load_args.begin();
	i != farm_load_args.end(); ++i) {
	lsfSequence.addModule(LSFDSAnalysisMod::newFarmLoad(lsfSequence.tail(), *i));
    }

    if (options[optTraceMetaId]) {
	LSFDSAnalysisMod::addTraceMetaIdModules(lsfSequence);
    }
    if (options[optJobReport]) {
	lsfSequence.addModule(new JobReport(lsfSequence.tail(),
						job_report_start, 
						job_report_end,
						job_report_noerstest));
    }

    if (options[optProductionReport]) {
	LSFDSAnalysisMod::addProductionReportModules(lsfSequence);
    }

    if (options[optLSFJobTimePredict]) {
      lsfSequence.addModule(new LSFJobTimePredict(lsfSequence.tail())); 
    }

    if (options[optHostAvailabilityReport]) {
	lsfSequence.addModule(new HostAvailabilityReport(lsfSequence.tail()));
    }

    if (options[optComplexityAnalysis]) {
      lsfSequence.addModule(new ComplexityAnalysis(lsfSequence.tail())); 
    }

    if (options[optLSFJobStats]) {
	lsfSequence.addModule(new LSFJobStats(lsfSequence.tail()));
    }

    if (options[optLSFSlowJobs]) {
	lsfSequence.addModule(new LSFSlowJobs(lsfSequence.tail(),
						  lsfslowjobs_show_finished));
    }

    if (options[optHostCalibration]) {
	lsfSequence.addModule(new HostCalibration(lsfSequence.tail(),
						      hostcalibration_refhost));
    }

    if (options[optBestEffort]) {
	lsfSequence.addModule(new BestEffort(lsfSequence.tail(),
						 best_effort_window_start, 
						 best_effort_window_end,
						 best_effort_reserved));
    }

    if (&(lsfSequence.tail()) != prefetcha) {
	DataSeriesModule::getAndDelete(lsfSequence);

	RowAnalysisModule::printAllResults(lsfSequence,1);
	printf("extents: %.2f MB -> %.2f MB in %.2f secs decode time\n",
	       (double)(sourcea->total_compressed_bytes)/(1024.0*1024),
	       (double)(sourcea->total_uncompressed_bytes)/(1024.0*1024),
	       sourcea->decode_time);
	printf("                   common\n");
	printf("MB compressed:   %8.2f\n",
	       (double)sourcea->total_compressed_bytes/(1024.0*1024));
	printf("MB uncompressed: %8.2f\n",
	       (double)sourcea->total_uncompressed_bytes/(1024.0*1024));
	printf("decode seconds:  %8.2f\n",
	       sourcea->decode_time);
	printf("wait fraction :  %8.2f\n",
	       sourcea->waitFraction());
    }
}
