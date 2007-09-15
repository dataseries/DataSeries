// -*-C++-*-
/*
   (c) Copyright 2003,2004,2007 Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file 
    Convert a LSF bacct log file to dataseries.  The current
    implementation also knows how to parse the job names used in
    a number of clusters.
*/

// TODO: fix this up so that it has a portion which parses the
// "standard" LSF fields and a portion which parses any cluster
// specific lsf naming/directory naming conventions.

// TODO: remove AssertAlways, printf, replace with INVARIANT,
// boost::format, then remove LintelAssert.

#include <stdio.h>
#include <string>
#include <vector>

#include <pcre.h>
#include <openssl/sha.h>

#include <Lintel/LintelAssert.H>
#include <Lintel/HashMap.H>
#include <Lintel/HashUnique.H>
#include <Lintel/Clock.H>
#include <Lintel/StringUtil.H>

#include <DataSeries/commonargs.H>
#include <DataSeries/DataSeriesModule.H>
#include <DataSeries/cryptutil.H>

using namespace std;
using boost::format;

bool print_parse_warnings = false;
bool print_parse_warning_non_override_frame_number = false;
bool print_directories_failed_to_parse = false;
bool print_frame_lsfidx_warnings = false;
string cluster_name_str;

HashMap<string,bool> noframerange_map;

vector<string> encrypted_parse_directory;
vector<string> encrypted_parse_periodsep;
vector<string> encrypted_parse_colonsep;
vector<string> encrypted_parse_pipesep;
vector<string> encrypted_parse_special;

HashMap<string,string> encrypted_hostgroups;
HashMap<string, HashUnique<string> > unrecognized_hostgroups;

HashMap<string,bool> encrypted_ok_idx_noframe;

// In 2003, found that g++ re-converted static strings each time if
// they weren't pulled out :(
// In 2007, it still seemed to be true.

const string empty_string("");
const string null_string("NULL");
const string ver_62 = "6.2";
const string ver_61 = "6.1";
const string ver_60 = "6.0";
const string ver_51 = "5.1";
const string ver_42 = "4.2";
const string ver_41 = "4.1";
const string ver_40 = "4.0";
const string ver_32 = "3.2";
const string ver_31 = "3.1";
const string ver_30 = "3.0";
const string ver_na = "N/A"; // from converting running jobs
const string status_pend = "PEND";
const string status_run = "RUN";
const string status_ssusp = "SSUSP";
const string status_exit = "EXIT";
const string status_done = "DONE";
const string str_ddr("ddr");
const string str_imax("_imax");
const string str_f("f");
const string str_period(".");
const string str_underbar("_");
const string str_pipe("|");
const string str_colon(":");
const string str_dev_null("/dev/null");
const string str_ers_java_service("ers_java_service");
const string str_java_service("java-service");
const string str_perl_service("perl_service");
const string str_perl_service2("perl-service");
const string str_tuscany("tuscany");
const string str_chroot_jail("chroot_jail");
const string str_ers("ers");
const string str_sstress_0_1_0("sstress-0.1.0");
const string str_sstress("sstress");
const string str_ers_host_traces("ers_host_traces");
const string str_BPMake("BPMake");
const string str_batch_parallel("batch-parallel");
const string str_ers_trace_data("ers-trace-data");
const string str_dotcount(".count");
const string str_trace_dash("trace-");
const string str_dotbsubdashlogdot(".bsub-log.");
const string str_minus1("-1");
const string str_tmp("tmp");
const string job_finish("JOB_FINISH");
const string job_cache("JOB_CACHE"); // from conversion of running jobs
const string str_lsb_jobindex("LSB_JOBINDEX");
const string str_dollar_lsb_jobindex("$LSB_JOBINDEX");
const string str_quote_dollar_lsb_jobindex("'$LSB_JOBINDEX'");
const string str_frame_number("FRAME_NUMBER");
const string str_percent_d("%d");

int jobname_parse_fail_count = 0;
int jobname_odd_fail_count = 0;
int jobdirectory_parse_fail_count = 0;
int jobdirectory_odd_fail_count = 0;

// needed to make g++-3.3 not suck.
extern int printf (__const char *__restrict __format, ...)
   __attribute__ ((format (printf, 1, 2)));

// TODO: add conversion statistics
const string lsf_grizzly_xml(
  "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Batch::LSF::Grizzly\" version=\"1.0\">\n"
  "  <field type=\"variable32\" name=\"cluster_name\" pack_unique=\"yes\" />\n"

// This gets passed through encrypted, which, unless you're archiving
// the lsf logs as dataseries, serves little purpose.  Worse, it
// generates lots of large unique encrypted strings, which really
// hurts the compression process.
//  "  <field type=\"variable32\" name=\"job_name\" opt_nullable=\"yes\" pack_unique=\"yes\" />\n"

  "  <field type=\"bool\" name=\"job_name_unpacked\" note=\"true if the conversion managed to decode the job name to generate production,sequence,shot,task,object,subtask,jobname_username, may have some of production/sequence/shot anyway from decoding the directory path\" />\n"
  "  <field type=\"bool\" name=\"directory_path_unpacked\" note=\"true if the conversion managed to decode the directory path, may have production/sequence/shot anyway from decoding the job name\" />\n"
  "  <field type=\"bool\" name=\"directory_name_info_matched\" opt_nullable=\"yes\" note=\"true if prod/seq/shot matched in decode of directory name and job name, false on mismatch, null if one or the other didn't decode\" />\n"
  "  <field type=\"int32\" name=\"meta_id\" opt_nullable=\"yes\" pack_relative=\"meta_id\" />\n"
  "  <field type=\"variable32\" name=\"production\" pack_unique=\"yes\" opt_nullable=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"sequence\" pack_unique=\"yes\" opt_nullable=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"shot\" pack_unique=\"yes\" opt_nullable=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"task\" pack_unique=\"yes\" opt_nullable=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"object\" pack_unique=\"yes\" opt_nullable=\"yes\" note=\"usually object the task is operating on\" />\n"
  "  <field type=\"variable32\" name=\"subtask\" pack_unique=\"yes\" opt_nullable=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"jobname_username\" pack_unique=\"yes\" opt_nullable=\"yes\" note=\"sometimes the generic submission account, sometimes the user that actually put in the request which is submitted by the generic account, the username field always contains the LSF username\" />\n"
  "  <field type=\"variable32\" name=\"frames\" pack_unique=\"yes\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"start_frame\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"end_frame\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"nframes\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"frame_step\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"job_parallel_limit\" opt_nullable=\"yes\" note=\"how many jobs can run at the same time\" />\n"
  "  <field type=\"variable32\" name=\"command\" pack_unique=\"yes\" note=\"this is the full command with all arguments\" />\n"
  "  <field type=\"variable32\" name=\"command_path\" pack_unique=\"yes\" note=\"this is the full path to the binary executed\" />\n"
  "  <field type=\"variable32\" name=\"command_name\" pack_unique=\"yes\" note=\"this is the name only of the binary executed\" />\n"
  "  <field type=\"double\" name=\"job_resolution\" opt_nullable=\"yes\" note=\"special resolution of 2.33 is ddr resolution, larger numbers mean fewer pixels, e.g. job_res=2 ==> 1/2 the pixels in each dimension.\" />\n"
  "  <field type=\"int32\" name=\"job_frame\" opt_nullable=\"yes\" />\n"
  "  <field type=\"int32\" name=\"created\" pack_relative=\"created\" />\n"
  "  <field type=\"int32\" name=\"job_id\" pack_relative=\"job_id\" />\n"
  "  <field type=\"int32\" name=\"job_idx\" opt_nullable=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"username\" pack_unique=\"yes\" />\n"
  "  <field type=\"int32\" name=\"user_id\" />\n"
  "  <field type=\"int32\" name=\"event_time\" note=\"current time for a still pending/running job, end_time for a finished job\" pack_relative=\"event_time\" />\n"
  "  <field type=\"int32\" name=\"submit_time\" pack_relative=\"submit_time\" />\n"
  "  <field type=\"int32\" name=\"req_start_time\" opt_nullable=\"yes\" note=\"requested start-after time, if any\" pack_relative=\"req_start_time\" />\n"
  "  <field type=\"int32\" name=\"start_time\" opt_nullable=\"yes\" pack_relative=\"submit_time\" />\n"
  "  <field type=\"int32\" name=\"end_time\" opt_nullable=\"yes\" pack_relative=\"start_time\" />\n"
  "  <field type=\"variable32\" name=\"queue\" pack_unique=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"email\" pack_unique=\"yes\" opt_nullable=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"status\" pack_unique=\"yes\" note=\"status of LSF job, usually EXIT or DONE\" />\n"
  "  <field type=\"int32\" name=\"status_int\" note=\"integer version of status, 32=EXIT, 64=DONE\" />\n"
  "  <field type=\"variable32\" name=\"team\" pack_unique=\"yes\" note=\"from the LSF fair-share name\" />\n"
  "  <field type=\"int32\" name=\"exit_code\" />\n"
  "  <field type=\"double\" name=\"user_time\" pack_scale=\"1e-6\" />\n"
  "  <field type=\"double\" name=\"system_time\" pack_scale=\"1e-6\" />\n"
  "  <field type=\"double\" name=\"cpu_time\" pack_scale=\"1e-6\" />\n"
  "  <field type=\"int64\" name=\"max_memory\" units=\"bytes\" opt_nullable=\"yes\" note=\"0 if job didn't run, null if unknown\" />\n"
  "  <field type=\"int64\" name=\"max_swap\" units=\"bytes\" opt_nullable=\"yes\" note=\"0 if job didn't run, null if unknown\" />\n"
  "  <field type=\"variable32\" name=\"exec_host\" pack_unique=\"yes\" opt_nullable=\"yes\" />\n"
  "  <field type=\"variable32\" name=\"exec_host_group\" pack_unique=\"yes\" opt_nullable=\"yes\" note=\"usually machines are purchased in batches; if we can identify the batch, which one is it? dedicated groups (almost always serving LSF jobs) are named dedicated-\" />\n"
  "</ExtentType>\n");

ExtentSeries lsf_grizzly_series;
OutputModule *lsf_grizzly_outmodule;

Variable32Field cluster_name(lsf_grizzly_series, "cluster_name");
//Variable32Field job_name(lsf_grizzly_series, "job_name", Field::flag_nullable);
BoolField job_name_unpacked(lsf_grizzly_series, "job_name_unpacked");
BoolField directory_path_unpacked(lsf_grizzly_series, "directory_path_unpacked");
BoolField directory_name_info_matched(lsf_grizzly_series,"directory_name_info_matched",
				      Field::flag_nullable);
Int32Field meta_id(lsf_grizzly_series, "meta_id", Field::flag_nullable);
Variable32Field production(lsf_grizzly_series, "production", Field::flag_nullable);
Variable32Field sequence(lsf_grizzly_series, "sequence", Field::flag_nullable);
Variable32Field shot(lsf_grizzly_series, "shot", Field::flag_nullable);
Variable32Field task(lsf_grizzly_series, "task", Field::flag_nullable);
Variable32Field object(lsf_grizzly_series, "object", Field::flag_nullable);
Variable32Field subtask(lsf_grizzly_series, "subtask", Field::flag_nullable);
Variable32Field jobname_username(lsf_grizzly_series, "jobname_username", Field::flag_nullable);

Variable32Field frames(lsf_grizzly_series, "frames", Field::flag_nullable);
Int32Field start_frame(lsf_grizzly_series, "start_frame", Field::flag_nullable);
Int32Field end_frame(lsf_grizzly_series, "end_frame", Field::flag_nullable);
Int32Field nframes(lsf_grizzly_series, "nframes", Field::flag_nullable);
Variable32Field command(lsf_grizzly_series, "command");
Variable32Field command_name(lsf_grizzly_series, "command_name");
Variable32Field command_path(lsf_grizzly_series, "command_path");
DoubleField job_resolution(lsf_grizzly_series, "job_resolution", Field::flag_nullable);
Int32Field job_frame(lsf_grizzly_series, "job_frame", Field::flag_nullable);
Int32Field created(lsf_grizzly_series, "created");
Int32Field job_id(lsf_grizzly_series, "job_id");
Int32Field job_idx(lsf_grizzly_series, "job_idx", Field::flag_nullable);
Variable32Field username(lsf_grizzly_series, "username");
Int32Field user_id(lsf_grizzly_series, "user_id");
Int32Field event_time(lsf_grizzly_series, "event_time");
Int32Field submit_time(lsf_grizzly_series, "submit_time");
Int32Field req_start_time(lsf_grizzly_series, "req_start_time", Field::flag_nullable);
Int32Field start_time(lsf_grizzly_series, "start_time", Field::flag_nullable);
Int32Field end_time(lsf_grizzly_series, "end_time", Field::flag_nullable);
Variable32Field queue(lsf_grizzly_series, "queue");
Variable32Field email(lsf_grizzly_series, "email", Field::flag_nullable);
Variable32Field status(lsf_grizzly_series, "status");
Int32Field status_int(lsf_grizzly_series, "status_int");
Variable32Field team(lsf_grizzly_series, "team");
Int32Field exit_code(lsf_grizzly_series, "exit_code");
DoubleField user_time(lsf_grizzly_series, "user_time");
DoubleField system_time(lsf_grizzly_series, "system_time");
DoubleField cpu_time(lsf_grizzly_series, "cpu_time");
Int64Field max_memory(lsf_grizzly_series, "max_memory", Field::flag_nullable);
Int64Field max_swap(lsf_grizzly_series, "max_swap", Field::flag_nullable);
Variable32Field exec_host(lsf_grizzly_series, "exec_host", Field::flag_nullable);
Variable32Field exec_host_group(lsf_grizzly_series, "exec_host_group", Field::flag_nullable);

// was doing into.push_back(char), but this implementation is
// way faster
char *
parselsfstring(char *start,string &into, int maxlen)
{
    static string tmpstring;
    if ((int)tmpstring.size() < maxlen) {
	tmpstring.resize(maxlen);
    }
    AssertAlways(start[0] == '"',("bad %s\n",start));
    char *finish = start+1;

    int i = 0;
    while(true) {
	if (finish[0] == '"' && finish[1] == '"') {
	    tmpstring[i] = '"'; ++i;
	    // into.push_back('"');
	    finish += 2;
	} else if (finish[0] == '"') {
	    finish += 1;
	    break;
	} else {
	    tmpstring[i] = finish[0]; ++i;
	    // into.push_back(finish[0]);
	    finish += 1;
	}
	AssertAlways(*finish != '\n' && *finish != '\0',
		     ("parse error"));
    }

    AssertAlways(i < maxlen,("internal"));
    into = tmpstring.substr(0,i);
    return finish;
}

char *
parsenumber(char *start,string &into, int line_num)
{
    char *finish = start;
    if (*finish == '-') {
	++finish;
    }
    while(true) {
	if (isdigit(*finish) || *finish == '.') {
	    ++finish;
	} else {
	    AssertAlways(*finish == ' ' || *finish == '\n',
			 ("parse error, got '%c' at line %d from '%s'",*finish,
			  line_num, start));
	    break;
	}
    }
    into.assign(start,finish-start);
    return finish;
}

void
extract_fields(char *buf, vector<string> &fields, int linenum)
{
    static const bool debug_extract_fields = false;

    if (debug_extract_fields)
	printf("%s parsed as:\n",buf);
    int maxlen = strlen(buf);
    while(*buf != '\n') {
	string tmp;
	AssertAlways(*buf != '\0',("parse error"));
	if (*buf == '"') {
	    buf = parselsfstring(buf,tmp,maxlen);
	} else {
	    buf = parsenumber(buf,tmp, linenum);
	}
	if (debug_extract_fields) {
	    cout << boost::format("%d. %s\n") % fields.size() % tmp.c_str();
	}
	fields.push_back(tmp);
	if (*buf == ' ') {
	    ++buf;
	    AssertAlways(*buf != '\n' && *buf != '\0',
			 ("parse error"));
	} else {
	    AssertAlways(*buf == '\n',("parse error, got '%c' from '%s' at line %d",*buf,buf, linenum));
	}
    }
}

bool encmatch(const string &in, const string &match)
{
    if (encryptString(in) == match)
	return true;
    if (match == empty_string) {
	fprintf(stderr,"%s -> %s\n",in.c_str(),
		hexstring(encryptString(in)).c_str());
    }
    return false;
}

// 3.x, 4.x, 5.x, 6.x lsb.acct format
//  0.  Event type (%s)
//  1.  Version Number (%s)
//  2.  Event Time (%d)
//  3.  jobId (%d)
//  4.  userId (%d)
//  5.  options (%d)
//  6.  numProcessors (%d)
//  7.  submitTime (%d)
//  8.  beginTime (%d)  // requested start-after time
//  9.  termTime (%d)   // requested kill-after time
// 10.  startTime (%d)
// 11.  userName (%s)
// 12.  queue (%s)
// 13.  resReq (%s)
// 14.  dependCond (%s)
// 15.  preExecCmd (%s)
// 16.  fromHost (%s)
// 17.  cwd (%s)
// 18.  inFile (%s)
// 19.  outFile (%s)
// 20.  errFile (%s)
// 21.  jobFile (%s)
// 22.  numAskedHosts (%d) -- creates list
// 23.  askedHosts (%s)
// 24.  numExHosts (%d) -- creates list
// 25.  execHosts (%s)
// 26.  jStatus (%d)
// 27.  hostFactor (%f)
// 28.  jobName (%s)
// 29.  command (%s)
// 30.  ru_utime (%f)
// 31.  ru_stime (%f)
// 32.  ru_maxrss (%d)  // there's no point in printing the next set of these
// 33.  ru_ixrss (%d)   // it looks like the last time there were valid values
// 34.  ru_ismrss (%d)  // in these fields was during lsf 3.1 on the Bear
// 35.  ru_idrss (%d)   // cluster :(
// 36.  ru_isrss (%d)
// 37.  ru_minflt (%d)
// 38.  ru_magflt (%d)
// 39.  ru_nswap (%d)
// 40.  ru_inblock (%d)
// 41.  ru_oublock (%d)
// 42.  ru_ioch (%d)
// 43.  ru_msgsnd (%d)
// 44.  ru_msgrcv (%d)
// 45.  ru_nsignals (%d)
// 46.  ru_nvcsw (%d)
// 47.  ru_nivcsw (%d)
// 48.  ru_exutime (%d)
// 49.  mailUser (%s)
// 50.  projectName (%d)
// 51.  exitStatus (%d)
// 52.  maxNumProcessors (%d)
// 53.  loginShell (%s)
// 54.  timeEvent (%s) --      >= 3.1
// 55.  idx (%d) --            >= 3.1
// 56.  maxRMem (%d) --        >= 3.1
// 57.  maxRSwap (%d) --       >= 3.1
// 58.  inFileSpool (%s) --    >= 4.0
// 59.  commandSpool (%s) --   >= 4.0
// 60.  Reservation ID (%s) -- >= 5.1
// 61.  additionalInfo (%s) -- >= 6.0
// 62.  exitInfo (%d)	       >= 6.0
// 63.  warningAction (%s)     >= 6.0
// 64.  warningTimePeriod (%d) >= 6.0
// 65.  chargedSAAP (%s)       >= 6.0
// 66.  unknown (%d)           >= 6.0 // docs say this field doesn't exist, but it's in the files
// 67.  sla(%s)		       >= 6.0
// 68.  unknown(%s)            >= 6.1

// by default when printing as SQL, unsigned 0's -> NULL,
// empty strings -> NULL, and double -1's -> NULL
struct job_info {
    // these are extracted by parse_jobname
    string job_name; // full string, unpacked below if possible
    bool job_name_unpacked; // if unpacking completely successed
    bool directory_name_unpacked; // if directory name unpacked
    unsigned meta_id;
    string production;
    string sequence;
    string shot;
    string task;
    string object;
    string subtask;
    string jobname_username;
    string frames;
    unsigned start_frame, end_frame, frame_step, nframes, job_parallel_limit;
    string dir_production, dir_sequence, dir_shot;
    // these are extracted by parse_command
    string command, command_name, command_path;
    double job_resolution;
    unsigned job_frame;
    // these are extracted by parse_jobline42
    unsigned created; // is Event Time in bacct log
    unsigned job_id; // is jobId in bacct log
    unsigned job_idx; // is idx in bacct log
    string username; // userName in bacct log
    unsigned user_id; // userId in bacct log
    unsigned event_time; // Event Time in bacct log
    unsigned submit_time; // submitTime in bacct log
    unsigned req_start_time; // beginTime in bacct log
    unsigned start_time; // startTime in bacct log
    unsigned end_time; // Event Time in bacct log (if event type was LSF_FINISH)
    string queue;
    string email; // mailUser in bacct log
    string status; // jStatus (converted from int -> string) in bacct log
    int status_int; // jStatus in bacct log
    string team; // projectName in bacct log
    unsigned exit_code; // exitStatus in bacct log
    double user_time; // ru_utime in bacct log
    double system_time; // ru_stime in bacct log
    double cpu_time; // user_time + system_time
    ExtentType::int64 max_memory, max_swap;
    string exec_host;
    job_info()
	: job_name_unpacked(false), directory_name_unpacked(false), meta_id(0), start_frame(0), end_frame(0),
	  frame_step(0), nframes(0), job_parallel_limit(0), job_resolution(-1), job_frame(0),
	  created(0), job_id(0), job_idx(0), user_id(0), event_time(0),
	  submit_time(0), req_start_time(0), start_time(0), end_time(0),
	  status_int(0), exit_code(0), user_time(-1), system_time(-1),
	  cpu_time(-1), max_memory(-1), max_swap(-1)
    { }

    void parse_command(const string &lsf_command, const string &lsf_idx_str) {
	command = lsf_command;
	int space_idx = command.find(" ");
	if (space_idx == -1)
	    space_idx = command.size();
	int slash_idx = command.rfind("/",space_idx);
	if (slash_idx == -1) {
	    slash_idx = 0;
	} else {
	    slash_idx += 1; //skip the slash
	}
	command_name = command.substr(slash_idx,space_idx - slash_idx);
	command_path = command.substr(0,space_idx);

	if (false)
	    printf("XX %s -> '%s' ; '%s'\n",
		   command.c_str(),command_name.c_str(),
		   command_path.c_str());
	job_resolution = -1;
	job_frame = 0;
	int res_idx = lsf_command.find("-r ");
	if (res_idx == -1) {
	    job_resolution = -1;
	} else {
	    AssertAlways(lsf_command[res_idx] == '-' &&
			 lsf_command[res_idx+1] == 'r' &&
			 lsf_command[res_idx+2] == ' ',("bad"));
	    res_idx = res_idx+2;
	    bool found_res = true;
	    while(isspace(lsf_command[res_idx])) {
		++res_idx;
		if (res_idx >= (int)lsf_command.size()) {
		    found_res = false;
		    break;
		}
	    }
	    int res_end = res_idx;
	    while(found_res && res_end < (int)lsf_command.size() &&
		  !isspace(lsf_command[res_end]) &&
		  lsf_command[res_end] != '"' &&
		  lsf_command[res_end] != ';') {
		if (isdigit(lsf_command[res_end]) ||
		    lsf_command[res_end] == '.') {
		    ++res_end;
		} else if (lsf_command[res_end] == 'd' && lsf_command[res_end+1] == 'd' &&
			   lsf_command[res_end+2] == 'r') {
		    res_end += 3;
		    break;
		} else {
		    found_res = false;
		    if (print_parse_warnings) {
		      fprintf(stderr,"bad resolution char '%c' '%c' in %s\n",
			      lsf_command[res_end],lsf_command[res_end+1],lsf_command.c_str());
		    }
		    break;
		}

	    }
	    if (found_res) {
		string res_str = lsf_command.substr(res_idx,res_end-res_idx);
		if (res_str == str_ddr) {
		    job_resolution = 2.33; // if you change this, update the note.
		} else {
		    job_resolution = atof(res_str.c_str());
		}
		if (false)
		    printf("%s -> %d..%d %s ; %f\n",lsf_command.c_str(),res_idx,res_end,
			   res_str.c_str(),job_resolution);
	    } else {
		job_resolution = -1;
	    }
	}
	unsigned lsf_idx = uintfield(lsf_idx_str.c_str());
	int frame_idx = lsf_command.rfind("-c ");
	int array_idx = lsf_command.rfind("-array");
	int frames_idx = lsf_command.rfind("-frames ");
	if (lsf_idx == 0) {
	    if (frame_idx >= 0 && isdigit(lsf_command[frame_idx+3])) { // some commands take -c, but it isn't a frame #
		job_frame = atoi(lsf_command.c_str() + (frame_idx + 3));
		if (false) printf("XX %s -> %d\n",lsf_command.c_str(),job_frame);
	    }
	    //	    AssertAlways(frame_idx == -1,("bad %s %d",lsf_command.c_str(),lsf_idx));
	} else if (array_idx > 0) {
	    job_frame = lsf_idx;
	} else if (frame_idx == -1) {
	    string enccmd = encryptString(command_name);
	    bool *v = encrypted_ok_idx_noframe.lookup(enccmd);
	    if (v) {
		AssertAlways(*v,("internal"));
	    } else if (print_frame_lsfidx_warnings) {
		fprintf(stderr,"Warning, got lsf_idx (but not frame option) for unrecognized '%s'/'%s'/'%s'\n",
			command_name.c_str(),hexstring(encryptString(command_name)).c_str(),command.c_str());
	    }
	    job_frame = 0;
	} else if (frame_idx > 0 && frames_idx > 0) {
	    string encx = encryptString(lsf_command.substr(frame_idx,  6));
	    if (encx == encrypted_parse_special[2] &&
		lsf_command.size() >= static_cast<uint32_t>(frames_idx) + 9 + 12 && 
		lsf_command.substr(frames_idx+9, 12) == str_lsb_jobindex) {
		job_frame = lsf_idx;
	    } else {
		job_frame = 0;
		if (print_parse_warning_non_override_frame_number) {
		    fprintf(stderr,"Warning non-override frame numbers in '%s':\n",
			    lsf_command.c_str());
		    if (encx != encrypted_parse_special[2]) {
			fprintf(stderr, "  '%s' => '%s' != '%s'\n",
				lsf_command.substr(frame_idx, 6).c_str(),
				hexstring(encx).c_str(), hexstring(encrypted_parse_special[2]).c_str());
		    }
		    if (lsf_command.substr(frames_idx+9, 12) != str_lsb_jobindex) {
			fprintf(stderr, "  '%s' != '%s'\n",
				lsf_command.substr(frames_idx+9, 12).c_str(),
				str_lsb_jobindex.c_str());
		    }			
		}
	    }
	} else {
	    INVARIANT(frame_idx >= 0,
		      format("bad frame index %d in command '%s' lsf_idx=%d")
		      % frame_idx % lsf_command % lsf_idx);
	    if (false) printf("XX %s -> %d %d '%s'\n",lsf_command.c_str(),
		   lsf_idx,frame_idx,lsf_command.substr(frame_idx,5).c_str());
	    if (lsf_command.substr(frame_idx+3,13) == str_dollar_lsb_jobindex ||
		lsf_command.substr(frame_idx+3,15) == str_quote_dollar_lsb_jobindex ||
		lsf_command.substr(frame_idx+3,12) == str_frame_number ||
		lsf_command.substr(frame_idx+3,2) == str_percent_d ||
		atoi(lsf_command.c_str()+frame_idx+3) == (int)lsf_idx) {
		job_frame = lsf_idx;
	    } else {
		job_frame = 0;
		if (print_parse_warnings) {
		  fprintf(stderr,"Warning, inconsistent frame numbers lsf_idx = %d; -c = %s ; %s\n",
			  lsf_idx, lsf_command.substr(frame_idx+3,13).c_str(),
			  lsf_command.c_str());
		}
	    }
	    if (false) printf("YY %s -> %d\n",lsf_command.c_str(),job_frame);
	}
    }
    static const int jStatusPEND = 1;
    static const int jStatusRUN  = 4;
    static const int jStatusSSUSP = 8;
    static const int jStatusEXIT = 32;
    static const int jStatusDONE = 64;
    void parse_jobline42(vector<string> &fields, int exechostsoffset, int tailoffset) {
	created = uintfield(fields[2]);
	job_id = uintfield(fields[3]);
	job_idx = uintfield(fields[55+tailoffset]);
	username = fields[11];
	user_id = uintfield(fields[4]);
	event_time = uintfield(fields[2]);
	submit_time = uintfield(fields[7]);
	req_start_time = uintfield(fields[8]);
	start_time = uintfield(fields[10]);
	if (fields[0] == job_finish) {
	    end_time = event_time;
	} else if (fields[0] == job_cache) {
	    end_time = 0;
	} else {
	    AssertFatal(("internal"));
	}
	queue = fields[12];
	email = fields[49+tailoffset];
	if (fields[26+tailoffset] == empty_string) {
	    status_int = -1;
	} else {
	    status_int = uintfield(fields[26+tailoffset]);
	}
	if (status_int == jStatusPEND) {
	    status = status_pend;
	} else if (status_int == jStatusRUN) {
	    status = status_run;
	} else if (status_int == jStatusSSUSP) {
	    status = status_ssusp;
	} else if (status_int == jStatusEXIT) {
	    status = status_exit;
	} else if (status_int == jStatusDONE) {
	    status = status_done;
	} else {
	    AssertFatal(("unrecognized job status %d\n",status_int));
	}
	team = fields[50+tailoffset];
	exit_code = uintfield(fields[51+tailoffset]);
	user_time = dblfield(fields[30+tailoffset]);
	system_time = dblfield(fields[31+tailoffset]);
	cpu_time = user_time + system_time;
	int nhosts = uintfield(fields[24+exechostsoffset]);
	if (nhosts == 0) {
	    // empty
	} else if (nhosts == 1) {
	    exec_host = fields[25+tailoffset];
	} else {
	    exec_host = "*lots; not handled correctly yet*";
	    fprintf(stderr,"Warning, got %d exechosts\n",nhosts);
	}
    }
    void parse_maxrmem(vector<string> &fields, int tailoffset, int linenum) {
	int rmem_kb = intfield(fields[56+tailoffset]);
	int rswap_kb = intfield(fields[57+tailoffset]);
	if (start_time > 0) {
	    if (rmem_kb <= 0 || rswap_kb <= 0) {
		if (print_parse_warnings) {
		    fprintf(stderr,"warning, invalid rmem/rswap for started job %d/%d on line %d, sse %d .. %d .. %d\n",rmem_kb,rswap_kb,linenum,submit_time,start_time-submit_time,end_time-start_time);
		}
		rmem_kb = -1;
		rswap_kb = -1;
	    }
	} else {
	    // sometimes lsf uses -1 in not run things, sometimes 0; dunno
	    // what causes it to choose -1
	    if (rmem_kb == -1) rmem_kb = 0;
	    if (rswap_kb == -1) rswap_kb = 0;
	    if (rmem_kb != 0 || rswap_kb != 0) {
		if (print_parse_warnings) {
		    fprintf(stderr,"warning, invalid rmem/rswap %d/%d on line %d\n",rmem_kb,rswap_kb,linenum);
		}
		rmem_kb = 0;
		rswap_kb = 0;
	    }
	}
	if (rmem_kb >= 0) {
	    max_memory = (ExtentType::int64)rmem_kb * 1024;
	    max_swap = (ExtentType::int64)rswap_kb * 1024;
	}
    }
};

pcre *regex_frame_one;
pcre *regex_frame_range;
pcre *regex_frame_range_step_1;
pcre *regex_frame_range_step_2;
pcre *regex_frame_range_step_3;
pcre *regex_frame_list;
pcre *regex_frame_complex;
pcre *regex_frame_single_step;

void
xpcre_get_substring(const string &str, int *ovector, int rc, int stringnum, string &outstr)
{
    const char *stringptr;
    AssertAlways(pcre_get_substring(str.c_str(),ovector,rc,stringnum,&stringptr) >= 0,
		 ("get substring failed"));
    outstr = stringptr;
    pcre_free((void *)stringptr);
}

bool
extract_framerange_step_parallel(job_info &jinfo,const string &framebits,
				 int ovector[], int rc,
				 int step_pos, int limit_pos)
{
    jinfo.frames = framebits;
    string str;

    xpcre_get_substring(framebits,ovector,rc,1,str);
    jinfo.start_frame = uintfield(str);

    xpcre_get_substring(framebits,ovector,rc,2,str);
    jinfo.end_frame = uintfield(str);

    if (step_pos > 0) {
	xpcre_get_substring(framebits,ovector,rc,step_pos,str);
	jinfo.frame_step = uintfield(str);
    }  else {
	jinfo.frame_step = 1;
    }

    if (limit_pos > 0) {
	xpcre_get_substring(framebits,ovector,rc,limit_pos,str);
	jinfo.job_parallel_limit = uintfield(str);
    }

    jinfo.nframes = (jinfo.end_frame - jinfo.start_frame + 1)/jinfo.frame_step;
    return true;
}

pcre *
xpcre_compile(const char *regex)
{
    const char *errptr;
    int erroffset;

    pcre *ret = pcre_compile(regex,0,&errptr,&erroffset,NULL);
    INVARIANT(ret != NULL,
	      boost::format("pcre compile(%s) failed at: %s")
	      % regex % errptr);
    return ret;
}

bool // true on success
parse_frameinfo(const string &framebits, job_info &jinfo)
{
    if (regex_frame_one == NULL) {
	regex_frame_one = xpcre_compile("^(\\d+)$");
	regex_frame_range = xpcre_compile("^\\[(\\d+)-(\\d+)\\]$");
	regex_frame_range_step_1 =
	    xpcre_compile("^\\[(\\d+)-(\\d+):(\\d+)\\]$");
	regex_frame_range_step_2 =
	    xpcre_compile("^\\[(\\d+)-(\\d+)\\]%(\\d+)$");
	regex_frame_range_step_3 =
	    xpcre_compile("^\\[(\\d+)-(\\d+):(\\d+)\\]%(\\d+)$");
	regex_frame_list = xpcre_compile("^\\[(\\d+)(,\\d+)*,(\\d+)\\]$");
	regex_frame_complex = xpcre_compile("^\\[(\\d|-|:|,)+\\](%\\d+)?$");
	regex_frame_single_step = 
	    xpcre_compile("^\\[(\\d)+\\]%(\\d+)$");
    }
    const int novector = 30;
    int ovector[novector];

    int rc = pcre_exec(regex_frame_one, NULL, framebits.c_str(),
		       framebits.length(),0,0,ovector,novector);
    if (rc == 2) {
	jinfo.frames = framebits;
	string str;
	xpcre_get_substring(framebits,ovector,rc,1,str);
	jinfo.start_frame = jinfo.end_frame = uintfield(str);
	jinfo.nframes = 1;
	return true;
    }
    AssertAlways(rc == PCRE_ERROR_NOMATCH,
		 ("inexplicable error from pcre: %d\n",rc));

    rc = pcre_exec(regex_frame_range, NULL, framebits.c_str(),
		   framebits.length(),0,0,ovector,novector);
    if (rc == 3) {
	jinfo.frames = framebits;
	string str;
	xpcre_get_substring(framebits,ovector,rc,1,str);
	jinfo.start_frame = uintfield(str);
	xpcre_get_substring(framebits,ovector,rc,2,str);
	jinfo.end_frame = uintfield(str);
	jinfo.nframes = jinfo.end_frame - jinfo.start_frame + 1;
	jinfo.frame_step = 1;
	return true;
    }
    AssertAlways(rc == PCRE_ERROR_NOMATCH,
		 ("inexplicable error from pcre: %d\n",rc));

    rc = pcre_exec(regex_frame_range_step_1, NULL, framebits.c_str(),
		   framebits.length(), 0,0,ovector,novector);
    if (rc == 4) {
	return extract_framerange_step_parallel(jinfo,framebits,ovector,rc,3,-1);
    }

    rc = pcre_exec(regex_frame_range_step_2, NULL, framebits.c_str(),
		   framebits.length(), 0,0,ovector,novector);
    if (rc == 4) {
	return extract_framerange_step_parallel(jinfo,framebits,ovector,rc,-1,3);
    }

    rc = pcre_exec(regex_frame_range_step_3, NULL, framebits.c_str(),
		   framebits.length(), 0,0,ovector,novector);
    if (rc == 5) {
	return extract_framerange_step_parallel(jinfo,framebits,ovector,rc,3,4);
    }

    rc = pcre_exec(regex_frame_list, NULL, framebits.c_str(),
		   framebits.length(), 0,0,ovector,novector);
    if (rc == 4) {
	jinfo.frames = framebits;
	string str;
	xpcre_get_substring(framebits,ovector,rc,1,str);
	jinfo.start_frame = uintfield(str);
	xpcre_get_substring(framebits,ovector,rc,3,str);
	jinfo.end_frame = uintfield(str);
	jinfo.nframes = 1;
	for(unsigned i = 0;i<framebits.length();++i) {
	    if (framebits[i] == ',')
		jinfo.nframes += 1;
	}
	return true;
    }
    rc = pcre_exec(regex_frame_complex, NULL, framebits.c_str(),
		   framebits.length(), 0,0,ovector,novector);
    if (rc == 2 || rc == 3) {
	jinfo.frames = framebits;
	// dunno how to extract start, end, count, step from that mess
	return true;
    }
    AssertAlways(rc == PCRE_ERROR_NOMATCH,
		 ("inexplicable error from pcre: %d\n",rc));

    rc = pcre_exec(regex_frame_single_step, NULL, framebits.c_str(),
		   framebits.length(), 0,0,ovector,novector);
    if (rc == 3) {
	jinfo.frames = framebits;
	string str;
	xpcre_get_substring(framebits,ovector,rc,1,str);
	jinfo.start_frame = uintfield(str);
	jinfo.end_frame = uintfield(str);
	jinfo.nframes = 1;
	xpcre_get_substring(framebits,ovector,rc,2,str);
	jinfo.frame_step = uintfield(str);
	return true;
    }

    if (false)
	printf("Unable to parse frameinfo %s // %s\n",
	       framebits.c_str(),hexstring(encryptString(framebits)).c_str());
    return false;

}

// Clock::Tll pcre_cycles;
// bool
// parse_frameinfo(const string &framebits, job_info &jinfo)
// {
//     Clock::Tll start = Clock::now();
//     bool ret = _parse_frameinfo(framebits,jinfo);
//     Clock::Tll end = Clock::now();
//     pcre_cycles += end - start;
//     return ret;
// }

// will be field 4 in the colonsep entry, if we get a match here,
// and there are 6 fields, field 5 becomes the subtask
static string noframerange_strings[] =
{
    "c9341a96444da200f40f9e9c9b42d6133bd7fafedefc3a96f3abf30682ef59a9",
    "d00869021221e7e25aa82b45da860017b56e418f44d60a6c13aeeb832fadd715",
    "8304c01157174d6977b14b0aa5b47b4cdfe350f2eb00e258473c92bd76db41ef",
    "56fc7feb52df547bfbd2dbefd376fd6bc0da3834be70f24bbcd1c01cbf9b9426",
    "75e3475cc1fd4f15c0c9c21d27637222865d1116e889a26f374016e18f272e2f",
    "7a493b636bbbc657b9c26718b5c4288728c82a422974270aacfe324f5741e400",
    "5d6804de800a2d0964c8394e478420b8",
    "7a493b636bbbc657b9c26718b5c4288728c82a422974270aacfe324f5741e400",
    "2e8f9a38f46133e17cbed3a7c2e190f91cf0cc97d3da15defd68316328fed6e8",
    "125e97f503dace08a44b77e2f346164676837483ed474f62d17680d0c4d4776f",
    "c9682ead2d0e07e4417086fbbcfae0a5560fb5693d4f80afe23796982f175667",
};

static string ok_idx_noframe_strings[] =
{
    "75e3475cc1fd4f15c0c9c21d27637222865d1116e889a26f374016e18f272e2f",
    "71cd75bfdce19031a43b5e3342db5af0dd3c5336b492ed673f114d0f40326437",
    "f63829f79b8fabaf88e267f4b5a43664",
    "fbc82cf76fc0429417bdf9a05262e1f2",
    "91aae7490cf00717e7d5d5e252f899fb",
    "b4f83acc653a0618d368c1e83fbdf07c",
};

static string encrypted_parse_directory_hexstrings[] =
{
    "8914d22682a9432e3f6fb5613ed5ace3", // 0
    "74ab269f45eb27e5cbc84d112b16d0f0", // 1
    "21fa2b28a2cd04c44ae763544bb87ba3", // 2
    "c934239c8e07d7f5f940fd8c987e26f6", // 3
    "4c3aba05d7ab6232dc6a21b899466c63", // 4
    "34815ecf2b5065cfdba36cff820461d1", // 5
    "5213c8a174ecb986d541823363fe749d", // 6
    "bf9546ed1700f387f1bd70f595c3a4bf", // 7
    "60645be5d0b08b5483b49407d2054d1d", // 8
    "a99b4ff482ac9ea0be62301757a3b062", // 9
    "6fa4ccda0f992a9c6fcdac1d1a7e0272", // 10
    "f369fc83cf6e71ddcfd1f80753ab55fd", // 11
    "6e80c853329c4a3ae89d3be24c67509a7be37aad5aa3d92c0086d22ae368193b", // 12
    "906b4c45848239de1dcee5e28fe36edc", // 13
    "fffbfad882b47bb3c6e764c75ac315fb", // 14
    "1f90c3cf258685a0eac263a391eb91af", // 15
    "a7dc941cba17e45c7f2ddfd1c3649563", // 16
    "60e1ce1bf45612fdcbaf73f34c93c84b", // 17
    "24584bc865cbbb62278abdd9e36db036", // 18
    "3b3b21e9cbf0e297f5b2a62d840e569e", // 19
    "85b2477420326d070170ca4bc3cf6de6", // 20
    "919e48ee9ba3ac3af82aa60930ec6ad7", // 21
    "16abce3f978476a51009c797f6dbd2cb", // 22
    "3f07929d2f7fab67094641060c0b4e14", // 23
    "8e25353b9600867df8ef8702a2f800cc", // 24
    "37df604e7df6372df46c42bbd31305f9", // 25
    "dfa8f9c03f92a49ef632ee32f1ad5cfacd42349dc4f5967fe0f61599876407a7", // 26
    "9bfad163f7756797ecc6c793f4993977", // 27
    "96d707b0012ca09a53c50612d7f1e697b5a1c674c1bd75d22731f05855383759", // 28
    "a82ecc3866de950070198cc2c5851f14", // 29
    "46eeda336e9cf99f191e248eb545669c", // 30
    "72b760da88dfb3c0065f19036be4daf3", // 31
    "bf5ad0cb6e855c433472f3c47e9c9d5cb204bde28fa66be0d2009a43fa9b088c", // 32
    "d59992975ab97a4e98fd75e1e0237f5f", // 33
    "50d218d5420e065fe5e610108e14b7d8", // 34
    "eaa95bf0977f3c2bf809c03179c9139c", // 35
    "ba3a5957216c57c7c69d4bf1864401b2", // 36
    "dbad03b2164ed454d5deac0294f7ca65", // 37
    "65d26b87305cf8d3c0869832ad954813", // 38
    "17d719fbc2ff4f44fb0d3f3f8a22e137", // 39
    "231961e06a165027a31a98563c3a5016", // 40
    "c6738d7081dc07860b85155ad8f198899c992fd1f198f24064ea90662717ccfd", // 41
    "fc9b12c7a8aa4cbb58af84cccc513bfc", // 42
    "4cc4777a72e42ad68691d6001881704b", // 43
    "7717820abbe8d68516f4024f843668da", // 44
    "b7f049e2bab09b0761e690e7820e058b9c88b48f72e4758ec2f062cc7ff2bd09", // 45
    "779bfe6f9620318d49fb48c14425ad88", // 46
    "81bdd1db7ad2e53e858c7e33e9dc2590", // 47
    "ea532ef40f578534d473134efe3b8e6f", // 48
    "f45359c6dbbe7a6170cae6703f492dc7", // 49
    "dfef57c40143fcf9ff0548216ffce371", // 50
    "080a6b11cda7c898d245f3116221e4484486cf97bde400fbdfac827af860a4e9", // 51
    "8304c01157174d6977b14b0aa5b47b4cdfe350f2eb00e258473c92bd76db41ef", // 52
    "dca29202f026d1a738720532c61bdd80", // 53
    "6b3d58218674c00afcc25db561be7850", // 54
    "7c162a48b82e1275d50d087d888a7536", // 55
    "eea804289c9590405a7991e06757193c", // 56
    "22e25742f59e96057ee8345bc25618ffc102e075735e8a6172dfd19812e4caf6", // 57
    "a7bc841743cfa426067c47d27d234474", // 58
    "2bc5a232027882952d60a59b92e246a9", // 59
    "15116e1f509e30fee3fab85ade9dc3a13d4ae5e938ecb3dac216ebf9f0637b9f", // 60
    "df4ea3ec66f3360471c25a2e67f9f713", // 61
    "95dc822b960fb2c248b146e0d2f1fa0037647522da1eb3cc715ea0d067e04768", // 62
    "4f0844d65e8e9cfa89472c617db09736", // 63
    "bd5acba93c4ee54d893524005aa15966", // 64
};

static string encrypted_parse_periodsep_hexstrings[] =
{
    "9ab9b358c72014ab889f85c2ce4d7985", // 0
    "4a8e4672163f443fc46678a34d983208", // 1
    "7dc0b5cbe674848cdb9ad051c28f02f2", // 2
    "9bca0807745ac83549aa27ca9c8f35fa", // 3
    "7f1a9a177fcf62564a29a7893529f756", // 4
    "9dea402d60a7bba2688f2c36ac9121ea", // 5
    "4c3aba05d7ab6232dc6a21b899466c63", // 6
    "16ec503857f183037b3f47aad2e5acb6", // 7
    "8c057803c2ea44febe490697d7165a2f", // 8
    "962c2b5e296407bd6ed485e3f26e1225", // 9
    "5b6a2f5a937bb3e8e225387415132207", // 10
    "34b8d1cf550d3742b959174b146be5cf", // 11
    "a7dc941cba17e45c7f2ddfd1c3649563", // 12
    "6f8a654b42a57e08e6d16a782713aa1a", // 13
    "4f09b6a1f0507c84371776082b4567e12e04f681ffd6b2dc98eeec0675484531", // 14
    "438aa836fe3489b5399a7b1678cfed4b8055c4f109d1d399615cb8883c6cc59f", // 15
    "460d5f25e081cb5d921a289b0aff0bb1c2a62b15bbf765eade69bb1e17c77826", // 16
};

static string encrypted_parse_colonsep_hexstrings[] =
{
    "75e3475cc1fd4f15c0c9c21d27637222865d1116e889a26f374016e18f272e2f", // 0
    "810157012571d1f89676e129461e0af7c85c3497d39a99716d26b63df07e38d4", // 1
    "007bfbba256fbcee4a6fa31fa892de28ee6f3cb18312e59011c654d57fe7e048", // 2
    "4ea45b3afb5c68b9525fa1613a6d6a14f6dc20eada29e0fbcc0a795cc3337243", // 3
    "03f1f09ba3dd7bc62f32263c334bcc85", // 4
    "4cc4777a72e42ad68691d6001881704b", // 5
    "77fc33ccf33ca6c32f91a1493cda603aba7bf25906b06f1e250507145f601acb", // 6
    "8304c01157174d6977b14b0aa5b47b4cdfe350f2eb00e258473c92bd76db41ef", // 7
    "08ff08bc5fecbef71645756cb36a7d78", // 8
    "b4f83acc653a0618d368c1e83fbdf07c", // 9
    "95edc0e385936e4e159fdfc7dd6765c156f3eb48394d0a2d3a70e78b8c470e73", // 10
    "65960b373d1039d71ada97767888e5ae0323fe91b6dbf41ba729551c7bb57472", // 11
    "653b85aac7d89c7dd762a5ce3dc9f7b1", // 12
    "f6315130ed7914a886bf00f5f7603eb12e5f7c1ec436660e689ac1b21219d7b6", // 13
    "c62925ba110ae4726a4775ecef999243026953096bbc27e4523c9361837a5f25", // 14
    "935518d9b6342c83450d24198b67c0ffa1b684ad29b2355063e625ad145f2a84", // 15
    "d5cbd5c4952c9fec3531064254993d79473cf32cd960579f9c5713050e03606b", // 16
    "5b0524f1c689efbbd7c1d8eac90de1ae8408d9564a18253be1fbd0f8d1bb44c8", // 17
    "bcc72dccacb22fb6d933af8545b3f33bcf6f83115e407efccd65e01d89305b9a", // 18
};

static string encrypted_parse_pipesep_hexstrings[] =
{
    "1572884d12c778b2e045268b34d29717", // 0
    "8304c01157174d6977b14b0aa5b47b4cdfe350f2eb00e258473c92bd76db41ef", // 1
};

static string encrypted_parse_special_hexstrings[] =
{
    "c46a577963e4d94a09fa9883404f1e513c28e19c9a288ecb8f2db12bc48202bd235c59ddb53a33fa30aacbf84f8d13eda35f1a44d4af383ea5c751735ec3edb560d19b649a744a5fad95ae9b6e13a680", // 0
    "e46590727b75bc3f93213a9b9d8092d3", // 1
    "137ae8d16a955ab56e9de720bc380b6e", // 2
    "b07118f56abd485564a6d39014faaf2f", // 3
    "1572884d12c778b2e045268b34d29717", // 4
};

void unhex_array(string hexstrings[], int nhexstrings,
		 vector<string> &raw)
{
    raw.resize(nhexstrings);
    for(int i = 0;i<nhexstrings;++i) {
	raw[i] = hex2raw(hexstrings[i]);
    }
}

struct hostgroup_defnsT {
    string encrypted;
    string group;
};

static hostgroup_defnsT hostgroup_defns[] = 
{
    // Roughly sorted in order of performance, some of the early ones
    // might be incorrect
    { "badf0582371e7dbefe193d5d523f3aed", "dedicated-bear-0" },
    { "cecc2842c468e4c51ad942d95a2537c3", "dedicated-bear-1" },
    { "202bed4b810145ec1391e4c5fd8ab363", "dedicated-bear-2" },
    { "f6c1fb207c5ba0a5f7f08637c222e53f", "dedicated-bear-3" },
    { "773a8af8ee6ffb48faf5dd4de2dcb0b2", "dedicated-bear-4" },
    { "a002b3e30a23779709da2f192c171dbe", "dedicated-bear-5" },
    { "0697f0c72a559d090ca02a7d2946ba4c", "dedicated-bear-6" },
    { "2979bc3e40d19293d68b3e019455fb3c", "dedicated-bear-7" },
    { "660d9778aee6bc3701abc195b8365437", "dedicated-bear-8" },
};

void prepEncryptedStuff()
{
    unhex_array(encrypted_parse_directory_hexstrings,
		sizeof(encrypted_parse_directory_hexstrings)/sizeof(string),
		encrypted_parse_directory);
    unhex_array(encrypted_parse_periodsep_hexstrings,
		sizeof(encrypted_parse_periodsep_hexstrings)/sizeof(string),
		encrypted_parse_periodsep);
    unhex_array(encrypted_parse_colonsep_hexstrings,
		sizeof(encrypted_parse_colonsep_hexstrings)/sizeof(string),
		encrypted_parse_colonsep);
    unhex_array(encrypted_parse_pipesep_hexstrings,
		sizeof(encrypted_parse_pipesep_hexstrings)/sizeof(string),
		encrypted_parse_pipesep);
    unhex_array(encrypted_parse_special_hexstrings,
		sizeof(encrypted_parse_special_hexstrings)/sizeof(string),
		encrypted_parse_special);

    int count = sizeof(noframerange_strings)/sizeof(string);

    for(int i=0;i<count;++i) {
	string f = hex2raw(noframerange_strings[i]);
	noframerange_map[f] = true;
    }

    count = sizeof(ok_idx_noframe_strings)/sizeof(string);
    for(int i=0;i<count;++i) {
	string f = hex2raw(ok_idx_noframe_strings[i]);
	encrypted_ok_idx_noframe[f] = true;
    }

    for(unsigned i = 0; i < sizeof(hostgroup_defns)/sizeof(hostgroup_defnsT); 
	++i) {
	encrypted_hostgroups[hex2raw(hostgroup_defns[i].encrypted)] 
	    = hostgroup_defns[i].group;
    }
}

bool
isuint(const string &a)
{
    for(unsigned i=0;i<a.size();++i) {
	if (!isdigit(a[i]))
	    return false;
    }
    return true;
}

bool
parse_framerange_addbrackets(const string &field, job_info &jinfo)
{
    string tmp_frames = "[";
    tmp_frames.append(field);
    tmp_frames.append("]");
    return parse_frameinfo(tmp_frames,jinfo);
}

bool // true on success
parse_pipesep_jobname(const string &jobname, job_info &jinfo)
{
    vector<string> fields;

    string error;
    split(jobname,str_pipe,fields);

    if (fields.size() == 9) {
	if (!isuint(fields[6])) {
	    error = "!isuint(fields[6])";
	    goto failparse;
	}
	if (!(fields[8] == empty_string ||
	      parse_frameinfo(fields[8],jinfo))) {
	    error = "noparse frames";
	    goto failparse;
	}
	jinfo.jobname_username = fields[0];
	jinfo.meta_id = uintfield(fields[1]);
	jinfo.production = fields[2];
	jinfo.sequence = fields[3];
	jinfo.shot = fields[4];
	jinfo.task = fields[5];
	if (fields[7] != jinfo.task &&
	    fields[7] != empty_string) {
	    vector<string> tasks;
	    split(fields[7],str_colon,tasks);
	    if (tasks.size() > 0 && tasks[0] == jinfo.task) {
		tasks.erase(tasks.begin());
	    }
	    if (tasks.size() > 0 && tasks[0] == jinfo.jobname_username) {
		tasks.erase(tasks.begin());
	    }
	    if (tasks.size() == 2 &&
		jinfo.production == jinfo.sequence &&
		encryptString(jinfo.production) == encrypted_parse_pipesep[0] &&
		tasks[0] == jinfo.dir_production &&
		tasks[1][0] == 's' && tasks[1][1] == 'q' && // not always seeing match between dir and task sequence, and task one looks right.
		encryptString(jinfo.task) == encrypted_parse_pipesep[1]) {
		// prod/seq encoded in task, and not in name
		jinfo.production = tasks[0];
		jinfo.sequence = tasks[1];
		jinfo.shot = empty_string; // no shot for this task
		tasks.clear();
	    }

	    if (tasks.size() >= 3 &&
		tasks[0] == jinfo.production &&
		tasks[1] == jinfo.sequence &&
		tasks[2] == jinfo.shot) {
		tasks.erase(tasks.begin(),tasks.begin()+3);
	    }
	    if (tasks.size() == 0) {
		// no subtask
	    } else if (tasks.size() == 1) {
		jinfo.subtask = tasks[0];
	    } else if (tasks.size() == 2 &&
		       parse_framerange_addbrackets(tasks[1],jinfo)) {
		jinfo.subtask = tasks[0];
	    } else if (tasks.size() == 2) {
		jinfo.subtask = tasks[0];
		jinfo.object = tasks[1];
	    } else if (tasks.size() == 3) {
		if (tasks[1].empty()) {
		    jinfo.subtask = tasks[0];
		    jinfo.object = tasks[1];
		} else {
		    if (print_parse_warnings) {
			fprintf(stderr, "confused (tasks.size() == 3) on %s\n",
				jobname.c_str());
		    }
		}
	    } else if (tasks.size() == 4) {
		if (tasks[0] == jinfo.production &&
		    tasks[1] == jinfo.sequence &&
		    tasks[2] == jinfo.shot) {
		    jinfo.subtask = tasks[3];
		} else if (encryptString(jinfo.production) ==
			   encrypted_parse_special[3] &&
			   encryptString(jinfo.sequence) ==
			   encrypted_parse_special[4] &&
			   encryptString(jinfo.shot) ==
			   encrypted_parse_special[4]) {
		    // job info bits are useless; extract from subinfo.
		    // could check for prefix for sequence, shot.
		    jinfo.production = tasks[0];
		    jinfo.sequence = tasks[1];
		    jinfo.shot = tasks[2];
		    jinfo.subtask = tasks[3];
		} else {
		    if (print_parse_warnings) {
			fprintf(stderr, "pipesep: 4 subtask mismatch (%s,%s), (%s,%s), (%s,%s) in %s\n",
				tasks[0].c_str(), jinfo.production.c_str(),
				tasks[1].c_str(), jinfo.sequence.c_str(),
				tasks[2].c_str(), jinfo.shot.c_str(),
				jobname.c_str());
		    }
		}
	    } else {
		error = "too many subtasks in fields[7]";
		// ought to clean up, but for now, who cares
		goto failparse;
	    }
	}
	return true;
    }

    if (fields.size() == 11 &&
	isuint(fields[6]) &&
	fields[5] == fields[7] &&
	parse_frameinfo(fields[10],jinfo) &&
	parse_framerange_addbrackets(fields[9],jinfo)) {
	jinfo.jobname_username = fields[0];
	jinfo.meta_id = uintfield(fields[1]);
	jinfo.production = fields[2];
	jinfo.sequence = fields[3];
	jinfo.shot = fields[4];
	jinfo.task = fields[5];
	jinfo.subtask = fields[9];
	return true;
    }

    error = "unknown";
    // PIPESEP
 failparse:
    if (fields.size() > 1) {
	if (print_parse_warnings) {
	    fprintf(stderr,"pipesep failed to parse(%s) %s (%d)\n",
		    error.c_str(), jobname.c_str(),fields.size());
	    for(unsigned i=0;i<fields.size();++i) {
		fprintf(stderr, "  field %d: %s  // %s\n",i,fields[i].c_str(),
			hexstring(encryptString(fields[i])).c_str());
	    }
	}
    }
    return false;
}

bool // true on success
parse_colonsep_jobname(const string &jobname, const string &jobdirectory,
		       job_info &jinfo)
{
    vector<string> fields;
    unsigned start_offset = 0;
    //    printf("XX %s\n",jobname.c_str());
    while (start_offset < jobname.size()) {
	bool inbracket = jobname[start_offset] == '[';
	unsigned end_offset = start_offset;
	for(;end_offset < jobname.size();++end_offset) {
	    if (inbracket) {
		if (jobname[end_offset] == ']') {
		    end_offset += 1; // want to include the close \]
		    inbracket = false;
		}
	    } else {
		if (jobname[end_offset] == ':') break;
	    }
	}
	string foo = jobname.substr(start_offset,end_offset - start_offset);
	if (false) cout << boost::format("  ColonSep JobName field %d: %s\n") % fields.size() % foo.c_str();
	fields.push_back(foo);
	start_offset = end_offset + 1;
    }
    if (fields.size() > 5 &&
	(fields[5] == "0" || fields[5] == "1" || fields[5] == "2")) {
	fields.erase(fields.begin() + 5,fields.begin() + 6);

	if ((fields.size() == 8 || fields.size() == 9) &&
	    encryptString(fields[4]) == encrypted_parse_colonsep[0]) {
	    string f5enc = encryptString(fields[5]);
	    if (f5enc == encrypted_parse_colonsep[0] ||
		f5enc == encrypted_parse_colonsep[1] ||
		f5enc == encrypted_parse_colonsep[2]) {
		jinfo.meta_id = uintfield(fields[0]);
		jinfo.production = fields[1];
		jinfo.sequence = fields[2];
		jinfo.shot = fields[3];
		jinfo.task = fields[4];
		// parsing frame range for this type of entry doesn't quite
		// make sense, but if it did, either fields 7 or 8 would
		// be right, 7 has the real range, 8 has the batching
		if (f5enc == encrypted_parse_colonsep[2]) {
		    jinfo.object = fields[5];
		    jinfo.frames = fields[6];
		} else {
		    jinfo.object = fields[6];
		    jinfo.frames = fields[7];
		}
		return true;
	    }
	}

	if (fields.size() == 11 &&
	    fields[1] == jinfo.dir_production &&
	    fields[2] == jinfo.dir_sequence &&
	    fields[3] == jinfo.dir_shot &&
	    fields[6] == fields[8] &&
	    parse_framerange_addbrackets(fields[10],jinfo)) {

	    jinfo.production = fields[1];
	    jinfo.sequence = fields[2];
	    jinfo.shot = fields[3];
	    jinfo.task = fields[4];
	    jinfo.subtask = fields[5];
	    jinfo.object = fields[6];
	    return true;
	}

	if (fields.size() == 7 && parse_frameinfo(fields[6],jinfo)) {
	    jinfo.meta_id = uintfield(fields[0]);
	    jinfo.production = fields[1];
	    jinfo.sequence = fields[2];
	    jinfo.shot = fields[3];
	    jinfo.task = fields[4];
	    jinfo.subtask = fields[5];
	    return true;
	}
	if (fields.size() == 6 && parse_frameinfo(fields[5],jinfo)) {
	    jinfo.meta_id = uintfield(fields[0]);
	    jinfo.production = fields[1];
	    jinfo.sequence = fields[2];
	    jinfo.shot = fields[3];
	    jinfo.task = fields[4];
	    return true;
	}
    }
    if ((fields.size() == 5 || fields.size() == 6) &&
	fields[1] == jinfo.dir_production) {
	bool *v = noframerange_map.lookup(encryptString(fields[4]));
	if (v != NULL) {
	    AssertAlways(*v,("internal"));
	    jinfo.meta_id = uintfield(fields[0]);
	    jinfo.production = fields[1];
	    jinfo.sequence = fields[2];
	    jinfo.shot = fields[3];
	    jinfo.task = fields[4];
	    if (fields.size() == 6) {
		jinfo.subtask = fields[5];
	    }
	    return true;
	}
    }
    if (fields.size() == 7 &&
	encryptString(fields[4]) == encrypted_parse_colonsep[0]) {
	jinfo.meta_id = uintfield(fields[0]);
	jinfo.production = fields[1];
	jinfo.sequence = fields[2];
	jinfo.shot = fields[3];
	jinfo.task = fields[4];
	jinfo.subtask = fields[5];
	jinfo.object = fields[6];
	return true;
    }
    if ((fields.size() == 5 || fields.size() == 6) &&
	isuint(fields[0]) &&
	fields[2] == jinfo.dir_sequence &&
	fields[3] == jinfo.dir_shot) {
	jinfo.meta_id = uintfield(fields[0]);
	jinfo.production = fields[1];
	jinfo.sequence = fields[2];
	jinfo.shot = fields[3];
	jinfo.task = fields[4];
	if (fields.size() == 6) {
	    jinfo.subtask = fields[5];
	}
	return true;
    }

    if (fields.size() == 5 && fields[0] == empty_string &&
	encryptString(fields[1]) == encrypted_parse_colonsep[3] &&
	isuint(fields[2]) && parse_frameinfo(fields[4],jinfo)) {
	jinfo.task = fields[3];
	return true;
    }

    if (fields.size() == 5 &&
	encryptString(fields[2]) == encrypted_parse_colonsep[4] &&
	isuint(fields[0])) {
	jinfo.meta_id = uintfield(fields[0]);
	jinfo.task = fields[1];
	return true;
    }

    bool meta_pss = false;
    if (fields.size() >= 4 && isuint(fields[0]) &&
	fields[1] == jinfo.dir_production &&
	fields[2] == jinfo.dir_sequence && fields[3] == jinfo.dir_shot) {
	meta_pss = true;
    }

    if ((fields.size() == 10 || fields.size() == 11) && meta_pss &&
	fields[5] == fields[7] && isuint(fields[8]) &&
	encryptString(fields[5]) == encrypted_parse_colonsep[5] &&
	parse_framerange_addbrackets(fields[9],jinfo)) {
	jinfo.meta_id = uintfield(fields[0]);
	jinfo.task = fields[4];
	jinfo.subtask = fields[5];
	return true;
    }

    if (fields.size() == 8 && meta_pss &&
	encryptString(fields[4]) == encrypted_parse_colonsep[6] &&
	parse_framerange_addbrackets(fields[5],jinfo)) {
	jinfo.task = fields[4];
	return true;
    }

    if (fields.size() == 3 && fields[2] == jinfo.sequence &&
	encryptString(fields[0]) == encrypted_parse_colonsep[7]) {
	jinfo.task = fields[0];
	jinfo.subtask = fields[1];
	return true;
    }

    if (fields.size() == 6 && fields[1] == jinfo.production &&
	encryptString(fields[0]) == encrypted_parse_colonsep[7] &&
	encryptString(fields[4]) == encrypted_parse_colonsep[5]) {
	jinfo.task = fields[0];
	jinfo.sequence = fields[2];
	jinfo.shot = fields[3];
	jinfo.subtask = fields[4];
	return true;
    }

    if (fields.size() == 5 && fields[1] == jinfo.production &&
	fields[2] == jinfo.sequence && fields[3] == jinfo.shot &&
	encryptString(fields[0]) == encrypted_parse_colonsep[7] &&
	encryptString(fields[4]) == encrypted_parse_colonsep[5]) {
	jinfo.task = fields[0];
	return true;
    }

    if (fields.size() == 3 && fields[1] == jinfo.production &&
	encryptString(fields[0]) == encrypted_parse_colonsep[7] &&
	jinfo.sequence == empty_string) {
	jinfo.sequence = fields[2];
	return true;
    }

    if (fields.size() == 4 && fields[2] == jinfo.sequence &&
	encryptString(fields[0]) == encrypted_parse_colonsep[7] &&
	fields[3][0] == 's') {
	jinfo.task = fields[0];
	jinfo.shot = fields[3];
	return true;
    }

    if (fields.size() == 6 && fields[1] == jinfo.production &&
	fields[2] == jinfo.sequence && fields[3] == jinfo.shot &&
	encryptString(fields[0]) == encrypted_parse_colonsep[7]) {
	jinfo.task = fields[0];
	jinfo.subtask = fields[4];
	jinfo.object = fields[5];
	return true;
    }

    if (fields.size() == 5 && fields[1] == jinfo.production &&
	fields[2] == jinfo.sequence && fields[3] == jinfo.shot &&
	encryptString(fields[0]) == encrypted_parse_colonsep[7]) {
	jinfo.task = fields[0];
	jinfo.subtask = fields[4];
	return true;
    }

    if (fields.size() == 3 &&
	encryptString(fields[0]) == encrypted_parse_colonsep[7] &&
	fields[2][0] == 's' && fields[2][1] == 'q') {
	jinfo.sequence = fields[2];
	jinfo.task = fields[0];
	return true;
    }

    if (fields.size() == 5 &&
	encryptString(fields[0]) == encrypted_parse_colonsep[7] &&
	fields[2] == jinfo.sequence &&
	fields[3][0] == 's' &&
	encryptString(fields[4]) == encrypted_parse_colonsep[8]) {
	jinfo.shot = fields[3];
	jinfo.task = fields[0];
	return true;
    }

    if (fields.size() == 4 &&
	fields[0] == str_java_service &&
	fields[0] == jinfo.production &&
	parse_frameinfo(fields[3],jinfo)) {
	jinfo.sequence = fields[1];
	jinfo.shot = fields[2];
	return true;
    }

    if (fields.size() == 8 && fields[0] == str_batch_parallel &&
	fields[3] == str_BPMake && parse_frameinfo(fields[7],jinfo)) {
	jinfo.sequence = fields[3];
	jinfo.task = fields[5];
	return true;
    }

    if (fields.size() == 3 && fields[0] == str_sstress &&
	parse_frameinfo(fields[2],jinfo)) {
	jinfo.sequence = fields[0];
	jinfo.task = fields[1];
	return true;
    }

    if (fields.size() == 7 &&
	encryptString(fields[4]) == encrypted_parse_colonsep[3] &&
	fields[3].size() > fields[2].size() &&
	fields[3].substr(0,fields[2].size()) == fields[2] &&
	encryptString(fields[5]) == encrypted_parse_colonsep[9] &&
	parse_frameinfo(fields[6],jinfo)) {
	jinfo.production = fields[1];
	jinfo.sequence = fields[2];
	jinfo.task = fields[3];
	return true;
    }

    if (fields.size() == 7 &&
	fields[0] == empty_string &&
	fields[1] == jinfo.dir_production &&
	fields[2] == jinfo.dir_sequence &&
	fields[3] == jinfo.dir_shot &&
	encryptString(fields[4]) == encrypted_parse_colonsep[10] &&
	isuint(fields[6])) {
	jinfo.task = fields[4];
	jinfo.subtask = fields[5];
	return true;
    }

    if (fields.size() == 7 &&
	fields[0] == empty_string &&
	fields[1] == jinfo.dir_production &&
	fields[2] == jinfo.dir_sequence &&
	fields[3] == jinfo.dir_shot &&
	encryptString(fields[4]) == encrypted_parse_colonsep[3] &&
	parse_frameinfo(fields[6],jinfo)) {
	jinfo.task = fields[5];
	return true;
    }

    if (fields.size() == 5 && isuint(fields[0]) &&
	fields[1] == jinfo.dir_production &&
	fields[3] == jinfo.dir_shot) {
	jinfo.sequence = fields[2];
	jinfo.task = fields[4];
	return true;
    }

    if (fields.size() == 5 && isuint(fields[0]) &&
	fields[1] == jinfo.dir_production &&
	fields[2].size() > jinfo.dir_sequence.size() &&
	fields[2].substr(0,jinfo.dir_sequence.size()) == jinfo.dir_sequence) {
	jinfo.sequence = fields[2];
	jinfo.shot = fields[3];
	jinfo.task = fields[4];
	return true;
    }

    if (fields.size() == 5 && fields[0].size() >= 12 &&
	encmatch(fields[0].substr(0,12),encrypted_parse_colonsep[11]) &&
	fields[2] == jinfo.shot && fields[3].size() >= 4 &&
	encmatch(fields[3].substr(0,4),encrypted_parse_colonsep[12]) &&
	parse_frameinfo(fields[4],jinfo)) {
	jinfo.task = fields[0];
	return true;
    }

    if (fields.size() == 2 &&
	encmatch(fields[0],encrypted_parse_colonsep[13]) &&
	fields[1].size() > jobdirectory.size() &&
	fields[1].substr(0,jobdirectory.size()) == jobdirectory) {
	int off = jobdirectory.size() + 1;
	AssertAlways(off < (int)fields[1].size(),("internal"));
	jinfo.task = fields[0];
	jinfo.subtask = fields[1].substr(off,fields[1].size() - off);
	return true;
    }

    if (fields.size() == 5 && fields[0].size() >= 12 &&
	encmatch(fields[0].substr(0,12),encrypted_parse_colonsep[11]) &&
	fields[2] == jinfo.shot && parse_frameinfo(fields[4],jinfo)) {
	jinfo.task = fields[0];
	jinfo.subtask = fields[3];
	return true;
    }

    if (fields.size() == 5 && fields[0].size() >= 9 &&
	encmatch(fields[0].substr(0,9),encrypted_parse_colonsep[14]) &&
	fields[2] == jinfo.shot && parse_frameinfo(fields[4],jinfo)) {
	jinfo.task = fields[0];
	jinfo.subtask = fields[3];
	return true;
    }

    if (fields.size() == 4 &&
	encmatch(fields[0],encrypted_parse_colonsep[15]) &&
	isuint(fields[2]) && parse_frameinfo(fields[3],jinfo)) {
	jinfo.task = fields[0];
	return true;
    }

    if (fields.size() == 6 && fields[0].size() > 11 &&
	encmatch(fields[0].substr(0,11),encrypted_parse_colonsep[16]) &&
	fields[2] == jinfo.sequence && fields[3] == jinfo.shot &&
	parse_frameinfo(fields[5],jinfo)) {
	jinfo.task = fields[0];
	jinfo.subtask = fields[4];
	return true;
    }

    if (fields.size() == 5 &&
	encmatch(fields[0],encrypted_parse_colonsep[15]) &&
	fields[2] == jinfo.shot && fields[3].size() > 4 &&
	encmatch(fields[3].substr(0,4),encrypted_parse_colonsep[12]) &&
	parse_frameinfo(fields[4],jinfo)) {
	jinfo.task = fields[0];
	return true;
    }

    if (fields.size() == 5 &&
	encmatch(fields[0],encrypted_parse_colonsep[17]) &&
	fields[2] == jinfo.shot && fields[3].size() > 4 &&
	encmatch(fields[3].substr(0,4),encrypted_parse_colonsep[12]) &&
	parse_frameinfo(fields[4],jinfo)) {
	jinfo.task = fields[0];
	return true;
    }

    if (fields.size() == 5 && fields[0].size() >= 15 &&
	encmatch(fields[0].substr(0,15),encrypted_parse_colonsep[18]) &&
	fields[2] == jinfo.shot && fields[3].size() > 4 &&
	encmatch(fields[3].substr(0,4),encrypted_parse_colonsep[12]) &&
	parse_frameinfo(fields[4],jinfo)) {
	jinfo.task = fields[0].substr(0,14);
	jinfo.subtask = fields[0].substr(15,fields[0].size() - 15);
	return true;
    }

    if (fields.size() == 3 && fields[0] == str_perl_service2 &&
	jinfo.dir_production == str_tuscany &&
	parse_frameinfo(fields[2],jinfo)) {
	jinfo.production = str_tuscany;
	jinfo.sequence = str_perl_service2;
	jinfo.task = jinfo.dir_shot;
	jinfo.dir_shot = jinfo.shot = fields[1];
	return true;
    }

    // COLONSEP PARSE -- for finding place to add parsing in code

    if (print_parse_warnings && fields.size() > 2) {
	fprintf(stderr,"colonsep failed to parse %s (%d)\n",
		jobname.c_str(),fields.size());
	for(unsigned i=0;i<fields.size();++i) {
	    fprintf(stderr, "  field %d: %s  // %s\n",i,fields[i].c_str(),
		    hexstring(encryptString(fields[i])).c_str());
	}
    }
    return false;
}

bool xpcre_check_encrypted(const string &instr, int *ovector, int rc, int stringnum,
			   const string &match_encrypted)
{
    string str;
    xpcre_get_substring(instr,ovector,rc,stringnum,str);
    if (encryptString(str) == match_encrypted) {
	return true;
    } else {
	if (print_parse_warnings) {
	    fprintf(stderr,"******* Unable to match %s -> %s == %d/%s in %s\n",
		    str.c_str(),hexstring(encryptString(str)).c_str(),
		    match_encrypted.size(),hexstring(match_encrypted).c_str(),
		    instr.c_str());
	}
	return false;
    }
}

pcre *xpcre_compile(const string &regex)
{
    const char *errptr;
    int erroffset;

    pcre *ret = pcre_compile(regex.c_str(),0,&errptr,&erroffset,NULL);
    AssertAlways(ret != NULL,("pcre compile of %s failed at: %s\n",
			      regex.c_str(),errptr));
    return ret;
}

bool
match_directory_str13(const string &part)
{
    if (part.size() == 3 &&
	encryptString(part) == encrypted_parse_directory[13]) {
	return true;
    }
    if (part.size() != 4)
	return false;
    if (!isdigit(part[3]))
	return false;
    return encryptString(part.substr(0,3)) == encrypted_parse_directory[13];
}

void
trimstring(string &str,const string &trimafter)
{
    int idx = str.find(trimafter);
    if (idx == -1)
	return;
    str = str.substr(0,idx);
}

// common operations, but can't implement as functions, but don't want
// as all caps because that will look ugly

#define encdirmatch(x,y) (encrypted_strs[(x)] == encrypted_parse_directory[(y)])
#define noretdirpss(a,b,c) jinfo.dir_production = parts[(a)]; \
  jinfo.dir_sequence = parts[(b)]; \
  jinfo.dir_shot = parts[(c)];
#define dirpss(a,b,c) noretdirpss(a,b,c); return true;
#define dirps(a,b) jinfo.dir_production = parts[(a)]; \
  jinfo.dir_sequence = parts[(b)]; \
  jinfo.dir_shot = empty_string; \
  return true;
#define sqsmatch(x,y) (parts[(x)][0] == 's' && parts[(x)][1] == 'q' && \
		       parts[(y)][0] == 's')

const string str_slash("/");
bool
try_parse_directory_jobinfo(const string &jobdirectory, job_info &jinfo)
{
    vector<string> parts;
    split(jobdirectory,str_slash,parts);
    if (parts[0] != empty_string) {
	return false; // non absolute path, time to give up
    }

    parts.erase(parts.begin());

    vector<string> encrypted_strs;
    encrypted_strs.resize(parts.size());
    for(unsigned i=0;i<parts.size();++i) {
	  encrypted_strs[i] = encryptString(parts[i]);
    }

    if (parts.size() > 1 && encdirmatch(0,8)) {
	parts.erase(parts.begin());
	encrypted_strs.erase(encrypted_strs.begin());
    }

    if (parts.size() == 6 && encdirmatch(0,0) && encdirmatch(3,1)) {
	dirpss(1,4,5);
    }
    if (parts.size() == 9 && encdirmatch(0,0) && encdirmatch(3,2) &&
	encdirmatch(4,3) && encdirmatch(8,4)) {
	dirpss(1,6,7);
    }

    if (parts.size() == 10 && encdirmatch(0,0) && encdirmatch(3,2) &&
	encdirmatch(4,3) && encdirmatch(9,4)) {
	// sequence could be 7 also, but I think 6 makes a little more sense
	// shot as 8 needed to get match with job names
	dirpss(1,6,8);
    }

    if (parts.size() == 8 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,1)) {
	dirpss(3,6,7);
	return true;
    }

    if (parts.size() == 10 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,2) && encdirmatch(6,3) &&	encdirmatch(9,4)) {
	// decode seq/shot into two possible forms that show up in job names
	// for consistency with next form
	noretdirpss(3,8,8);
	jinfo.dir_sequence.append(str_slash).append(parts[9]);
	return true;
    }

    if (parts.size() == 10 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,2) && encdirmatch(6,3) &&
	(parts[9].size() > 4 && parts[9][4] == '_' &&
	 encryptString(parts[9].substr(0,4)) == encrypted_parse_directory[4])) {
	// decode seq/shot into two possible forms that show up in job names
	noretdirpss(3,8,8);
	jinfo.dir_sequence.append(str_slash).append(parts[9]);
	jinfo.dir_shot.append(parts[9].substr(4,parts[9].size()-4));
	return true;
    }

    if (parts.size() == 11 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,2) && encdirmatch(6,3) &&	encdirmatch(10,4)) {
	dirpss(3,8,9);
    }

    if (parts.size() == 8 && encdirmatch(0,5) && encdirmatch(2,7) &&
	encdirmatch(5,1)) {
	dirpss(3,6,7);
    }

    if (parts.size() == 7 && encdirmatch(0,5) && encdirmatch(2,7) &&
	encdirmatch(4,9) && encdirmatch(5,10)) {
	// these productions appear to only have one sequence
	dirpss(3,5,6);
    }

    if (parts.size() == 8 && encdirmatch(0,0) && encdirmatch(2,11) &&
	encdirmatch(3,4)) {
	// sequence could also be parts[6], but 5 makes a little more sense
	dirpss(1,5,7);
    }

    if (parts.size() == 7 && encdirmatch(0,0) && encdirmatch(2,12) &&
	encdirmatch(3,4)) {
	noretdirpss(1,4,6);
	jinfo.dir_sequence.append(str_slash).append(parts[5]);
	return true;
    }

    if (parts.size() == 5 && encdirmatch(0,0) && encdirmatch(2,13) &&
	encdirmatch(4,14)) {
	dirpss(1,3,4);
    }

    if (parts.size() == 6 && encdirmatch(0,0) && encdirmatch(3,4) &&
	encdirmatch(4,15)) {
	// same problem as above for the only slightly makes sense,
	// could also be 8
	dirpss(1,4,5);
    }

    if (parts.size() == 7 && encdirmatch(0,0) && encdirmatch(2,11) &&
	encdirmatch(3,4)) {
	dirpss(1,5,6);
    }

    if (parts.size() == 8 && encdirmatch(0,0) && encdirmatch(3,4) &&
	encdirmatch(4,16)) {
	dirpss(1,5,7);
    }

    if (parts.size() == 7 && encdirmatch(0,0) && encdirmatch(3,4) &&
	encdirmatch(4,17)) {
	dirpss(1,5,6);
    }

    if (parts.size() == 5 && encdirmatch(0,0) && encdirmatch(2,12) &&
	encdirmatch(3,1)) {
	dirps(1,4);
    }

    if (parts.size() == 8 && encdirmatch(0,0) && encdirmatch(2,13) &&
	sqsmatch(3,4)) {
	dirpss(1,3,4);
    }

    if (parts.size() == 6 && encdirmatch(0,0) && encdirmatch(3,4) &&
	encdirmatch(4,16)) {
	// could make sequence parts[5] also, only weakly this part
	dirpss(1,4,5);
    }

    if (parts.size() == 8 && encdirmatch(0,0) && encdirmatch(3,2) &&
	encdirmatch(4,3) && encdirmatch(7,4)) {
	// oddly, the sample for this example is actually classified
	// with the wrong production, but that was the directory it
	// ran out of.
	dirpss(1,6,6);
    }

    if (parts.size() == 7 && encdirmatch(0,0) && encdirmatch(2,11) &&
	encdirmatch(3,18) && encdirmatch(4,19)) {
	dirpss(1,5,6);
    }

    if (parts.size() == 8 && encdirmatch(0,0) && encdirmatch(2,11) &&
	encdirmatch(3,2)) {
	// sequence/shot for this is a little goofy
	dirpss(1,6,7);
    }

    if (parts.size() == 5 && encdirmatch(0,0) && encdirmatch(2,20) &&
	encdirmatch(3,1)) {
	dirps(1,4);
    }

    if (parts.size() == 8 && encdirmatch(0,0) && encdirmatch(2,13) &&
	encdirmatch(3,1)) {
	dirpss(1,4,5);
    }

    if (parts.size() == 7 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) &&
	(int)parts[4].find("_") > 0 &&
	sqsmatch(5,6)) {
	jinfo.dir_production = parts[4].substr(0,parts[4].find("_"));
	jinfo.dir_sequence = parts[5];
	jinfo.dir_shot = parts[6];
	return true;
    }

    if (parts.size() == 9 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,18) && encdirmatch(6,19)) {
	dirpss(3,7,8);
    }

    if (parts.size() == 12 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,2) && encdirmatch(6,3) && encdirmatch(11,4)) {
	noretdirpss(3,8,10);
	jinfo.dir_sequence.append(str_slash).append(parts[9]);
	return true;
    }

    if (parts.size() == 9 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,2) && encdirmatch(6,22)) {
	dirpss(3,7,8);
    }

    if (parts.size() == 8 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,23) && encdirmatch(6,16)) {
	// duplicate seq/shot to match with next form
	dirpss(3,7,7);
    }

    if (parts.size() == 9 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,23) && encdirmatch(6,16)) {
	dirpss(3,7,8);
    }

    if (parts.size() == 8 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,2) && encdirmatch(6,24)) {
	// same seq/shot, nothing else better
	dirpss(3,7,7);
    }

    if (parts.size() == 9 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,2) && encdirmatch(7,24)) {
	// same seq/shot, nothing else better
	dirpss(3,8,8);
    }

    if (parts.size() == 9 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(6,1)) {
	dirpss(4,7,8);
    }

    if (parts.size() == 8 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(5,1)) {
	dirpss(4,6,7);
    }

    if (parts.size() == 11 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,2) && encdirmatch(6,3)) {
	dirpss(3,8,9);
    }

    if (parts.size() == 9 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(4,13) && encdirmatch(5,1)) {
	dirpss(3,6,7);
    }

    if (parts.size() == 7 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(3,25) && encdirmatch(4,1) &&
	parts[6][0] == 's' && parts[6][1] == 'q') {
	int dot = parts[6].find(".");
	if (dot > 0 && (dot + 2) < (int)parts[6].size() && parts[6][dot+1] == 's') {
	    jinfo.dir_production = parts[4];
	    jinfo.dir_sequence = parts[6].substr(0,dot);
	    jinfo.dir_shot = parts[6].substr(dot+1,parts[6].size()-(dot+1));
	    return true;
	}
    }

    if (parts.size() == 7 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(5,10)) {
	jinfo.dir_production = parts[4];
	int idx = jinfo.dir_production.find("_");
	if (idx > 0) {
	    jinfo.dir_production = jinfo.dir_production.substr(0,idx);
	}
	jinfo.dir_sequence = parts[4]; // these productions appear to only have one sequence, but preserve _ bit
	jinfo.dir_shot = parts[6];
	return true;
    }

    if (parts.size() == 7 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(5,1)) {
	jinfo.dir_production = parts[4];
	int idx = jinfo.dir_production.find("_");
	if (idx > 0) {
	    jinfo.dir_production = jinfo.dir_production.substr(0,idx);
	}
	jinfo.dir_sequence = parts[4]; // these productions appear to only have one sequence, but preserve _ bit
	jinfo.dir_shot = parts[6];
	return true;
    }

    if (parts.size() == 8 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(5,9) &&
	encdirmatch(6,10)) {
	// these productions appear to only have one sequence
	dirpss(4,6,7);
    }

    if (parts.size() == 11 && encdirmatch(0,5) && encdirmatch(2,8) &&
	encdirmatch(3,5) && encdirmatch(5,21) &&
	match_directory_str13(parts[6]) && encdirmatch(8,1)) {
	dirpss(7,9,10);
    }

    if (parts.size() == 6 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(4,26)) {
	// either a special case, or a case where the production name isn't
	// in the pathname
	dirpss(4,4,5);
    }

    if (parts.size() == 11 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(5,23) &&
	encdirmatch(6,16)) {
	dirpss(4,9,10);
    }

    if (parts.size() == 6 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(5,1) &&
	(int)parts[4].find("_") > 0) {
	jinfo.dir_production = parts[4].substr(0,parts[4].find("_"));
	jinfo.dir_sequence = jinfo.dir_shot = empty_string;
	return true;
    }

    if (parts.size() == 7 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(4,1)) {
	dirpss(3,5,6);
    }

    if (parts.size() == 11 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(5,2) &&
	encdirmatch(7,16)) {
	dirpss(4,9,10); // kinda arbitrary, could be 4,8,9 also
    }

    if (parts.size() == 6 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && match_directory_str13(parts[4])) {
	jinfo.dir_production = parts[5]; // only have a production here
	jinfo.dir_sequence = jinfo.dir_shot = empty_string;
	return true;
    }

    if (parts.size() == 9 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(5,1) &&
	(int)parts[4].find("_") > 0) {
	noretdirpss(4,6,7);
	jinfo.dir_production = parts[4].substr(0,parts[4].find("_"));
	return true;
    }

    if (parts.size() == 8 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(5,23)) {
	dirpss(4,6,7); // this one is a little odd.
    }

    if (parts.size() == 7 && encdirmatch(0,5) && encdirmatch(2,7) &&
	encdirmatch(5,10)) {
	dirpss(3,5,6); // only one sequence here
    }

    if (parts.size() == 6 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(4,27)) {
	dirps(4,5); // ?? not clear what to choose
    }

    if (parts.size() == 9 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(5,23)) {
	dirpss(6,7,8); // could also be 4,7,8
    }

    if (parts.size() == 2 && encdirmatch(0,21) && encdirmatch(1,28)) {
	return false; // no information
    }

    if (parts.size() == 10 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(4,13) && encdirmatch(5,1) && encdirmatch(9,29)
	&& (int)parts[7].find("_") > 0) {
       	noretdirpss(3,6,7);
	trimstring(jinfo.dir_shot,"_");
	return true;
    }

    if (parts.size() == 10 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(4,6) &&
	encdirmatch(7,1)) {
	dirpss(5,8,9);
    }

    if (parts.size() == 10 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,2) && encdirmatch(6,3) && encdirmatch(9,30)) {
	dirpss(3,7,8); // 3,8,8 would also make sense
    }

    if (parts.size() == 8 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,31)) {
	dirpss(3,6,7);
    }

    if (parts.size() == 8 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,2) && encdirmatch(6,32)) {
	jinfo.dir_production = parts[3];
	jinfo.dir_sequence = jinfo.dir_shot = empty_string;
	return true;
    }

    if (parts.size() == 9 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(3,13) && encdirmatch(5,13) && encdirmatch(6,1)) {
	dirpss(4,7,8);
    }

    if (parts.size() == 10 && encdirmatch(0,5) && encdirmatch(2,6) &&
	encdirmatch(5,1) && encdirmatch(8,16)) {
	dirpss(3,6,7);
    }

    if (parts.size() == 8 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(4,33)) {
	noretdirpss(6,7,7); // not really a production
	trimstring(jinfo.dir_shot,str_underbar);
	return true;
    }

    if (parts.size() == 7 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(4,34)) {
	dirpss(4,5,6); // not really a production
    }

    if (parts.size() == 6 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && encdirmatch(4,35)) {
	dirps(4,5); // not really a production
    }

    if (parts.size() == 5 && encdirmatch(0,21) &&
	match_directory_str13(parts[1]) && encdirmatch(3,1)) {
	dirpss(2,2,4); // only one sequence in this production
    }

    if (parts.size() == 4 && encdirmatch(0,0) &&
	encdirmatch(1,36) && encdirmatch(2,37)) {
	dirps(1,3); // not really a production
    }

    if (parts.size() == 7 && encdirmatch(0,5) &&
	encdirmatch(2,38) && match_directory_str13(parts[3])) {
	noretdirpss(4,5,6);
	trimstring(jinfo.dir_production,str_underbar);
	return true;
    }

    if (parts.size() == 6 && encdirmatch(0,0) &&
	encdirmatch(2,13) && encdirmatch(3,39)) {
	dirps(1,4);
    }

    if (parts.size() == 7 && encdirmatch(0,0) &&
	encdirmatch(2,13) && encdirmatch(3,39)) {
	dirpss(1,4,6);
    }

    if (parts.size() == 5 && encdirmatch(0,0) &&
	encdirmatch(2,13) && sqsmatch(3,4)) {
	dirpss(1,3,4);
    }

    if (parts.size() == 7 && encdirmatch(0,0) && encdirmatch(3,1) &&
	sqsmatch(4,5)) {
	dirpss(1,4,5);
    }

    if (parts.size() == 4 && encdirmatch(0,0) && encdirmatch(2,40) &&
	encdirmatch(3,41)) {
	dirpss(1,2,3); // not really a sequence/shot
    }

    if (parts.size() == 7 && encdirmatch(0,0) && encdirmatch(2,12) &&
	encdirmatch(3,2) && encdirmatch(4,42)) {
	dirpss(1,5,6);
    }

    if (parts.size() == 6 && encdirmatch(0,0) &&
	match_directory_str13(parts[2]) &&
	(int)parts[3].find(str_underbar) > 0) {
	noretdirpss(3,4,5); // not really sequence/shot
	trimstring(jinfo.dir_production,str_underbar);
	return true;
    }

    if (parts.size() == 9 && encdirmatch(0,0) && encdirmatch(2,11) &&
	encdirmatch(3,1) && sqsmatch(4,5) && encdirmatch(6,16)) {
	dirpss(1,4,5);
    }

    if (parts.size() == 7 && encdirmatch(0,0) &&
	match_directory_str13(parts[2]) && sqsmatch(3,4) &&
	encdirmatch(6,43)) {
	dirpss(1,3,4);
    }

    if (parts.size() == 7 && encdirmatch(0,0) && encdirmatch(3,4) &&
	encdirmatch(4,16)) {
	dirpss(1,5,6);
    }

    if (parts.size() == 4 && encdirmatch(0,0) && encdirmatch(2,44)) {
	dirps(1,3); // not really a sequence
    }

    if (parts.size() == 5 && encdirmatch(0,0) &&
	match_directory_str13(parts[2]) && encdirmatch(4,45) &&
	parts[3].find("_")) {
	jinfo.dir_production = parts[3];
	trimstring(jinfo.dir_production,str_underbar);
	return true;
    }

    if (parts.size() == 9 && encdirmatch(0,0) && encdirmatch(2,13) &&
	sqsmatch(3,4) && encdirmatch(7,46)) {
	dirpss(1,3,4);
    }

    if (parts.size() == 7 && encdirmatch(0,0) && encdirmatch(2,47) &&
	sqsmatch(3,4) && encdirmatch(5,16)) {
	dirpss(1,3,4);
    }

    if (parts.size() == 7 && encdirmatch(0,0) && encdirmatch(3,4) &&
	encdirmatch(4,48)) {
	dirpss(1,5,6);
    }

    if (parts.size() == 7 && encdirmatch(0,0) && encdirmatch(2,13) &&
	sqsmatch(3,4) && encdirmatch(6,46)) {
	dirpss(1,3,4);
    }

    if ((parts.size() == 5 || parts.size() == 6) &&
	encdirmatch(0,0) && encdirmatch(2,40) && encdirmatch(3,49)) {
	dirps(1,4);
    }

    if (parts.size() == 8 && encdirmatch(0,0) && encdirmatch(2,11) &&
	encdirmatch(3,1) && sqsmatch(4,5) && encdirmatch(6,16)) {
	dirpss(1,4,5);
    }

    if (parts.size() == 6 && encdirmatch(0,0) && encdirmatch(2,40) &&
	sqsmatch(3,4)) {
	dirpss(1,3,4);
    }

    if (parts.size() == 3 && encdirmatch(0,0) && encdirmatch(2,12)) {
	jinfo.dir_production = parts[1];
	return true;
    }

    if (parts.size() == 6 && encdirmatch(0,0) && encdirmatch(3,4)) {
	dirpss(1,4,5);
    }

    if (parts.size() == 8 && encdirmatch(0,0) && encdirmatch(2,50) &&
	encdirmatch(4,51) && encdirmatch(6,52)) {
	dirps(1,5); // no shot
    }

    if (parts.size() == 7 && encdirmatch(2,53) && encdirmatch(3,54) &&
	encdirmatch(5,52)) {
	dirps(1,4); // no shot
    }

    if (parts.size() == 10 && encdirmatch(0,0) && encdirmatch(2,53) &&
	encdirmatch(3,55) && encdirmatch(8,52)) {
	dirpss(1,6,7);
    }

    if (parts.size() == 5 && encdirmatch(0,0) && encdirmatch(2,50) &&
	encdirmatch(4,56)) {
	jinfo.dir_production = parts[1];
	return true;
    }

    if (parts.size() == 5 && encdirmatch(0,0) && encdirmatch(2,50) &&
	encdirmatch(4,49)) {
	jinfo.dir_production = parts[1];
	return true;
    }

    if (parts.size() == 2 && parts[1] == str_ers_java_service) {
	jinfo.dir_production = str_java_service;
	return true;
    }

    if (parts.size() == 3 && parts[1] == str_ers &&
	parts[2] == str_sstress_0_1_0) {
	jinfo.dir_production = str_sstress_0_1_0;
	return true;
    }

    if (parts.size() >= 2 && parts[1] == str_ers_host_traces) {
	jinfo.dir_production = str_ers_host_traces;
	return true;
    }

    if (parts.size() == 6 && encdirmatch(0,0) && encdirmatch(2,50) &&
	encdirmatch(4,56)) {
	dirpss(1,3,5);
    }

    if (parts.size() == 7 && encdirmatch(0,5) && encdirmatch(2,38) &&
	encdirmatch(4,36) && encdirmatch(5,37)) {
	dirps(4,6); // not a movie
    }

    if (parts.size() == 10 && encdirmatch(0,0) && encdirmatch(2,13) &&
	encdirmatch(3,4) && encdirmatch(7,43) && encdirmatch(8,58)) {
	noretdirpss(1,4,5);
	jinfo.subtask = parts[9];
	return true;
    }

    if (parts.size() == 10 && encdirmatch(0,0) && encdirmatch(2,13) &&
	encdirmatch(3,4) && encdirmatch(7,43)) {
	noretdirpss(1,4,5);
	jinfo.subtask = parts[8];
	return true;
    }

    if (parts.size() == 11 && encdirmatch(0,5) && encdirmatch(2,21) &&
	match_directory_str13(parts[3]) && sqsmatch(5,6) &&
	encdirmatch(7,43) && encdirmatch(9,52)) {
	jinfo.dir_production = "*unknown*";
	jinfo.dir_sequence = parts[5];
	jinfo.dir_shot = parts[6];
	return true;
    }

    if (parts.size() == 8 && encdirmatch(0,0) && encdirmatch(3,4) &&
	encdirmatch(4,48)) {
	dirpss(1,5,7);
    }

    if (parts.size() == 8 && encdirmatch(0,0) && encdirmatch(3,4) &&
	encdirmatch(4,17)) {
	dirpss(1,4,5);
    }

    if (parts.size() == 9 && encdirmatch(0,0) && encdirmatch(2,13) &&
	sqsmatch(3,4) && encdirmatch(6,43)) {
	dirpss(1,3,4);
    }

    if (parts.size() == 4 && encdirmatch(0,0) && encdirmatch(2,59) &&
	encdirmatch(3,60)) {
	dirps(2,3);
    }

    if (parts.size() == 5 && encdirmatch(0,0) && encdirmatch(2,59) &&
	encdirmatch(3,60)) {
	dirpss(2,3,4);
    }

    if (parts.size() == 5 && encdirmatch(0,0) && encdirmatch(3,1) &&
	parts[4][0] == 's' && parts[4][1] == 'q') {
	dirps(1,4);
    }

    if (parts.size() == 5 && encdirmatch(0,61) && encdirmatch(2,62) &&
	parts[3].size() >= 4 &&
	encmatch(parts[3].substr(0,4),encrypted_parse_directory[63]) &&
	(int)parts[4].find(str_dotcount) > 0) {
	jinfo.dir_production = str_ers_trace_data;
	jinfo.dir_sequence = parts[3];
	jinfo.dir_shot = parts[4].substr(0,parts[4].find(str_dotcount));
	return true;
    }

    if (parts.size() == 8 && encdirmatch(0,0) && encdirmatch(2,64) &&
	sqsmatch(3,4) && encdirmatch(6,43)) {
	noretdirpss(1,3,4);
	jinfo.task = parts[7];
	return true;
    }

    if (parts.size() == 7 && encdirmatch(0,0) && encdirmatch(2,64) &&
	sqsmatch(3,4) && encdirmatch(6,43)) {
	dirpss(1,3,4);
    }

    if (parts.size() == 5 && encdirmatch(0,61) && encdirmatch(2,62) &&
	parts[3].size() >= 6 && parts[3].substr(0,6) == str_trace_dash &&
	isdigit(parts[4][0])) {
	jinfo.dir_production = str_ers_trace_data;
	jinfo.dir_sequence = parts[4];
	jinfo.dir_shot = parts[3];
	return true;
    }

    if (parts.size() == 6 && encdirmatch(0,61) && encdirmatch(2,62) &&
	parts[3].size() >= 6 && parts[3].substr(0,6) == str_trace_dash &&
	isdigit(parts[4][0]) && (int)parts[5].find(str_dotbsubdashlogdot)>0) {
	jinfo.dir_production = str_ers_trace_data;
	jinfo.dir_sequence = parts[4];
	jinfo.dir_shot = parts[5].substr(0,parts[5].find(str_dotbsubdashlogdot));
	return true;
    }

    if (parts.size() == 5 && encdirmatch(0,61) && encdirmatch(2,62) &&
	parts[3].size() >= 4 &&
	encmatch(parts[3].substr(0,4),encrypted_parse_directory[63]) &&
	(int)parts[4].find(str_dotbsubdashlogdot) > 0) {
	jinfo.dir_production = str_ers_trace_data;
	jinfo.dir_sequence = parts[3];
	jinfo.dir_shot = parts[4].substr(0,parts[4].find(str_dotbsubdashlogdot));
	return true;
    }

    if (parts.size() == 6 && encdirmatch(0,0) && encdirmatch(2,50) &&
	encdirmatch(4,51) && parts[5][0] == 's' && parts[5][1] == 'q') {
	jinfo.dir_production = parts[1];
	jinfo.dir_sequence = parts[5];
	return true;
    }

    if (parts.size() == 7 && parts[0] == str_tuscany &&
	parts[2] == str_perl_service && parts[3] == str_chroot_jail) {
	jinfo.dir_production = parts[0];
	jinfo.dir_sequence = str_perl_service2;
	jinfo.dir_shot = parts[5]; // will be replaced with shot from jobname in jobname parsing
	return true;
    }

    // DIRECTORY PARSE -- for finding place to add parsing in code
    return false;
}

// true if this ought to be parsable
bool
parse_directory_warning(const string &dirpath, int linenum)
{
    if (dirpath == empty_string)
	return false;
    if (dirpath == str_dev_null) // we don't parse this log name
	return false;
    if (hexstring(encryptString(dirpath)) == "dd171f15f9772a4bdb2b4f04e117c69c4a2f897af26fbf5bbe0550526022a033154dae897b1a4f322392c22d666b208a2dfa3daf38f6ff6e8248618c05d2d218") {
	return false; // no useful information, don't generate warning
    }
    vector<string> parts;
    split(dirpath,str_slash,parts);

    if (parts[0] != empty_string) {
	return false; // we don't try to parse these, so don't generate warnings
    }
    if (parts.size() == 2 && parts[0] == empty_string &&
	parts[1] == empty_string) {
	return false; // no information in "/"
    }

    if (parts.size() == 4 && parts[0] == empty_string &&
	parts[1] == str_tmp) {
      return false; // no useful information in here, logged into /tmp/x/y
    }

    if (false) {
	// Dunno why this was here, leaving it in in case it once made
        // sense.  TODO: remove once under version control.
	parts.erase(parts.begin());
	if (parts.size() > 0 &&
	    encryptString(parts[0]) == encrypted_parse_directory[8]) {
	    parts.erase(parts.begin());
	}
    }

    if (print_parse_warnings) {
	fprintf(stderr,"Unable to parse directory path(line%d) (%d parts) '%s' --> '%s'\n",
		linenum, parts.size(), dirpath.c_str(),
		hexstring(encryptString(dirpath)).c_str());
	for(unsigned i=0;i<parts.size();++i) {
	    fprintf(stderr,"  %d: %s -> %s\n",i,parts[i].c_str(),
		    hexstring(encryptString(parts[i])).c_str());
	}
    }
    return true;
}

void
parse_directory_jobinfo(const string &run_directory,
			const string &log_filename,
			job_info &jinfo,
			int linenum)
{
    jinfo.directory_name_unpacked =
	try_parse_directory_jobinfo(run_directory,jinfo);
    if (jinfo.directory_name_unpacked == false) {
	jinfo.directory_name_unpacked =
	    try_parse_directory_jobinfo(log_filename,jinfo);
    }

    if (jinfo.directory_name_unpacked) {
	AssertAlways(jinfo.dir_production != empty_string,("internal"));
	jinfo.production = jinfo.dir_production;
	jinfo.sequence = jinfo.dir_sequence;
	jinfo.shot = jinfo.dir_shot;
    } else {
	bool p1 = parse_directory_warning(run_directory,linenum);
	bool p2 = parse_directory_warning(log_filename,linenum);
	if (p1 || p2) {
	    ++jobdirectory_parse_fail_count;
	} else {
	    ++jobdirectory_odd_fail_count;
	}
	AssertAlways(jinfo.dir_production == empty_string &&
		     jinfo.dir_sequence == empty_string &&
		     jinfo.dir_shot == empty_string,("internal"));
    }
}

bool
period_parse_any(string &tmp_jobname, string &write_into,
		 bool entire_string_ok = false,
		 const string &sepstr = str_period)
{
    int idx = tmp_jobname.find(sepstr);
    if (idx > 0) {
	if (false) printf("XX %s -> ",tmp_jobname.c_str());
	write_into = tmp_jobname.substr(0,idx);
	tmp_jobname = tmp_jobname.substr(idx+1,
					 tmp_jobname.length() - (idx+1));
	if (false) printf("%s/%s\n",write_into.c_str(),tmp_jobname.c_str());
	return true;
    } else if (idx == -1 && entire_string_ok) {
	write_into = tmp_jobname;
	tmp_jobname = empty_string;
	return true;
    }

    return false;
}

bool
period_parse_match(string &tmp_jobname, const string &target_str,
		   const char sep_char = '.')
{
    if (tmp_jobname == target_str) {
	tmp_jobname = empty_string;
	return true;
    }
    if (tmp_jobname.length() <= target_str.length())
	return false;
    if (tmp_jobname.substr(0,target_str.length()) != target_str)
	return false;
    if (tmp_jobname[target_str.length()] != sep_char)
	return false;
    tmp_jobname = tmp_jobname.substr(target_str.length()+1,
				     tmp_jobname.length() - (target_str.length()+1));
    return true;
}

bool
period_parse_xmatch(string &tmp_jobname, const string &encrypted_str,
		    string *target_str = NULL)
{
    int period_idx = tmp_jobname.find(".");
    if (period_idx == -1)
	return false;
    if (encryptString(tmp_jobname.substr(0,period_idx)) == encrypted_str) {
	if (target_str != NULL) {
	    *target_str = tmp_jobname.substr(0,period_idx);
	}
	tmp_jobname = tmp_jobname.substr(period_idx + 1,
					 tmp_jobname.length() - (period_idx + 1));
	return true;
    } else {
	if (encrypted_str.size() == 0) { // finding new string...
	    fprintf(stderr,"Unable to match %s -> %s == '%s'\n",
		    tmp_jobname.substr(0,period_idx).c_str(),
		    hexstring(encryptString(tmp_jobname.substr(0,period_idx))).c_str(),
		    hexstring(encrypted_str).c_str());
	}
	return false;
    }
}

bool
period_parse_digits(string &tmp_jobname, const char sepchar = '.')
{
    for(unsigned i=0;i<tmp_jobname.size();++i) {
	if (tmp_jobname[i] == sepchar) {
	    tmp_jobname = tmp_jobname.substr(i+1,tmp_jobname.size() - (i+1));
	    return true;
	}
	if (!isdigit(tmp_jobname[i])) {
	    return false;
	}
    }
    tmp_jobname = empty_string;
    return true;
}

pcre *regex_jobname_framespec;

bool
parse_jobname_trim_framespec(string &jobname, job_info &jinfo)
{
    if (regex_jobname_framespec == NULL) {
	regex_jobname_framespec =
	    xpcre_compile(":?(\\[(\\d|-|:|,)+\\])(%\\d+)?$");
    }

    const int novector = 30;
    int ovector[novector];
    // trim frame spec
    int rc = pcre_exec(regex_jobname_framespec, NULL, jobname.c_str(),
		       jobname.length(),0,0,ovector,novector);
    if (rc == 3 || rc == 4) {
	string str;
	xpcre_get_substring(jobname,ovector,rc,1,str);
	if (parse_frameinfo(str,jinfo) == false) {
	    if (print_parse_warnings) {
		fprintf(stderr,"can't parse '%s' in '%s'\n",str.c_str(),jobname.c_str());
	    }
	    return true; //
	}
	xpcre_get_substring(jobname,ovector,rc,0,str);
	if (false) printf("trimmed %s to ",jobname.c_str());
	jobname = jobname.substr(0,jobname.length() - str.length());
	if(jobname.size() > 0 && jobname[jobname.size()-1] == '.') {
	    jobname = jobname.substr(0,jobname.length() - 1);
	}
	if (false) printf("%s\n",jobname.c_str());
    } else {
	if (false) printf("nomatch on %s\n",jobname.c_str());
	AssertAlways(rc == PCRE_ERROR_NOMATCH,
		     ("inexplicable error from pcre: %d on %s\n",rc,jobname.c_str()));
    }
    return true;
}

bool
parse_periodsep_trim_periods(string &tmp_jobname)
{
    if (tmp_jobname.length() > 0 && tmp_jobname[0] == '.') {
	tmp_jobname = tmp_jobname.substr(1,tmp_jobname.length()-1);
    }
    if (tmp_jobname.length() > 0 && tmp_jobname[tmp_jobname.length()-1] == '.') {
	tmp_jobname = tmp_jobname.substr(0,tmp_jobname.length()-1);
    }
    return true; // always succeeds
}

pcre *regex_periodsep_jobname_1a;
pcre *regex_periodsep_jobname_1b;
pcre *regex_periodsep_jobname_task_subtask;
pcre *regex_periodsep_bsub_path_task_1;
pcre *regex_periodsep_bsub_path_task_2;
pcre *regex_periodsep_taskCAPS_sq_shot;
pcre *regex_periodsep_tasknumnum;

bool // true on success, assumes the directory parsed
parse_periodsep_jobname(const string &_jobname, const string &jobdirectory,
			job_info &jinfo)
{
    if (regex_periodsep_jobname_1a == NULL) {
	regex_periodsep_jobname_1a =
	    xpcre_compile("^(\\w+)\\.(\\w+)\\.([A-Za-z0-9_\\.]+)$");

	regex_periodsep_jobname_1b = xpcre_compile("\\.?({[rs]\\d+})$");

	regex_periodsep_jobname_task_subtask =
	    xpcre_compile("^(\\w+)\\.(\\w*)$");

	regex_periodsep_bsub_path_task_1 =
	    xpcre_compile("^BSUB.(\\w+)/(.+)/(\\w+)$");

	regex_periodsep_bsub_path_task_2 =
	    xpcre_compile("^BSUB.[\\./]+lib/(\\w+)/(.+)/(\\w+)$");

	regex_periodsep_taskCAPS_sq_shot =
	    xpcre_compile("^([A-Z_]+)\\.(sq\\d+)\\.(s.+)$");

	regex_periodsep_tasknumnum =
	    xpcre_compile("^(\\w+)_\\d+\\.\\d+$");
    }

    const int novector = 30;
    int ovector[novector];

    string framespec_trimmed_jobname = _jobname;
    parse_jobname_trim_framespec(framespec_trimmed_jobname,jinfo);
    string jobname = framespec_trimmed_jobname;
    // remove weird brace thing ...
    int rc = pcre_exec(regex_periodsep_jobname_1b, NULL, jobname.c_str(),
		       jobname.length(),0,0,ovector,novector);
    if (rc == 2) {
	// drop the {[rs]#}
	string str;

	xpcre_get_substring(jobname,ovector,rc,0,str);
	framespec_trimmed_jobname
	    = jobname.substr(0,jobname.length() - str.length());
    } else {
	AssertAlways(rc == PCRE_ERROR_NOMATCH,
		     ("inexplicable error from pcre: %d\n",rc));
    }

    // seq.shot.task.?[framespec]
    jobname = framespec_trimmed_jobname;
    if (period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot) &&
	parse_periodsep_trim_periods(jobname) &&
	period_parse_any(jobname,jinfo.task,true) &&
	jobname == empty_string) {
	if (false) printf("YY %s -> ss %s\n",jobname.c_str(),jinfo.task.c_str());
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // seq.shot.task.subtask
    jobname = framespec_trimmed_jobname;
    if (period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_any(jobname,jinfo.task) &&
	period_parse_any(jobname,jinfo.subtask,true) &&
	jobname == empty_string) {
	if (false) printf("YY %s -> ss %s\n",jobname.c_str(),jinfo.task.c_str());
	return true;
    } else {
	jinfo.task = empty_string;
	jinfo.subtask = empty_string;
    }

    // seq.shot.task.subtask.object
    jobname = framespec_trimmed_jobname;
    if (period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_any(jobname,jinfo.task) &&
	period_parse_any(jobname,jinfo.subtask) &&
	period_parse_any(jobname,jinfo.object,true) &&
	jobname == empty_string) {
	if (false) printf("YY %s -> ss %s\n",jobname.c_str(),jinfo.task.c_str());
	return true;
    } else {
	jinfo.task = empty_string;
	jinfo.subtask = empty_string;
	jinfo.object = empty_string;
    }

    // task.prod.seq.shot[framespec]
    jobname = framespec_trimmed_jobname;

    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,jinfo.production) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task.seq.shot[framespec]
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task.object.seq.shot[framespec]
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_any(jobname,jinfo.object) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
	jinfo.object = empty_string;
    }

    // task.subtask.seq.shot.object[framespec]
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_any(jobname,jinfo.subtask) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_any(jobname,jinfo.object,true) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
	jinfo.subtask = empty_string;
	jinfo.object = empty_string;
    }

    // *match-1/task*.shot.*match-2*.subtask.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_xmatch(jobname,encrypted_parse_periodsep[1],
			    &jinfo.task) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_xmatch(jobname,encrypted_parse_periodsep[2]) &&
	period_parse_any(jobname,jinfo.subtask) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
	jinfo.subtask = empty_string;
    }

    // task-object.seq.shot.subtask ; but currently merging task and object
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_any(jobname,jinfo.subtask,true) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
	jinfo.subtask = empty_string;
    }

    // *task-1*.object[framespec]
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	encryptString(jinfo.task) == encrypted_parse_periodsep[0]) {
	jinfo.object = jobname;
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task.sequence.shot_subtask[framespec]
    jobname = framespec_trimmed_jobname;
    if (jobname.length() > 10) {
	for(int i=5;i<=7;++i) {
	    if (jobname[jobname.length()-i] == '_') {
		jobname[jobname.length()-i] = '.';
	    }
	}
	if (period_parse_any(jobname,jinfo.task) &&
	    period_parse_match(jobname,jinfo.sequence) &&
	    period_parse_match(jobname,jinfo.shot) &&
	    period_parse_any(jobname,jinfo.subtask,true) &&
	    (encryptString(jinfo.subtask) == encrypted_parse_periodsep[3] ||
	     encryptString(jinfo.subtask) == encrypted_parse_periodsep[4] ||
	     encryptString(jinfo.subtask) == encrypted_parse_periodsep[5])) {
	    return true;
	} else {
	    if (false)
		fprintf(stderr,"XX %s -> %s\n",jinfo.subtask.c_str(),
			hexstring(encryptString(jinfo.subtask)).c_str());
	    jinfo.task = empty_string;
	    jinfo.subtask = empty_string;
	}
    }

    // task.seq/shot.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,jinfo.sequence,'/') &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task.seq.shot; no shot in directory parse
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,jinfo.sequence) &&
	jinfo.dir_shot == empty_string &&
	jobname[0] == 's' &&
	period_parse_any(jobname,jinfo.shot,true) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // production.*match-1/task*.shot.*match-2*.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_match(jobname,jinfo.production) &&
	period_parse_xmatch(jobname,encrypted_parse_periodsep[6],
			    &jinfo.task) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_xmatch(jobname,encrypted_parse_periodsep[7]) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // *match-1/task*.shot.subtask.object.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_xmatch(jobname,encrypted_parse_periodsep[1],
			    &jinfo.task) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_any(jobname,jinfo.subtask) &&
	period_parse_any(jobname,jinfo.object) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
	jinfo.subtask = empty_string;
	jinfo.object = empty_string;
    }

    // *match-1/task*.sequence.subtask.object.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_xmatch(jobname,encrypted_parse_periodsep[1],
			    &jinfo.task) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_any(jobname,jinfo.subtask) &&
	period_parse_any(jobname,jinfo.object) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
	jinfo.subtask = empty_string;
	jinfo.object = empty_string;
    }

    // odd construction here, match the shot directory, then pickup the
    // sequence and shot remainder
    jobname = framespec_trimmed_jobname;
    if (period_parse_match(jobname,jinfo.dir_shot) &&
	jobname[0] == 's' && jobname[1] == 'q' &&
	period_parse_any(jobname,jinfo.sequence) &&
	jobname[0] == 's' && jobname.size() >= 2) {
	bool allok = true;
	for(unsigned i=1;i<jobname.size();++i) {
	    if (isdigit(jobname[i]) || jobname[i] == '.') {
		// ok
	    } else {
		allok = false;
		break;
	    }
	}
	if (allok) {
	    jinfo.shot = jobname;
	    return true;
	} else {
	    jinfo.sequence = jinfo.dir_sequence;
	}
    } else {
	jinfo.sequence = jinfo.dir_sequence;
    }

    // *match-1*.shot/(match-4chars)+.task.subtask.#
    // then replace shot->seq and match-4chars+ -> shot
    jobname = framespec_trimmed_jobname;
    if (period_parse_xmatch(jobname,encrypted_parse_periodsep[1]) &&
	period_parse_match(jobname,jinfo.shot,'/') &&
	jobname.size() > 4 &&
	encryptString(jobname.substr(0,4)) == encrypted_parse_periodsep[6] &&
	period_parse_any(jobname,jinfo.object) &&  // replace later
	period_parse_any(jobname,jinfo.task) &&
	period_parse_any(jobname,jinfo.subtask) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	jinfo.sequence = jinfo.shot;
	jinfo.shot = jinfo.object;
	jinfo.object = empty_string;
	return true;
    } else {
	jinfo.task = jinfo.subtask = jinfo.object = empty_string;
    }

    // odd construction here, have to trim off an 'a'/'b' at the end of the
    // shot name to get a match
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,jinfo.sequence,'/') &&
	(jinfo.shot[jinfo.shot.size()-1] == 'a' ||
	 jinfo.shot[jinfo.shot.size()-1] == 'b') &&
	period_parse_match(jobname,jinfo.shot.substr(0,jinfo.shot.size()-1)) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // *match-1/task*.shot.subtask.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_xmatch(jobname,encrypted_parse_periodsep[6],
			    &jinfo.task) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_any(jobname,jinfo.subtask) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task...# or task...f
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,empty_string) &&
	period_parse_match(jobname,empty_string) &&
	(period_parse_digits(jobname) ||
	 period_parse_match(jobname,str_f)) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task.*match*.shot.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_xmatch(jobname,encrypted_parse_periodsep[8]) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task.*match-1*.*match-2*.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_xmatch(jobname,encrypted_parse_periodsep[9]) &&
	period_parse_xmatch(jobname,encrypted_parse_periodsep[10]) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task.shot.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task.production.shot.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,jinfo.production) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task.*match-1*.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_xmatch(jobname,encrypted_parse_periodsep[11]) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task.sq.shot_#
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot,'_') &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    jobname = framespec_trimmed_jobname;
    rc = pcre_exec(regex_periodsep_bsub_path_task_1, NULL, jobname.c_str(),
		   jobname.length(),0,0,ovector,novector);
    if (rc == 4) {
	string str;
	xpcre_get_substring(jobname,ovector,rc,1,str);
	if (encryptString(str) == encrypted_parse_periodsep[12]) {
	    xpcre_get_substring(jobname,ovector,rc,3,jinfo.task);
	    xpcre_get_substring(jobname,ovector,rc,2,jinfo.subtask);
	    return true;
	} else {
	    printf("nomatch %s\n",hexstring(encryptString(str)).c_str());
	}
    } else {
	AssertAlways(rc == PCRE_ERROR_NOMATCH,
		     ("inexplicable error from pcre: %d\n",rc));
    }

    rc = pcre_exec(regex_periodsep_bsub_path_task_2, NULL, jobname.c_str(),
		   jobname.length(),0,0,ovector,novector);
    if (rc == 4) {
	string str;
	xpcre_get_substring(jobname,ovector,rc,1,str);
	if (encryptString(str) == encrypted_parse_periodsep[12]) {
	    xpcre_get_substring(jobname,ovector,rc,3,jinfo.task);
	    xpcre_get_substring(jobname,ovector,rc,2,jinfo.subtask);
	    return true;
	} else {
	    printf("nomatch %s\n",hexstring(encryptString(str)).c_str());
	}
    } else {
	AssertAlways(rc == PCRE_ERROR_NOMATCH,
		     ("inexplicable error from pcre: %d\n",rc));
    }

    rc = pcre_exec(regex_periodsep_taskCAPS_sq_shot, NULL, jobname.c_str(),
		   jobname.length(),0,0,ovector,novector);
    if (rc == 4) {
	xpcre_get_substring(jobname,ovector,rc,1,jinfo.task);
	xpcre_get_substring(jobname,ovector,rc,2,jinfo.sequence);
	xpcre_get_substring(jobname,ovector,rc,3,jinfo.shot);
	return true;
    } else {
	AssertAlways(rc == PCRE_ERROR_NOMATCH,
		     ("inexplicable error from pcre: %d\n",rc));
    }

    // *match-1/task*.shot.*match-2*.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_xmatch(jobname,encrypted_parse_periodsep[1],
			    &jinfo.task) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_xmatch(jobname,encrypted_parse_periodsep[2]) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // *match-1/task*.shot.subtask.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_xmatch(jobname,encrypted_parse_periodsep[1],
			    &jinfo.task) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_any(jobname,jinfo.subtask) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
	jinfo.subtask = empty_string;
	jinfo.object = empty_string;
    }

    // shot.?
    jobname = framespec_trimmed_jobname;
    if (period_parse_match(jobname,jinfo.shot) &&
	jobname.size() == 1 &&
	period_parse_any(jobname,jinfo.task,true)) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task.prod.subtask.shot
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,jinfo.production) &&
	period_parse_any(jobname,jinfo.subtask) &&
	period_parse_match(jobname,jinfo.shot)) {
	return true;
    } else {
	jinfo.task = jinfo.subtask = empty_string;
    }

    // task.sequence.shot.empty.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_match(jobname,empty_string) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // prod.sequence.shot.task_#.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_match(jobname,jinfo.production) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot)) {
	rc = pcre_exec(regex_periodsep_tasknumnum, NULL, jobname.c_str(),
		       jobname.length(),0,0,ovector,novector);
	if (rc == 2) {
	    xpcre_get_substring(jobname,ovector,rc,1,jinfo.task);
	    return true;
	}
    }

    // test.sq.shot/subtask
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname, jinfo.task) &&
	period_parse_match(jobname, jinfo.sequence) &&
	period_parse_match(jobname, jinfo.shot, '/') &&
	period_parse_any(jobname, jinfo.subtask, true) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = jinfo.subtask = empty_string;
    }

    // task.sq.shot:#
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname, jinfo.task) &&
	period_parse_match(jobname, jinfo.sequence) &&
	period_parse_match(jobname, jinfo.shot, ':') &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // *empty*.seq.shot
    jobname = framespec_trimmed_jobname;
    if (period_parse_match(jobname,empty_string) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot) &&
	jobname == empty_string) {
	return true;
    }

    // task.#.shot.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_digits(jobname) && jinfo.shot.size() > 1 &&
	period_parse_match(jobname,jinfo.shot.substr(1,jinfo.shot.size()-1)) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task.seq.shot.{#}
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot.substr(1,jinfo.shot.size()-1)) &&
	jobname[0] == '{' && jobname[jobname.size()-1] == '}') {
	jobname = jobname.substr(1,jobname.size()-2);
	if (period_parse_digits(jobname) &&
	    jobname == empty_string) {
	    return true;
	} else {
	    jinfo.task = empty_string;
	}
    } else {
	jinfo.task = empty_string;
    }

    // production.sequence.shot.task.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_match(jobname,jinfo.production) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_any(jobname,jinfo.task) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // production.sequence.shot.task.#_#
    jobname = framespec_trimmed_jobname;
    if (period_parse_match(jobname,jinfo.production) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_any(jobname,jinfo.task) &&
	period_parse_digits(jobname,'_') &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // task.sequence.shot.#.#
    jobname = framespec_trimmed_jobname;
    if (period_parse_any(jobname,jinfo.task) &&
	period_parse_match(jobname,jinfo.sequence) &&
	period_parse_match(jobname,jinfo.shot) &&
	period_parse_digits(jobname) &&
	period_parse_digits(jobname) &&
	jobname == empty_string) {
	return true;
    } else {
	jinfo.task = empty_string;
    }

    // PERIODSEP PARSE -- for finding place to add parsing in code

    // two fallback cases at the end

    // task only, without any unexpected separators
    jobname = framespec_trimmed_jobname;
    if ((int)jobname.find(str_period) == -1 &&
	(int)jobname.find(str_colon) == -1 &&
	(int)jobname.find(str_pipe) == -1) {
	jinfo.task = jobname;
	return true;
    }

    rc = pcre_exec(regex_periodsep_jobname_task_subtask, NULL, jobname.c_str(),
		   jobname.length(),0,0,ovector,novector);
    if (rc == 3) {
	xpcre_get_substring(jobname,ovector,rc,1,jinfo.task);
	xpcre_get_substring(jobname,ovector,rc,2,jinfo.subtask);
	return true;
    }
    AssertAlways(rc == PCRE_ERROR_NOMATCH,
		 ("inexplicable error from pcre: %d\n",rc));

    // bizarre special case; copied stuff
    if (encryptString(jinfo.production) == encrypted_parse_periodsep[13] &&
	encryptString(jinfo.sequence) == encrypted_parse_periodsep[14] &&
	encryptString(jinfo.shot) == encrypted_parse_periodsep[15]) {
	jobname = framespec_trimmed_jobname;
	period_parse_any(jobname,jinfo.task);
	period_parse_any(jobname,jinfo.subtask);
	if (encryptString(jinfo.subtask) == encrypted_parse_periodsep[16]) {
	    jinfo.sequence = jinfo.subtask;
	    jinfo.subtask = empty_string;
	    period_parse_any(jobname,jinfo.shot,true);
	    AssertAlways(jobname == empty_string,("internal %s",jobname.c_str()));
	    return true;
	}
	jinfo.task = jinfo.subtask = empty_string;
    }

    return false;
}

const string jobcommand_A_sha1("bd9c767839e140a4fae8cc8c68aecd016008a32b");

bool
parse_jobname_special(const string &jobname, const string &jobcommand,
		      const string &jobdirectory, job_info &jinfo)
{
    static const string systest_prod = "system-testing";
    static const string systest_seq = "lsf-verification";
    if (jobname.size() == 0) {
	if (encmatch(jobcommand,encrypted_parse_special[0])) {
	    jinfo.production = systest_prod;
	    jinfo.sequence = systest_seq;
	    return true;
	}
    }
    if (jobname.size() > 4 &&
	encmatch(jobname.substr(0,4),encrypted_parse_special[1])) {
	int idx = jobname.find(str_underbar,4);
	if (idx > 0) {
	    jinfo.production = jobname.substr(4,idx-4);
	    idx++;
	    jinfo.sequence = jobname.substr(idx,jobname.size() - idx);
	    return true;
	}
    }
    if (jobdirectory.size() == 0) {
	string tmp_name = jobname;
	if (parse_jobname_trim_framespec(tmp_name,jinfo)) {
	    for(unsigned i=0;i<tmp_name.size();i++) {
		if (isalpha(tmp_name[i]) == false) {
		    return false;
		}
	    }
	    jinfo.task = tmp_name;
	    return true;
	}
    }

    return false;
}

void
parse_jobname(const string &jobname, const string &jobcommand,
	      const string &jobdirectory,
	      const string &outfilename, job_info &jinfo,
	      int linenum)
{
    jinfo.job_name = jobname;
    jinfo.job_name_unpacked = false;
    jinfo.meta_id = 0;
    jinfo.start_frame = jinfo.end_frame = jinfo.frame_step = jinfo.nframes = 0;

    parse_directory_jobinfo(jobdirectory, outfilename, jinfo, linenum);

    // special cases
    if (parse_jobname_special(jobname, jobcommand, jobdirectory, jinfo)) {
	jinfo.job_name_unpacked = true;
	return;
    }

    if (parse_pipesep_jobname(jobname,jinfo)) {
	jinfo.job_name_unpacked = true;
	return;
    }

    if (parse_colonsep_jobname(jobname,jobdirectory,jinfo)) {
	if (false) {
	    printf("parse %s as meta_id = %d, prod %s, seq %s, shot %s, task %s, object %s, subtask %s, frames %s (%d-%d, step %d #%d)\n",
		   jobname.c_str(),jinfo.meta_id,jinfo.production.c_str(),jinfo.sequence.c_str(),
		   jinfo.shot.c_str(),jinfo.task.c_str(),jinfo.object.c_str(),jinfo.subtask.c_str(),
		   jinfo.frames.c_str(),jinfo.start_frame,jinfo.end_frame,jinfo.frame_step,
		   jinfo.nframes);
	}
	jinfo.job_name_unpacked = true;
	return;
    }

    if (jinfo.directory_name_unpacked && parse_periodsep_jobname(jobname,jobdirectory,jinfo)) {
	if (false) {
	    printf("parse %s as meta_id = %d, prod %s, seq %s, shot %s, task %s, object %s, subtask %s, frames %s (%d-%d, step %d #%d)\n",
		   jobname.c_str(),jinfo.meta_id,jinfo.production.c_str(),jinfo.sequence.c_str(),
		   jinfo.shot.c_str(),jinfo.task.c_str(),jinfo.object.c_str(),jinfo.subtask.c_str(),
		   jinfo.frames.c_str(),jinfo.start_frame,jinfo.end_frame,jinfo.frame_step,
		   jinfo.nframes);
	}
	jinfo.job_name_unpacked = true;
	return;
    }

    if (jobname == empty_string) {
	jobname_odd_fail_count++;
	return;
    }

    if (print_parse_warnings) {
	if (jinfo.directory_name_unpacked) {
	    fprintf(stderr,"Warning: unable to parse jobname, directory ok; linenum %d name = '%s'\n  cmd='%s'\n  dir='%s'\n",
		    linenum,
		    jobname.c_str(),jobcommand.c_str(),jobdirectory.c_str());
	} else {
	    fprintf(stderr,"Warning: unable to parse jobname or directory\n  name='%s'\n  cmd='%s'\n  dir='%s'\n",
		    jobname.c_str(),jobcommand.c_str(),jobdirectory.c_str());
	}
    }
    ++jobname_parse_fail_count;
}

Clock::Tll encrypt_cycles, hex_cycles, lookup_cycles;
int hexcount;

string
sqluint(unsigned val) // "NULL" value is 0
{
    if (val == 0) {
	return "NULL";
    }
    char buf[30];
    sprintf(buf,"%d",val);
    string ret(buf);
    return ret;
}

string
sqlint(int val, int null_val = -1)
{
    if (val == null_val) {
	return "NULL";
    }
    char buf[30];
    sprintf(buf,"%d",val);
    string ret(buf);
    return ret;
}

string
sqldouble(double val) // "NULL" value is -1
{
    if (val == -1) {
	return "NULL";
    }
    char buf[50];
    sprintf(buf,"%.15g",val);
    string ret(buf);
    return ret;
}

void
framelike(const string &v)
{
    for(unsigned i = 0;i<v.size();++i) {
	AssertAlways(isdigit(v[i]) || v[i] == '[' || v[i] == ']' || v[i] == '-'
		     || v[i] == '%' || v[i] == ',' || v[i] == ':',
		     ("whoa, not framelike '%c' %s\n",
		      v[i],v.c_str()));
    }
}

string
hostGroupUnrecognized(const string &group, const string &hostname)
{
    HashUnique<string> &tmp = unrecognized_hostgroups[group];
    tmp.add(hostname);
    if (tmp.size() > 10) {
	vector<string> hostnames;
	for(HashUnique<string>::iterator i = tmp.begin();
	    i != tmp.end(); ++i) {
	    hostnames.push_back(*i);
	}
	FATAL_ERROR(boost::format("too many hosts in group %s -> %s: %s")
		    % group % hexstring(encryptString(group)) 
		    % join(", ", hostnames));
    }
    return empty_string;
}

pcre *regex_hostgroup_one;
pcre *regex_hostgroup_two; 

string
hostGroupWordPrefixCheck(const string &hostname)
{
    const int novector = 30;
    int ovector[novector];

    int rc = pcre_exec(regex_hostgroup_two, NULL, hostname.c_str(),
		       hostname.length(), 0,0,ovector,novector);
    if (rc == 2) {
	string group;
	xpcre_get_substring(hostname, ovector, rc, 1, group);
	if (group.size() != hostname.size()) {
	    return hostGroupUnrecognized(group, hostname);
	} else {
	    // full match, no unexpected group here unless someone 
	    // names their group hosta hostb hostc
	    return empty_string;
	}
    }
    INVARIANT(rc == PCRE_ERROR_NOMATCH,
	      boost::format("unexpected error from pcre on %s: %d") 
	      % hostname % rc);

    return empty_string;
}

string
hostGroup(const string &hostname)
{
    if (regex_hostgroup_one == NULL) {
	regex_hostgroup_one = xpcre_compile("^([a-z]+)\\d+$");
	regex_hostgroup_two = xpcre_compile("^(\\w+)\\b");
    }

    if (cluster_name_str == "rwc" || cluster_name_str == "gld") {
	const int novector = 30;
	int ovector[novector];

	int rc = pcre_exec(regex_hostgroup_one, NULL, hostname.c_str(),
			   hostname.length(), 0,0,ovector,novector);
	if (rc == 2) {
	    string group;
	    xpcre_get_substring(hostname, ovector, rc, 1, group);
	    string encrypted_group = encryptString(group);
	    if (encrypted_hostgroups.exists(encrypted_group)) {
		return encrypted_hostgroups[encrypted_group];
	    } else {
		return hostGroupUnrecognized(group, hostname);
	    }
	}
	INVARIANT(rc == PCRE_ERROR_NOMATCH,
		  "unexpected error from pcre");

	return hostGroupWordPrefixCheck(hostname);
    } else if (cluster_name_str == "uc-hpl-pa") {
	static string batch_cli = "batch-cli-";
	static string dedicated_hpl_1 = "dedicated-hpl-1";
	if (hostname.substr(0,10) == batch_cli) {
	    return dedicated_hpl_1;
	}
	return hostGroupWordPrefixCheck(hostname);
    } else {
	FATAL_ERROR(boost::format("Don't know how to determine host groups for cluster %s")
		    % cluster_name_str);
	return empty_string;
    }
}

const string str_zero("0");

void
process_line(char *buf, int linenum)
{
    vector<string> fields;
    extract_fields(buf, fields, linenum);
    bool has_maxrmem = true;

    INVARIANT(fields.size() > 22,
	      boost::format("bad field count %d\n") % fields.size());
    int fieldcount = -1;
    if (false) {
    } else if (fields[1] == ver_62) {
	fieldcount = 69; 
    } else if (fields[1] == ver_61) {
        fieldcount = 69;
    } else if (fields[1] == ver_60) {
	fieldcount = 68;
    } else if (fields[1] == ver_51 || fields[1] == ver_na) {
	fieldcount = 61;
    } else if (fields[1] == ver_42 || fields[1] == ver_41 ||
	       fields[1] == ver_40) {
	fieldcount = 60;
    } else if (fields[1] == ver_32 || fields[1] == ver_31) {
	fieldcount = 58;
    } else if (fields[1] == ver_30) {
	has_maxrmem = false;
	fieldcount = 58;
	// we're faking up the maxRMem, maxRSwap, but leaving the rest
	// with actually correct values
	fields.push_back(empty_string);
	fields.push_back(str_zero);
	fields.push_back(str_zero);
	fields.push_back(str_zero);
    } else {
	FATAL_ERROR(boost::format("bad version '%s' %d fields")
		    % fields[1] % fields.size());
    }
    AssertAlways(fieldcount > 0, ("no version %s??", fields[1].c_str()));
    AssertAlways(fields[0] == job_finish || fields[0] == job_cache,
		 ("don't know how to handle a %s line\n",
		  fields[0].c_str()));
    int naskhosts = uintfield(fields[22]);
    int exechostsoffset = naskhosts - 1;
    INVARIANT((int)fields.size() > 24+exechostsoffset,
	      boost::format("bad field count %d at line %d\n") 
	      % fields.size() % linenum);
    int nexechosts = uintfield(fields[24+exechostsoffset]);
    int tailoffset = exechostsoffset + nexechosts - 1;

    // 4.2 doesn't have the reservation entry added to the end.
    INVARIANT((int)fields.size() == fieldcount+tailoffset,
	      boost::format("bad %d != %d + %d on line %d")
	      % fields.size() % fieldcount % tailoffset % linenum);
    AssertAlways(fields[54+tailoffset].size() == 0,("bad"));
    AssertAlways(fieldcount <= 61 || fields[66+tailoffset] == str_minus1,("bad"));
	
    job_info jinfo;
    jinfo.parse_command(fields[29+tailoffset],fields[55+tailoffset]);
    jinfo.parse_jobline42(fields,exechostsoffset,tailoffset);
    if (has_maxrmem) {
	jinfo.parse_maxrmem(fields,tailoffset,linenum);
    }
    parse_jobname(fields[28+tailoffset],fields[29+tailoffset],fields[17],
		  fields[19], jinfo, linenum);
    bool jinfo_pss_match = true;
    if (jinfo.job_name_unpacked && jinfo.directory_name_unpacked) {
	if (jinfo.dir_production != jinfo.production ||
	    jinfo.dir_sequence != jinfo.sequence ||
	    jinfo.dir_shot != jinfo.shot) {
	    if (false)
		fprintf(stderr,"Error: mismatch on shot info '%s;%s;%s' vs '%s;%s;%s' '%s'\n",
			jinfo.production.c_str(), jinfo.sequence.c_str(), jinfo.shot.c_str(),
			jinfo.dir_production.c_str(), jinfo.dir_sequence.c_str(), jinfo.dir_shot.c_str(),
		    fields[17].c_str());
	    jinfo_pss_match = false;
	}
    }

    {
	int i = jinfo.sequence.find(str_imax);
	if (i > 0) {
	    if (false) printf("rewrite IMAX %s %s -> ",jinfo.production.c_str(),jinfo.sequence.c_str());
	    jinfo.production.append(str_imax);
	    jinfo.sequence = jinfo.sequence.substr(0,jinfo.sequence.length() - str_imax.length());
	    if (false) printf("%s %s\n",jinfo.production.c_str(),jinfo.sequence.c_str());
	}
    }

    if (false) cout << boost::format("%d fields\n") % (fields.size() - tailoffset);
    if (false) cout << boost::format("YY %d %s %s\n") % jinfo.job_name_unpacked % sqlstring(jinfo.production) % sqlstring(jinfo.team).c_str();
    if (false) printf("%s\n",sqlstring(jinfo.team).c_str());
    framelike(jinfo.frames);
    AssertAlways(jinfo.queue != empty_string,("internal error queue empty?!\n"));

    lsf_grizzly_outmodule->newRecord();
    cluster_name.set(cluster_name_str);
    //    job_name.nset(dsstring(jinfo.job_name),empty_string);
    job_name_unpacked.set(jinfo.job_name_unpacked);
    directory_path_unpacked.set(jinfo.directory_name_unpacked);
    meta_id.nset(jinfo.meta_id,0);
    if (jinfo.job_name_unpacked && jinfo.directory_name_unpacked) {
	    directory_name_info_matched.set(jinfo_pss_match);
    } else {
	directory_name_info_matched.setNull();
    }
    production.nset(dsstring(jinfo.production),empty_string);
    sequence.nset(dsstring(jinfo.sequence),empty_string);
    shot.nset(dsstring(jinfo.shot),empty_string);
    task.nset(dsstring(jinfo.task),empty_string);
    object.nset(dsstring(jinfo.object),empty_string);
    subtask.nset(dsstring(jinfo.subtask),empty_string);
    jobname_username.nset(dsstring(jinfo.jobname_username),empty_string);
    frames.nset(jinfo.frames,empty_string);
    start_frame.nset(jinfo.start_frame,0);
    end_frame.nset(jinfo.end_frame,0);
    nframes.nset(jinfo.nframes,0);
    command.set(dsstring(jinfo.command));
    command_name.set(dsstring(jinfo.command_name));
    command_path.set(dsstring(jinfo.command_path));
    if (jinfo.job_resolution == -1) {
	job_resolution.setNull(true);
    } else {
	job_resolution.set(jinfo.job_resolution);
    }
    job_frame.nset(jinfo.job_frame,0);
    created.set(jinfo.created);
    job_id.set(jinfo.job_id);
    job_idx.nset(jinfo.job_idx,0);
    username.set(dsstring(jinfo.username));
    user_id.set(jinfo.user_id);
    event_time.set(jinfo.event_time);
    submit_time.set(jinfo.submit_time);
    req_start_time.nset(jinfo.req_start_time,0);
    start_time.nset(jinfo.start_time,0);
    end_time.nset(jinfo.end_time,0);
    queue.set(dsstring(jinfo.queue));
    email.nset(dsstring(jinfo.email),empty_string);
    status.set(jinfo.status);
    status_int.nset(jinfo.status_int,-1);
    team.set(dsstring(jinfo.team));
    exit_code.set(jinfo.exit_code);
    user_time.set(jinfo.user_time);
    system_time.set(jinfo.system_time);
    cpu_time.set(jinfo.cpu_time);
    max_memory.nset(jinfo.max_memory);
    max_swap.nset(jinfo.max_swap);
    exec_host.nset(dsstring(jinfo.exec_host), empty_string);
    exec_host_group.nset(hostGroup(jinfo.exec_host), empty_string);
}

int
main(int argc,char *argv[])
{
    commonPackingArgs packing_args;
    packing_args.extent_size = 8*1024*1024;
    getPackingArgs(&argc,argv,&packing_args);

    AssertAlways(argc == 4,("Usage: %s inname cluster-name outdsname; - valid for inname",argv[0]));
    cluster_name_str = argv[2];
    prepareEncryptEnvOrRandom();
    prepEncryptedStuff();
    FILE *infile;
    if (strcmp(argv[1],"-")==0) {
	infile = stdin;
    } else {
	infile = fopen(argv[1],"r");
    }
    AssertAlways(infile != NULL,
		 ("Unable to open %s for read: %s\n",
		  argv[1],strerror(errno)));
    DataSeriesSink outds(argv[3],
			 packing_args.compress_modes,
			 packing_args.compress_level);
    ExtentTypeLibrary library;
    ExtentType *lsf_grizzly_type = library.registerType(lsf_grizzly_xml);
    lsf_grizzly_series.setType(lsf_grizzly_type);
    lsf_grizzly_outmodule = new OutputModule(outds,lsf_grizzly_series,
					     lsf_grizzly_type,
					     packing_args.extent_size);
    outds.writeExtentLibrary(library);
    // stupid, ought to be a way to read an entire line into a STL string;
    // can't find one.
    const unsigned bufsize = 1024*1024;
    char buf[bufsize];
    int nlines = 0;
    while(true) {
	buf[0] = '\0';
	fgets(buf,bufsize,infile);
	++nlines;
	if (buf[0] == '#') {
	    continue;
	}
	if (buf[0] == '\0') {
	    break;
	}
	AssertAlways(strlen(buf) < (bufsize - 1),
		     ("increase bufsize constant.\n"));
	process_line(buf,nlines);
    }
    fprintf(stderr,"\nProcessed %d lines; failed to decode %d jobnames (%.2f%%), %d directories (%.2f%%), %d/%d odd names/directories\n",
	    nlines, jobname_parse_fail_count,
	    jobname_parse_fail_count * 100.0 / (double)nlines,
	    jobdirectory_parse_fail_count,
	    jobdirectory_parse_fail_count * 100.0 / (double)nlines,
	    jobname_odd_fail_count,
	    jobdirectory_odd_fail_count);
    lsf_grizzly_outmodule->flushExtent();
    delete lsf_grizzly_outmodule;
    //     fprintf(stderr,"PCRE cycles: %lld\n",pcre_cycles);

    return 0;
}
