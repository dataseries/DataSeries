/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <Lintel/HashTable.H>
#include <Lintel/Double.H>
#include <Lintel/Stats.H>

#include <DataSeries/DataSeriesFile.H>
#include <DataSeries/DataSeriesModule.H>
#include <DataSeries/DStoTextModule.H>
#include <DataSeries/TypeIndexModule.H>

#ifndef __HP_aCC
using namespace std;
#endif

struct mergeinfo {
    int pid;
    string command, username;
    double first_seen, last_seen;
    double first_io, last_io;
    long long bytes_read;
    long long bytes_written;
    int reads, writes;
    ExtentType::int32 devid,lvid;
    mergeinfo() {}
    mergeinfo(int a) : pid(a) {}
    mergeinfo(int a, double b, ExtentType::int32 c, ExtentType::int32 d)
	: pid(a), first_seen(b), devid(c), lvid(d) {}
    void init() {
	pid = 0; 
	command.resize(0);
	username.resize(0);
	first_seen = last_seen = first_io = last_io = 0;
	bytes_read = bytes_written = 0;
	reads = writes = 0;
    }
};

class mergeinfo_comparepid {
public:
    bool operator()(const mergeinfo &a, const mergeinfo &b) {
	return a.pid == b.pid;
    }
};

class mergeinfo_hashpid {
public:
    int operator()(const mergeinfo &a) {
	return a.pid;
    }
};

class mergeinfo_comparelots {
public:
    bool operator()(const mergeinfo &a, const mergeinfo &b) {
	return a.pid == b.pid && a.first_seen == b.first_seen &&
	    a.devid == b.devid && a.lvid == b.lvid;
    }
};

class mergeinfo_hashlots {
public:
    int operator()(const mergeinfo &a) {
	int hash = 1776;
	hash = BobJenkinsHash(hash,&a.pid,sizeof(int));
	hash = BobJenkinsHash(hash,&a.first_seen,sizeof(double));
	hash = BobJenkinsHash(hash,&a.devid,sizeof(ExtentType::int32));
	hash = BobJenkinsHash(hash,&a.lvid,sizeof(ExtentType::int32));
	return hash;
    }
};

typedef map<ExtentType::int32, vector<mergeinfo *> > RollupPVLV;

class mergeinfo_byIOs {
public:
    bool operator()(const mergeinfo *a, const mergeinfo *b) {
	return (a->reads + a->writes) > (b->reads + b->writes);
    }
};

class mergeinfo_byBytes {
public:
    bool operator()(const mergeinfo *a, const mergeinfo *b) {
	return (a->bytes_read + a->bytes_written) > (b->bytes_read + b->bytes_written);
    }
};


void
sumByCommand(vector<mergeinfo *> &in, 
	     mergeinfo &sum,
	     vector<mergeinfo> &sum_list,
	     int max_sum_list_size)
{
    sum.init();
    
    map<string, mergeinfo *> rollup;
    for(vector<mergeinfo *>::iterator i = in.begin();i!= in.end();++i) {
	sum.bytes_read += (**i).bytes_read;
	sum.bytes_written += (**i).bytes_written;
	sum.reads += (**i).reads;
	sum.writes += (**i).writes;
	mergeinfo *x = rollup[(**i).command];
	if (x == NULL) {
	    x = new mergeinfo;
	    x->init();
	    rollup[(**i).command] = x;
	    x->command = (**i).command;
	}
	x->bytes_read += (**i).bytes_read;
	x->bytes_written += (**i).bytes_written;
	x->reads += (**i).reads;
	x->writes += (**i).writes;
    }
    vector<mergeinfo *> tmp;
    for(map<string, mergeinfo *>::iterator i = rollup.begin();
	i != rollup.end();i++) {
	tmp.push_back(i->second);
    }
    sort(tmp.begin(),tmp.end(),mergeinfo_byIOs());
    vector<mergeinfo *> topios;
    if (max_sum_list_size > (int)tmp.size()) {
	max_sum_list_size = tmp.size();
    }
    topios.insert(topios.begin(),tmp.begin(),tmp.begin() + max_sum_list_size);
    sort(tmp.begin(),tmp.end(),mergeinfo_byBytes());
    vector<mergeinfo *> topbytes;
    topbytes.insert(topbytes.begin(),tmp.begin(),tmp.begin() + max_sum_list_size);

    map<mergeinfo *,bool> used;
    for(int i=0;i<max_sum_list_size;i++) {
	if (used[topios[i]] == false) {
	    sum_list.push_back(*topios[i]);
	    used[topios[i]] = true;
	}
	if ((int)sum_list.size() == max_sum_list_size)
	    break;
	if (used[topbytes[i]] == false) {
	    sum_list.push_back(*topbytes[i]);
	    used[topbytes[i]] = true;
	}
	if ((int)sum_list.size() == max_sum_list_size)
	    break;
    }
    for(vector<mergeinfo *>::iterator i = tmp.begin();i!= tmp.end();i++) {
	delete *i;
    }
}

#if 0
// would need to have the convert extent values into a "record" array
// to get this to work at all sanely as we may be picking out records
// from different extents, but we have to handle multi-typeing as the
// records may come in with slightly different types
class SlopMergeJoin : public DataSeriesModule {
public:
    SlopMergeJoin(const string &output_type_name,
		  DataSeriesModule &_a,DataSeriesModule &_b,
		  ExtentType *atype, ExtentType *btype,
		  const string &exact_match_fieldname_a,
		  const string &exact_match_fieldname_b,
		  const string &slop_merge_fieldname_a,
		  const string &slop_merge_fieldname_b) 
	: input_a(_a), input_b(_b), 
	a_series(ExtentSeries::typeLoose), 
	exact_match_a(a_series,exact_match_fieldname_a),
	exact_match_b(b_series,exact_match_fieldname_b),
	slop_merge_a(a_series,slop_merge_fieldname_a),
	slop_merge_b(b_series,slop_merge_fieldname_b)
    {
	string xmloutdesc("<ExtentType name=\"");
	xmloutdesc.append(output_type_name);
	xmloutdesc.append("\">\n");
	for(int i=0;i<atype->getNField();++i) {
	    xmloutdesc.append("  ");
	    xmloutdesc.append(atype->xmlFieldDesc(i));
	    xmloutdesc.append("\n");
	    input_fields.push_back(GeneralField::create(NULL,a_series,
							atype->getFieldName(i)));
	}

	for(int i=0;i<btype->getNField();++i) {
	    if (btype->getFieldName(i) != exact_match_fieldname_b) {
		xmloutdesc.append("  ");
		xmloutdesc.append(btype->xmlFieldDesc(i));
		xmloutdesc.append("\n");
		input_fields.push_back(GeneralField::create(NULL,b_series,
							    btype->getFieldName(i)));
	    }
	}

	xmloutdesc.append("</ExtentType>\n");
	printf("%s\n",xmloutdesc.c_str());

	ExtentType *outtype = new ExtentType(xmloutdesc);
	b_series.setType(outtype);
	
	for(int i=0;i<atype->getNField();++i) {
	    output_fields.push_back(GeneralField::create(NULL,out_series,
							 atype->getFieldName(i)));
	}

	for(int i=0;i<btype->getNField();++i) {
	    if (btype->getFieldName(i) != exact_match_fieldname_b) {
		output_fields.push_back(GeneralField::create(NULL,out_series,
							     btype->getFieldName(i)));
	    }
	}
    }
private:
    DataSeriesModule &input_a, &input_b;
    ExtentSeries a_series, b_series, out_series;
    Int32Field exact_match_a, exact_match_b;
    DoubleField slop_merge_a, slop_merge_b;
    vector<GeneralField *> input_fields, output_fields;
};
#endif

static const bool debug_ps_read = false;
static const bool debug_io_read = false;

const string merge_type( // not intended to get written, just internal, so pack options left off
"<ExtentType name=\"psIOmerge\">"
"  <field type=\"double\" name=\"io_time\" />"
"  <field type=\"int32\" name=\"device_number\"/>"
"  <field type=\"int32\" name=\"logical_volume_number\"/>"
"  <field type=\"bool\" name=\"is_read\"/>"
"  <field type=\"int64\" name=\"disk_offset\" />"
"  <field type=\"int64\" name=\"lv_offset\" opt_nullable=\"yes\" />"
"  <field type=\"int32\" name=\"bytes\"/>"
"  <field type=\"variable32\" name=\"command\" />"
"  <field type=\"variable32\" name=\"username\" />"
"</ExtentType>");

struct psinfo {
    int pid;
    string command, username;
    double first_seen, last_seen;
    psinfo() {}
    psinfo(int a) : pid(a) {}
};

class psinfo_comparepid {
public:
    bool operator()(const psinfo &a, const psinfo &b) {
	return a.pid == b.pid;
    }
};

class psinfo_hashpid {
public:
    int operator()(const psinfo &a) {
	return a.pid;
    }
};

class PSIOSlopMergeJoin : public DataSeriesModule {
public:
    // time slop can be small, it's just to cover time errors between
    // the PS and I/O traces and the time to run a ps; 1-2 seconds
    // should be good, 5 seconds should be safe.  min_ps_interval_time
    // assumed to be larger than 1e-6 seconds
    // writes will be accounted to a given process so long as
    // iotime < last ps sample for the pid + delayed_write_max_time

    // for vxfs on harp, it looks like most of the delayed writes are
    // within 60 seconds, but some stretch out until ~1800 seconds,
    // and there are some super-delayed writes, but those are probably
    // missed ps records as it looks like we cycle through the pid
    // space in about 8 hours.  Probably what should be implemented is
    // an attempt to estimate when pid's have been recycled and assign
    // the *unknown* name if the pid is too old.
    PSIOSlopMergeJoin(DataSeriesModule &_ps_input, DataSeriesModule &_io_input,
		      double _time_slop, double _min_ps_interval_time,
		      double _delayed_write_max_time)
      : ioextents(0), psextents(0), output_extents(0),
	io_record_count(0), ps_record_count(0), output_record_count(0),
	io_byte_count(0), ps_byte_count(0), output_byte_count(0),
	ps_input(_ps_input), io_input(_io_input), 
	time_slop(_time_slop), min_ps_interval_time(_min_ps_interval_time),
	delayed_write_max_time(_delayed_write_max_time),
	ps_series(ExtentSeries::typeLoose),
	io_series(ExtentSeries::typeLoose),
	iotime(io_series,"enter_driver",DoubleField::flag_allownonzerobase),
	pstime(ps_series,"sampletime"), output_time(output_series,"io_time"),
	iopid(io_series,"pid"), 
	pspid(ps_series,"pid"), 
	devid(io_series,"device_number"), output_devid(output_series,"device_number"),
	lvid(io_series,"logical_volume_number"), output_lvid(output_series,"logical_volume_number"),
	iobytes(io_series,"bytes"), output_iobytes(output_series,"bytes"),
	is_read(io_series,"is_read"), output_is_read(output_series,"is_read"),
	diskoffset(io_series,"disk_offset"), output_diskoffset(output_series,"disk_offset"),
	lvoffset(io_series,"lv_offset", Field::flag_nullable), output_lvoffset(output_series,"lv_offset", Field::flag_nullable),
	command(ps_series,"command"), output_command(output_series,"command"),
	username(ps_series,"username"), output_username(output_series,"username"),
	all_done(false),
	cur_ps_time(-Double::Inf), prev_ps_time(-Double::Inf), 
	time_base(Double::NaN), first_io_time(Double::Inf),
	last_io_time(-Double::Inf)
    {
        output_type = new ExtentType(merge_type);
	output_series.setType(output_type);
    }

    virtual ~PSIOSlopMergeJoin() { }

    virtual Extent *getExtent() {
	if (all_done) {
	    //printf("read %d/%d I/O records, %d/%d PS records\n",
	    //   ioextents,io_record_count, psextents, ps_record_count);
	    return NULL;
	}
	Extent *io_extent = io_input.getExtent();
	if (io_extent == NULL) {
	    AssertAlways(ioextents > 0 && psextents > 0,
			 ("didn't get both I/O and PS data??"));
	    delete ps_series.extent();
	    ps_series.clearExtent();
	    all_done = true;
	    //	    printf("read %d/%d I/O records, %d/%d PS records\n",
	    //   ioextents,io_record_count, psextents, ps_record_count);
	    return NULL;
	}
	++ioextents;
	io_byte_count += io_extent->fixeddata.size() + io_extent->variabledata.size();
	io_series.setExtent(io_extent);
	if (time_base != time_base) {
	    time_base = iotime.absval();
	}
	Extent *outExtent = new Extent(output_type);
	++output_extents;
	output_series.setExtent(outExtent);
	if (ps_series.extent() == NULL) {
	    Extent *ps_extent = ps_input.getExtent();
	    AssertAlways(ps_extent != NULL,("must get at least one ps extent\n"));
	    ps_series.setExtent(ps_extent);
	    ++psextents;
	    ps_byte_count += ps_extent->fixeddata.size() + ps_extent->variabledata.size();
	}
	while(true) {
	    if (io_series.pos.morerecords() == false) {
		delete io_series.extent();
		io_series.clearExtent();
		output_byte_count += outExtent->fixeddata.size() + outExtent->variabledata.size();
		return outExtent;
	    }
	    if (ps_series.pos.morerecords() == false) {
		// could break here and continue until we run out of I/O 
		// records, runs the risk of mis-classifying a few of the I/Os
		// as the *unknown* process
		delete ps_series.extent();
		Extent *ps_extent = ps_input.getExtent();
		//	     		printf("read ps extent of size %d/%d\n",ps_extent->fixeddata.size(),ps_extent->variabledata.size());
		if (ps_extent == NULL) {
		    all_done = true;
		    ps_series.clearExtent();
		    delete io_series.extent();
		    io_series.clearExtent();
		    output_byte_count += outExtent->fixeddata.size() + outExtent->variabledata.size();
		    return outExtent;
		}
		++psextents;
		ps_byte_count += ps_extent->fixeddata.size() + ps_extent->variabledata.size();
		ps_series.setExtent(ps_extent);
	    }
	    AssertAlways(iopid.val() >= 0 && pspid.val() >= 0,
			 ("can't handle negative pid's\n"));
	    // Each ps sample effectively represents the process from
	    // the previous interval to the next interval, so we need
	    // to keep reading records until the previous ps time has
	    // passed the current I/O time to make sure we pick up any
	    // process seen only in the next interval.
	    if (pstime.val() == cur_ps_time || // always read all the ps entries in a batch
		cur_ps_time <= iotime.absval() + time_slop) { // read until we finish the batch after the I/O time + the time slop
		doPSRecord();
	    } else {
		doIORecord();
	    }
	}
    }

    void doPSRecord() {
	if (pstime.val() > (cur_ps_time + 1e-6)) {
	    AssertAlways(pstime.val() > (cur_ps_time + min_ps_interval_time),
			 ("min_ps_interval_time too small %.4f vs %.4f\n",
			  pstime.val(),cur_ps_time));
	    prev_ps_time = cur_ps_time;
	    cur_ps_time = pstime.val();
	}
	AssertAlways(pstime.val() == cur_ps_time,("sanity check error\n")); // needed in doOldPid()
	psinfo *x = pid_to_psinfo.lookup(psinfo(pspid.val()));
	if (x == NULL) {
	    doNewPid();
	} else {
	    doOldPid(x);
	}
	++ps_record_count;
	++ps_series.pos;
    }

    void doNewPid() {
	// new pid
	psinfo newx(pspid.val());
	newx.command = command.stringval();
	newx.username = username.stringval();
	newx.first_seen = prev_ps_time; // new pid represents time starting at prev time.
	newx.last_seen = pstime.val();
	pid_to_psinfo.add(newx);
	//	printf("newpid %d %.0f\n",pspid.val(),pstime.val() - time_base);
	if (debug_ps_read) {
	    printf("new-pid %d [%.0f .. %.0f] %s\n",pspid.val(),
		   newx.first_seen - time_base, newx.last_seen - time_base,
		   newx.command.c_str());
	}
    }

    void doOldPid(psinfo *x) {
	if (x->last_seen == prev_ps_time) { // safe because of sanity check in doPSRecord
	    // was present in the previous sample; is all good.
	    if (x->command != command.stringval()) {
		fprintf(stderr,"warning at (relative) time %.0f pid %d changed from command '%s' to '%s' between two ps samples\n",
			cur_ps_time - time_base,
			pspid.val(), x->command.c_str(), 
			command.stringval().c_str());
		x->command = command.stringval();
	    }
	    if (x->username != username.stringval()) {
		fprintf(stderr,"warning at (relative) time %.0f pid %d changed from user '%s' to '%s' between two ps samples\n",
			cur_ps_time - time_base,
			pspid.val(), x->username.c_str(), 
			username.stringval().c_str());
		x->username = username.stringval();
	    }

	    x->last_seen = cur_ps_time;
	    if (debug_ps_read) {
		printf("update %d [%.0f .. %.0f] %s\n",pspid.val(),
		       x->first_seen - time_base, x->last_seen - time_base,
		       x->command.c_str());
	    }
	} else {
	    // printf("newoldpid %d %.0f\n",pspid.val(),pstime.val() - time_base);
	    // new command
	    x->command = command.stringval();
	    x->username = username.stringval();
	    x->last_seen = cur_ps_time;
	    x->first_seen = prev_ps_time; // same rationale as in doNewPid()
	    if (debug_ps_read) {
		printf("new-cmd %d [%.0f .. %.0f] %s\n",pspid.val(),
		       x->first_seen - time_base, x->last_seen - time_base,
		       x->command.c_str());
	    }
	}
    }

    void doIORecord() {
	if (iotime.absval() < first_io_time) {
	    first_io_time = iotime.absval();
	} 
	// allow a little bit of backwardness because it doesn't hurt,
	// and the traces we have seem to get a little bit of slop in the
	// time ordering
	AssertAlways(iotime.absval()+time_slop/10 >= last_io_time,
		     ("merge join doesn't work if merge field goes backwards; %.20g < %.20g\n",
		      iotime.absval(),last_io_time));
	if (iotime.absval() > last_io_time) {
	    last_io_time = iotime.absval();
	}

//	    if (lvid.val() != 0x40010009) {
//		++io_series.pos;
//		return;
//	    }
	++io_record_count;
	psinfo *x = forceIOFound();
	AssertAlways(x != NULL,("internal error\n"));
	
	output_series.newRecord();
	++output_record_count;

	output_time.set(iotime.absval());
	output_devid.set(devid.val());
	output_lvid.set(lvid.val());
	output_iobytes.set(iobytes.val());
	output_is_read.set(is_read.val());
	output_diskoffset.set(diskoffset.val());
	if (lvoffset.isNull()) {
	    output_lvoffset.setNull(true);
	} else {
	    output_lvoffset.set(lvoffset.val());
	}
	output_command.set(x->command.data(),x->command.size());
	output_username.set(x->username.data(),x->username.size());

	++io_series.pos;
    }

    psinfo *forceIOFound() {
	psinfo *x = pid_to_psinfo.lookup(psinfo(iopid.val()));
	if (x == NULL) {
	    if (debug_io_read) {
		printf("new-pid %d at %.0f *unknown*\n",
		       iopid.val(),iotime.absval() - time_base);
	    }
	    psinfo newx(iopid.val());
	    newx.command = "*unknown*";
	    newx.username = "*unknown*";
	    newx.first_seen = newx.last_seen = iotime.absval();
	    pid_to_psinfo.add(newx);
	    return pid_to_psinfo.lookup(newx);
	} 
	AssertAlways(x->pid == iopid.val(),("internal error\n"));
	if (x->last_seen >= prev_ps_time || // present in the last fully-read sample, defined as ok.
	    iotime.absval() < (x->last_seen + time_slop)) { // another way that's valid
	    if (debug_io_read) {
		printf("update %d [%.0f .. %.0f to %.0f] %s\n",x->pid,
		       x->first_seen, x->last_seen, iotime.absval(), x->command.c_str());
	    }
	    if (x->command == "*unknown*" && iotime.absval() > x->last_seen) {
		x->last_seen = iotime.absval();
	    }
	    return x;
	} 
	if (false && iotime.absval() < (x->last_seen + 100000)) {
	    printf("late %s (%.2f): pid=%d, dev=%x lv=%x, bytes=%d by %s?\n",
		   is_read.val() ? "read" : "write",iotime.absval() - x->last_seen,
		   iopid.val(),
		   devid.val(),lvid.val(),iobytes.val(),x->command.c_str());
	}
	if (debug_io_read) {
	    if (x->command != "*unknown*") {
		printf("new-unknown %d at %.0f (+%.0f), was [%.0f..%.0f/%.0f,%.0f] %s\n",x->pid,
		       iotime.absval(), iotime.absval() - x->last_seen, 
		       x->first_seen, x->last_seen, prev_ps_time, cur_ps_time, x->command.c_str());
	    }
	}
	x->command = "*unknown*";
	x->username = "*unknown*";
	x->first_seen = x->last_seen = iotime.absval();
	return x;
    }		    

    double firstIOTime() { return first_io_time; }
    double lastIOTime() { return last_io_time; }
    int ioextents, psextents, output_extents;
    int io_record_count, ps_record_count, output_record_count;
    long long io_byte_count, ps_byte_count, output_byte_count;
private:
    DataSeriesModule &ps_input, &io_input;
    const double time_slop, min_ps_interval_time, delayed_write_max_time;
    ExtentSeries ps_series, io_series, output_series;
    DoubleField iotime, pstime, output_time;
    Int32Field iopid, pspid, devid, output_devid, lvid, output_lvid, iobytes, output_iobytes;
    BoolField is_read, output_is_read;
    Int64Field diskoffset, output_diskoffset, lvoffset, output_lvoffset;
    Variable32Field command, output_command, username, output_username;

    ExtentType *output_type;
    bool all_done;
    double cur_ps_time, prev_ps_time, prev_prev_ps_time, time_base, first_io_time, last_io_time;
    HashTable<psinfo,psinfo_hashpid,psinfo_comparepid> pid_to_psinfo;
};

// A variant verison of constantstring from lintel allowing for
// multiple pools and deletion of a pool once usage is done.
// probably should find a way to merge them at some point.

// constantstring stores the length for fast access; we skip that
// as it's not needed for this.
class StringPool {
public:
    StringPool() {}
    ~StringPool() { 
	for(bufvect::iterator i = buffers.begin();i != buffers.end();i++) {
	    delete [] i->data;
	}
    }
    const char *getPoolPointer(const std::string &str) {
	const char *const *hptr = hashtable.lookup(str.c_str());
	if (hptr != NULL) {
	    return *hptr;
	}
	for(bufvect::iterator i = buffers.begin();i != buffers.end();i++) {
	    if ((i->size - i->amt_used) >= (int)(str.size() + 1)) {
		char *ptr = i->data + i->amt_used;
		memcpy(ptr,str.c_str(),str.size() + 1);
		i->amt_used += str.size() + 1;
		hashtable.add(ptr);
		return ptr;
	    }
	}
	buffer new_buffer;
	new_buffer.data = new char[buffer_size];
	new_buffer.size = buffer_size;
	new_buffer.amt_used = str.size() + 1;

	char *ptr = new_buffer.data;
	memcpy(ptr,str.c_str(),str.size() + 1);
	hashtable.add(ptr);
	buffers.push_back(new_buffer);
	return ptr;
    }

    class hteHash {
    public:
	unsigned int operator()(const char *k) {
	    return HashTable_hashbytes(k,strlen(k));
	}
    };
    class hteEqual {
    public:
	bool operator()(const char *a, const char *b) {
	    return strcmp(a,b) == 0;
	}
    };

    struct buffer {
	char *data;
	int size;
	int amt_used;
    };
    typedef std::vector<buffer> bufvect;
    static const int buffer_size = 128*1024;
    bufvect buffers;
    HashTable<const char *, hteHash, hteEqual> hashtable;
};
    
struct rollupinfo {
    Stats read, write;
};

struct rollupptr {
    ExtentType::int32 id;
    const char *strval; // expected to be uniqueified
    rollupinfo *data;
    rollupptr(ExtentType::int32 a,const char *b) : id(a), strval(b) {}
};

class rollupptr_compare {
public:
    bool operator()(const rollupptr &a, const rollupptr &b) {
	return a.id == b.id && a.strval == b.strval;
    }
};

class rollupptr_hash {
public:
    int operator()(const rollupptr &a) {
	return a.id ^ (long)a.strval;
    }
};

const string rollupbyidstring_type( // not intended to get written, just internal, so pack options left off
"<ExtentType name=\"IDStringRollup\">"
"  <field type=\"int32\" name=\"rollup_id\"/>"
"  <field type=\"variable32\" name=\"rollup_string\" />"
"  <field type=\"int32\" name=\"read_count\"/>"
"  <field type=\"double\" name=\"read_bytes\" print_format=\"%.0f\"/>"
"  <field type=\"int32\" name=\"write_count\"/>"
"  <field type=\"double\" name=\"write_bytes\" print_format=\"%.0f\"/>"
"</ExtentType>");

class RollupByIdString : public DataSeriesModule {
public:
    RollupByIdString(DataSeriesModule &_input,
		     const string &id_fieldname,
		     const string &string_fieldname)
	: input(_input),
	  id_field(input_series,id_fieldname),
	  string_field(input_series,string_fieldname),
	  is_read(input_series,"is_read"),
	  bytes(input_series,"bytes")
	  
    {
    }
    virtual ~RollupByIdString() { }

    typedef HashTable<rollupptr,rollupptr_hash,rollupptr_compare> rollupHT;
    void dumpInfo() {
	for(rollupHT::iterator i = rollup.begin();i != rollup.end();++i) {
	    printf("rollup %x/%s: reads %ld %.5f, writes %ld %.5f\n",
		   i->id,i->strval,i->data->read.count(),i->data->read.mean(),
		   i->data->write.count(),i->data->write.mean());
	}
	printf("\n\n");
    }
    Extent *infoAsExtent() {
	Extent *ret = new Extent(rollupbyidstring_type);
	ExtentSeries retseries(ret);
	Int32Field retid(retseries,"rollup_id");
	Variable32Field retstring(retseries,"rollup_string");
	Int32Field readcount(retseries,"read_count");
	DoubleField readbytes(retseries,"read_bytes");
	Int32Field writecount(retseries,"write_count");
	DoubleField writebytes(retseries,"write_bytes");

	for(rollupHT::iterator i = rollup.begin();i != rollup.end();++i) {
	    retseries.newRecord();
	    retid.set(i->id);
	    retstring.set(i->strval);
	    readcount.set(i->data->read.count());
	    readbytes.set(i->data->read.total());
	    writecount.set(i->data->write.count());
	    writebytes.set(i->data->write.total());
	}
	return ret;
    }
    virtual Extent *getExtent() {
	Extent *e = input.getExtent();
	if (e == NULL)
	    return e;
	
	for(input_series.setExtent(e);input_series.pos.morerecords();++input_series.pos) {
	    const char *pool_strval = mypool.getPoolPointer(string_field.stringval());

	    rollupptr key(id_field.val(),pool_strval);
	    const rollupptr *val = rollup.lookup(key);
	    if (val == NULL) {
		key.data = new rollupinfo;
		rollup.add(key); // ought to change add to return a pointer to the new data as it's done the work of the lookup.
		val = rollup.lookup(key);
		AssertAlways(val->data == key.data,("internal error\n"));
	    }
	    if (is_read.val()) {
		val->data->read.add(bytes.val());
	    } else {
		val->data->write.add(bytes.val());
	    }
	    //	    printf("%d %s %s %p\n",id_field.val(),string_field.stringval().c_str(),pool_strval,pool_strval);
	}
	return e;
    }
    
    DataSeriesModule &input;
    ExtentSeries input_series;
    Int32Field id_field;
    Variable32Field string_field;
    BoolField is_read;
    Int32Field bytes;

    StringPool mypool;
    rollupHT rollup;
};

class RollupByIdStringInfoExtent : public DataSeriesModule {
public:
    RollupByIdStringInfoExtent(RollupByIdString &_from)
	: from(_from), done(false)
    { }

    virtual ~RollupByIdStringInfoExtent() { }
    virtual Extent *getExtent() {
	if (done) return NULL;
	done = true;
	return from.infoAsExtent();
    }
    
    RollupByIdString &from;
    bool done;
};

const string rollupbyid_type( // not intended to get written, just internal, so pack options left off
"<ExtentType name=\"IDRollup\">"
"  <field type=\"int32\" name=\"rollup_id\"/>"
"  <field type=\"int32\" name=\"read_count\"/>"
"  <field type=\"double\" name=\"read_bytes\" print_format=\"%.0f\"/>"
"  <field type=\"int32\" name=\"write_count\"/>"
"  <field type=\"double\" name=\"write_bytes\" print_format=\"%.0f\"/>"
"</ExtentType>");

class RollupById : public DataSeriesModule {
public:
    RollupById(DataSeriesModule &_input,
	       const string &id_fieldname)
	: input(_input),
	  id_field(input_series,id_fieldname),
	  is_read(input_series,"is_read"),
	  bytes(input_series,"bytes")
	  
    {
    }
    virtual ~RollupById() { }

    typedef HashTable<rollupptr,rollupptr_hash,rollupptr_compare> rollupHT;
    void dumpInfo() {
	for(rollupHT::iterator i = rollup.begin();i != rollup.end();++i) {
	    printf("rollup %x: reads %ld %.5f, writes %ld %.5f\n",
		   i->id,i->data->read.count(),i->data->read.mean(),
		   i->data->write.count(),i->data->write.mean());
	}
	printf("\n\n");
    }
    Extent *infoAsExtent() {
	Extent *ret = new Extent(rollupbyid_type);
	ExtentSeries retseries(ret);
	Int32Field retid(retseries,"rollup_id");
	Int32Field readcount(retseries,"read_count");
	DoubleField readbytes(retseries,"read_bytes");
	Int32Field writecount(retseries,"write_count");
	DoubleField writebytes(retseries,"write_bytes");

	for(rollupHT::iterator i = rollup.begin();i != rollup.end();++i) {
	    retseries.newRecord();
	    retid.set(i->id);
	    readcount.set(i->data->read.count());
	    readbytes.set(i->data->read.total());
	    writecount.set(i->data->write.count());
	    writebytes.set(i->data->write.total());
	}
	return ret;
    }
    virtual Extent *getExtent() {
	Extent *e = input.getExtent();
	if (e == NULL)
	    return e;
	
	for(input_series.setExtent(e);input_series.pos.morerecords();++input_series.pos) {
	    rollupptr key(id_field.val(),NULL);
	    const rollupptr *val = rollup.lookup(key);
	    if (val == NULL) {
		key.data = new rollupinfo;
		rollup.add(key); // ought to change add to return a pointer to the new data as it's done the work of the lookup.
		val = rollup.lookup(key);
		AssertAlways(val->data == key.data,("internal error\n"));
	    }
	    if (is_read.val()) {
		val->data->read.add(bytes.val());
	    } else {
		val->data->write.add(bytes.val());
	    }
	}
	return e;
    }
    
    DataSeriesModule &input;
    ExtentSeries input_series;
    Int32Field id_field;
    BoolField is_read;
    Int32Field bytes;

    StringPool mypool;
    rollupHT rollup;
};

class RollupByIdInfoExtent : public DataSeriesModule {
public:
    RollupByIdInfoExtent(RollupById &_from)
	: from(_from), done(false)
    { }

    virtual ~RollupByIdInfoExtent() { }
    virtual Extent *getExtent() {
	if (done) return NULL;
	done = true;
	return from.infoAsExtent();
    }
    
    RollupById &from;
    bool done;
};

static const bool debug_level1_rollup = false;

int 
main(int argc, char *argv[])
{
    AssertAlways(argc >= 3,("Usage: %s <io-trace.ds> ... <ps-sample.ds> ...\n",argv[0]));
    
    TypeIndexModule iotrace_source("I/O trace");
    TypeIndexModule pstrace_source("Process sample");
    for(int i=1;i<argc;i++) {
	iotrace_source.addSource(argv[i]);
	pstrace_source.addSource(argv[i]);
    }
    
    PSIOSlopMergeJoin psiosmj(pstrace_source, iotrace_source,
			      5.0, 0.5, 300.0);
    RollupByIdString rollupbypv_cmd(psiosmj,"device_number","command");
    RollupByIdString rollupbylv_cmd(rollupbypv_cmd,"logical_volume_number","command");
    RollupById rollupbypv(rollupbylv_cmd,"device_number");
    RollupById rollupbylv(rollupbypv,"logical_volume_number");
    
    while(true) {
	//	Extent *e = dstext.getExtent();
	Extent *e = rollupbylv.getExtent();
	if (e == NULL) break;
	delete e;
    }
    fprintf(stderr,"I/O: %d extents, %d records, %lld bytes\n",
	    psiosmj.ioextents,psiosmj.io_record_count,psiosmj.io_byte_count);
    fprintf(stderr,"ps: %d extents, %d records, %lld bytes\n",
	    psiosmj.psextents,psiosmj.ps_record_count,psiosmj.ps_byte_count);
    fprintf(stderr,"merge-output: %d extents, %d records, %lld bytes\n",
	    psiosmj.output_extents,psiosmj.output_record_count,psiosmj.output_byte_count);
	    
    printf("LVPVProcessUsage-0.3\n");
    printf("first I/O %.6f; last I/O %.6f\n",psiosmj.firstIOTime(),psiosmj.lastIOTime());
    RollupByIdStringInfoExtent rollupbypv_cmd_info(rollupbypv_cmd);
    RollupByIdStringInfoExtent rollupbylv_cmd_info(rollupbylv_cmd);
    DStoTextModule a(rollupbypv_cmd_info);
    delete a.getExtent();
    DStoTextModule b(rollupbylv_cmd_info);
    delete b.getExtent();

    RollupByIdInfoExtent rollupbypv_info(rollupbypv);
    RollupByIdInfoExtent rollupbylv_info(rollupbylv);
    DStoTextModule c(rollupbypv_info);
    delete c.getExtent();
    DStoTextModule d(rollupbylv_info);
    delete d.getExtent();

    //    rollupbypv.dumpInfo();
    //    rollupbylv.dumpInfo();
    exit(0);

    const double time_slop = 90; 
    const double unknown_pid_time_slop = 4*60;

    ExtentSeries iotrace(ExtentSeries::typeLoose);
    ExtentSeries pstrace(ExtentSeries::typeLoose);

    DoubleField iotime(iotrace,"enter_driver",DoubleField::flag_allownonzerobase);
    DoubleField pstime(pstrace,"sampletime");
    Int32Field iopid(iotrace,"pid");
    Int32Field pspid(pstrace,"pid");
    Int32Field devid(iotrace,"device_number");
    Int32Field lvid(iotrace,"logical_volume_number");
    BoolField is_read(iotrace,"is_read");
    Int32Field iobytes(iotrace,"bytes");
    Variable32Field command(pstrace,"command");

    Extent *ioextent = iotrace_source.getExtent();
    Extent *psextent = pstrace_source.getExtent();
    iotrace.setExtent(ioextent);
    pstrace.setExtent(psextent);
    double time_base = iotime.val(); 
    if (pstime.val() < time_base) {
	time_base = pstime.val();
    }

    HashTable<mergeinfo,mergeinfo_hashpid,mergeinfo_comparepid> pid_to_process;
    typedef HashTable<mergeinfo,mergeinfo_hashlots,mergeinfo_comparelots> AccessHashTable;
    AccessHashTable access_info;

    double prev_ps_time = pstime.val() - time_slop;
    double cur_ps_time = pstime.val();
    int psextents = 1;
    int ioextents = 1;
    int iocount = 0;
    double first_io_time = Double::Inf, last_io_time = -Double::Inf;
    while(true) {
	if (iotrace.pos.morerecords() == false) {
	    delete ioextent;
	    ioextent = iotrace_source.getExtent();
	    if (ioextent == NULL) {
		break;
	    }
	    ++ioextents;
	    iotrace.setExtent(ioextent);
	}
	if (pstrace.pos.morerecords() == false) {
	    delete psextent;
	    psextent = pstrace_source.getExtent();
	    if (psextent == NULL) 
		break;
	    ++psextents;
	    pstrace.setExtent(psextent);
	}
	AssertAlways(iopid.val() >= 0 && pspid.val() >= 0,
		     ("can't handle negative pid's\n"));
	// keep filling the pid_to_process mapping until the current
	// pid time is at least the io time + time_slop
	if (pstime.val() <= iotime.absval() + time_slop) {
	    if (pstime.val() > (cur_ps_time + time_slop / 2)) {
		prev_ps_time = cur_ps_time;
		cur_ps_time = pstime.val();
	    }
	    mergeinfo *x = pid_to_process.lookup(mergeinfo(pspid.val()));
	    if (x == NULL) {
		// new pid
		mergeinfo newx(pspid.val());
		newx.command = command.stringval();
		newx.first_seen = pstime.val();
		newx.last_seen = pstime.val();
		pid_to_process.add(newx);
		if (debug_ps_read) {
		    printf("new-pid %d [%.0f .. %.0f] %s\n",pspid.val(),
			   newx.first_seen - time_base, newx.last_seen - time_base,
			   newx.command.c_str());
		}
	    } else {
		if (x->last_seen + (time_slop + cur_ps_time - prev_ps_time) >= pstime.val()) {
		    if (x->command != command.stringval()) {
			fprintf(stderr,"warning at (relative) time %.0f pid %d changed from '%s' to '%s' within %.4g seconds\n",
				pstime.val() - time_base,
				pspid.val(), x->command.c_str(), 
				command.stringval().c_str(), time_slop * 2);
			x->command = command.stringval();
		    }
		    x->last_seen = pstime.val();
		    if (debug_ps_read) {
			printf("update %d [%.0f .. %.0f] %s\n",pspid.val(),
			       x->first_seen - time_base, x->last_seen - time_base,
			       x->command.c_str());
		    }
		} else {
		    // new command
		    x->command = command.stringval();
		    x->last_seen = x->first_seen = pstime.val();
		    if (debug_ps_read) {
			printf("new-cmd %d [%.0f .. %.0f] %s\n",pspid.val(),
			       x->first_seen - time_base, x->last_seen - time_base,
			       x->command.c_str());
		    }
		}
	    }
	    ++pstrace.pos;
	} else {
	    if (iotime.absval() < first_io_time) {
		first_io_time = iotime.absval();
	    } 
	    // allow a little bit of backwardness because it doesn't hurt,
	    // and the traces we have seem to get a little bit of slop in the
	    // time ordering
	    AssertAlways(iotime.absval()+time_slop/1.0e3 >= last_io_time,
			 ("merge join doesn't work if merge field goes backwards; %.20g < %.20g\n",
			  iotime.absval(),last_io_time));
	    if (iotime.absval() > last_io_time) {
		last_io_time = iotime.absval();
	    }
	    
	    ++iocount;
	    mergeinfo *x = pid_to_process.lookup(mergeinfo(iopid.val()));
	    bool found;
	    if (x == NULL) {
		if (debug_io_read) {
		    printf("unknown access (pid %d) to dev %x, lv %x at %.4f\n",
			   iopid.val(),devid.val(),lvid.val(),iotime.absval() - time_base);
		}
		found = false;
	    } else if ((x->first_seen - time_slop) < iotime.absval() && 
		       (x->last_seen + time_slop) > iotime.absval()) {
		if (debug_io_read) {
		    printf("pid %d (%s) accessed dev %x, lv %x at %.4f\n",
			   x->pid,x->command.c_str(),devid.val(),lvid.val(), 
			   iotime.absval() - time_base);
		}
		found = true;
	    } else {
		if (debug_io_read) {
		    printf("pid %d *no-time-match* accessed dev %x, lv %x at %.4f not in [%.4f,%.4f]\n",
			   x->pid,devid.val(),lvid.val(),
			   iotime.absval() - time_base, x->first_seen - time_base,
			   x->last_seen - time_base);
		}
		found = false;
	    }
	    if (!found) {
		x = pid_to_process.lookup(mergeinfo(-iopid.val()));
		if (x == NULL) {
		    if (debug_io_read) {
			printf("new-pid %d at %.0f *unknown*\n",
			       -iopid.val(),iotime.absval() - time_base);
		    }
		    mergeinfo newx(iopid.val());
		    newx.command = "*unknown*";
		    newx.first_seen = iotime.absval();
		    newx.last_seen = iotime.absval();
		    pid_to_process.add(newx);
		    x = pid_to_process.lookup(newx);
		} else if (iotime.absval() < (x->last_seen + unknown_pid_time_slop)) {
		    if (x->last_seen < iotime.absval())
			x->last_seen = iotime.absval();
		    if (debug_io_read) {
			printf("update %d [%.0f .. %.0f] %s\n",x->pid,
			       x->first_seen, x->last_seen, x->command.c_str());
		    }
		} else {
		    x->first_seen = x->last_seen = iotime.absval();
		    if (debug_io_read) {
			printf("new-cmd %d at %.0f %s\n",x->pid,
			       x->first_seen, x->command.c_str());
		    }
		}
	    }		    
	    AssertAlways(x != NULL,("internal error\n"));
	    mergeinfo *access = access_info.lookup(mergeinfo(x->pid,x->first_seen,
							     devid.val(),lvid.val()));
	    if (access == NULL) {
		if (debug_io_read) {
		    printf("new access by pid %d to %d/%d\n",
			   x->pid,devid.val(),lvid.val());
		}
		mergeinfo newa(x->pid,x->first_seen,devid.val(),lvid.val());
		newa.bytes_read = newa.bytes_written = 0;
		newa.reads = newa.writes = 0;
		newa.command = x->command;
		newa.last_seen = Double::NaN;
		newa.first_io = newa.last_io = iotime.absval();
		access_info.add(newa);
		access = access_info.lookup(newa);
	    } 
	    access->last_io = iotime.absval();
	    if (is_read.val()) {
		++access->reads;
		access->bytes_read += iobytes.val();
	    } else {
		++access->writes;
		access->bytes_written += iobytes.val();
	    }
	    ++iotrace.pos;
	    if (iocount > 10000000) {
		break;
	    }
	}
    }

    RollupPVLV rollup_pv, rollup_lv;

    for(AccessHashTable::iterator i=access_info.begin();i != access_info.end();++i) {
	if (debug_level1_rollup) {
	    printf("accesses: pid %d, command '%s', lv %x, pv %x, ios [%.3f .. %.3f], %.2f MB/%d ios read; %.2f MB/%d ios written\n",
		   i->pid,i->command.c_str(),i->lvid,i->devid, 
		   i->first_io - time_base, 
		   i->last_io - time_base, (double)i->bytes_read/(1024*1024.0), i->reads,
		   (double)i->bytes_written/(1024*1024.0), i->writes);
	}
	rollup_pv[i->devid].push_back(&(*i));
	rollup_lv[i->lvid].push_back(&(*i));
    }

    printf("LVPVProcessUsage-0.2\n");
    printf("first I/O %.6f; last I/O %.6f\n",first_io_time, last_io_time);

    printf("Rollup by PV:\n");
    for(RollupPVLV::iterator i=rollup_pv.begin();i != rollup_pv.end();++i) {
	vector<mergeinfo> sum_list;
	mergeinfo sum;
	sumByCommand(i->second,sum,sum_list,5);
	printf("  pv=%x reads=%.2f MB/%d ios, writes=%.2f MB/%d ios; commands:\n",
	       i->first,(double)sum.bytes_read/(1024*1024.0),sum.reads,
	       (double)sum.bytes_written/(1024*1024.0),sum.writes);
	for(vector<mergeinfo>::iterator i = sum_list.begin();
	    i != sum_list.end();i++) {
	    printf("    reads=%.2f MB/%d ios writes=%.2f MB/%d ios, command='%s'\n",
		   (double)i->bytes_read/(1024*1024.0),i->reads,
		   (double)i->bytes_written/(1024*1024.0),i->writes,i->command.c_str());
	}
    }

    printf("Rollup by LV:\n");
    for(RollupPVLV::iterator i=rollup_lv.begin();i != rollup_lv.end();++i) {
	vector<mergeinfo> sum_list;
	mergeinfo sum;
	sumByCommand(i->second,sum,sum_list,5);
	printf("  lv=%x reads=%.2f MB/%d ios, writes=%.2f MB/%d ios; commands:\n",
	       i->first,(double)sum.bytes_read/(1024*1024.0),sum.reads,
	       (double)sum.bytes_written/(1024*1024.0),sum.writes);
	for(vector<mergeinfo>::iterator i = sum_list.begin();
	    i != sum_list.end();i++) {
	    printf("    reads=%.2f MB/%d ios writes=%.2f MB/%d ios, command='%s'\n",
		   (double)i->bytes_read/(1024*1024.0),i->reads,
		   (double)i->bytes_written/(1024*1024.0),i->writes,i->command.c_str());
	}
    }
	
    printf("read %d ps extents, %d io extents\n",psextents,ioextents);
}
