/* -*-C++-*-
   (c) Copyright 2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    indexer for NFS Common DataSeries files
*/

// TODO: obsolete this program, merge it into dsextentindex

#include <sys/types.h>
#include <sys/stat.h>

#include <map>

#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

using namespace std;
using boost::format;

const ExtentType::int64 max_backward_ns_in_extent = 20000000LL;
const ExtentType::int64 max_backward_ns_between_extent = 5000000LL;
const ExtentType::int64 min_backward_ns_print_warning = 250000LL;

struct fileinfo {
    ExtentType::int64 start_id, end_id;
    ExtentType::int64 start_time, end_time;
    string filename;
    ExtentType::int64 mtime;
};

typedef map<ExtentType::int64,fileinfo> indexmapT;
indexmapT indexmap; // startid -> info

map<const string,ExtentType::int64> filename_to_mtime;

ExtentType::int64 mtimens(struct stat &buf)
{
    return (ExtentType::int64)buf.st_mtime * (ExtentType::int64)1000000000 + buf.st_mtim.tv_nsec;
}

void
readExistingIndex(char *out_filename)
{
    TypeIndexModule src("NFS trace: common index");
    src.addSource(out_filename);
    Extent *e = src.getExtent();
    INVARIANT(e->type.getName() == "NFS trace: common index",
	      format("whoa, extent type %s bad") % e->type.getName());

    ExtentSeries s(e);
    Int64Field start_id(s,"start-id"), end_id(s,"end-id"), 
	start_time(s,"start-time"), end_time(s,"end-time"),
	mtime(s,"modify-time");
    Variable32Field filename(s,"filename");
    for(;s.pos.morerecords();++s.pos) {
	fileinfo f;
	f.start_id = start_id.val();
	f.end_id = end_id.val();
	f.start_time = start_time.val();
	f.end_time = end_time.val();
	f.filename = filename.stringval();
	f.mtime = mtime.val();
	indexmap[f.start_id] = f;
	filename_to_mtime[f.filename] = f.mtime;
    }

    delete e;
    
    e = src.getExtent(); 
    SINVARIANT(e->type.getName() == "DataSeries: ExtentIndex");
    delete e;
    INVARIANT(src.getExtent() == NULL, "whoa, index had incorrect extents");
}

struct backward_timeness {
    ExtentType::int64 packettime, end_time;
};

backward_timeness ok_backwards[] = {
    { 1063919462671218000LL, 1063919462681109000LL },
    { 1063919527931539000LL, 1063919527941406000LL },
    { 1063919534511698000LL, 1063919534521648000LL },
    { 1063919534652061000LL, 1063919534661858000LL },
    { 1063919534751990000LL, 1063919534761936000LL },
    { 1063919534791328000LL, 1063919534801253000LL },
    { 1063919534893204000LL, 1063919534903125000LL },
    { 1063919534923560000LL, 1063919534933462000LL },
    { 1063919534951917000LL, 1063919534961752000LL },
    { 1063919535081528000LL, 1063919535091485000LL },
    { 1065672552011597000LL, 1065672552011625000LL }, // ignoring < 50us ones programmatically; set-8 has lots of these
    { 1065677570611604000LL, 1065677570611669000LL },
    { 1065680013961022000LL, 1065680013961084000LL },
    { 1065682948655623000LL, 1065682948655695000LL },
    { 1065725113821603000LL, 1065725113821657000LL },
    { 1065725736211604000LL, 1065725736211659000LL },
    { 1065725807172901000LL, 1065725807172980000LL },
    { 1065792805579551000LL, 1065792805579827000LL },
    { 1065833055231605000LL, 1065833055231712000LL }, // set-8/cqracks.10801.ds
    { 1065834220752853000LL, 1065834220752910000LL }, // set-8/cqracks.10989.ds
};

int nok_backwards = sizeof(ok_backwards) / sizeof(backward_timeness);

void
updateFileInfo(DataSeriesSource &source, off64_t offset, fileinfo &f)
{
    ExtentSeries s;
    Int64Field recordid(s,"record-id");
    Int64Field packettime(s,"packet-at");

    Extent *e = source.preadExtent(offset);

    for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	if (f.start_id < 0) {
	    f.start_id = f.end_id = recordid.val();
	    f.start_time = f.end_time = packettime.val();
	} else {
	    SINVARIANT(recordid.val() > f.end_id);
	    f.end_id = recordid.val();
	    if (packettime.val() < f.end_time) {
		// this happens all the time; the large jumps are
		// probably ntp time stepping, the smaller ones are
		// probably linux's clock sucking.
		bool found_ok_backwardness = false;
		for(int i=0;i<nok_backwards;++i) {
		    if (packettime.val() == ok_backwards[i].packettime &&
			f.end_time == ok_backwards[i].end_time) {
			SINVARIANT(found_ok_backwardness == false);
			found_ok_backwardness = true;
		    }
		}
		long long backward_ns = f.end_time - packettime.val();
		if (backward_ns <= min_backward_ns_print_warning) {
		    // 50 us backwards is a little high to complete
		    // ignore, but starts occurring alot in set-8;
		    // don't even bother to print a warning.

		    // this is probably happening because we're tracing on two interfaces, and so
		    // the packets from the different interfaces can get to user level slightly out
		    // of order.
		} else if (found_ok_backwardness) {
		    printf("tolerating backwards timeness on recordid %lld of %lld ns: %lld < %lld\n",
			   recordid.val(),f.end_time - packettime.val(),
			   packettime.val(),f.end_time);
		} else {
		    INVARIANT(backward_ns <= max_backward_ns_in_extent,
			      format("bad2 %lld < %lld -> %lld")
			      % packettime.val() % f.end_time % backward_ns);
		    printf("warning backwards timeness on recordid %lld of %lld ns: %lld < %lld\n",
			   recordid.val(),f.end_time - packettime.val(),packettime.val(),f.end_time);
		}
	    } else {
		f.end_time = packettime.val();
	    }
	}
    }
    delete e;
}

bool
updateIndexMap(char *filename)
{
    struct stat statbuf;
    CHECKED(stat(filename,&statbuf)==0,
	    format("stat(%s) failed: %s") % filename % strerror(errno));
    ExtentType::int64 mtime = mtimens(statbuf);
    if (filename_to_mtime[filename] == mtime) {
	printf("%s: up to date\n",filename);
	return false;
    }
    DataSeriesSource source(filename);

    ExtentSeries s(source.indexExtent);
    Variable32Field extenttype(s,"extenttype");
    Int64Field offset(s,"offset");

    ExtentType::int64 first_offset = -1, last_offset = -1;
    for(;s.pos.morerecords();++s.pos) {
	if (extenttype.equal("NFS trace: common")) {
	    if (first_offset < 0) {
		first_offset = last_offset = offset.val();
	    }
	    if (offset.val() > last_offset) {
		last_offset = offset.val();
	    }
	}
    }
    printf("%s: first offset @ %lld, last @ %lld\n",
	   filename,first_offset,last_offset);
    fileinfo f;
    f.start_id = f.end_id = -1;
    f.start_time = f.end_time = -1;
    f.filename = filename;
    f.mtime = mtime;
    updateFileInfo(source,first_offset,f);
    if (last_offset > first_offset) {
	updateFileInfo(source,last_offset,f);
    }
    printf("  ids: %lld .. %lld; time: %.6f .. %.6f\n",
	   f.start_id,f.end_id,
	   (double)f.start_time / 1.0e9,
	   (double)f.end_time / 1.0e9);
    indexmapT::iterator exist = indexmap.find(f.start_id);
    if (exist != indexmap.end()) {
	SINVARIANT(exist->second.start_id == f.start_id &&
		   exist->second.end_id == f.end_id &&
		   exist->second.start_time == f.start_time &&
		   exist->second.end_time == f.end_time &&
		   exist->second.filename == f.filename);
	exist->second.mtime = mtime;
    } else {
	indexmap[f.start_id] = f;
    }
    return true;
}

backward_timeness ok_validate[] = {
    { 1065685186348250000LL , 1065685186348251000LL },
    { 1065834786302831000LL , 1065834786302832000LL },
};

int nok_validate = sizeof(ok_validate) / sizeof(backward_timeness);

void
validateIndexMap()
{
    ExtentType::int64 last_id = -1, last_time = -1;
    for(indexmapT::iterator i = indexmap.begin();
	i != indexmap.end();++i) {
	SINVARIANT(i->second.start_id > last_id);
	last_id = i->second.end_id;
	if (i->second.start_time < last_time) {
	    bool found_ok_backwardness = false;
	    for(int j=0;j<nok_validate;++j) {
		if (i->second.start_time == ok_validate[j].packettime &&
		    last_time == ok_validate[j].end_time) {
		    SINVARIANT(found_ok_backwardness == false);
		    found_ok_backwardness = true;
		}
	    }
	    long long backward_ns = last_time - i->second.start_time;
	    SINVARIANT(backward_ns > 0);
	    if (backward_ns < max_backward_ns_between_extent)
		found_ok_backwardness = true;
	    if (found_ok_backwardness) {
		if (backward_ns > min_backward_ns_print_warning) {
		    printf("tolerating validation backwards of %lld ns: %lld < %lld\n",
			   backward_ns, i->second.start_time,
			   last_time);
		}
	    } else {
		INVARIANT(i->second.start_time >= last_time,
			  format("bad out of order %lld < %lld -> %lld")
			  % i->second.start_time % last_time % backward_ns);
	    }
	}
	last_time = i->second.end_time;
    }
}

const string indextype_xml = 
"<ExtentType name=\"NFS trace: common index\">\n"
"  <field type=\"int64\" name=\"start-id\" />\n"
"  <field type=\"int64\" name=\"end-id\" />\n"
"  <field type=\"int64\" name=\"start-time\" />\n"
"  <field type=\"int64\" name=\"end-time\" />\n"
"  <field type=\"variable32\" name=\"filename\" />\n"
"  <field type=\"int64\" name=\"modify-time\" comment=\"file modify time in ns since unix epoch\" />\n"
"</ExtentType>";

void
writeIndexMap(char *out_filename)
{
    DataSeriesSink sink(out_filename);
    ExtentTypeLibrary library;
    const ExtentType *indextype = library.registerType(indextype_xml);
    sink.writeExtentLibrary(library);

    Extent *e = new Extent(*indextype);
    ExtentSeries s(e);
    Int64Field start_id(s,"start-id"), end_id(s,"end-id"), 
	start_time(s,"start-time"), end_time(s,"end-time"),
	mtime(s,"modify-time");
    Variable32Field filename(s,"filename");

    for(indexmapT::iterator i = indexmap.begin();
	i != indexmap.end();++i) {
	s.newRecord();
	start_id.set(i->second.start_id);
	end_id.set(i->second.end_id);
	start_time.set(i->second.start_time);
	end_time.set(i->second.end_time);
	filename.set(i->second.filename);
	SINVARIANT(i->second.mtime > 0);
	mtime.set(i->second.mtime);
    } 
    sink.writeExtent(*e, NULL);
}

int
main(int argc, char *argv[])
{
    INVARIANT(argc > 2 && strncmp(argv[0],"-h",2) != 0,
	      format("Usage: %s <index-dataseries> <nfs common dataseries...>")
	      % argv[0]);
    struct stat buf;

    if (stat(argv[1],&buf) == 0) {
	readExistingIndex(argv[1]);
    }
    bool any_change = false;
    for(int i=2;i<argc;++i) {
	bool t = updateIndexMap(argv[i]);
	any_change = any_change || t;
    }
    validateIndexMap();
    if (any_change) {
	writeIndexMap(argv[1]);
    }
}

