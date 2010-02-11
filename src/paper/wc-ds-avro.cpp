//
// (c) Copyright 2010, Hewlett-Packard Development Company, LP
//
//  See the file named COPYING for license details


#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <string>

#include <Lintel/ProgramOptions.hpp>

#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

#include <avro.h>

using namespace std;
using boost::format;

// Probably worth having something like this as a convinience function, having all of the
// escaped " seems really annoying.
string quoteReplace(const string &in) {
    string ret = in;
    size_t pos = 0;
    while (true) {
	pos = ret.find("'", pos);
	if (pos != string::npos) {
	    ret[pos] = '\"';
	} else {
	    break;
	}
    }
    return ret;
}
	
const string wcweb_schema_json
(quoteReplace
 ("{ 'type': 'record',\n"
  "  'name': 'wcweb',\n" // Are you going to need the equivalent of namespace/version?
  "  'fields': [\n"
  "    { 'name': 'timestamp', 'type': 'int' },\n" 
  // I'd slightly prefer int32, int64 to int, long; it seems less ambiguous, it
  // also gives a standard pattern for int16 and int128
  "    { 'name': 'clientID', 'type': 'int' },\n"
  "    { 'name': 'objectID', 'type': 'int' },\n"
  "    { 'name': 'size', 'type': 'int' },\n"
  "    { 'name': 'method', 'type': 'int' },\n" // ought to be int8, maybe bytes?
  "    { 'name': 'http_version_and_status', 'type': 'int' },\n" // same here and below
  "    { 'name': 'type', 'type': 'int' },\n"
  "    { 'name': 'server', 'type': 'int' }\n"
  "  ]}\n"));

// Having to do all of this decref stuff in C seems error prone, might be worth
// giving people a C++ wrapper like the below from the start.

class AvroDatum {
public:
    AvroDatum() : val(NULL) { }
    ~AvroDatum() {
	avro_datum_decref(val);
    }

    void setInt32(int32_t v) {
	if (val) {
	    avro_datum_decref(val);
	}
	val = avro_int32(v);
    }

    avro_datum_t val;
};

// I first tried the begining structure, but then realized that the next one would work better.
// there may be cases for the first one, I think the equivalent of avro_set_field_int32()
// could be done in C, and would be more convinient.

class AvroRecord {
public:
    AvroRecord(const char *record_type) 
	: record(avro_record(record_type, NULL))
    {
	// can avro_record return NULL?
    }

    ~AvroRecord() {
	avro_datum_decref(record);
    }

    void setInt32(const char *field_name, int32_t val) {
	avro_datum_t a_val = avro_int32(val);
	
	CHECKED(avro_record_set(record, field_name, a_val) == 0,
		"avro_record_set failed");
	avro_datum_decref(a_val);
    }

    int32_t getInt32(const char *field_name) {
	avro_datum_t a_ret;
	int32_t ret;

	if (avro_record_get(record, field_name, &a_ret) == 0) {
	    avro_int32_get(a_ret, &ret);
	    // Don't I need avro_datum_decref(&a_ret) here?  It's not in the example, but
	    // I would expect to need it.
	    return ret;
	} else {
	    avro_datum_print(record, stdout);
	    FATAL_ERROR(format("Could not get field %s in above") % field_name);
	}
    }

    void appendTo(avro_file_writer_t to) {
	CHECKED(avro_file_writer_append(to, record) == 0,
		"error appending record");
    }

    bool readFrom(avro_file_reader_t from, avro_schema_t schema) {
	avro_datum_decref(record);
	int rval = avro_file_reader_read(from, schema, &record);
	if (rval == 0) {
	    return true;
	} else {
	    // Can I do = NULL here and have that work with decref?
	    record = avro_record("Unknown", NULL); 
	    return false;
	}
    }

    void destroy() {
	avro_datum_decref(record);
    }

    avro_datum_t record;
};

/*
--- gzip to ds conversion (no compression)...
1061: time gunzip -c ~/nobackup-data/world-cup/wc_day51_1.gz| wcweb2ds --compress-gz --disable-gz - ~/nobackup-data/world-cup/wc_day51_1.ds.none
gunzip -c ~/nobackup-data/world-cup/wc_day51_1.gz  1.83s user 0.11s system 62% cpu 3.128 total
wcweb2ds --compress-gz --disable-gz -   1.83s user 0.61s system 78% cpu 3.130 total
1063: time gzip -9v ~/nobackup-data/world-cup/wc_day51_1.ds.none
gzip -9v ~/nobackup-data/world-cup/wc_day51_1.ds.none  165.40s user 0.17s system 99% cpu 2:46.77 total

--- gzip to ds conversion (built-in compression)
1050: time gunzip -c ~/nobackup-data/world-cup/wc_day51_1.gz| wcweb2ds --compress-gz - ~/nobackup-data/world-cup/wc_day51_1.ds 
WARN: Warning, pack_pad_record under testing, may not be safe for use.

WARN: Warning, pack_field_ordering under testing, may not be safe for use.

Processed 6999999 records
gunzip -c ~/nobackup-data/world-cup/wc_day51_1.gz  1.36s user 0.04s system 3% cpu 35.675 total
wcweb2ds --compress-gz - ~/nobackup-data/world-cup/wc_day51_1.ds  138.13s user 0.48s system 386% cpu 35.854 total

--- read the ds file, skip per record ds stuff.
1055: xmake && time ./wc-ds-avro --skip-avro ds2avro ~/nobackup-data/world-cup/wc_day51_1.ds /tmp/wcweb-50-4.head.avro
./wc-ds-avro --skip-avro ds2avro ~/nobackup-data/world-cup/wc_day51_1.ds   1.49s user 0.12s system 377% cpu 0.425 total

--- ds to avro conversion... (two steps, convert, then compress)
1057: xmake && time ./wc-ds-avro ds2avro ~/nobackup-data/world-cup/wc_day51_1.ds ~/nobackup-data/world-cup/wc_day51_1.avro
./wc-ds-avro ds2avro ~/nobackup-data/world-cup/wc_day51_1.ds   44.80s user 0.50s system 98% cpu 46.112 total
1059: time gzip -9v ~/nobackup-data/world-cup/wc_day51_1.avro
gzip -9v ~/nobackup-data/world-cup/wc_day51_1.avro  48.07s user 0.16s system 98% cpu 49.214 total

1062: ls -l ~/nobackup-data/world-cup/wc_day51_1.ds.none
-rw-rw-r-- 1 anderse anderse 140225548 2010-02-10 19:56 /home/anderse/nobackup-data/world-cup/wc_day51_1.ds.none
1058: ls -l ~/nobackup-data/world-cup/wc_day51_1.avro
-rw-rw-r-- 1 anderse anderse 125989050 2010-02-10 19:52 /home/anderse/nobackup-data/world-cup/wc_day51_1.avro
1060: ls -l ~/nobackup-data/world-cup/wc_day51_1.avro.gz 
-rw-rw-r-- 1 anderse anderse 45723382 2010-02-10 19:52 /home/anderse/nobackup-data/world-cup/wc_day51_1.avro.gz
1051: ls -l ~/nobackup-data/world-cup/wc_day51_1.ds 
-rw-rw-r-- 1 anderse anderse 45139808 2010-02-10 19:39 /home/anderse/nobackup-data/world-cup/wc_day51_1.ds
1052: ls -l ~/nobackup-data/world-cup/wc_day51_1.gz 
-rw-rw-r-- 1 anderse anderse 46190362 2008-03-19 01:37 /home/anderse/nobackup-data/world-cup/wc_day51_1.gz


*/

lintel::ProgramOption<bool> po_skip_avro("skip-avro", "skip the per-record avro work?");
    
class DS2Avro : public RowAnalysisModule {
public:
    DS2Avro(DataSeriesModule &source, const string &avro_out_name)
        : RowAnalysisModule(source),
          timestamp(series, "timestamp"),
          clientID(series, "clientID"),
          objectID(series, "objectID"),
          size(series, "size"),
          method(series, "method"),
          http_version_and_status(series, "http_version_and_status"),
          type(series, "type"),
          server(series, "server"), 
	  records(0), skip_avro(po_skip_avro.get())
    { 
	avro_schema_error_t error;
	
	// Would be nice to have a char * rather than void/size version of this.
	// Also what is a struct avro_schema_error_t?  I can't find anything that would
	// get me the error in avro.h.
	CHECKED(avro_schema_from_json(wcweb_schema_json.data(), wcweb_schema_json.size(),
				      &avro_schema, &error) == 0, 
		format("error parsing schema '%s'") % wcweb_schema_json);

	CHECKED(unlink(avro_out_name.c_str()) == 0 || errno == ENOENT, "bad unlink");

	CHECKED(avro_file_writer_create(avro_out_name.c_str(), avro_schema, &avro_out) == 0,
		"error setting up writer");
    }

    virtual ~DS2Avro() { }

    virtual void processRow() {
	++records;
	if (skip_avro) return;
	AvroRecord record("wcweb");

	record.setInt32("timestamp", timestamp());
	record.setInt32("clientID", clientID());
	record.setInt32("objectID", objectID());
	record.setInt32("size", size());
	record.setInt32("method", method());
	record.setInt32("http_version_and_status", http_version_and_status());
	record.setInt32("type", type());
	record.setInt32("server", server());

	record.appendTo(avro_out);
    }

    virtual void printResult() {
	// example doesn't check this
	CHECKED(avro_file_writer_close(avro_out) == 0, "close failed");
	cout << format("converted %d records\n") % records;
    }

private:
    Int32Field timestamp;
    Int32Field clientID;
    Int32Field objectID;
    Int32Field size;
    ByteField method;
    ByteField http_version_and_status;
    ByteField type;
    ByteField server;

    avro_file_writer_t avro_out;
    avro_schema_t avro_schema;
    int64_t records;
    bool skip_avro;
};

void ds2avro(const string &in, const string &out) {
    TypeIndexModule source("Log::Web::WorldCup::Custom");

    source.addSource(in);
    DS2Avro conv(source, out);
    conv.getAndDelete();
    conv.printResult();
}

void avroanalysis(const string &in) {
    avro_schema_error_t error;
    avro_schema_t avro_schema;

    CHECKED(avro_schema_from_json(wcweb_schema_json.data(), wcweb_schema_json.size(),
				  &avro_schema, &error) == 0, 
	    format("error parsing schema '%s'") % wcweb_schema_json);

    avro_file_reader_t avro_in;
    // example also missing check here
    CHECKED(avro_file_reader(in.c_str(), &avro_in) == 0, "reader failed");

    uint32_t request_count = 0;
    uint32_t out_of_order = 0;
    int32_t last_time = 0;
    uint32_t min_time = numeric_limits<uint32_t>::max();
    uint32_t max_time = numeric_limits<uint32_t>::min();
    uint32_t min_object_size = numeric_limits<uint32_t>::max();
    uint32_t max_object_size = numeric_limits<uint32_t>::min();
    uint32_t min_object_id = numeric_limits<uint32_t>::max();
    uint32_t max_object_id = numeric_limits<uint32_t>::min();
    uint32_t min_client_id = numeric_limits<uint32_t>::max();
    uint32_t max_client_id = numeric_limits<uint32_t>::min();
    vector<uint32_t> server_op_count;
    server_op_count.resize(256);
    uint32_t unique_server_count = 0;

    double bytes = 0;

    if (false) {
	avro_datum_t record;
	
	int rval = avro_file_reader_read(avro_in, avro_schema, &record);
	
	cout << format("HI %d %s\n") % rval % record;

	if (rval == 0) {
	    avro_datum_print(record, stdout);
	    fflush(stdout);
	}

	avro_datum_t a_size;

	rval = avro_record_get(record, "size", &a_size);

	cout << format("HI2 %d %s\n") % rval % a_size;

	if (rval == 0) {
	    avro_datum_print(a_size, stdout);
	    int32_t ival;
	    avro_int32_get(a_size, &ival);
	    cout << format("HI3 %d\n") % ival;
	}
    }
    
    AvroRecord record("N/A");
    while (record.readFrom(avro_in, avro_schema)) {
	// seems like it might be better to bind the schema when you create the reader.
	// does it make sense to read from the same reader with different schemas?

	if ((request_count % 1000000) == 0) {
	    cerr << format("%d\n") % request_count;
	}

	++request_count;
	int32_t size = record.getInt32("size");
	if (size != -1) {
	    bytes += size;
	    min_object_size = min(min_object_size, static_cast<uint32_t>(size));
	    max_object_size = max(max_object_size, static_cast<uint32_t>(size));
	}
	int32_t timestamp = record.getInt32("timestamp");
	if (timestamp < last_time) {
	    ++out_of_order;
	}
	last_time = timestamp;
	min_time = min(min_time, static_cast<uint32_t>(timestamp));
	max_time = max(max_time, static_cast<uint32_t>(timestamp));

	int32_t object_id = record.getInt32("objectID");
	min_object_id = min(min_object_id, static_cast<uint32_t>(object_id));
	max_object_id = max(max_object_id, static_cast<uint32_t>(object_id));

	int32_t client_id = record.getInt32("clientID");
	min_client_id = min(min_client_id, static_cast<uint32_t>(client_id));
	max_client_id = max(max_client_id, static_cast<uint32_t>(client_id));

	int32_t server = record.getInt32("server");

	if (server_op_count[server] == 0) {
	    ++unique_server_count;
	}
	
	++server_op_count[server];

	cout << format("%d %d %d %d\n") % timestamp % client_id % object_id % size;
	// Skip the other sanity checks in wcanalysis.cpp/checklog.c for now...
    }

    cout << format("    Total Requests: %d\n") % request_count;
    cout << format("       Total Bytes: %.0f\n") % bytes;
    cout << format("Mean Transfer Size: %.6f\n") 
	% (bytes / static_cast<double>(request_count));
    cout << format("     Min Client ID: %d\n") % min_client_id;
    cout << format("     Max Client ID: %d\n") % max_client_id;
    cout << format("     Min Object ID: %d\n") % min_object_id;
    cout << format("     Max Object ID: %d\n") % max_object_id;
    cout << format("          Min Size: %d\n") % min_object_size;
    cout << format("          Max Size: %d\n") % max_object_size;
    cout << format("          Min Time: %d\n") % min_time;
    cout << format("          Max Time: %d\n") % max_time;
    //    cout << format("        Start Time: %d\n") % first_time;
    cout << format("       Finish Time: %d\n") % last_time;
    cout << format("      Out of Order: %d\n") % out_of_order;
    cout << format("    Unique Servers: %d\n") % unique_server_count;
    uint32_t used_servers = 0;
    uint64_t check_count = 0;
    for(vector<uint32_t>::iterator i = server_op_count.begin();
	i != server_op_count.end(); ++i) {
	if (*i > 0) {
	    ++used_servers;
	    check_count += *i;
	    cout << format("Server %d: %d\n") 
		% (i - server_op_count.begin()) % *i;
	}
    }
    cout << "Check:\n";
    SINVARIANT(used_servers == unique_server_count);
    SINVARIANT(check_count == request_count);
    cout << format("Total Requests: %d\n") % check_count;
    cout << format("Unique Servers: %d\n") % used_servers;
}

int main(int argc, char *argv[]) {
    vector<string> args = lintel::parseCommandLine(argc, argv, true);

    SINVARIANT(args.size() >= 1);
    if (args[0] == "ds2avro") {
	SINVARIANT(args.size() == 3);
	ds2avro(args[1], args[2]);
    } else if (args[0] == "avro-analysis") {
	SINVARIANT(args.size() == 2);
	avroanalysis(args[1]);
    } else {
	FATAL_ERROR("?");
    }
    return 0;
}

    
