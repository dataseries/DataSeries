#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

using namespace std;
using boost::format;

/*
=pod
=head1 NAME

wcanalysis - analysis the 1998 World Cup web traces

=head1 SYNOPSIS

  % wcanalysis <input-ds-paths...>

=head1 DESCRIPTION

wcanalysis performs the same analysis as the checklog program from the
tools that shipped along with the 1998 World Cup traces.  It was
designed to produce exactly the same output so the same check files
can be compared.  It also has a slight inefficiency built in that
maintains parity with the calculation done in that program so that the
performance comparison is fairer.

The analysis calculated the total number of requests and total bytes
transferred and uses that to calculate the mean transfer size.  It
also calculates the minimum and maximum over the client ids, object
ids, size, and time fields.  It keeps the first time seen and the last
one and uses the latter to determine if there are any out of order
requests.  Note that you only get one out of order request if you tell
it to process two files out of order.  Finally it calculates the
number of unique servers that are accessed and the number of
operations that go to each of the different servers.  It calculates
the number of total requests and unique servers in two ways so that
they can be compared.

=head1 EXAMPLES

% wcanalysis wc_day?_*.ds
% wcanalysis wc_day[123]?_*.ds

=head1 SEE ALSO

wcweb2ds(1), DataSeries(5)

=head1 AUTHOR/CONTACT

Eric Anderson <software@cello.hpl.hp.com>
=cut
*/

class WorldCupSimpleAnalysis : public RowAnalysisModule {
public:
    static const uint32_t max_region = 4;
    static const uint32_t http_protocol_count = 4;
    static const uint32_t http_return_code_count = 38;
    static const uint32_t method_count = 9;
    static const uint32_t file_type_count = 13;

    WorldCupSimpleAnalysis(DataSeriesModule &source)
        : RowAnalysisModule(source),
	  request_count(0), bytes(0), 
	  min_object_size(numeric_limits<uint32_t>::max()),
	  max_object_size(numeric_limits<uint32_t>::min()),
	  min_client_id(numeric_limits<uint32_t>::max()),
	  max_client_id(numeric_limits<uint32_t>::min()),
	  min_object_id(numeric_limits<uint32_t>::max()),
	  max_object_id(numeric_limits<uint32_t>::min()),
	  min_time(numeric_limits<uint32_t>::max()),
	  max_time(numeric_limits<uint32_t>::min()),
	  first_time(numeric_limits<uint32_t>::max()),
	  last_time(numeric_limits<uint32_t>::min()),
	  out_of_order(0), unique_server_count(0), 

          timestamp(series, "timestamp"),
          clientID(series, "clientID"),
          objectID(series, "objectID"),
          size(series, "size"),
          method(series, "method"),
          status(series, "http_version_and_status"),
          file_type(series, "type"),
          server(series, "server")
    {
	server_op_count.resize(256);
    }

    virtual ~WorldCupSimpleAnalysis() { }

    virtual void prepareForProcessing() {
	first_time = timestamp.val();
    }

    virtual void processRow() {
	if ((request_count % 1000000) == 0) {
	    cerr << format("%d\n") % request_count;
	}
	++request_count;
	if (size.val() != -1) {
	    bytes += size.val();
	    min_object_size 
		= min(min_object_size, static_cast<uint32_t>(size.val()));
				  
	    max_object_size 
		= max(max_object_size, static_cast<uint32_t>(size.val()));
	}
	if (static_cast<uint32_t>(timestamp.val()) < last_time) {
	    ++out_of_order;
	}
	last_time = timestamp.val();
	min_time = min(min_time, static_cast<uint32_t>(timestamp.val()));
	max_time = max(max_time, static_cast<uint32_t>(timestamp.val()));
	min_object_id 
	    = min(min_object_id, static_cast<uint32_t>(objectID.val()));
	max_object_id 
	    = max(max_object_id, static_cast<uint32_t>(objectID.val()));
	min_client_id 
	    = min(min_client_id, static_cast<uint32_t>(clientID.val()));
	max_client_id 
	    = max(max_client_id, static_cast<uint32_t>(clientID.val()));
	
	if (server_op_count[server.val()] == 0) {
	    // inefficient, but matches what checklog.c does; so we aren't
	    // inappropriately faster
	    ++unique_server_count;
	}

	++server_op_count[server.val()];
	uint32_t server_region = (server.val() & 0xe0) >> 5;
	SINVARIANT(server_region < max_region);
	uint32_t server_num = (server.val() & 0x1f);
	SINVARIANT(server_num < 32);

	uint32_t http_protocol = (status.val() & 0xc0) >> 6;
	uint32_t http_return_code = (status.val() & 0x3f);
	SINVARIANT(http_protocol < http_protocol_count);
	SINVARIANT(http_return_code < http_return_code_count);
	SINVARIANT(method.val() < method_count);
	SINVARIANT(file_type.val() < file_type_count);
    }

    virtual void completeProcessing() {
    }

    virtual void printResult() {
	if (request_count == 0) {
	    cerr << "no requests, skipping output for consistency with wc tool\n";
	    return;
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
	cout << format("        Start Time: %d\n") % first_time;
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

private:
    uint32_t request_count;
    double bytes;
    uint32_t min_object_size, max_object_size;
    uint32_t min_client_id, max_client_id;
    uint32_t min_object_id, max_object_id;
    uint32_t min_time, max_time;
    uint32_t first_time, last_time;
    uint32_t out_of_order, unique_server_count;
    vector<uint32_t> server_op_count;
    
    Int32Field timestamp;
    Int32Field clientID;
    Int32Field objectID;
    Int32Field size;
    ByteField method;
    ByteField status;
    ByteField file_type;
    ByteField server;
};

int
main(int argc, char *argv[]) 
{
    TypeIndexModule *source 
        = new TypeIndexModule("Log::Web::WorldCup::Custom");

    INVARIANT(argc >= 2 && strcmp(argv[1], "-h") != 0,
              boost::format("Usage: %s <file...>\n") % argv[0]);

    for(int i = 1; i < argc; ++i) {
        source->addSource(argv[i]);
    }

    SequenceModule seq(source);
    
    seq.addModule(new WorldCupSimpleAnalysis(seq.tail()));
    
    seq.getAndDelete();
    RowAnalysisModule::printAllResults(seq);
    return 0;
}
