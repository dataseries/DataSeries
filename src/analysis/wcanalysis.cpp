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

class TestByteField : public FixedField {
  public:
    TestByteField(ExtentSeries &_dataseries, const std::string &field)
            : FixedField(_dataseries,field, ExtentType::ft_byte, 0) {
        dataseries.addField(*this); 
    }

    uint8_t val() const { 
        return *(uint8_t *)(rowPos() + offset);
    }
    void set(int32_t val) {
        *(uint8_t *)(rowPos() + offset) = val;
        setNull(false);
    }
};

class TestInt32Field : public FixedField {
  public:
    TestInt32Field(ExtentSeries &_dataseries, const std::string &field)
            : FixedField(_dataseries,field, ExtentType::ft_int32, 0) {
        dataseries.addField(*this); 
    }

    int32_t val() const { 
        return *(int32_t *)(rowPos() + offset);
    }
    void set(int32_t val) {
        *(int32_t *)(rowPos() + offset) = val;
        setNull(false);
    }
};

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

    inline void inlineProcessRow() {
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

#if 0
    virtual Extent::Ptr getSharedExtent() {
        Extent::Ptr e = source.getSharedExtent();
        if (e == NULL) {
            completeProcessing();
            return NULL;
        }
        series.setExtent(e);
        if (!prepared) {
            prepareForProcessing();
            prepared = true;
        }
        for (;series.pos.morerecords();++series.pos) {
            inlineProcessRow();
        }
        series.clearExtent();
        return e;
    }
    virtual void processRow() { 
        FATAL_ERROR("no");
    }
#else
    virtual void processRow() {
        inlineProcessRow();
    }
#endif

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
        for (vector<uint32_t>::iterator i = server_op_count.begin();
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

    //-- Measurements using wc_day51_1.ds (gz compression) 
    //-- timing in seconds for just user time.

    //-- Measurements on a HP nw9440 - T2600 cpu, debian etch,
    //-- frequency scaling off

    // Timing gunzip | checklog: 
    // 1.85 | 0.96; 1.90 | 1.06; 1.99 | 1.04; 1.89 | 0.97; 1.95 | 1.05
    // 3,059,581,616 instructions in checklog; 959,195,741 in ReadLog

    // Timing 1.95, 1.92, 1.88, 1.90, 1.96 -- base
    // 5,133,672,411 total instructions
    // 1,774,993,925 instructions below SequenceModule::getExtent

    // 1.84, 1.84, 1.86, 1.77, 1.85, 1.83 -- with TestInt32Field
    // 4,784,308,999 total instructions
    // 1,424,290,037 instructions below SequenceModule::getExtent

    // 1.76, 1.80, 1.75, 1.76 -- with TestInt32Field and TestByteField
    // 4,588,277,698 total instructions
    // 1,228,209,841 instructions below SequenceModule::getExtent

    // 1.71, 1.71, 1.74, 1.72 -- with inlineProcessRow
    // 4,439,796,689 total instructions 
    // 1,081,011,994 instructions below SequenceModule::getExtent

    // 1.74, 1.73, 1.75, 1.73, 1.74 -- with virtual processRow,
    // template fields.
    // 4,586,668,439 total instructions
    // 1,227,962,351 instructions below SequenceModule::getExtent

    // 1,081,051,478 below with gcc-4.1.2 on debian 4.0 (etch)
    // 875,204,012 below with gcc-4.3.1 on debian testing/x86 (lenny)
    // 1,593,631,927 below with gcc-4.3.1 and in base configuration
    // So all the optimization is cenetered around the lowest level;
    // the delta between gcc-4.3.1 base and all code optimized is the
    // same as for gcc-4.1.

    //-- Measurements on a 2-cpu HP DL365g1 Opteron 2216HE (4 cores
    //-- total), RHEL4 frequency scaling off

    // Timing gunzip | checklog: (more measurements, was less stable)
    // 1.33 | 0.76; 1.60 | 0.56; 1.42 | 0.63; 1.31 | 0.74; 1.66 | 0.52
    // 1.57 | 0.49; 1.59 | 0.53; 1.60 | 0.46; 1.60 | 0.48; 1.60 | 0.51
    // mean = 1.528 | 0.568, sum = 2.096
    // 2,465,016,298 instructions in checklog; 800,996,515 in ReadLog

    // Timing 1.85, 1.86, 1.85, 1.87, 1.85 -- base mean 1.854
    // 5,852,543,291 total instructions
    // 2,546,724,980 instructions below SequenceModule::getExtent

    // 1.72, 1.73, 1.73, 1.73, 1.72 -- with TestInt32Field
    // 5,197,941,237 total instructions
    // 1,892,067,616 instructions below SequenceModule::getExtent

    // 1.58, 1.59, 1.57, 1.57, 1.61 -- with TestInt32Field and TestByteField
    // 4,715,044,617 total instructions
    // 1,409,180,175 instructions below SequenceModule::getExtent

    // 1.57, 1.58, 1.56, 1.59, 1.60 -- with inlineProcessRow
    // 4,673,103,022 total instructions 
    // 1,367,163,203 instructions below SequenceModule::getExtent

    //-- gcc-4.3 same machine
    // 1.32, 1.33, 1.32, 1.32, 1.32 -- both test fields and inline
    // 4,023,558,639 total instructions
    // 801,086,524 below SequenceModule::getExtent

    // gunzip | checklog: 1.32 | 0.76, 1.34 | 0.74, 1.34 | 0.74, 1.33 | 0.76
    // 2,465,016,163 instructions in checklog; 800,996,515 in ReadLog

    // zcat wc_day*.gz | bin/checklog; user,sys | user,sys; wall
    // 218.08,26.14 | 142.50,21.06; 7:11.92 // 360.58,47.20 431.92
    // 232.24,26.18 | 133.70,20.58; 7:09.98 // 365.94,46.76 429.98
    // 251.79,24.57 | 121.94,19.34; 7:05.03 // 373.73,43.91 425.03
    // 224.68,25.10 | 138.31,19.26; 7:08.15 // 362.99,44.36 428.15
    // 226.67,25.43 | 134.47,18.83; 7:09.58 // 361.14,44.26 429.58 wall
    // 230.69,25.48 | 134.18,19.81   mean   // 364.88,45.30 428.93; 410.18 cpu

    // -- base configuration RHEL4 -- gz ds, 10^6 byte extents
    // 353.55,42.90; 2:46.52 - 166.52
    // 351.83,41.26; 2:44.33 - 164.33
    // 351.70,40.21; 2:44.24 - 164.24
    // 352.35,41.46; mean    - 165.03 wall ; 393.81 cpu

    // -- template configuration RHEL4 -- gz ds, 10^6 byte extents
    // 308.67,41.79; 2:04.01 - 124.01
    // 308.80,42.71; 2:03.91 - 123.91
    // 308.82,41.81; 2:03.51 - 123.51
    // 308.76,42.10; mean    - 123.81 wall; 350.86 cpu

    // -- template configuration RHEL4 -- lzo ds, 10^6 byte extents
    // 237.70,47.01; 2:42.76 - 162.76 -- disk bandwidth limited. (55MiB/s)
    
    // -- template configuration RHEL4 -- lzo ds, wc_day[12345]*ds
    // 117.83, 21.66; 1:00.65 -- 60.65
    // 117.58, 21.14; 1:00.18 -- 60.18
    // 117.56, 21.12; 1:00.25 -- 60.25

    // -- template configuration RHEL4 -- gz ds, wc_day[12345]*ds
    // 152.38, 21.05; 1:00.93 -- 60.93
    // 152.48, 20.66; 1:00.52 -- 60.52
    // 152.03, 20.30; 1:00.45 -- 60.45

    // -- zcat wc_day[12345]*ds | checklog
    // 128.93, 11.48 | 55.89, 8.39; 3:28.14 - 208.14 // 184.82,19.87
    // 130.57, 11.36 | 54.12, 8.42; 3:28.38 - 208.38 // 184.69,19.78
    // 128.95, 11.42 | 55.22, 8.22; 3:28.14 - 208.14 // 184.17,19.64

    // alternating between these three (were copied in this order to
    // local disk)
    // cat /tmp/wc-gz/*: 1:36.49, 1:36.87, 1:37.97
    // cat /tmp/gz-ds/*: 1:32.10, 1:33.56, 1:34.07
    // cat /tmp/lzo-ds/*: 1:52.03, 1:53.86, 1:55.18

    dataseries::TFixedField<int32_t, true> timestamp;
    dataseries::TFixedField<int32_t> clientID;
    dataseries::TFixedField<int32_t> objectID;
    dataseries::TFixedField<int32_t> size;
    dataseries::TFixedField<uint8_t, true> method;
    dataseries::TFixedField<uint8_t> status;
    dataseries::TFixedField<uint8_t> file_type;
    dataseries::TFixedField<uint8_t> server;
};

int main(int argc, char *argv[]) {
    TypeIndexModule *source 
            = new TypeIndexModule("Log::Web::WorldCup::Custom");

    INVARIANT(argc >= 2 && strcmp(argv[1], "-h") != 0,
              boost::format("Usage: %s <file...>\n") % argv[0]);

    for (int i = 1; i < argc; ++i) {
        source->addSource(argv[i]);
    }

    SequenceModule seq(source);
    
    seq.addModule(new WorldCupSimpleAnalysis(seq.tail()));
    
    seq.getAndDeleteShared();
    RowAnalysisModule::printAllResults(seq);
    return 0;
}
