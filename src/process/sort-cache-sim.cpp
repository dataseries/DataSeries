/*
  (c) Copyright 2009, Hewlett-Packard Development Company, LP

  See the file named COPYING for license details

  =pod

  =head1 NAME

  sort-cache-sim - Re-sort CooperativeCacheSimulation files by request time

  =head1 SYNOPSIS

  % sort-cache-sim [common-args] <input.ds...> <output.ds>

  =head1 DESCRIPTION

  Some of the nfs analysis data is stored in order of response time rather than request time.  Most
  cache simulations want the data ordered by request.  This code does a windows (single pass) re-sort
  of the cache-sim data in order to make it faster to process.

  =head1 SEE ALSO

  extract-cache-sim(1), dataseries-utils(7)

  =cut
*/

/// analysis module/program for CooperativeCacheSimulation (ns = ticoli.hpl.hp.com, version = 1.0)

#include <boost/scoped_ptr.hpp>

#include <Lintel/HashMap.hpp>
#include <Lintel/HashUnique.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/PriorityQueue.hpp>

#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

#include <DataSeries/commonargs.hpp>

using namespace std;
using boost::scoped_ptr;
using boost::format;

const string output_xml(
    "<ExtentType namespace=\"ticoli.hpl.hp.com\" name=\"CooperativeCacheSimulationFiles\" version=\"1.0\" >\n"
    "  <field type=\"variable32\" name=\"file_id\" comment=\"globally-unique\" />\n"
    "  <field type=\"int64\" name=\"file_size\" />\n"
    "  <field type=\"int64\" name=\"first_read\" units=\"2^-32 seconds\" epoch=\"unix\" pack_relative=\"first_read\" comment=\"sorted by this field\" />\n"
    "</ExtentType>\n"
                        );

class CooperativeCacheSimulationSort : public RowAnalysisModule {
  public:
    CooperativeCacheSimulationSort(DataSeriesModule &source, const string &out_name,
                                   const commonPackingArgs &pack_args)
            : RowAnalysisModule(source), last(0),
              in_request_at(series, "request_at"),
              in_reply_at(series, "reply_at"),
              in_operation_type(series, "operation_type"),
              in_client_id(series, "client_id"),
              in_file_id(series, "file_id"),
              in_offset(series, "offset"),
              in_bytes(series, "bytes"),
              out_name(out_name), pack_args(pack_args), out_sink(NULL), out_module(NULL),
              out_request_at(out_series, "request_at"),
              out_reply_at(out_series, "reply_at"),
              out_operation_type(out_series, "operation_type"),
              out_client_id(out_series, "client_id"),
              out_file_id(out_series, "file_id"),
              out_offset(out_series, "offset"),
              out_bytes(out_series, "bytes"),
              file_out_module(NULL),
              file_out_file_id(file_out_series, "file_id"),
              file_out_file_size(file_out_series, "file_size"),
              file_out_first_read(file_out_series, "first_read")
    { }

    virtual ~CooperativeCacheSimulationSort() { }

    virtual void firstExtent(const Extent &e) {
        out_sink.reset(new DataSeriesSink(out_name, pack_args.compress_modes, 
                                          pack_args.compress_level));
        ExtentTypeLibrary library;
        library.registerType(e.getTypePtr());

        const ExtentType::Ptr extent_type(e.getTypePtr());
        out_series.setType(extent_type);

        out_module.reset(new OutputModule(*out_sink, out_series, extent_type, 
                                          pack_args.extent_size));


        const ExtentType::Ptr file_extent_type(library.registerTypePtr(output_xml));
        file_out_series.setType(file_extent_type);
        file_out_module.reset(new OutputModule(*out_sink, file_out_series, file_extent_type,
                                               pack_args.extent_size));

        out_sink->writeExtentLibrary(library);
    }

    virtual void processRow() {
        string file_id(in_file_id.stringval());

        unique_clients.add(in_client_id());
        FileInfo &file_info(file_id_to_file_info[file_id]);
        if (in_operation_type() == 0) { // read
            file_info.size = max(file_info.size, in_offset() + in_bytes());
            file_info.first_read = min(file_info.first_read, in_request_at());
        } else if (in_operation_type() == 1) { // write
            file_info.first_write = min(file_info.first_write, in_request_at());
        } else {
            FATAL_ERROR("unhandled");
        }

        if (ops.size() < 5000000) { // > 100k required for deasna
            ops.push(Op(in_request_at(), in_reply_at(), in_operation_type(), 
                        in_client_id(), file_id, in_offset(), in_bytes()));
        } else {
            writeRow();

            ops.replaceTop(Op(in_request_at(), in_reply_at(), in_operation_type(), 
                              in_client_id(), file_id, in_offset(), in_bytes()));
        }
    }

    struct FileInfo {
        int64_t size, first_read, first_write;
        FileInfo(int64_t a, int64_t b, int64_t c) : size(a), first_read(b), first_write(c) { }
        FileInfo() : size(0), first_read(numeric_limits<int64_t>::max()), 
                     first_write(numeric_limits<int64_t>::max()) { }
        bool operator <(const FileInfo &rhs) const {
            return first_read < rhs.first_read;
        }
    };

    typedef HashTable_hte<HashMap<string, FileInfo>::value_type> DataVal;

    struct sortHTE {
        bool operator () (const DataVal &a, const DataVal &b) const {
            return a.data.second.first_read < b.data.second.first_read;
        }
    };

    virtual void printResult() {
        while (!ops.empty()) {
            writeRow();
            ops.pop();
        }
        cout << format("%d unique files accessed by read in trace\n") 
                % file_id_to_file_info.size();

        cout << format("%d unique clients in trace\n") 
                % unique_clients.size();
        
        vector<DataVal> &data(file_id_to_file_info.getHashTable().unsafeGetRawDataVector());

        sort(data.begin(), data.end(), sortHTE());

        for (vector<DataVal>::iterator di = data.begin(); di != data.end(); ++di) {
            const pair<string, FileInfo> &i(di->data);
            if (i.second.first_read == numeric_limits<int64_t>::max()) {
                LintelLogDebug("info", format("skip %s - noread") % maybehexstring(i.first));
                continue;
            } else if (i.second.first_write != numeric_limits<int64_t>::max() 
                       && i.second.first_read > i.second.first_write) {
                LintelLogDebug("info", format("skip %s - read %d after write %d")
                               % maybehexstring(i.first) % i.second.first_read
                               % i.second.first_write);
                continue;
            }
            file_out_module->newRecord();
            file_out_file_id.set(i.first);
            file_out_file_size.set(i.second.size);
            file_out_first_read.set(i.second.first_read);
        }
    }

    void writeRow() {
        Op &top = ops.top();
        INVARIANT(last <= top.req_at, format("%d > %d; increase reorder window?")
                  % last % top.req_at);
        last = top.req_at;
        out_module->newRecord();
        out_request_at.set(top.req_at);
        out_reply_at.set(top.rep_at);
        out_operation_type.set(top.op);
        out_client_id.set(top.client_id);
        out_file_id.set(top.file_name);
        out_offset.set(top.offset);
        out_bytes.set(top.bytes);
    }

    struct Op {
        int64_t req_at, rep_at;
        uint8_t op;
        int32_t client_id;
        string file_name;
        int64_t offset;
        int32_t bytes;

        Op(int64_t a, int64_t b, uint8_t c, int32_t d, const string &e, int64_t f, int32_t g)
                : req_at(a), rep_at(b), op(c), client_id(d), file_name(e), offset(f), bytes(g) { }
        
        Op() : req_at(-1) { }

        bool operator >(const Op &rhs) const {
            return req_at > rhs.req_at;
        }
    };

  private: 
    int64_t last;
    PriorityQueue<Op, greater<Op> > ops;

    Int64Field in_request_at;
    Int64Field in_reply_at;
    ByteField in_operation_type;
    Int32Field in_client_id;
    Variable32Field in_file_id;
    Int64Field in_offset;
    Int32Field in_bytes;

    string out_name;
    commonPackingArgs pack_args;
    scoped_ptr<DataSeriesSink> out_sink;
    ExtentSeries out_series;
    scoped_ptr<OutputModule> out_module;
                     
    Int64Field out_request_at;
    Int64Field out_reply_at;
    ByteField out_operation_type;
    Int32Field out_client_id;
    Variable32Field out_file_id;
    Int64Field out_offset;
    Int32Field out_bytes;

    HashMap<string, FileInfo> file_id_to_file_info;
    scoped_ptr<OutputModule> file_out_module;
    ExtentSeries file_out_series;
    Variable32Field file_out_file_id;
    Int64Field file_out_file_size, file_out_first_read;

    HashUnique<int32_t> unique_clients;
};

int main(int argc, char *argv[]) {
    LintelLog::parseEnv();
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    TypeIndexModule *source = new TypeIndexModule("CooperativeCacheSimulation");

    INVARIANT(argc >= 2 && strcmp(argv[1], "-h") != 0,
              format("Usage: %s <file...> <output-name>\n") % argv[0]);

    for (int i = 1; i < argc - 1; ++i) {
        source->addSource(argv[i]);
    }

    SequenceModule seq(source);
    
    seq.addModule(new CooperativeCacheSimulationSort(seq.tail(), argv[argc-1], packing_args));
    
    seq.getAndDeleteShared();
    RowAnalysisModule::printAllResults(seq);
    return 0;
}
