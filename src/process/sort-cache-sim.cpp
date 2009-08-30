/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/// @file Re-sort CooperativeCacheSimulation files by request time

/// analysis module/program for CooperativeCacheSimulation (ns = ticoli.hpl.hp.com, version = 1.0)

#include <boost/scoped_ptr.hpp>

#include <Lintel/PriorityQueue.hpp>

#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

#include <DataSeries/commonargs.hpp>

using namespace std;
using boost::scoped_ptr;

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
          out_bytes(out_series, "bytes")
    { }

    virtual ~CooperativeCacheSimulationSort() { }

    virtual void firstExtent(const Extent &e) {
	out_sink.reset(new DataSeriesSink(out_name, pack_args.compress_modes, 
					  pack_args.compress_level));
	ExtentTypeLibrary library;
	library.registerType(e.getType());

	const ExtentType *extent_type = &e.getType();
	out_series.setType(*extent_type);

	out_module.reset(new OutputModule(*out_sink, out_series, extent_type, 
					  pack_args.extent_size));

	out_sink->writeExtentLibrary(library);
    }

    virtual void processRow() {
	if (ops.size() < 10000) {
	    ops.push(Op(in_request_at(), in_reply_at(), in_operation_type(), 
			in_client_id(), in_file_id.stringval(), in_offset(), in_bytes()));
	} else {
	    writeRow();

	    ops.replaceTop(Op(in_request_at(), in_reply_at(), in_operation_type(), 
			      in_client_id(), in_file_id.stringval(), in_offset(), in_bytes()));
	}
    }

    virtual void printResult() {
	while (!ops.empty()) {
	    writeRow();
	    ops.pop();
	}
    }

    void writeRow() {
	Op &top = ops.top();
	SINVARIANT(last <= top.req_at);
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
    
};

int main(int argc, char *argv[]) {
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    TypeIndexModule *source = new TypeIndexModule("CooperativeCacheSimulation");

    INVARIANT(argc >= 2 && strcmp(argv[1], "-h") != 0,
              boost::format("Usage: %s <file...> <output-name>\n") % argv[0]);

    for(int i = 1; i < argc - 1; ++i) {
        source->addSource(argv[i]);
    }

    SequenceModule seq(source);
    
    seq.addModule(new CooperativeCacheSimulationSort(seq.tail(), argv[argc-1], packing_args));
    
    seq.getAndDelete();
    RowAnalysisModule::printAllResults(seq);
    return 0;
}
