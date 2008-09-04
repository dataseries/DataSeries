#include <Lintel/HashMap.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/RotatingHashMap.hpp>
#include <Lintel/StatsQuantile.hpp>

#include <DataSeries/ExtentField.hpp>
#include <DataSeries/RowAnalysisModule.hpp>

#include <analysis/nfs/common.hpp>
#include <analysis/nfs/Tuples.hpp>

using namespace std;
using boost::format;
using dataseries::TFixedField;

// Given a set of operations, sort them by request time.  
// While the reply time of the first operation is < current_time - reset_interval:
//   Start with the first operation, take all operations with request
//   times up to the reply time of the first operation, find the
//   request with the minimal offset (absolute) relative to the last
//   offset, breaking ties using the operation with the least request
//   time.  That operation "sorts" as first, so calculate the jump
//   distance from that operation.  Remove it from the vector and
//   repeat.

class Sequentiality : public RowAnalysisModule {
public:
    enum Mode { ReplyOrder, OverlappingReorder, RequestOrder };
    Sequentiality(DataSeriesModule &source, const string &arg_str)
        : RowAnalysisModule(source),
          request_at(series, "request_at"),
          reply_at(series, "reply_at"),
          server(series, "server"),
          client(series, "client"),
          filehandle(series, "filehandle"),
          is_read(series, "is_read"),
          file_size(series, "file_size", Field::flag_nullable),
          modify_time(series, "modify_time", Field::flag_nullable),
          offset(series, "offset"),
          bytes(series, "bytes"),
	  last_rotate_time_raw(numeric_limits<int64_t>::min()),
	  reset_interval_raw(0), 
	  skip_distribution(0.001, static_cast<uint64_t>(1.0e10)),
	  operations_memory_usage(0),
	  last_reported_memory_usage(0),
	  operation_count(0), process_count(0), reset_count(0), continue_count(0),
	  reorder_count(0),
	  overlapping_reorder_slop_seconds(0), overlapping_reorder_slop_raw(0),
	  mode(OverlappingReorder)
	// mode(ReplyOrder)
    { 
	ignore_client = false;
	ignore_server = false;
	vector<string> args = split(arg_str, ",");
	for(unsigned i = 0; i < args.size(); ++i) {
	    if (args[i] == "ignore_client") {
		ignore_client = true;
	    } else if (args[i] == "ignore_server") {
		ignore_server = true;
	    } else if (args[i].empty()) {
		// ignore
	    } else if (args[i] == "reply_order") {
		mode = ReplyOrder;
	    } else if (prefixequal(args[i], "overlapping_reorder=")) {
		mode = OverlappingReorder;
		overlapping_reorder_slop_seconds = doubleModArg("overlapping_reorder", args[i]);
	    } else if (args[i] == "overlapping_reorder") {
		mode = OverlappingReorder;
	    } else if (args[i] == "request_order") {
		mode = RequestOrder;
	    } else {
		FATAL_ERROR("?");
	    }
	}
	if (LintelLog::wouldDebug("memory_usage")) {
	    last_reported_memory_usage = 1;
	    reportMemoryUsage();
	} 
    }

    virtual ~Sequentiality() { }

    struct Operation {
	// reply_at == min() --> unused entry
	int64_t request_at, reply_at;
	int64_t start_offset;
	uint32_t len;
	bool is_read;

	Operation(int64_t a, int64_t b, int64_t c, uint32_t d, bool e) 
	    : request_at(a), reply_at(b), start_offset(c), len(d), is_read(e)
	{ }
	Operation() : request_at(0), reply_at(0), start_offset(0), len(0), is_read(false) { }
	bool operator <(const Operation &rhs) const {
	    return request_at < rhs.request_at;
	}
    };

    struct FHState {
	int64_t last_end_offset;
	int64_t last_operation_at;
	uint32_t write_count, read_count;
	uint32_t random_count, sequential_count;
	uint64_t write_bytes, read_bytes;
	uint32_t reorder_count;

	vector<Operation> ops;
	typedef vector<Operation>::iterator iterator;

	FHState() { reset(); }

	void reset() {
	    last_end_offset = numeric_limits<int64_t>::min();
	    last_operation_at = numeric_limits<int64_t>::min();
	    write_count = 0; 
	    read_count = 0; 
	    random_count = 0;
	    sequential_count = 0;
	    read_bytes = 0;
	    write_bytes = 0;
	    reorder_count = 0;
	}
	size_t opsMemUsage() {
	    return ops.capacity() * sizeof(Operation);
	}
    };

    typedef boost::tuple<uint64_t, int32_t, int32_t> Key;

    virtual void prepareForProcessing() {
	reset_interval_raw = request_at.secNanoToRaw(2, 0);
	int32_t seconds = static_cast<int32_t>(floor(overlapping_reorder_slop_seconds));
	uint32_t nanoseconds = static_cast<uint32_t>
	    (round((overlapping_reorder_slop_seconds - seconds) * 1.0e9));
	overlapping_reorder_slop_raw = request_at.secNanoToRaw(seconds, nanoseconds);
	SINVARIANT(overlapping_reorder_slop_raw < reset_interval_raw / 2);
    }

    void reportMemoryUsage() {
	size_t a = states.memoryUsage();
	size_t b = skip_distribution.memoryUsage();
	LintelLogDebug("memory_usage",
		       format("# Memory-Usage: Sequentiality %d = %d + %d") % (a + b) % a % b);
	last_reported_memory_usage = a + b;
    }
	
    virtual void newExtentHook(const Extent &e) {
	if (last_reported_memory_usage > 0) {
	    size_t memory_usage = states.memoryUsage() + skip_distribution.memoryUsage();
	    if (memory_usage > (last_reported_memory_usage  + 4 * 1024 * 1024)) {
		reportMemoryUsage();
	    }
	}
    }

    void completeOpRun(FHState &state) {
	state.reset();
    }

    void processOneOp(FHState &state, Operation &op) {
	++process_count;
	if (state.last_operation_at + reset_interval_raw < op.reply_at
	    && (state.write_count + state.read_count) > 0) {
	    completeOpRun(state);
	}

	if (state.last_end_offset == op.start_offset) {
	    ++state.sequential_count;
	} else {
	    ++state.random_count;
	}
	if (op.is_read) {
	    ++state.read_count;
	    state.read_bytes += op.len;
	} else {
	    ++state.write_count;
	    state.write_bytes += op.len;
	}
	if (state.last_end_offset != numeric_limits<int64_t>::min()) {
	    skip_distribution.add(op.start_offset - state.last_end_offset);
	}
	state.last_end_offset = op.start_offset + op.len;
	state.last_operation_at = max(state.last_operation_at, op.reply_at);
	op.reply_at = numeric_limits<int64_t>::min();
    }

    void eraseOpsPrefix(FHState &state, FHState::iterator end) {
	operations_memory_usage -= state.opsMemUsage();
	state.ops.erase(state.ops.begin(), end);
	operations_memory_usage += state.opsMemUsage();
    }

    void processOpsORReset(FHState &state, uint32_t first) {
	++reset_count;
	// Restarting runs, pick the operation with the least offset
	int64_t min_offset = state.ops[first].start_offset;
	int64_t max_request_time = state.ops[first].reply_at + overlapping_reorder_slop_raw;
	uint32_t selected = first;
	for(uint32_t i = first + 1;
	    i < state.ops.size() && min_offset > 0 
		&& state.ops[i].request_at < max_request_time; ++i) {
	    INVARIANT(state.ops[i].reply_at > numeric_limits<int64_t>::min(),
		      "should not see ignored op during reset");
	    if (state.ops[i].start_offset < min_offset) {
		min_offset = state.ops[i].start_offset;
		selected = i;
	    }
	}
	LintelLogDebug("Sequentiality::po", format("reset selected #%d @ %d") 
		       % selected % min_offset);
	processOneOp(state, state.ops[selected]);
	if (selected != first) {
	    ++state.reorder_count;
	}
    }

    void processOpsORContinue(FHState &state, uint32_t first) {
	++continue_count;
	SINVARIANT(state.last_operation_at > numeric_limits<int64_t>::min());

	// select the next most sequential operation within the window
	int64_t closest_skip = state.ops[first].start_offset - state.last_end_offset;
	int64_t max_request_time = state.ops[first].reply_at + overlapping_reorder_slop_raw;
	uint32_t selected = first;
	for(uint32_t i = first + 1; i < state.ops.size() && closest_skip != 0 
		&& state.ops[i].request_at < max_request_time; ++i) {
	    if (state.ops[i].reply_at == numeric_limits<int64_t>::min()) {
		continue; // already used this operation.
	    }
	    int64_t skip = state.ops[i].start_offset - state.last_end_offset;
	    if (skip == 0 || skip < closest_skip) { 
		// preferentially skip backwards; this means that if
		// we are at pos 100, and we get I/Os for 94,96,95,97;
		// we will skip all the way back to 94, rather than
		// jumping to 97 and reading backwards which we would
		// do if we selected the closest skip by absolute
		// value.
		closest_skip = skip;
		selected = i;
	    }
	}
	int64_t skip = state.ops[selected].start_offset - state.last_end_offset;
	LintelLogDebug("Sequentiality::po", format("continue selected #%d(%d) skip %d") 
		       % selected % first % skip);
	processOneOp(state, state.ops[selected]);
	if (selected != first) {
	    ++reorder_count;
	    ++state.reorder_count;
	}
    }

    void processOpsOverlappingReorder(const Key &key, FHState &state, int64_t cur_reply_at) {
	LintelLogDebug("Sequentiality::po", format("CRA %d for %x") % cur_reply_at % key.get<0>());
	sort(state.ops.begin(), state.ops.end());
	if (false) {
	    for(FHState::iterator i = state.ops.begin(); 
		i != state.ops.end(); ++i) {
		if (i->reply_at < cur_reply_at - reset_interval_raw) {
		    LintelLogDebug("Sequentiality::po", format("Entry [%d,%d] %d %d") 
				   % i->request_at % i->reply_at % i->start_offset 
				   % (i->start_offset + i->len));
		} else { 
		    LintelLogDebug("Sequentiality::po", format("... %d more beyond cutoff ...")
				   % (state.ops.end() - i));
		    break;
		}
	    }
	}

	uint32_t first = 0;
	while(first < state.ops.size() 
	      && state.ops[first].reply_at < cur_reply_at - reset_interval_raw) {
	    SINVARIANT(state.ops[first].reply_at != numeric_limits<int64_t>::min());
	    if (state.last_operation_at + reset_interval_raw < state.ops[first].request_at) {
		LintelLogDebug("Sequentiality::po", format("reset %d + %d < %d")
			       % state.last_operation_at % reset_interval_raw 
			       % state.ops[first].request_at);
		processOpsORReset(state, first);
	    } else {
		processOpsORContinue(state, first);
	    }
	    while(first < state.ops.size() 
		  && state.ops[first].reply_at == numeric_limits<int64_t>::min()) {
		++first;
	    }
	}
	for(unsigned i = 0; i < first; ++i) {
	    SINVARIANT(state.ops[i].reply_at == numeric_limits<int64_t>::min());
	}

	eraseOpsPrefix(state, state.ops.begin() + first);
	
	LintelLogDebug("Sequentiality::po", "");
    }

    void processOpsRequestOrder(const Key &key, FHState &state, int64_t cur_reply_at) {
	sort(state.ops.begin(), state.ops.end());
	FHState::iterator i = state.ops.begin();
	for(; i != state.ops.end() && i->reply_at < cur_reply_at - reset_interval_raw; ++i) {
	    processOneOp(state, *i);
	}

	eraseOpsPrefix(state, i);
    }

    void processOps(const Key &key, FHState &state, int64_t cur_reply_at) {
	switch(mode)
	    {
	    case ReplyOrder: SINVARIANT(state.ops.empty());
		break;
	    case OverlappingReorder: processOpsOverlappingReorder(key, state, cur_reply_at);
		break;
	    case RequestOrder: processOpsRequestOrder(key, state, cur_reply_at);
		break;
	    default: FATAL_ERROR("?");
	    }
    }

    void rotateEntry(int64_t cur_reply_at, const Key &key, FHState &val) {
	processOps(key, val, cur_reply_at);
	SINVARIANT(val.ops.empty());
    }

    virtual void processRow() {
	if (reply_at.valRaw() > last_rotate_time_raw + reset_interval_raw) {
	    // TODO: verify they are clean.
	    states.rotate(boost::bind(&Sequentiality::rotateEntry, this, reply_at.valRaw(), 
				      _1, _2));
	    last_rotate_time_raw = reply_at.valRaw();
	}
	Key tmp(md5FileHash(filehandle), ignore_server ? 0 : server.val(), 
		ignore_client ? 0 : client.val());
	FHState &state = states[tmp];

	++operation_count;
	if (mode == ReplyOrder) {
	    // Can do something special here to go faster.
	    Operation tmp(request_at.valRaw(), reply_at.valRaw(),
			  offset.val(), bytes.val(), is_read.val());
	    processOneOp(state, tmp);
	} else {
	    operations_memory_usage -= state.opsMemUsage();
	    state.ops.push_back(Operation(request_at.valRaw(), reply_at.valRaw(),
					  offset.val(), bytes.val(), is_read.val()));
	    operations_memory_usage += state.opsMemUsage();
	    
	    if (state.ops.front().reply_at + reset_interval_raw < 
		reply_at.valRaw()) {
		processOps(tmp, state, reply_at.valRaw());
	    }
	}
    }

    virtual void printResult() {
	states.flushRotate(boost::bind(&Sequentiality::rotateEntry, this, 
				       numeric_limits<int64_t>::max(), _1, _2));
	INVARIANT(operation_count == process_count,
		  format("%d != %d") % operation_count % process_count);
	INVARIANT(mode != OverlappingReorder || process_count == reset_count + continue_count,
		  format("%d != %d + %d") % process_count % reset_count % continue_count);
	    
	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;
	reportMemoryUsage();
	cout << "Analysis configuration: ";
	if (ignore_client) {
	    cout << "ignore client, ";
	} else {
	    cout << "distinct client, ";
	}
	if (ignore_server) {
	    cout << "ignore server\n";
	} else {
	    cout << "distinct server\n";
	}

	cout << format("Reorder count: %d/%d, %.3f%%\n")
	    % reorder_count % operation_count % (100.0 * reorder_count / operation_count);
	cout << "Skip distribution:\n";
	skip_distribution.printTextRanges(cout, 1000);

	cout << format("End-%s\n") % __PRETTY_FUNCTION__;
    }

private:
    Int64TimeField request_at;
    Int64TimeField reply_at;
    TFixedField<int32_t> server;
    TFixedField<int32_t> client;
    Variable32Field filehandle;
    BoolField is_read;
    Int64Field file_size;
    Int64Field modify_time;
    TFixedField<int64_t> offset;
    TFixedField<int32_t> bytes;

    RotatingHashMap<Key, FHState, TupleHash<Key> > states;
    int64_t last_rotate_time_raw;
    int64_t reset_interval_raw;

    bool ignore_client, ignore_server;
    StatsQuantile skip_distribution;

    size_t operations_memory_usage, last_reported_memory_usage;
    uint64_t operation_count, process_count, reset_count, continue_count, reorder_count;

    double overlapping_reorder_slop_seconds;
    int64_t overlapping_reorder_slop_raw;
    Mode mode;
};
    
namespace NFSDSAnalysisMod {
    RowAnalysisModule *
    newSequentiality(DataSeriesModule &prev, const string &arg) {
	return new Sequentiality(prev, arg);
    }
}
