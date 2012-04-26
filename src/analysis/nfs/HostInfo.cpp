/* -*-C++-*-
   (c) Copyright 2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <vector>

#include <boost/bind.hpp>

#include <analysis/nfs/common.hpp>
#include <analysis/nfs/HostInfo.hpp>

#include <Lintel/HashMap.hpp>
#include <Lintel/HashUnique.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/StatsCube.hpp>
#include <Lintel/StatsQuantile.hpp>

#include <DataSeries/GeneralField.hpp>

using namespace std;
using boost::format;
using boost::tuples::null_type;
using lintel::tuples::AnyPair;
using lintel::tuples::BitsetAnyTuple;
using lintel::HashTupleStats;
using lintel::StatsCube;
using dataseries::TFixedField;

//                   host,   is_send,   time, operation, is_request
typedef boost::tuple<int32_t, bool,   int32_t,  uint8_t,    bool> 
    HostInfoTuple;
static const size_t host_index = 0;
static const size_t is_send_index = 1;
static const size_t time_index = 2;
static const size_t operation_index = 3;
static const size_t is_request_index = 4;

namespace lintel { namespace tuples {
    template<> struct TupleHash<HostInfoTuple> {
	uint32_t operator()(const HostInfoTuple &v) const {
	    BOOST_STATIC_ASSERT(boost::tuples::length<HostInfoTuple>::value == 5);
	    uint32_t a = v.get<host_index>();
	    uint32_t b = v.get<time_index>();
	    uint32_t c = v.get<operation_index>() | (v.get<is_send_index>() ? 0x100 : 0)
		| (v.get<is_request_index>() ? 0x200 : 0);
	    return BobJenkinsHashMix3(a,b,c);
	}
    };
	       
    template<> struct BitsetAnyTupleHash<HostInfoTuple> {
	uint32_t operator()(const BitsetAnyTuple<HostInfoTuple> &v) const {
	    BOOST_STATIC_ASSERT(boost::tuples::length<HostInfoTuple>::value == 5);
	    uint32_t a = v.any[host_index] ? 0 : v.data.get<host_index>();
	    uint32_t b = v.any[time_index] ? 0 : v.data.get<time_index>();
	    uint32_t c = (v.any[operation_index] ? 0 : v.data.get<operation_index>())
		| (!v.any[is_send_index] && v.data.get<is_send_index>() ? 0x100 : 0)
		| (!v.any[is_request_index] && v.data.get<is_request_index>() ? 0x200 : 0);
	    return BobJenkinsHashMix3(a,b,c);
	}
    };
} }

template<class T0, class T1>
struct ConsToAnyPairCons {
    typedef ConsToAnyPairCons<typename T1::head_type,
			      typename T1::tail_type> tail;
    typedef boost::tuples::cons<AnyPair<T0>, typename tail::type> type;
};

template<class T0>
struct ConsToAnyPairCons<T0, null_type> {
    typedef boost::tuples::cons<AnyPair<T0>, null_type> type;
};

template<class T>
struct TupleToAnyTuple 
  : ConsToAnyPairCons<typename T::head_type, typename T::tail_type> 
{ };

template<> struct HashMap_hash<const bool> {
    uint32_t operator()(const bool x) const {
	return x;
    }
};

template<> struct HashMap_hash<const unsigned char> {
    uint32_t operator()(const bool x) const {
	return x;
    }
};

void zeroAxisAdd(null_type, null_type) {
}

template<class HUT, class T>
void zeroAxisAdd(HUT &hut, const T &v) {
    hut.get_head().add(v.get_head());
    zeroAxisAdd(hut.get_tail(), v.get_tail());
}

void hutPrint(null_type, int) { }

template<class HUT>
void hutPrint(HUT &hut, int pos) {
    typedef typename HUT::head_type::iterator iterator;
    cout << format("begin tuple part %d:\n") % pos;
    if (false) {
	Stats chain_lens;
	hut.get_head().chainLengthStats(chain_lens);
	
	cout << format("  chain [%.0f,%.0f] %.2f +- %.2f\n")
	    % chain_lens.min() % chain_lens.max() % chain_lens.mean()
	    % chain_lens.stddev();
    }
    
    for(iterator i = hut.get_head().begin(); i != hut.get_head().end(); ++i) {
	cout << *i << " ";
    }
    cout << format("\nend tuple part %d:\n") % pos;

    hutPrint(hut.get_tail(), pos + 1);
}

double zeroCubeBaseCount(const null_type) { 
    return 1; 
}

template<class HUT>
double zeroCubeBaseCount(const HUT &hut) {
    return hut.get_head().size() * zeroCubeBaseCount(hut.get_tail());
}

template<class KeyBase, class BaseData, class Function>
void zeroWalk(const null_type, const null_type, KeyBase &key_base, 
	      const BaseData &base_data, Stats &null_stat, 
	      const Function &fn) {
    typedef typename BaseData::const_iterator iterator;
    iterator i = base_data.find(key_base);
    if (i == base_data.end()) {
	fn(key_base, null_stat); 
    } else {
	fn(key_base, *(i->second)); 
    }
}

template<class HUT, class KeyTail, class KeyBase, class BaseData, 
	 class Function>
void zeroWalk(const HUT &hut, KeyTail &key_tail, KeyBase &key_base, 
	      const BaseData &base_data, Stats &null_stat, 
	      const Function &fn) {
    typedef typename HUT::head_type::const_iterator const_iterator;

    for(const_iterator i = hut.get_head().begin(); 
	i != hut.get_head().end(); ++i) {
	key_tail.get_head() = *i;

	zeroWalk(hut.get_tail(), key_tail.get_tail(), key_base,
		 base_data, null_stat, fn);
    }
}

// We do the rollup internal to this module rather than an external
// program so if we switch to using statsquantile the rollup will
// still work correctly.

// To graph:
// create a table like: create table nfs_perf (sec int not null, ops double not null);
// perl -ane 'next unless $F[0] eq "*" && $F[1] =~ /^\d+$/o && $F[2] eq "send" && $F[3] eq "*" && $F[4] eq "*";print "insert into nfs_perf values ($F[1], $F[5]);\n";' < output of nfsdsanalysis -l ## run. | mysql test
// use mercury-plot (from Lintel) to graph, for example:
/*
unplot
plotwith * lines
plot 3600*round((sec - min_sec)/3600,0) as x, avg(ops/(2*60.0)) as y from nfs_perf, (select min(sec) as min_sec from nfs_perf) as ms group by round((sec - min_sec)/3600,0)
plottitle _ mean ops/s 
plot 3600*round((sec - min_sec)/3600,0) as x, max(ops/(2*60.0)) as y from nfs_perf, (select min(sec) as min_sec from nfs_perf) as ms group by round((sec - min_sec)/3600,0)
plottitle _ max ops/s
plot 3600*round((sec - min_sec)/3600,0) as x, min(ops/(2*60.0)) as y from nfs_perf, (select min(sec) as min_sec from nfs_perf) as ms group by round((sec - min_sec)/3600,0)
plottitle _ min ops/s
gnuplot set xlabel "seconds"
gnuplot set ylabel "operations (#requests+#replies)/2 per second"
pngplot set-18.png
*/

namespace {
    const string str_send("send");
    const string str_recv("recv");
    const string str_request("request");
    const string str_response("response");
    const string str_star("*");
    const string str_null("null");
}

class HostInfo : public RowAnalysisModule {
public:
    typedef HostInfoTuple Tuple;
    typedef StatsCube<Tuple>::MyAny BitsetAnyTuple;

    static const double rate_quantile_maxerror = 0.005;
    // about 1/3 of a year with 1 second datapoints
    static const unsigned rate_quantile_maxgroups = 10*1000*1000;
    struct Rates {
	StatsQuantile ops_rate;
	StatsQuantile bytes_rate;
	Rates() : ops_rate(rate_quantile_maxerror, rate_quantile_maxgroups), 
		  bytes_rate(rate_quantile_maxerror, rate_quantile_maxgroups)
	{ 
	    if (memory_usage == 0) {
		memory_usage = 2*ops_rate.memoryUsage();
	    }
	}
	void add(double ops, double bytes) {
	    ops_rate.add(ops);
	    bytes_rate.add(bytes);
	}

	static size_t memory_usage;
    };

    typedef TupleToAnyTuple<Tuple>::type AnyTuple;

    Rates *createRates() {
	checkMemoryUsage();
	return new Rates();
    }

    HostInfo(DataSeriesModule &_source, const std::string &arg) 
	: RowAnalysisModule(_source),
	  packet_at(series, ""),
	  payload_length(series, ""),
	  op_id(series, "", Field::flag_nullable),
	  nfs_version(series, "", Field::flag_nullable),
	  source_ip(series,"source"), 
	  dest_ip(series, "dest"),
	  is_request(series, ""),
	  group_seconds(1),
	  incremental_batch_size(100000),
	  rate_hts(boost::bind(&HostInfo::createRates, this)),
	  min_group_seconds(numeric_limits<int32_t>::max()),
	  max_group_seconds(numeric_limits<int32_t>::min()),
	  last_rollup_at(numeric_limits<int32_t>::min()),
	  group_count(0), printed_base_header(false), 
	  printed_cube_header(false), printed_rates_header(false),
	  print_rates_quantiles(true), sql_output(false), 
	  print_base(true), print_cube(true), zero_groups(0),
	  last_reported_memory_usage(0)
    {
	// Usage: group_seconds[,{no_cube_time, no_cube_host, 
	//                        no_print_base, no_print_cube}]+
	vector<string> args = split(arg, ",");
	group_seconds = stringToInteger<int32_t>(args[0]);
	SINVARIANT(group_seconds > 0);
	options["cube_time"] = true;
	options["cube_host"] = true;
	options["cube_host_detail"] = true;

	// Cubing over the unique_vals_tuple; is very expensive -- ~6x on
	// some simple initial testing.  Calculating the unique tuple is 
	// very cheap (overlappping runtimes, <1% instruction count difference),
	// but if we're not going to use it, then there isn't any point.
	options["print_base"] = true;
	options["print_cube"] = true;
	options["print_rates"] = true;
	options["print_rates_quantiles"] = true;
	options["sql_output"] = false;
	options["sql_create_table"] = false;
	for(unsigned i = 1; i < args.size(); ++i) {
	    if (prefixequal(args[i], "no_")) {
		INVARIANT(options.exists(args[i].substr(3)),
			  format("unknown option '%s'") % args[i]);
		options[args[i].substr(3)] = false;
	    } else if (prefixequal(args[i], "incremental=")) {
		// TODO: make uint32ModArg in common.hpp, use it.
		incremental_batch_size = stringToInteger<uint32_t>(args[i].substr(12));
		LintelLogDebug("HostInfo", format("ibs=%d") % incremental_batch_size);
	    } else {
		INVARIANT(options.exists(args[i]),
			  format("unknown option '%s'") % args[i]);
		options[args[i]] = true;
	    }
	}
	INVARIANT(!options["print_rates"] || options["cube_time"],
		  "HostInfo: Can't print_rates unless we have cube_time enabled.");
	print_rates_quantiles = options["print_rates_quantiles"];
	if (!options["print_rates"]) {
	    options["print_rates_quantiles"] = false;
	}
	sql_output = options["sql_output"];
	configCube();
	if (options["sql_create_table"]) {
	    printCreateTables();
	}
	print_base = options["print_base"];
	print_cube = options["print_cube"];
	skip_cube_time = !options["cube_time"];
	skip_cube_host = !options["cube_host"];
	skip_cube_host_detail = !options["cube_host_detail"];
    }

    virtual ~HostInfo() { }

    static int32_t host(const Tuple &t) {
	return t.get<host_index>();
    }
    static string host(const BitsetAnyTuple &atuple) {
	if (atuple.any[host_index]) {
	    return str_star;
	} else {
	    return str(format("%08x") % host(atuple.data));
	}
    }
    static string sqlHost(const BitsetAnyTuple &atuple) {
	if (atuple.any[host_index]) {
	    return str_null;
	} else {
	    return str(format("0x%08x") % host(atuple.data));
	}
    }

    static int32_t time(const Tuple &t) {
	return t.get<time_index>();
    }

    static string time(const BitsetAnyTuple &atuple,
		       const std::string &unused_str = str_star) {
	if (atuple.any[time_index]) {
	    return unused_str;
	} else {
	    return str(format("%d") % time(atuple.data));
	}
    }

    static bool isSend(const Tuple &t) {
	return t.get<is_send_index>();
    }
    static const string &isSendStr(const Tuple &t) {
	return isSend(t) ? str_send : str_recv;
    }

    static string isSendStr(const BitsetAnyTuple &atuple) {
	if (atuple.any[is_send_index]) {
	    return str_star;
	} else {
	    return isSendStr(atuple.data);
	}
    }

    static uint8_t operation(const Tuple &t) {
	return t.get<operation_index>();
    }

    static const string &operationStr(const Tuple &t) {
	return unifiedIdToName(operation(t));
    }
    static string operationStr(const BitsetAnyTuple &atuple) {
	if (atuple.any[operation_index]) {
	    return str_star;
	} else {
	    return operationStr(atuple.data);
	}
    }

    static bool isRequest(const Tuple &t) {
	return t.get<is_request_index>();
    }
    static const string &isRequestStr(const Tuple &t) {
	return isRequest(t) ? str_request : str_response;
    }
    static const string &isRequestStr(const BitsetAnyTuple &atuple) {
	if (atuple.any[is_request_index]) {
	    return str_star;
	} else {
	    return isRequestStr(atuple.data);
	}
    }

    static string sqlify(const string &basestr) {
	if (basestr == str_star) {
	    return str_null;
	} else {
	    return str(format("'%s'") % basestr);
	}
    }

    void newExtentHook(const Extent &e) {
	if (series.getTypePtr() != NULL) {
	    return; // already did this
	}
	const ExtentType::Ptr type = e.getTypePtr();
	if (type->getName() == "NFS trace: common") {
	    SINVARIANT(type->getNamespace() == "" &&
		       type->majorVersion() == 0 &&
		       type->minorVersion() == 0);
	    packet_at.setFieldName("packet-at");
	    payload_length.setFieldName("payload-length");
	    op_id.setFieldName("op-id");
	    nfs_version.setFieldName("nfs-version");
	    is_request.setFieldName("is-request");
	} else if (type->getName() == "Trace::NFS::common"
		   && type->versionCompatible(1,0)) {
	    packet_at.setFieldName("packet-at");
	    payload_length.setFieldName("payload-length");
	    op_id.setFieldName("op-id");
	    nfs_version.setFieldName("nfs-version");
	    is_request.setFieldName("is-request");
	} else if (type->getName() == "Trace::NFS::common"
		   && type->versionCompatible(2,0)) {
	    packet_at.setFieldName("packet_at");
	    payload_length.setFieldName("payload_length");
	    op_id.setFieldName("op_id");
	    nfs_version.setFieldName("nfs_version");
	    is_request.setFieldName("is_request");
	} else {
	    FATAL_ERROR("?");
	}
    }

    static bool timeLessEqual(const BitsetAnyTuple &t, 
			      int32_t max_group_seconds) {
	if (t.any[time_index]) {
	    return false;
	} else {
	    SINVARIANT(t.data.get<time_index>() <= max_group_seconds);
	
	    return true;
	} 
    }
									 
    void checkMemoryUsage() {
	static LintelLog::Category category("HostInfo");

	if (!LintelLog::wouldDebug(category)) {
	    return;
	}

	size_t memory_usage = base_data.memoryUsage()
	    + rates_cube.memoryUsage();
	memory_usage += rate_hts.memoryUsage()
	    + Rates::memory_usage * rate_hts.size();
	if (memory_usage > (last_reported_memory_usage + 32*1024*1024)) {
	    LintelLogDebug
		("HostInfo", format("# HostInfo-%ds: memory usage %d bytes"
				    " (%d + %d + %d + %d * %d) @ %ds")
		 % group_seconds % memory_usage 
		 % base_data.memoryUsage() % rates_cube.memoryUsage()
		 % rate_hts.memoryUsage() 
		 % Rates::memory_usage %  rate_hts.size()
		 % (max_group_seconds - min_group_seconds));
	    last_reported_memory_usage = memory_usage;
	}
    }

    void rollupBatch() {
	base_data.fillHashUniqueTuple(unique_vals_tuple);
	rates_cube.cube(base_data);

	if (print_base) {
	    base_data.walkOrdered
		(boost::bind(&HostInfo::printBaseIncremental, 
			     this, _1, _2));
	}
	base_data.clear();

	if (options["print_rates"]) {
	    rates_cube.walk(boost::bind(&HostInfo::rateRollupAdd, this, _1, _2));
	}
	if (print_cube) {
	    rates_cube.walkOrdered
		(boost::bind(&HostInfo::printCubeIncrementalTime, this, _1, _2));
	}
	rates_cube.prune(boost::bind(&HostInfo::timeLessEqual, _1, max_group_seconds));
	SINVARIANT(base_data.size() == 0);
	checkMemoryUsage();
    }

    void processGroup(int32_t seconds) {
	SINVARIANT(max_group_seconds >= min_group_seconds);
	if (seconds > min_group_seconds) {
	    ++group_count;
	}
	if (base_data.size() >= incremental_batch_size) {
	    LintelLogDebug
		("HostInfo", format("should incremental ]%d,%d[ bd=%d cube=%d ratescube=%d rate_hts=%d") 
		 % min_group_seconds % max_group_seconds % base_data.size()
		 % rates_cube.size() % rates_cube.size() % rate_hts.size());

	    last_rollup_at = max_group_seconds;
	    rollupBatch();
	} else {
	    LintelLogDebug("HostInfo", format("skip incremental %d < %d")
			   % base_data.size() % incremental_batch_size);
	}
    }

    void finalGroup() {
	--group_count;
	int32_t elapsed_seconds = max_group_seconds - min_group_seconds;
	SINVARIANT((elapsed_seconds % group_seconds) == 0);
	// Total group count is elapsed / group_seconds + 1, but we 
	// ignore the first and last groups, so we end up with -1
	SINVARIANT(elapsed_seconds / group_seconds - 1 == group_count);

	rollupBatch();
	if (print_cube) {
	    rates_cube.walkOrdered
		(boost::bind(&HostInfo::printCubeIncrementalNonTime, this, _1, _2));
	}
	
    }

    void nextSecondsGroup(int32_t next_group_seconds) {
	if (next_group_seconds <= max_group_seconds) {
	    
	    cout << format("# HostInfo-%ds: WARNING, out of order requests, %d <= %d\n")
		% group_seconds % next_group_seconds % max_group_seconds;
	    INVARIANT(next_group_seconds == max_group_seconds - group_seconds,
		      "Only allowing rollback by one group");
	    INVARIANT(last_rollup_at < (next_group_seconds - 2*group_seconds),
		      format("LRA=%d >= %d - %d; safety unclear")
		      % last_rollup_at % next_group_seconds 
		      % (2*group_seconds));
	    INVARIANT(base_data.size() < incremental_batch_size, 
		      "Size overflow, disallowing");
	    return;
	}
	// skip counting the first and last groups; we prune these
	// because we can't calculate the rates properly on them.
	INVARIANT(next_group_seconds > max_group_seconds,
		  format("out of order %d <= %d")
		  % next_group_seconds % max_group_seconds);
	
	if (max_group_seconds == numeric_limits<int32_t>::min()) {
	    max_group_seconds = next_group_seconds - group_seconds;
	}
	for(max_group_seconds += group_seconds; 
	    max_group_seconds < next_group_seconds; 
	    max_group_seconds += group_seconds) {
	    // Rarely we have no operations in some groups.
	    LintelLogDebug("HostInfo", format("empty seconds @%d")
			   % max_group_seconds);
	    ++zero_groups;
	    unique_vals_tuple.get<time_index>().add(max_group_seconds);
	    processGroup(max_group_seconds);
	}
	SINVARIANT(max_group_seconds == next_group_seconds);
	processGroup(max_group_seconds);
    }

    virtual void processRow() {
	int32_t seconds = packet_at.valSec();
	seconds -= seconds % group_seconds;
	uint8_t operation = opIdToUnifiedId(nfs_version.val(), op_id.val());

	// Lets see if we've moved onto the next time group...
	min_group_seconds = min(seconds, min_group_seconds);
	if (seconds != max_group_seconds) {
	    nextSecondsGroup(seconds);
	}
	// redundant calculation, but useful for checking that all the
	// cube rollup stuff is working properly.
	payload_overall.add(payload_length.val());

	// sender...
	Tuple cube_key(source_ip.val(), true, 
		       seconds, operation, is_request.val());
	base_data.add(cube_key, payload_length.val());

	// reciever...
	cube_key.get<host_index>() = dest_ip.val();
	cube_key.get<is_send_index>() = false;
	base_data.add(cube_key, payload_length.val());
    }

    void printCreateTables() {
	cout
	    << "--- The cube 'any' field is represented as null\n"
	    << "--- note that there is a 'null' operation which is different than null\n"
	    << "create table nfs_hostinfo_cube (group_seconds int null,\n"
	    << "                                host int null,\n"
	    << "                                group_time int null,\n"
	    << "                                direction enum('send', 'recv') null,\n"
	    << "                                operation varchar(32) null,\n"
	    << "                                op_dir enum('request','response') null,\n"
	    << "                                group_count bigint not null,\n"
	    << "                                mean_payload_bytes double not null,\n"
	    << "                                unique key pkey (group_seconds, host, group_time, direction, operation, op_dir)\n"
	    << ");\n"
	    << "\n"
	    << "create table nfs_hostinfo_rates (group_seconds int null,\n"
	    << "				 host int null,\n"
	    << "				 direction enum('send', 'recv') null,\n"
	    << "				 operation varchar(32) null,\n"
	    << "				 op_dir enum('request', 'response') null,\n"
	    << "				 mean_operations_per_second double not null,\n"
	    << "				 mean_payload_bytes_per_second double not null,\n"
	    << "                                 unique key pkey (group_seconds, host, direction, operation, op_dir)\n"
	    << ");\n"
 	    << "\n"
	    << "create table nfs_hostinfo_rate_quantiles (group_seconds int null,\n"
	    << "				          host int null,\n"
	    << "				          direction enum('send', 'recv') null,\n"
	    << "				          operation varchar(32) null,\n"
	    << "				          op_dir enum('request', 'response') null,\n"
	    << "				          quantile double not null,\n"
	    << "				          operations_per_second double not null,\n"
	    << "				          payload_bytes_per_second double not null,\n"
	    << "                                          unique key pkey (group_seconds, host, direction, operation, op_dir, quantile)\n"
	    << ");\n"
	    ;
	    }

    void printBaseIncremental(const Tuple &t, Stats &v) {
	if (sql_output) {
	    cout << format("insert into nfs_hostinfo_cube (group_seconds, host, group_time, direction, operation, op_dir, group_count, mean_payload_bytes) values (%d, %d, %d, '%s', '%s', '%s', %d, %.20g);\n")
		% group_seconds % host(t) % time(t) % isSendStr(t) 
		% operationStr(t) % isRequestStr(t) % v.countll() % v.mean();
	} else {
	    if (!printed_base_header) {
		cout << format("HostInfo %ds base: host     time        dir          op    op-dir   count mean-payload\n") % group_seconds;
		printed_base_header = true;
	    }
	    cout << format("HostInfo %ds base: %08x %10d %s %12s %8s %6lld %8.2f\n")
		% group_seconds % host(t) % time(t) % isSendStr(t) 
		% operationStr(t) % isRequestStr(t) % v.countll() % v.mean();
	}
    }	
    
    void printCubeIncremental(const BitsetAnyTuple &atuple, Stats &v) {
	if (atuple.any.none()) {
	    return; // all values real, skip
	} else {
	    if (!print_cube) {
		FATAL_ERROR("?");
	    }
	}
	
	if (sql_output) {
	    cout << format("insert into nfs_hostinfo_cube (group_seconds, host, group_time, direction, operation, op_dir, group_count, mean_payload_bytes) values (%d, %d, %d, %s, %s, %s, %d, %.20g);\n")
		% group_seconds % sqlHost(atuple) % time(atuple, str_null) 
		% sqlify(isSendStr(atuple)) % sqlify(operationStr(atuple)) 
		% sqlify(isRequestStr(atuple)) % v.countll() % v.mean();
	} else {
	    if (!printed_cube_header) {
		cout << format("HostInfo %ds cube: host     time        dir          op    op-dir   count mean-payload\n") % group_seconds;
		printed_cube_header = true;
	    }
	    cout << format("HostInfo %ds cube: %8s %10s %4s %12s %8s %6lld %8.2f\n")
		% group_seconds % host(atuple) % time(atuple) % isSendStr(atuple) 
		% operationStr(atuple) % isRequestStr(atuple) 
		% v.countll() % v.mean();
	}
    }

    void printCubeIncrementalTime(const BitsetAnyTuple &atuple, Stats &v) {
	if (!atuple.any[time_index]) {
	    printCubeIncremental(atuple, v);
	}
    }

    void printCubeIncrementalNonTime(const BitsetAnyTuple &atuple, Stats &v) { 
	if (atuple.any[time_index]) {
	    printCubeIncremental(atuple, v);
	}
    }

    bool cubeOptional(const BitsetAnyTuple &partial) {
	if (skip_cube_time && partial.any[time_index] == false) {
	    return false;
	}
	if (skip_cube_host && partial.any[host_index] == false) {
	    return false;
	}
	if (skip_cube_host_detail && partial.any[host_index] == false) {
	    BitsetAnyTuple::AnyT any = partial.any;
	    any[host_index] = true;
	    any[time_index] = true;
	    if ((~any).none()) {
		return true; // all any, so no detail on the host
	    } else {
		return false;
	    }
	}
	return true;
    }

    static bool cubeExceptTime(const BitsetAnyTuple &partial) {
	return partial.any[time_index];
    }

    static bool cubeExceptHost(const BitsetAnyTuple &partial) {
	return partial.any[host_index];
    }

    static bool cubeExceptTimeOrHost(const BitsetAnyTuple &partial) {
	return partial.any[time_index] && partial.any[host_index];
    }

    static void addFullStats(Stats &into, const Stats &val) {
	into.add(val);
    }

    void configCube() {
	using boost::bind; 

	if (options["cube_time"] && options["cube_host"] && options["cube_host_detail"]) {
	    rates_cube.setOptionalCubeFn(bind(&lintel::StatsCubeFns::cubeAll));
	} else {
	    rates_cube.setOptionalCubeFn(bind(&HostInfo::cubeOptional, this, _2));
	}

	rates_cube.setCubeStatsAddFn(bind(&lintel::StatsCubeFns::addFullStats, _1, _2));
    }

    //                    host  is_send operation is_req
    typedef boost::tuple<int32_t, bool, uint8_t, bool> RateRollupTupleBase;
    typedef TupleToAnyTuple<RateRollupTupleBase>::type RateRollupTuple;

    void rateRollupAdd(const BitsetAnyTuple &atuple, Stats &v) {
	if (atuple.any[time_index]) {
	    return; // Ignore unless time is set, this is all we will prune.
	}
	if (atuple.data.get<time_index>() == min_group_seconds ||
	    atuple.data.get<time_index>() == max_group_seconds) {
	    return;
	}
	// TODO: don't we need to skip the max_group_seconds also once we
	// have gotten to the max, i.e. when we do this under finalGroup?
	// we don't but only because we're skipping cubing the last bit of
	// the data, which will make us wrong in the cube output.
	INVARIANT(atuple.data.get<time_index>() > min_group_seconds &&
		  atuple.data.get<time_index>() < max_group_seconds,
		  format("%d \not in ]%d, %d[") % atuple.data.get<time_index>() 
		  % min_group_seconds % max_group_seconds); 
	if (false) {
	    cout << format("%8s %10s %4s %12s %8s %6lld %8.2f\n")
		% host(atuple) % time(atuple) % isSendStr(atuple) 
		% operationStr(atuple) % isRequestStr(atuple) 
		% v.countll() % v.mean();
	}

	RateRollupTuple rrt;
	if (!atuple.any[host_index]) {
	    rrt.get<0>().set(atuple.data.get<host_index>());
	}
	if (!atuple.any[is_send_index]) {
	    rrt.get<1>().set(atuple.data.get<is_send_index>());
	}
	if (!atuple.any[operation_index]) {
	    rrt.get<2>().set(atuple.data.get<operation_index>());
	}
	if (!atuple.any[is_request_index]) {
	    rrt.get<3>().set(atuple.data.get<is_request_index>());
	}

	double count = v.countll();
	double ops_per_sec = count / group_seconds;
	double bytes_per_sec = (count * v.mean()) / group_seconds;
	
	rate_hts.getHashEntry(rrt).add(ops_per_sec, bytes_per_sec);
    }

    void addMissingZeroRates(const RateRollupTuple &t, Rates &v) {
	int64_t count = v.ops_rate.countll();
	SINVARIANT(count == static_cast<int64_t>(v.bytes_rate.countll()));
	while (count < group_count) {
	    // Entry was late to the rollup, so is missing some of the
	    // early rate-rollup entries.  TODO: consider printing out 
	    // a warning about this; it should only be happening to hosts,
	    // and possibly but unlikely to operation types.
	    ++count;
	    v.ops_rate.add(0);
	    v.bytes_rate.add(0);
	}
	SINVARIANT(count == group_count 
		   && static_cast<uint64_t>(count) == v.ops_rate.countll() 
		   && static_cast<uint64_t>(count) == v.bytes_rate.countll());
    }

    static string hostStr(const RateRollupTuple &t, bool sql = false) {
	if (t.get<0>().any) {
	    return sql ? str_null : str_star;
	} else {
	    return str(format(sql ? "0x%08x" : "%08x") % t.get<0>().val);
	}
    }

    static string isSendStr(const RateRollupTuple &t) {
	if (t.get<1>().any) {
	    return str_star;
	} else if (t.get<1>().val) {
	    return str_send;
	} else {
	    return str_recv;
	}
    }
	    
    static string operationStr(const RateRollupTuple &t) {
	if (t.get<2>().any) {
	    return str_star;
	} else {
	    return unifiedIdToName(t.get<2>().val);
	}
    }
	
    static string isRequestStr(const RateRollupTuple &t) {
	if (t.get<3>().any) {
	    return str_star;
	} else if (t.get<3>().val) {
	    return str_request;
	} else {
	    return str_response;
	}
    }
    
    string humanQuantiles(StatsQuantile &q, double divide) {
	return str(format("%.8g %.8g %.8g %.8g %.8g %.8g %.8g")
		   % (q.getQuantile(0.05)/divide) 
		   % (q.getQuantile(0.1)/divide)
		   % (q.getQuantile(0.25)/divide)
		   % (q.getQuantile(0.5)/divide)
		   % (q.getQuantile(0.75)/divide)
		   % (q.getQuantile(0.9)/divide)
		   % (q.getQuantile(0.95)/divide));
    }

    void printRate(const RateRollupTuple &t, Rates &v) {
	SINVARIANT(v.ops_rate.count() == v.bytes_rate.count());
	SINVARIANT(v.ops_rate.count() == static_cast<uint64_t>(group_count));
	SINVARIANT(v.ops_rate.mean() > 0);

	if (sql_output) {
	    cout << format("insert into nfs_hostinfo_rates (group_seconds, host, direction, operation, op_dir, mean_operations_per_second, mean_payload_bytes_per_second) values (%d, %s, %s, %s, %s, %.8g, %.8g);\n")
		% group_seconds % hostStr(t, true) % sqlify(isSendStr(t))
		% sqlify(operationStr(t)) % sqlify(isRequestStr(t))
		% v.ops_rate.mean() % v.bytes_rate.mean();
	    
	    if (print_rates_quantiles) {
		static const double quantile_step = 0.01;
		cout << format("insert into nfs_hostinfo_rate_quantiles (group_seconds, host, direction, operation, op_dir, quantile, operations_per_second, payload_bytes_per_second) values ");
		for(double q = 0; q < 1.0000000001; q += quantile_step) {
		    if (q > 0) { cout << ", "; }
		    if (q > 1) { q = 1; } // doubles can have slight overflow
		    cout << format("(%d, %s, %s, %s, %s, %.5f, %.8g, %.8g)")
			% group_seconds % hostStr(t, true) % sqlify(isSendStr(t))
			% sqlify(operationStr(t)) % sqlify(isRequestStr(t))
			% q % v.ops_rate.getQuantile(q) % v.bytes_rate.getQuantile(q);
		}
		cout << ";\n";
	    }
	} else {
	    if (!printed_rates_header) {
		cout << format("HostInfo %ds rates: host     dir      op    op-dir  ops/s payload-MiB/s\n") % group_seconds;
		if (print_rates_quantiles) {
		    cout << format("HostInfo %ds rates-quantiles: 5%% 10%% 25%% 50%% 75%% 90%% 95%% ops/s : 5%% 10%% 25%% 50%% 75%% 90%% 95%% MiB/s\n") % group_seconds;
		}
		printed_rates_header = true;
	    }
	    
	    cout << format("HostInfo %ds rates: %8s %4s %8s %8s %10.2f %8.3f\n") 
		% group_seconds % hostStr(t) % isSendStr(t) % operationStr(t) 
		% isRequestStr(t) % v.ops_rate.mean() 
		% (v.bytes_rate.mean()/(1024.0*1024.0));
	    if (print_rates_quantiles) {
		cout << format("HostInfo %ds rates-quantiles: %s : %s\n")
		    % group_seconds % humanQuantiles(v.ops_rate, 1)
		    % humanQuantiles(v.bytes_rate, 1024 * 1024);
	    }
	}
    }

    void sanityCheckRates() {
	// sanity checks
	BitsetAnyTuple tmp(Tuple(0,true,0,0,true));
	tmp.any.set();
	tmp.any[is_send_index] = false;
	Stats &v = rates_cube.getCubeEntry(tmp);

	INVARIANT(v.countll() == payload_overall.countll(), 
		  format("%s: %d != %d") % tmp % v.countll() % payload_overall.countll());
	INVARIANT(Double::eq(v.mean(), payload_overall.mean()),
		  format("%.20g != %.20g") % v.mean() % payload_overall.mean());
	// Increase the tolerance to error on this value; we are summing
	// up squared values potentially over a very very large number of
	// entries (billions), hence the difference between adding in
	// batches (v) and one-by-one (payload_overall) can become 
	// substantial.  Observed error was .00000011079687316312; this
	// is 5x the tolerance.  Assuming it was fine as count, mean were
	// equal and given stddev was ~3575, the two values were pretty
	// close.
	INVARIANT(Double::eq(v.stddev(), payload_overall.stddev(), .0000005),
		  format("%.20g != %.20g; %d ents") % v.stddev() 
		  % payload_overall.stddev() % payload_overall.countll());
	    
	tmp.any[is_send_index] = true;
	Stats &w = rates_cube.getCubeEntry(tmp);
	// Complete rollup sees everything twice since we add it as
	// both send and receive
	INVARIANT(w.countll() == 2*payload_overall.countll(),
		  format("%d != %d") % w.countll() 
		  % (2*payload_overall.countll()));
	INVARIANT(Double::eq(w.mean(), payload_overall.mean()),
		  format("%.20g != %.20g") 
		  % w.mean() % payload_overall.mean());
	INVARIANT(Double::eq(w.stddev(), payload_overall.stddev(), .0000005),
		  format("%.20g != %.20g; %d ents") % w.stddev() 
		  % payload_overall.stddev() % payload_overall.countll());

	// Check that we have the right number of entries in the
	// rollups of the rates table.

	if (group_count > 0 && options["print_rates"]) {
	    RateRollupTuple rrt;
	    INVARIANT(rate_hts[rrt].ops_rate.countll() + zero_groups
		      == static_cast<uint64_t>(group_count),
		      format("%d != %d") % rate_hts[rrt].ops_rate.countll()
		      % group_count);
	    rrt.get<1>().set(true);
	    SINVARIANT(rate_hts[rrt].ops_rate.countll() + zero_groups
		       == static_cast<uint64_t>(group_count));
	    rrt.get<1>().set(false);
	    SINVARIANT(rate_hts[rrt].ops_rate.countll() + zero_groups
		       == static_cast<uint64_t>(group_count));
	}
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	
// ds data -> hash_tuple_stats(host, is_send, time, op, is_request, stat)
// -> zero-cube(h,i,t,o,i, sum(stat))
// hut(any(h), any(i), any(o), any(i), quantile(stat.count()/time_chunk), 
//     quantile(stats.mean()/time_chunk))

	finalGroup();

	cout << format("unique entries: %d time, %d host, %d operation\n")
	    % (1+(max_group_seconds - min_group_seconds)/group_seconds)
	    % unique_vals_tuple.get<host_index>().size()
	    % unique_vals_tuple.get<operation_index>().size();
	
	SINVARIANT(base_data.size() == 0);
	    
	sanityCheckRates();
	
	if (group_count > 0 && options["print_rates"]) {
	    rate_hts.walk(boost::bind
			  (&HostInfo::addMissingZeroRates, this, _1, _2));
	    
	    rate_hts.walkOrdered
		(boost::bind(&HostInfo::printRate, this, _1, _2));
	    cout  << "# Note that the all * rollup double counts"
		" operations since we count both the\n"
		  << "# send and the receive\n";
	}
	
	if (group_count == -1) { 
	    // special case for when we had no full groups, the calculation
	    // ends up at -1, but we want to print 0.
	    ++group_count;
	}
	cout << format("# Processed %d complete groups of size %d\n")
	    % group_count % group_seconds;
	cout << "# (ignored the partial first and last groups)\n";
	cout << format("# Total time range was [%d..%d], %d groups with no nfs ops\n")
	    % min_group_seconds % max_group_seconds % zero_groups;

	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

    static Stats *makeStats() {
	return new Stats();
    }

    Int64TimeField packet_at;
    TFixedField<int32_t> payload_length;
    ByteField op_id, nfs_version;
    TFixedField<int32_t> source_ip, dest_ip;
    BoolField is_request;
    int32_t group_seconds;

    HashMap<string, bool> options;
    uint32_t incremental_batch_size;

    HashTupleStats<Tuple> base_data;

    // Won't have all the time values in it, they are done incrementally.
    // only has anything in it if the constant enable_hash_unique_tuple is
    // true.
    HashTupleStats<Tuple>::HashUniqueTuple unique_vals_tuple;

    // Will be incrementally processed.
    StatsCube<Tuple> rates_cube;

    HashTupleStats<RateRollupTuple, Rates> rate_hts;
    Stats payload_overall;
    int32_t min_group_seconds, max_group_seconds, last_rollup_at;
    int64_t group_count;

    bool printed_base_header, printed_cube_header, printed_rates_header;

    bool print_rates_quantiles, sql_output, print_base, print_cube;
    uint32_t zero_groups;
    
    bool skip_cube_time, skip_cube_host, skip_cube_host_detail;
    size_t last_reported_memory_usage;
};

RowAnalysisModule *
NFSDSAnalysisMod::newHostInfo(DataSeriesModule &prev, char *arg) {
    return new HostInfo(prev, arg);
}

size_t HostInfo::Rates::memory_usage;
