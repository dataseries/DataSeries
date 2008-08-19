#include <vector>
#include <bitset>

#include <boost/bind.hpp>
#include <boost/tuple/tuple_comparison.hpp>
#include <boost/tuple/tuple_io.hpp>

#include <analysis/nfs/common.hpp>
#include <analysis/nfs/HostInfo.hpp>

#include <Lintel/HashMap.hpp>
#include <Lintel/HashUnique.hpp>

#include <DataSeries/GeneralField.hpp>

using namespace std;
using boost::format;

namespace boost { namespace tuples {
    inline uint32_t hash(const null_type &) { return 0; }
    inline uint32_t hash(const bool a) { return a; }
    inline uint32_t hash(const int32_t a) { return a; }

    template<class Head>
    inline uint32_t hash(const cons<Head, null_type> &v) {
	return hash(v.get_head());
    }

    // See http://burtleburtle.net/bob/c/lookup3.c for a discussion
    // about how we could have even fewer calls to the mix function.
    // Would want to upgrade to the newer hash function.
    template<class Head1, class Head2, class Tail>
    inline uint32_t hash(const cons<Head1, cons<Head2, Tail> > &v) {
	uint32_t a = hash(v.get_head());
	uint32_t b = hash(v.get_tail().get_head());
	uint32_t c = hash(v.get_tail().get_tail());
	return BobJenkinsHashMix3(a,b,c);
    }

    template<class BitSet>
    inline uint32_t partial_hash(const null_type &, const BitSet &, size_t) {
	return 0;
    }

    template<class Head, class BitSet>
    inline uint32_t partial_hash(const cons<Head, null_type> &v,
				 const BitSet &used, size_t cur_pos) {
	return used[cur_pos] ? hash(v.get_head()) : 0;
    }

    template<class Head1, class Head2, class Tail, class BitSet>
    inline uint32_t partial_hash(const cons<Head1, cons<Head2, Tail> > &v, 
				 const BitSet &used, size_t cur_pos) {
	uint32_t a = used[cur_pos] ? hash(v.get_head()) : 0;
	uint32_t b = used[cur_pos+1] ? hash(v.get_tail().get_head()) : 0;
	uint32_t c = partial_hash(v.get_tail().get_tail(), used, cur_pos + 2);
	return BobJenkinsHashMix3(a,b,c);
    }

    template<class BitSet>
    inline bool partial_equal(const null_type &lhs, const null_type &rhs,
			      const BitSet &used, size_t cur_pos) {
	return true;
    }

    template<class Head, class Tail, class BitSet>
    inline bool partial_equal(const cons<Head, Tail> &lhs,
			      const cons<Head, Tail> &rhs,
			      const BitSet &used, size_t cur_pos) {
	if (used[cur_pos] && lhs.get_head() != rhs.get_head()) {
	    return false;
	}
	return partial_equal(lhs.get_tail(), rhs.get_tail(), 
			     used, cur_pos + 1);
    }

    template<class BitSet>
    inline bool partial_strict_less_than(const null_type &lhs, 
					 const null_type &rhs,
					 const BitSet &used_lhs, 
					 const BitSet &used_rhs,
					 size_t cur_pos) {
	return false;
    }

    template<class Head, class Tail, class BitSet>
    inline bool partial_strict_less_than(const cons<Head, Tail> &lhs,
					 const cons<Head, Tail> &rhs,
					 const BitSet &used_lhs, 
					 const BitSet &used_rhs,
					 size_t cur_pos) {
	if (used_lhs[cur_pos] && used_rhs[cur_pos]) {
	    if (lhs.get_head() < rhs.get_head()) {
		return true;
	    } else if (lhs.get_head() > rhs.get_head()) {
		return false;
	    } else {
		// don't know, fall through to recursion.
	    }
	} else if (used_lhs[cur_pos] && !used_rhs[cur_pos]) {
	    return true; // used < *
	} else if (!used_lhs[cur_pos] && used_rhs[cur_pos]) {
	    return false; // * > used
	} 
	return partial_strict_less_than(lhs.get_tail(), rhs.get_tail(), 
					used_lhs, used_rhs, cur_pos + 1);
    }
} }

template<class Tuple> struct TupleHash {
    uint32_t operator()(const Tuple &a) {
	return boost::tuples::hash(a);
    }
};

//                   host,   is_send,   time, operation, is_request
typedef boost::tuple<int32_t, bool,   int32_t,  uint8_t,    bool> 
    HostInfoTuple;
static const size_t host_index = 0;
static const size_t is_send_index = 1;
static const size_t time_index = 2;
static const size_t operation_index = 3;
static const size_t is_request_index = 4;

template<> struct TupleHash<HostInfoTuple> {
    uint32_t operator()(const HostInfoTuple &v) {
	BOOST_STATIC_ASSERT(boost::tuples::length<HostInfoTuple>::value == 5);
	uint32_t a = v.get<host_index>();
	uint32_t b = v.get<time_index>();
	uint32_t c = v.get<operation_index>()
	    | (v.get<is_send_index>() ? 0x100 : 0)
	    | (v.get<is_request_index>() ? 0x200 : 0);
	return BobJenkinsHashMix3(a,b,c);
    }
};
	       
template<class Tuple> struct PartialTuple {
    BOOST_STATIC_CONSTANT(uint32_t, 
			  length = boost::tuples::length<Tuple>::value);

    Tuple data;
    typedef bitset<boost::tuples::length<Tuple>::value> UsedT;
    UsedT used;

    PartialTuple() { }
    PartialTuple(const Tuple &from) : data(from) { }

    bool operator==(const PartialTuple &rhs) const {
	if (used != rhs.used) { 
	    return false;
	}
	return boost::tuples::partial_equal(data, rhs.data, used, 0);
    }
    bool operator<(const PartialTuple &rhs) const {
	return boost::tuples::partial_strict_less_than(data, rhs.data, 
						       used, rhs.used, 0);
    }
};

template<class Tuple> struct PartialTupleHash {
    uint32_t operator()(const PartialTuple<Tuple> &v) {
	return boost::tuples::partial_hash(v.data, v.used, 0);
    }
};

template<> struct PartialTupleHash<HostInfoTuple> {
    uint32_t operator()(const PartialTuple<HostInfoTuple> &v) {
	BOOST_STATIC_ASSERT(boost::tuples::length<HostInfoTuple>::value == 5);
	uint32_t a = v.used[host_index] ? v.data.get<host_index>() : 0;
	uint32_t b = v.used[time_index] ? v.data.get<time_index>() : 0;
	uint32_t c 
	    = (v.used[operation_index] ? v.data.get<operation_index>() : 0)
	    | (v.used[is_send_index] && v.data.get<is_send_index>() 
	       ? 0x100 : 0)
	    | (v.used[is_request_index] && v.data.get<is_request_index>() 
	       ? 0x200 : 0);
	return BobJenkinsHashMix3(a,b,c);
    }
};

using boost::tuples::null_type;

template<class T0, class T1>
struct ConsToHashUniqueCons {
    typedef ConsToHashUniqueCons<typename T1::head_type, 
				 typename T1::tail_type> tail;
    typedef boost::tuples::cons<HashUnique<T0>, typename tail::type> type;
};

template<class T0>
struct ConsToHashUniqueCons<T0, null_type> {
    typedef boost::tuples::cons<HashUnique<T0>, null_type> type;
};

template<class T>
struct TupleToHashUniqueTuple 
  : ConsToHashUniqueCons<typename T::head_type, typename T::tail_type> {
};

class StatsCubeFns {
public:
    static Stats *createStats() {
	return new Stats();
    }

    static bool cubeAll() {
	return true;
    }

    static void addFullStats(Stats &into, const Stats &val) {
	into.add(val);
    }
    
    static void addMean(Stats &into, const Stats &val) {
	into.add(val.mean());
    }
};

template<> struct HashMap_hash<const bool> {
    uint32_t operator()(const bool x) {
	return x;
    }
};

template<> struct HashMap_hash<const unsigned char> {
    uint32_t operator()(const bool x) {
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

double zeroCubeBaseCount(null_type) { 
    return 1; 
}

template<class HUT>
double zeroCubeBaseCount(HUT &hut) {
    return hut.get_head().size() * zeroCubeBaseCount(hut.get_tail());
}

template<class KeyBase, class BaseData, class StatsCube>
void zeroCubeAll(null_type, null_type, KeyBase &key_base, 
		 BaseData &base_data, Stats &null_stat, StatsCube &cube) {
    typedef typename BaseData::iterator iterator;
    iterator i = base_data.find(key_base);
    typename StatsCube::MyPartial tmp_partial(key_base);
    if (i == base_data.end()) {
	cube.cubeAddOne(tmp_partial, 0, false, null_stat);
    } else {
	cube.cubeAddOne(tmp_partial, 0, false, *(i->second));
    }
}

template<class HUT, class KeyTail, class KeyBase, class BaseData, 
	 class StatsCube>
void zeroCubeAll(HUT &hut, KeyTail &key_tail, KeyBase &key_base, 
		 BaseData &base_data, Stats &null_stat, StatsCube &cube) {
    typedef typename HUT::head_type::iterator iterator;

    for(iterator i = hut.get_head().begin(); i != hut.get_head().end(); ++i) {
	key_tail.get_head() = *i;

	zeroCubeAll(hut.get_tail(), key_tail.get_tail(), key_base,
		    base_data, null_stat, cube);
    }
}
    
template<class Tuple, class StatsT = Stats>
class HashTupleStats {
public:
    // base types
    typedef HashMap<Tuple, StatsT *, TupleHash<Tuple> > HTSMap;
    typedef typename HTSMap::const_iterator HTSiterator;
    typedef vector<typename HTSMap::value_type> HTSValueVector;
    typedef typename HTSValueVector::iterator HTSVViterator;

    // zero cubing types
    typedef TupleToHashUniqueTuple<Tuple> HUTConvert;
    typedef typename HUTConvert::type HashUniqueTuple;

    // functions
    typedef boost::function<StatsT *()> StatsFactoryFn;
    typedef boost::function<void (const Tuple &key, StatsT *value)> WalkFn;

    explicit HashTupleStats(const StatsFactoryFn &fn1 
			    = boost::bind(&StatsCubeFns::createStats))
	: stats_factory_fn(fn1)
    { }

    void add(const Tuple &key, double value) {
	getHashEntry(key).add(value);
    }

    void walk(const WalkFn &walk_fn) const {
	for(HTSiterator i = data.begin(); i != data.end(); ++i) {
	    walk_fn(i->first, i->second);
	}
    }

    void walkOrdered(const WalkFn &walk_fn) const {
	HTSValueVector sorted;

	sorted.reserve(data.size());
	// TODO: figure out why the below doesn't work.
	//	sorted.push_back(base_data.begin(), base_data.end());
	for(HTSiterator i = data.begin(); i != data.end(); ++i) {
	    sorted.push_back(*i);
	}
	sort(sorted.begin(), sorted.end());
	for(HTSVViterator i = sorted.begin(); i != sorted.end(); ++i) {
	    walk_fn(i->first, i->second);
	}
    }

    void walkZeros(const WalkFn &walk_fn) {
	StatsT *zero = stats_factory_fn();
	
	HashUniqueTuple hut;
	delete zero;
    }
	

private:
    StatsT &getHashEntry(const Tuple &key) {
	StatsT * &v = data[key];

	if (v == NULL) {
	    v = stats_factory_fn();
	}
	return *v;
    }

    HTSMap data;
    StatsFactoryFn stats_factory_fn;
};

template<class Tuple, class StatsT = Stats>
class StatsCube {
public:
    // partial tuple types
    typedef PartialTuple<Tuple> MyPartial;
    typedef HashMap<MyPartial, StatsT *, PartialTupleHash<Tuple> > 
        PartialTupleCubeMap;
    typedef typename PartialTupleCubeMap::iterator PTCMIterator;
    typedef vector<typename PartialTupleCubeMap::value_type> PTCMValueVector;
    typedef typename PTCMValueVector::iterator PTCMVVIterator;

    // functions controlling cubing...
    typedef boost::function<StatsT *()> StatsFactoryFn;
    typedef boost::function<void (Tuple &key, StatsT *value)> PrintBaseFn;
    typedef boost::function<void (Tuple &key, typename MyPartial::UsedT, 
				  StatsT *value)> PrintCubeFn;
    typedef boost::function<bool (const typename MyPartial::UsedT &used)>
       OptionalCubePartialFn;
    typedef boost::function<void (StatsT &into, const StatsT &val)>
       CubeStatsAddFn;

    explicit StatsCube(const StatsFactoryFn &fn1
		       = boost::bind(&StatsCubeFns::createStats),
		       const OptionalCubePartialFn &fn2 
		       = boost::bind(&StatsCubeFns::cubeAll),
		       const CubeStatsAddFn &fn3
		       = boost::bind(&StatsCubeFns::addFullStats, _1, _2)) 
	: stats_factory_fn(fn1), optional_cube_partial_fn(fn2),
	  cube_stats_add_fn(fn3) 
    { }

    void setOptionalCubePartialFn(const OptionalCubePartialFn &fn2 
				  = boost::bind(&StatsCubeFns::cubeAll)) {
	optional_cube_partial_fn = fn2;
    }

    void setCubeStatsAddFn(const CubeStatsAddFn &fn3
			   = boost::bind(&StatsCubeFns::addFullStats)) {
	cube_stats_add_fn = fn3;
    }

    void add(const HashTupleStats<Tuple, StatsT> &hts) {
	hts.walk(boost::bind(&StatsCube<Tuple, StatsT>::cubeAddOne, 
			     this, _1, _2));
    }

    void cubeAddOne(const Tuple &key, StatsT *value) {
	MyPartial tmp_key(key);

	cubeAddOne(tmp_key, 0, false, *value);
    }

    void cubeAddOne(MyPartial &key, size_t pos, bool had_false, 
		    const StatsT &value) {
	if (pos == key.length) {
	    if (had_false && optional_cube_partial_fn(key.used)) {
		cube_stats_add_fn(getPartialEntry(key), value);
	    }
	} else {
	    DEBUG_SINVARIANT(pos < key.length);
	    key.used[pos] = true;
	    cubeAddOne(key, pos + 1, had_false, value);
	    key.used[pos] = false;
	    cubeAddOne(key, pos + 1, true, value);
	}
    }

    void zeroCube() {
	FATAL_ERROR("broken");
#if 0
	HashUniqueTuple hut;
	for(CMIterator i = base_data.begin(); i != base_data.end(); ++i) {
	    zeroAxisAdd(hut, i->first);
	}

	double expected_hut = zeroCubeBaseCount(hut);

	uint32_t tuple_len = boost::tuples::length<Tuple>::value;

	cout << format("Expecting to cube %.6g * 2^%d -1 = %.6g\n")
	    % expected_hut % tuple_len 
	    % (expected_hut * (pow(2.0, tuple_len) - 1));

	if (false) { hutPrint(hut, 0); }

	Tuple key;

	StatsT *tmp_empty = stats_factory_fn();
	zeroCubeAll(hut, key, key, base_data, *tmp_empty, *this);

	delete tmp_empty;
#endif
    }

    void print(const PrintCubeFn fn) {
	PTCMValueVector sorted;

	for(PTCMIterator i = cube_data.begin(); i != cube_data.end(); ++i) {
	    sorted.push_back(*i);
	}
	sort(sorted.begin(), sorted.end());

	for(PTCMVVIterator i = sorted.begin(); i != sorted.end(); ++i) {
	    fn(i->first.data, i->first.used, i->second);
	}
    }
private:
    Stats &getPartialEntry(const MyPartial &key) {
	Stats * &v = cube_data[key];

	if (v == NULL) {
	    v = stats_factory_fn();
	}
	return *v;
    }

    StatsFactoryFn stats_factory_fn;
    OptionalCubePartialFn optional_cube_partial_fn;
    CubeStatsAddFn cube_stats_add_fn;

    PartialTupleCubeMap cube_data;
};

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
    static const string str_send("send");
    static const string str_recv("recv");
    static const string str_request("request");
    static const string str_response("response");
    static const string str_star("*");
}

class HostInfo : public RowAnalysisModule {
public:
    HostInfo(DataSeriesModule &_source, const std::string &arg) 
	: RowAnalysisModule(_source),
	  packet_at(series, ""),
	  payload_length(series, ""),
	  op_id(series, "", Field::flag_nullable),
	  nfs_version(series, "", Field::flag_nullable),
	  source_ip(series,"source"), 
	  dest_ip(series, "dest"),
	  is_request(series, "")
    {
	// Usage: group_seconds[,{no_cube_time, no_cube_host, 
	//                        no_print_base, no_print_cube}]+
	vector<string> args = split(arg, ",");
	group_seconds = stringToUInt32(args[0]);
	SINVARIANT(group_seconds > 0);
	options["cube_time"] = true;
	options["cube_host"] = true;
	options["print_base"] = true;
	options["print_cube"] = true;
	options["test"] = false;
	for(unsigned i = 1; i < args.size(); ++i) {
	    if (prefixequal(args[i], "no_")) {
		INVARIANT(options.exists(args[i].substr(3)),
			  format("unknown option '%s'") % args[i]);
		options[args[i].substr(3)] = false;
	    } else {
		INVARIANT(options.exists(args[i]),
			  format("unknown option '%s'") % args[i]);
		options[args[i]] = true;
	    }
	}
    }

    virtual ~HostInfo() { }

    typedef HostInfoTuple Tuple;
    typedef bitset<5> Used;
    static int32_t host(const Tuple &t) {
	return t.get<host_index>();
    }
    static string host(const Tuple &t, const Used &used) {
	if (used[host_index] == false) {
	    return str_star;
	} else {
	    return str(format("%08x") % host(t));
	}
    }
    static bool isSend(const Tuple &t) {
	return t.get<is_send_index>();
    }
    static const string &isSendStr(const Tuple &t) {
	return isSend(t) ? str_send : str_recv;
    }

    static string isSendStr(const Tuple &t, const Used &used) {
	if (used[is_send_index] == false) {
	    return str_star;
	} else {
	    return isSendStr(t);
	}
    }

    static int32_t time(const Tuple &t) {
	return t.get<time_index>();
    }

    static string time(const Tuple &t, const Used &used) {
	if (used[time_index] == false) {
	    return str_star;
	} else {
	    return str(format("%d") % time(t));
	}
    }

    static uint8_t operation(const Tuple &t) {
	return t.get<3>();
    }

    static const string &operationStr(const Tuple &t) {
	return unifiedIdToName(operation(t));
    }
    static string operationStr(const Tuple &t, const Used &used) {
	if (used[3] == false) {
	    return str_star;
	} else {
	    return operationStr(t);
	}
    }

    static bool isRequest(const Tuple &t) {
	return t.get<4>();
    }
    static const string &isRequestStr(const Tuple &t) {
	return isRequest(t) ? str_request : str_response;
    }

    static const string &isRequestStr(const Tuple &t, const Used &used) {
	if (used[4] == false) {
	    return str_star;
	} else {
	    return isRequestStr(t);
	}
    }

    void newExtentHook(const Extent &e) {
	if (series.getType() != NULL) {
	    return; // already did this
	}
	const ExtentType &type = e.getType();
	if (type.getName() == "NFS trace: common") {
	    SINVARIANT(type.getNamespace() == "" &&
		       type.majorVersion() == 0 &&
		       type.minorVersion() == 0);
	    packet_at.setFieldName("packet-at");
	    payload_length.setFieldName("payload-length");
	    op_id.setFieldName("op-id");
	    nfs_version.setFieldName("nfs-version");
	    is_request.setFieldName("is-request");
	} else if (type.getName() == "Trace::NFS::common"
		   && type.versionCompatible(1,0)) {
	    packet_at.setFieldName("packet-at");
	    payload_length.setFieldName("payload-length");
	    op_id.setFieldName("op-id");
	    nfs_version.setFieldName("nfs-version");
	    is_request.setFieldName("is-request");
	} else if (type.getName() == "Trace::NFS::common"
		   && type.versionCompatible(2,0)) {
	    packet_at.setFieldName("packet_at");
	    payload_length.setFieldName("payload_length");
	    op_id.setFieldName("op_id");
	    nfs_version.setFieldName("nfs_version");
	    is_request.setFieldName("is_request");
	} else {
	    FATAL_ERROR("?");
	}
    }

    virtual void processRow() {
	uint32_t seconds = packet_at.valSec();
	seconds -= seconds % group_seconds;
	uint8_t operation = opIdToUnifiedId(nfs_version.val(), op_id.val());

	// sender...
	Tuple cube_key(source_ip.val(), true, 
		       seconds, operation, is_request.val());
	base_data.add(cube_key, payload_length.val());

	// reciever...
	cube_key.get<host_index>() = dest_ip.val();
	cube_key.get<is_send_index>() = false;
	base_data.add(cube_key, payload_length.val());
    }

    static void printFullRow(const Tuple &t, Stats *v) {
	cout << format("%08x %10d %s %12s %8s %6lld %8.2f\n")
	    % host(t) % time(t) % isSendStr(t) % operationStr(t) 
	    % isRequestStr(t) % v->countll() % v->mean();
    }	
    
    static void printPartialRow(const Tuple &t, const bitset<5> &used, 
				Stats *v) {
	cout << format("%8s %10s %4s %12s %8s %6lld %8.2f\n")
	    % host(t,used) % time(t,used) % isSendStr(t,used) 
	    % operationStr(t,used) % isRequestStr(t,used) 
	    % v->countll() % v->mean();
    }

    typedef StatsCube<Tuple>::MyPartial::UsedT UsedT;
    static bool cubeExceptTime(const UsedT &used) {
	return used[time_index] == false;
    }

    static bool cubeExceptHost(const UsedT &used) {
	return used[host_index] == false;
    }

    static bool cubeExceptTimeOrHost(const UsedT &used) {
	return used[time_index] == false && used[host_index] == false;
    }

    static double afs_count;

    static void addFullStats(Stats &into, const Stats &val) {
	into.add(val);
	++afs_count;
    }

    void configCube() {
	using boost::bind; 

	if (options["cube_time"] && options["cube_host"]) {
	    cube.setOptionalCubePartialFn(bind(&StatsCubeFns::cubeAll));
	} else if (options["cube_time"] && !options["cube_host"]) {
	    cube.setOptionalCubePartialFn
		(bind(&HostInfo::cubeExceptHost, _1));
	} else if (!options["cube_time"] && options["cube_host"]) {
	    cube.setOptionalCubePartialFn
		(bind(&HostInfo::cubeExceptTime, _1));
	} else if (!options["cube_time"] && !options["cube_host"]) {
	    cube.setOptionalCubePartialFn
		(bind(&HostInfo::cubeExceptTimeOrHost, _1));
	}

	cube.setCubeStatsAddFn(bind(&StatsCubeFns::addFullStats, _1, _2));
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	configCube();
	
	if (options["test"]) {
	    cube.zeroCube();
	    cout << format("Actual afs_count = %.0f\n") % afs_count;
	    //	cube.printCube(boost::bind(&HostInfo::printPartialRow, _1, _2, _3));
	    return;
	}

	cout << "host     time        dir          op    op-dir   count mean-payload\n";
	if (options["print_base"]) {
	    base_data.walkOrdered
		(boost::bind(&HostInfo::printFullRow, _1, _2));
	}
	if (options["print_cube"]) {
	    cube.add(base_data);
	    cube.print(boost::bind(&HostInfo::printPartialRow, _1, _2, _3));
	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

    static Stats *makeStats() {
	return new Stats();
    }

    Int64TimeField packet_at;
    Int32Field payload_length;
    ByteField op_id, nfs_version;
    Int32Field source_ip, dest_ip;
    BoolField is_request;
    uint32_t group_seconds;

    HashMap<string, bool> options;
    StatsCube<Tuple> cube;

    HashTupleStats<Tuple> base_data;
};
double HostInfo::afs_count;

RowAnalysisModule *
NFSDSAnalysisMod::newHostInfo(DataSeriesModule &prev, char *arg) {
    return new HostInfo(prev, arg);
}

