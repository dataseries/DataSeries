#include <vector>
#include <bitset>

#include <boost/bind.hpp>
#include <boost/tuple/tuple_comparison.hpp>
#include <boost/tuple/tuple_io.hpp>

#include <analysis/nfs/common.hpp>
#include <analysis/nfs/HostInfo.hpp>

#include <Lintel/HashMap.hpp>
#include <Lintel/HashUnique.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/StatsQuantile.hpp>

#include <DataSeries/GeneralField.hpp>

using namespace std;
using boost::format;
using dataseries::TFixedField;

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
    uint32_t operator()(const Tuple &a) const {
	return boost::tuples::hash(a);
    }
};

// Could make this a pair, but this makes it more obvious what's going on.
template<typename T>
struct AnyPair {
    bool any;
    T val;
    AnyPair() : any(true) { }
    AnyPair(const T &v) : any(false), val(v) { }
    void set(const T &v) { val = v; any = false; }
    
    bool operator ==(const AnyPair<T> &rhs) const {
	if (any != rhs.any) {
	    return false;
	} else if (any) {
	    return true;
	} else {
	    return val == rhs.val;
	}
    }
    bool operator <(const AnyPair<T> &rhs) const {
	if (any && rhs.any) {
	    return false;
	} else if (any && !rhs.any) {
	    return false;
	} else if (!any && rhs.any) {
	    return true;
	} else if (!any && !rhs.any) {
	    return val < rhs.val;
	}
	FATAL_ERROR("?");
    }
};

template<class T>
inline uint32_t hash(const AnyPair<T> &v) {
    if (v.any) {
	return 0;
    } else {
	using boost::tuples::hash;
	return hash(v.val);
    }
}


//                   host,   is_send,   time, operation, is_request
typedef boost::tuple<int32_t, bool,   int32_t,  uint8_t,    bool> 
    HostInfoTuple;
static const size_t host_index = 0;
static const size_t is_send_index = 1;
static const size_t time_index = 2;
static const size_t operation_index = 3;
static const size_t is_request_index = 4;

template<> struct TupleHash<HostInfoTuple> {
    uint32_t operator()(const HostInfoTuple &v) const {
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
    // defaults to all unused
    explicit PartialTuple(const Tuple &from) : data(from) { }
    
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
    uint32_t operator()(const PartialTuple<Tuple> &v) const {
	return boost::tuples::partial_hash(v.data, v.used, 0);
    }
};

template<> struct PartialTupleHash<HostInfoTuple> {
    uint32_t operator()(const PartialTuple<HostInfoTuple> &v) const {
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

class StatsCubeFns {
public:
    static Stats *createStats() {
	return new Stats();
    }

    static bool cubeAll() {
	return true;
    }

    static bool cubeHadFalse(bool had_false) {
	return had_false;
    }

    static void addFullStats(Stats &into, const Stats &val) {
	into.add(val);
    }
    
    static void addMean(Stats &into, const Stats &val) {
	into.add(val.mean());
    }
};

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

template<class Tuple, class StatsT = Stats>
class HashTupleStats {
public:
    // base types
    typedef HashMap<Tuple, StatsT *, TupleHash<Tuple> > HTSMap;
    typedef typename HTSMap::iterator HTSiterator;
    typedef typename HTSMap::const_iterator HTSconst_iterator;
    typedef vector<typename HTSMap::value_type> HTSValueVector;
    typedef typename HTSValueVector::iterator HTSVViterator;

    // zero cubing types
    typedef TupleToHashUniqueTuple<Tuple> HUTConvert;
    typedef typename HUTConvert::type HashUniqueTuple;

    // functions
    typedef boost::function<StatsT *()> StatsFactoryFn;
    typedef boost::function<void (const Tuple &key, StatsT &value)> WalkFn;
    typedef boost::function<bool (const Tuple &key)> PruneFn;

    explicit HashTupleStats(const StatsFactoryFn &fn1 
			    = boost::bind(&StatsCubeFns::createStats))
	: stats_factory_fn(fn1)
    { }

    ~HashTupleStats() {
	clear();
    }

    void add(const Tuple &key, double value) {
	getHashEntry(key).add(value);
    }

    void walk(const WalkFn &walk_fn) const {
	for(HTSconst_iterator i = data.begin(); i != data.end(); ++i) {
	    walk_fn(i->first, *i->second);
	}
    }

    void walkOrdered(const WalkFn &walk_fn) const {
	HTSValueVector sorted;

	sorted.reserve(data.size());
	// TODO: figure out why the below doesn't work.
	//	sorted.push_back(base_data.begin(), base_data.end());
	for(HTSconst_iterator i = data.begin(); i != data.end(); ++i) {
	    sorted.push_back(*i);
	}
	sort(sorted.begin(), sorted.end());
	for(HTSVViterator i = sorted.begin(); i != sorted.end(); ++i) {
	    walk_fn(i->first, *i->second);
	}
    }

    void fillHashUniqueTuple(HashUniqueTuple &hut) {
	for(HTSiterator i = data.begin(); i != data.end(); ++i) {
	    zeroAxisAdd(hut, i->first);
	}
    }

    void walkZeros(const WalkFn &walk_fn) const {
	HashUniqueTuple hut;

	fillHashUniqueTuple(hut);

	walkZeros(walk_fn, hut);
    }

    void walkZeros(const WalkFn &walk_fn, const HashUniqueTuple &hut) const {
	double expected_hut = zeroCubeBaseCount(hut);

	uint32_t tuple_len = boost::tuples::length<Tuple>::value;

	LintelLogDebug("HostInfo",
		       format("Expecting to cube %.6g * 2^%d = %.0f")
		       % expected_hut % tuple_len 
		       % (expected_hut * pow(2.0, tuple_len)));

	StatsT *zero = stats_factory_fn();

	Tuple tmp_key;
	zeroWalk(hut, tmp_key, tmp_key, data, *zero, walk_fn);
	
	delete zero;
    }

    StatsT &getHashEntry(const Tuple &key) {
	StatsT * &v = data[key];

	if (v == NULL) {
	    v = stats_factory_fn();
	    SINVARIANT(v != NULL);
	}
	return *v;
    }

    StatsT &operator[](const Tuple &key) {
	return getHashEntry(key);
    }

    size_t size() {
	return data.size();
    }

    void prune(PruneFn fn) {
	for(HTSiterator i = data.begin(); i != data.end(); ) {
	    if (fn(i->first)) {
		delete i->second;
		data.remove(i->first);
		i.partialReset();
	    } else {
		++i;
	    }
	}
    }
    
    void clear() {
	for(HTSiterator i = data.begin(); i != data.end(); ++i) {
	    delete i->second;
	}
	data.clear();
    }

    size_t memoryUsage() const {
	return data.memoryUsage() + sizeof(*this);
    }
private:

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

    typedef boost::function<bool (bool had_false, const MyPartial &)> 
        OptionalCubePartialFn;
    typedef boost::function<void (StatsT &into, const StatsT &val)>
       CubeStatsAddFn;
    typedef boost::function<bool (const MyPartial &partial)> PruneFn;

    explicit StatsCube(const StatsFactoryFn &fn1
		       = boost::bind(&StatsCubeFns::createStats),
		       const OptionalCubePartialFn &fn2 
		       = boost::bind(&StatsCubeFns::cubeHadFalse, _1),
		       const CubeStatsAddFn &fn3
		       = boost::bind(&StatsCubeFns::addFullStats, _1, _2)) 
	: stats_factory_fn(fn1), optional_cube_partial_fn(fn2),
	  cube_stats_add_fn(fn3) 
    { }

    ~StatsCube() {
	clear();
    }

    void setOptionalCubePartialFn(const OptionalCubePartialFn &fn2 
				  = boost::bind(&StatsCubeFns::cubeHadFalse, _1)) {
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

    typedef TupleToHashUniqueTuple<Tuple> HUTConvert;
    typedef typename HUTConvert::type HashUniqueTuple;

    void add(const HashTupleStats<Tuple, StatsT> &hts,
	     const HashUniqueTuple &hut) {
	hts.walkZeros(boost::bind(&StatsCube<Tuple, StatsT>::cubeAddOne, 
				  this, _1, _2), hut);
    }

    void cubeAddOne(const Tuple &key, StatsT &value) {
	MyPartial tmp_key(key);

	cubeAddOne(tmp_key, 0, false, value);
    }

    void cubeAddOne(MyPartial &key, size_t pos, bool had_false, 
		    const StatsT &value) {
	if (pos == key.length) {
	    if (optional_cube_partial_fn(had_false, key)) {
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

    void walk(const PrintCubeFn fn) {
	for(PTCMIterator i = cube_data.begin(); i != cube_data.end(); ++i) {
	    fn(i->first.data, i->first.used, i->second);
	}
    }

    void walkOrdered(const PrintCubeFn fn) {
	PTCMValueVector sorted;

	for(PTCMIterator i = cube_data.begin(); i != cube_data.end(); ++i) {
	    sorted.push_back(*i);
	}
	sort(sorted.begin(), sorted.end());

	for(PTCMVVIterator i = sorted.begin(); i != sorted.end(); ++i) {
	    fn(i->first.data, i->first.used, i->second);
	}
    }

    Stats &getPartialEntry(const MyPartial &key) {
	Stats * &v = cube_data[key];

	if (v == NULL) {
	    v = stats_factory_fn();
	}
	return *v;
    }

    size_t size() {
	return cube_data.size();
    }

    void prune(PruneFn fn) {
	for(PTCMIterator i = cube_data.begin(); i != cube_data.end(); ) {
	    if (fn(i->first)) {
		delete i->second;
		cube_data.remove(i->first);
		i.partialReset();
	    } else {
		++i;
	    }
	}
    }

    void clear() {
	for(PTCMIterator i = cube_data.begin(); i != cube_data.end(); ++i) {
	    delete i->second;
	}
	cube_data.clear();
    }

    size_t memoryUsage() const {
	return cube_data.memoryUsage() + sizeof(*this);
    }
private:
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
    typedef bitset<5> Used;

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
	  rate_hts(boost::bind(HostInfo::createRates)),
	  min_group_seconds(numeric_limits<int32_t>::max()),
	  max_group_seconds(numeric_limits<int32_t>::min()),
	  group_count(0), printed_base_header(false), 
	  printed_cube_header(false), printed_rates_header(false),
	  print_rates_quantiles(true), sql_output(false), zero_cube(false),
	  print_base(true), print_cube(true), zero_groups(0),
	  last_reported_memory_usage(0)
    {
	// Usage: group_seconds[,{no_cube_time, no_cube_host, 
	//                        no_print_base, no_print_cube}]+
	vector<string> args = split(arg, ",");
	group_seconds = stringToInt32(args[0]);
	SINVARIANT(group_seconds > 0);
	options["cube_time"] = true;
	options["cube_host"] = true;
	options["cube_host_detail"] = true;

	// Cubing over the unique_vals_tuple; is very expensive -- ~6x on
	// some simple initial testing.  Calculating the unique tuple is 
	// very cheap (overlappping runtimes, <1% instruction count difference),
	// but if we're not going to use it, then there isn't any point.
	options["zero_cube"] = false;
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
		incremental_batch_size = stringToUInt32(args[i].substr(12));
		LintelLogDebug("HostInfo", format("ibs=%d") % incremental_batch_size);
	    } else {
		INVARIANT(options.exists(args[i]),
			  format("unknown option '%s'") % args[i]);
		options[args[i]] = true;
	    }
	}
	INVARIANT(!options["print_rates"] || options["cube_time"],
		  "HostInfo: Can't print_rates unless we have cube_time enabled.");
	if (!options["print_rates"]) {
	    options["print_rates_quantiles"] = false;
	}
	print_rates_quantiles = options["print_rates_quantiles"];
	sql_output = options["sql_output"];
	configCube();
	if (options["sql_create_table"]) {
	    printCreateTables();
	}
	zero_cube = options["zero_cube"];
	print_base = options["print_base"];
	print_cube = options["print_cube"];
	skip_cube_time = !options["cube_time"];
	skip_cube_host = !options["cube_host"];
	skip_cube_host_detail = !options["cube_host_detail"];
    }

    virtual ~HostInfo() { }

    static Rates *createRates() {
	return new Rates();
    }

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
    static string sqlHost(const Tuple &t, const Used &used) {
	if (used[host_index] == false) {
	    return str_null;
	} else {
	    return str(format("0x%08x") % host(t));
	}
    }

    static int32_t time(const Tuple &t) {
	return t.get<time_index>();
    }

    static string time(const Tuple &t, const Used &used,
		       const std::string &unused_str = str_star) {
	if (used[time_index] == false) {
	    return unused_str;
	} else {
	    return str(format("%d") % time(t));
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

    static string sqlify(const string &basestr) {
	if (basestr == str_star) {
	    return str_null;
	} else {
	    return str(format("'%s'") % basestr);
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

    static bool timeLessEqual(const PartialTuple<Tuple> &t, 
			      int32_t max_group_seconds) {
	if (t.used[time_index]) {
	    SINVARIANT(t.data.get<time_index>() <= max_group_seconds);
	
	    return true;
	} else {
	    return false;
	}
    }
									 
    void rollupOneGroup() {
	size_t memory_usage = base_data.memoryUsage()
	    + rates_cube.memoryUsage();
	if (zero_cube) {
	    base_data.fillHashUniqueTuple(unique_vals_tuple);
	    rates_cube.add(base_data, unique_vals_tuple);
	    unique_vals_tuple.get<time_index>().clear();
	} else {
	    rates_cube.add(base_data);
	}

	if (print_base && !zero_cube) {
	    base_data.walkOrdered
		(boost::bind(&HostInfo::printBaseIncremental, 
			     this, _1, _2));
	}
	base_data.clear();

	rates_cube.walk(boost::bind(&HostInfo::rateRollupAdd, 
				    this, _1, _2, _3));
	if (print_cube || zero_cube) {
	    rates_cube.walkOrdered
		(boost::bind(&HostInfo::printCubeIncrementalTime,
			     this, _1, _2, _3));
	}
	rates_cube.prune
	    (boost::bind(&HostInfo::timeLessEqual, _1, max_group_seconds));
	SINVARIANT(base_data.size() == 0);
	
	memory_usage += rate_hts.memoryUsage()
	    + Rates::memory_usage * rate_hts.size();
	if (memory_usage > (last_reported_memory_usage + 4*1024*1024)) {
	    LintelLogDebug("HostInfo",
			   format("# HostInfo-%ds: memory usage %d bytes @ %ds\n")
			   % group_seconds % memory_usage 
			   % (max_group_seconds - min_group_seconds));
	    last_reported_memory_usage = memory_usage;
	}
    }

    void processGroup(int32_t seconds) {
	if (max_group_seconds < min_group_seconds) {
	    SINVARIANT(base_data.size() == 0);
	    return; // nothing has been added yet.
	}
	if (seconds > min_group_seconds) {
	    ++group_count;
	}
	if (base_data.size() >= incremental_batch_size) {
	    LintelLogDebug
		("HostInfo", format("should incremental ]%d,%d[ bd=%d cube=%d ratescube=%d rate_hts=%d") 
		 % min_group_seconds % max_group_seconds % base_data.size()
		 % rates_cube.size() % rates_cube.size() % rate_hts.size());

	    rollupOneGroup();
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

	rollupOneGroup();
	if (print_cube) {
	    rates_cube.walkOrdered
		(boost::bind(&HostInfo::printCubeIncrementalNonTime,
			     this, _1, _2, _3));
	}
	
    }

    void nextSecondsGroup(int32_t next_group_seconds) {
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
    
    void printCubeIncremental(const Tuple &t, const bitset<5> &used, 
			      Stats *v) {
	if ((~used).none()) {
	    // all bits used
	    if (!zero_cube) {
		return;
	    } else {
		// zero cubing; we print "base" bits here to fill in the zeros
	    }
	} else {
	    if (!print_cube) {
		SINVARIANT(zero_cube);
		return;
	    }
	}
	
	if (sql_output) {
	    cout << format("insert into nfs_hostinfo_cube (group_seconds, host, group_time, direction, operation, op_dir, group_count, mean_payload_bytes) values (%d, %d, %d, %s, %s, %s, %d, %.20g);\n")
		% group_seconds % sqlHost(t, used) % time(t, used, str_null) 
		% sqlify(isSendStr(t, used)) % sqlify(operationStr(t, used)) 
		% sqlify(isRequestStr(t, used)) % v->countll() % v->mean();
	} else {
	    if (!printed_cube_header) {
		cout << format("HostInfo %ds cube: host     time        dir          op    op-dir   count mean-payload\n") % group_seconds;
		printed_cube_header = true;
	    }
	    cout << format("HostInfo %ds cube: %8s %10s %4s %12s %8s %6lld %8.2f\n")
		% group_seconds % host(t,used) % time(t,used) % isSendStr(t,used) 
		% operationStr(t,used) % isRequestStr(t,used) 
		% v->countll() % v->mean();
	}
    }

    void printCubeIncrementalTime(const Tuple &t, const bitset<5> &used, 
				  Stats *v) {
	if (used[time_index]) {
	    printCubeIncremental(t, used, v);
	}
    }

    void printCubeIncrementalNonTime(const Tuple &t, const bitset<5> &used, 
				     Stats *v) {
	if (!used[time_index]) {
	    printCubeIncremental(t, used, v);
	}
    }

    bool cubeOptional(const PartialTuple<Tuple> &partial) {
	if (skip_cube_time && partial.used[time_index] == true) {
	    return false;
	}
	if (skip_cube_host && partial.used[host_index] == true) {
	    return false;
	}
	if (skip_cube_host_detail && partial.used[host_index] == true) {
	    PartialTuple<Tuple>::UsedT used = partial.used;
	    used[host_index] = false;
	    if (used.any()) {
		return false;
	    }
	}
	return true;
    }

    static bool cubeExceptTime(const PartialTuple<Tuple> &partial) {
	return partial.used[time_index] == false;
    }

    static bool cubeExceptHost(const PartialTuple<Tuple> &partial) {
	return partial.used[host_index] == false;
    }

    static bool cubeExceptTimeOrHost(const PartialTuple<Tuple> &partial) {
	return partial.used[time_index] == false 
	    && partial.used[host_index] == false;
    }

    static double afs_count;

    static void addFullStats(Stats &into, const Stats &val) {
	into.add(val);
	++afs_count;
    }

    void configCube() {
	using boost::bind; 

	if (options["cube_time"] && options["cube_host"]
	    && options["cube_host_detail"]) {
	    rates_cube.setOptionalCubePartialFn(bind(&StatsCubeFns::cubeAll));
	} else {
	    rates_cube.setOptionalCubePartialFn
		(bind(&HostInfo::cubeOptional, this, _2));
	}

	rates_cube.setCubeStatsAddFn
	    (bind(&StatsCubeFns::addFullStats, _1, _2));
    }

    //                    host  is_send operation is_req
    typedef boost::tuple<int32_t, bool, uint8_t, bool> RateRollupTupleBase;
    typedef TupleToAnyTuple<RateRollupTupleBase>::type RateRollupTuple;

    void rateRollupAdd(const Tuple &t, const bitset<5> &used, Stats *v) {
	if (!used[time_index]) {
	    return; // Ignore unless time is set, this is all we will prune.
	}
	if (t.get<time_index>() == min_group_seconds ||
	    t.get<time_index>() == max_group_seconds) {
	    return;
	}
	// TODO: don't we need to skip the max_group_seconds also once we
	// have gotten to the max, i.e. when we do this under finalGroup?
	// we don't but only because we're skipping cubing the last bit of
	// the data, which will make us wrong in the cube output.
	INVARIANT(t.get<time_index>() > min_group_seconds &&
		  t.get<time_index>() < max_group_seconds,
		  format("%d \not in ]%d, %d[") % t.get<time_index>() 
		  % min_group_seconds % max_group_seconds); 
	if (false) {
	    cout << format("%8s %10s %4s %12s %8s %6lld %8.2f\n")
		% host(t,used) % time(t,used) % isSendStr(t,used) 
		% operationStr(t,used) % isRequestStr(t,used) 
		% v->countll() % v->mean();
	}

	RateRollupTuple rrt;
	if (used[host_index]) {
	    rrt.get<0>().set(t.get<host_index>());
	}
	if (used[is_send_index]) {
	    rrt.get<1>().set(t.get<is_send_index>());
	}
	if (used[operation_index]) {
	    rrt.get<2>().set(t.get<operation_index>());
	}
	if (used[is_request_index]) {
	    rrt.get<3>().set(t.get<is_request_index>());
	}

	double count = v->countll();
	double ops_per_sec = count / group_seconds;
	double bytes_per_sec = (count * v->mean()) / group_seconds;
	
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
	    cout << format("insert into nfs_hostinfo_rates (group_seconds, host, direction, operation, op_dir, mean_operations_per_second, mean_payload_bytes_per_second) values (%d, %s, %s, %s, %s, %.20g, %.20g);\n")
		% group_seconds % hostStr(t, true) % sqlify(isSendStr(t))
		% sqlify(operationStr(t)) % sqlify(isRequestStr(t))
		% v.ops_rate.mean() % v.bytes_rate.mean();
	    
	    if (print_rates_quantiles) {
		static const double quantile_step = 0.01;
		cout << format("insert into nfs_hostinfo_rate_quantiles (group_seconds, host, direction, operation, op_dir, quantile, operations_per_second, payload_bytes_per_second) values ");
		for(double q = 0; q < 1.0000000001; q += quantile_step) {
		    if (q > 0) { cout << ", "; }
		    if (q > 1) { q = 1; } // doubles can have slight overflow
		    cout << format("(%d, %s, %s, %s, %s, %.5f, %.20g, %.20g)")
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
	PartialTuple<Tuple> tmp(Tuple(0,true,0,0,true));
	tmp.used[is_send_index] = true;
	Stats &v = rates_cube.getPartialEntry(tmp);

	INVARIANT(v.countll() == payload_overall.countll(),
		  format("%d != %d") % v.countll() 
		  % payload_overall.countll());
	INVARIANT(Double::eq(v.mean(), payload_overall.mean()),
		  format("%.20g != %.20g") 
		  % v.mean() % payload_overall.mean());
	INVARIANT(Double::eq(v.stddev(), payload_overall.stddev()),
		  format("%.20g != %.20g") % v.stddev() 
		  % payload_overall.stddev());
	    
	tmp.used[is_send_index] = false;
	Stats &w = rates_cube.getPartialEntry(tmp);
	// Complete rollup sees everything twice since we add it as
	// both send and receive
	SINVARIANT(w.countll() == 2*payload_overall.countll()
		   && Double::eq(w.mean(), payload_overall.mean())
		   && Double::eq(w.stddev(), payload_overall.stddev()));

	// Check that we have the right number of entries in the
	// rollups of the rates table.

	if (group_count > 0) {
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

	LintelLogDebug("HostInfo", 
		       format("Actual afs_count = %.0f\n") % afs_count);
	
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
    int32_t min_group_seconds, max_group_seconds;
    int64_t group_count;

    bool printed_base_header, printed_cube_header, printed_rates_header;

    bool print_rates_quantiles, sql_output, zero_cube, print_base, print_cube;
    uint32_t zero_groups;
    
    bool skip_cube_time, skip_cube_host, skip_cube_host_detail;
    size_t last_reported_memory_usage;
};

double HostInfo::afs_count;

RowAnalysisModule *
NFSDSAnalysisMod::newHostInfo(DataSeriesModule &prev, char *arg) {
    return new HostInfo(prev, arg);
}

size_t HostInfo::Rates::memory_usage;
