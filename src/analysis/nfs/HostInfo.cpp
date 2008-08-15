#include <vector>
#include <bitset>

#include <boost/bind.hpp>
#include <boost/tuple/tuple_comparison.hpp>
#include <boost/tuple/tuple_io.hpp>

#include <analysis/nfs/common.hpp>
#include <analysis/nfs/HostInfo.hpp>

#include <Lintel/HashMap.hpp>

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

template<class Tuple>
class StatsCube {
public:
    typedef HashMap<Tuple, Stats *, TupleHash<Tuple> > CubeMap;
    typedef typename CubeMap::iterator CMIterator;

    typedef vector<typename CubeMap::value_type> CMValueVector;
    typedef typename CMValueVector::iterator CMVVIterator;

    typedef PartialTuple<Tuple> MyPartial;
    typedef HashMap<MyPartial, Stats *, PartialTupleHash<Tuple> > 
        PartialTupleCubeMap;
    typedef typename PartialTupleCubeMap::iterator PTCMIterator;
    typedef vector<typename PartialTupleCubeMap::value_type> PTCMValueVector;
    typedef typename PTCMValueVector::iterator PTCMVVIterator;

    typedef boost::function<Stats *()> StatsFactoryFn;
    typedef boost::function<void (Tuple &key, Stats *value)> PrintBaseFn;
    typedef boost::function<void (Tuple &key, typename MyPartial::UsedT, 
				  Stats *value)> PrintCubeFn;

    StatsCube(const StatsFactoryFn fn) : stats_factory_fn(fn) { }

    void add(const Tuple &key, const Stats &value) {
	getCubeEntry(key).add(value);
    }

    void add(const Tuple &key, double value) {
	getCubeEntry(key).add(value);
    }

    void cube() {
	for(CMIterator i = base_data.begin(); i != base_data.end(); ++i) {
	    MyPartial tmp_key(i->first);
	    cubeAddOne(tmp_key, 0, false, *i->second);
	}
    }

    void cubeAddOne(MyPartial &key, size_t pos, bool had_false, 
		    const Stats &value) {
	if (pos == key.length) {
	    if (had_false) {
		getPartialEntry(key).add(value);
	    }
	} else {
	    DEBUG_SINVARIANT(pos < key.length);
	    key.used[pos] = true;
	    cubeAddOne(key, pos + 1, had_false, value);
	    key.used[pos] = false;
	    cubeAddOne(key, pos + 1, true, value);
	}
    }

    void printBase(const PrintBaseFn fn) {
	CMValueVector sorted;

	// TODO: figure out why the below doesn't work.
	//	sorted.push_back(base_data.begin(), base_data.end());
	for(CMIterator i = base_data.begin(); i != base_data.end(); ++i) {
	    sorted.push_back(*i);
	}
	sort(sorted.begin(), sorted.end());

	for(CMVVIterator i = sorted.begin(); i != sorted.end(); ++i) {
	    fn(i->first, i->second);
	}
    }

    void printCube(const PrintCubeFn fn) {
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
    Stats &getCubeEntry(const Tuple &key) {
	Stats * &v = base_data[key];

	if (v == NULL) {
	    v = stats_factory_fn();
	}
	return *v;
    }

    Stats &getPartialEntry(const MyPartial &key) {
	Stats * &v = cube_data[key];

	if (v == NULL) {
	    v = stats_factory_fn();
	}
	return *v;
    }

    StatsFactoryFn stats_factory_fn;
    CubeMap base_data;
    PartialTupleCubeMap cube_data;
    uint32_t foo;
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
	  is_request(series, ""),
	  cube(boost::bind(&HostInfo::makeStats))
    {
	group_seconds = stringToUInt32(arg);
    }

    virtual ~HostInfo() { }

    //                   host,   is_send, seconds, operation, is_request
    typedef boost::tuple<int32_t, bool,   int32_t,  uint8_t,    bool> Tuple;
    typedef bitset<5> Used;
    static int32_t host(const Tuple &t) {
	return t.get<0>();
    }
    static string host(const Tuple &t, const Used &used) {
	if (used[0] == false) {
	    return str_star;
	} else {
	    return str(format("%08x") % host(t));
	}
    }
    static bool isSend(const Tuple &t) {
	return t.get<1>();
    }
    static const string &isSendStr(const Tuple &t) {
	return isSend(t) ? str_send : str_recv;
    }

    static string isSendStr(const Tuple &t, const Used &used) {
	if (used[1] == false) {
	    return str_star;
	} else {
	    return isSendStr(t);
	}
    }

    static int32_t seconds(const Tuple &t) {
	return t.get<2>();
    }

    static string seconds(const Tuple &t, const Used &used) {
	if (used[2] == false) {
	    return str_star;
	} else {
	    return str(format("%d") % seconds(t));
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
	cube.add(cube_key, payload_length.val());
	// reciever...
	cube_key.get<0>() = dest_ip.val();
	cube_key.get<1>() = false;
	cube.add(cube_key, payload_length.val());
    }

    static void printFullRow(const Tuple &t, Stats *v) {
	cout << format("%08x %10d %s %12s %8s %6lld %8.2f\n")
	    % host(t) % seconds(t) % isSendStr(t) % operationStr(t) 
	    % isRequestStr(t) % v->countll() % v->mean();
    }	
    
    static void printPartialRow(const Tuple &t, const bitset<5> &used, 
				Stats *v) {
	cout << format("%8s %10s %4s %12s %8s %6lld %8.2f\n")
	    % host(t,used) % seconds(t,used) % isSendStr(t,used) 
	    % operationStr(t,used) % isRequestStr(t,used) 
	    % v->countll() % v->mean();
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	cout << "host     time        dir          op    op-dir   count mean-payload\n";
	cube.cube();
	cube.printBase(boost::bind(&HostInfo::printFullRow, _1, _2));
	cube.printCube(boost::bind(&HostInfo::printPartialRow, _1, _2, _3));
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

    StatsCube<Tuple> cube;
};

RowAnalysisModule *
NFSDSAnalysisMod::newHostInfo(DataSeriesModule &prev, char *arg) {
    return new HostInfo(prev, arg);
}

