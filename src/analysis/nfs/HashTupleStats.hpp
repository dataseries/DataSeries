/* -*-C++-*-
   (c) Copyright 2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#ifndef TMP_HASH_TUPLE_STATS_HPP
#define TMP_HASH_TUPLE_STATS_HPP

#include <boost/bind.hpp>

#include <Lintel/HashMap.hpp>
#include <Lintel/HashUnique.hpp>
#include <Lintel/LintelLog.hpp>

#include <analysis/nfs/Tuples.hpp>

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

template<class T0, class T1>
struct ConsToHashUniqueCons {
    typedef ConsToHashUniqueCons<typename T1::head_type, 
				 typename T1::tail_type> tail;
    typedef boost::tuples::cons<HashUnique<T0>, typename tail::type> type;
};

template<class T0>
struct ConsToHashUniqueCons<T0, boost::tuples::null_type> {
    typedef boost::tuples::cons<HashUnique<T0>, boost::tuples::null_type> type;
};

template<class T>
struct TupleToHashUniqueTuple 
  : ConsToHashUniqueCons<typename T::head_type, typename T::tail_type> {
};

template<class Tuple, class StatsT = Stats>
class HashTupleStats {
public:
    // base types
    typedef HashMap<Tuple, StatsT *, TupleHash<Tuple> > HTSMap;
    typedef typename HTSMap::iterator HTSiterator;
    typedef typename HTSMap::const_iterator HTSconst_iterator;
    typedef std::vector<typename HTSMap::value_type> HTSValueVector;
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
		       boost::format("Expecting to cube %.6g * 2^%d = %.0f")
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

    size_t size() const {
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

#endif
