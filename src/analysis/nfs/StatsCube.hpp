/* -*-C++-*-
   (c) Copyright 2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#ifndef TMP_STATS_CUBE_HPP
#define TMP_STATS_CUBE_HPP

#include <analysis/nfs/HashTupleStats.hpp>

template<class Tuple, class StatsT = Stats>
class StatsCube {
public:
    // partial tuple types
    typedef PartialTuple<Tuple> MyPartial;
    typedef HashMap<MyPartial, StatsT *, PartialTupleHash<Tuple> > 
        PartialTupleCubeMap;
    typedef typename PartialTupleCubeMap::iterator PTCMIterator;
    typedef std::vector<typename PartialTupleCubeMap::value_type> PTCMValueVector;
    typedef typename PTCMValueVector::iterator PTCMVVIterator;

    // functions controlling cubing...
    typedef boost::function<StatsT *()> StatsFactoryFn;
    // TODO: constify key references
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

#endif
