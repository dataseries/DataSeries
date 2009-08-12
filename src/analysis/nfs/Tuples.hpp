/*
   (c) Copyright 2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#ifndef TMP_TUPLES_HPP
#define TMP_TUPLES_HPP

#include <bitset>

#include <boost/tuple/tuple_comparison.hpp>
#include <boost/tuple/tuple_io.hpp>

#include <Lintel/HashTable.hpp>

namespace boost { namespace tuples {
    inline uint32_t hash(const null_type &) { return 0; }
    inline uint32_t hash(const bool a) { return a; }
    inline uint32_t hash(const int32_t a) { return a; }
    inline uint64_t hash(const uint64_t a) { return lintel::BobJenkinsHashMixULL(a); }

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
	return lintel::BobJenkinsHashMix3(a,b,c);
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
	return lintel::BobJenkinsHashMix3(a,b,c);
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

template<class Tuple> struct PartialTuple {
    BOOST_STATIC_CONSTANT(uint32_t, 
			  length = boost::tuples::length<Tuple>::value);

    Tuple data;
    typedef std::bitset<boost::tuples::length<Tuple>::value> UsedT;
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

#endif
