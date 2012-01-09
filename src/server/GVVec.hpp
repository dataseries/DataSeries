#ifndef DATASERIES_GVVEC_HPP
#define DATASERIES_GVVEC_HPP

#include <boost/foreach.hpp>

#include <Lintel/STLUtility.hpp>

#include <DataSeries/GeneralField.hpp>

struct GVVec {
    std::vector<GeneralValue> vec;

    bool operator ==(const GVVec &rhs) const {
        return lintel::iteratorRangeEqual(vec.begin(), vec.end(), rhs.vec.begin(), rhs.vec.end());
    }

    uint32_t hash() const {
        uint32_t partial_hash = 1942;

        BOOST_FOREACH(const GeneralValue &gv, vec) {
            partial_hash = gv.hash(partial_hash);
        }
        return partial_hash;
    }

    void print (std::ostream &to) const {
        BOOST_FOREACH(const GeneralValue &gv, vec) {
            to << gv;
        }
    }

    void resize(size_t size) {
        vec.resize(size);
    }

    size_t size() const {
        return vec.size();
    }

    GeneralValue &operator [](size_t offset) {
        return vec[offset];
    }
    
    void extract(std::vector<GeneralField::Ptr> &fields) {
        SINVARIANT(fields.size() == vec.size());
        for (uint32_t i = 0; i < fields.size(); ++i) {
            vec[i].set(fields[i]);
        }
    }
};

inline std::ostream & operator << (std::ostream &to, GVVec &gvvec) {
    gvvec.print(to);
    return to;
}

#endif
