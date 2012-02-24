#define DSM_DEPRECATED /* allow */
#include <iostream>

#include <boost/foreach.hpp>

#include <Lintel/MersenneTwisterRandom.hpp>
#include <Lintel/TestUtil.hpp>

#include <DataSeries/Extent.hpp>
#include <DataSeries/SequenceModule.hpp>

using namespace std;
using namespace boost;

namespace dataseries { namespace hack {
    Extent *releaseExtentSharedPtr(boost::shared_ptr<Extent> &p, Extent *e);
    size_t extentSharedPtrSize();
}}

using namespace dataseries::hack;

string extent_type
("<ExtentType name=\"test\" namespace=\"who-cares\" version=\"0.0\">\n"
 "  <field type=\"int32\" name=\"test\" />\n"
 "</ExtentType>\n");

void testConvertValid() {
    cout << "testing convert-valid...";
    ExtentTypeLibrary lib;
    const ExtentType &type(lib.registerTypeR(extent_type));

    // raw -> smart -> raw
    Extent *e = new Extent(type);
    boost::shared_ptr<Extent> se(e);
    SINVARIANT(sizeof(se) == extentSharedPtrSize());
    Extent *f = releaseExtentSharedPtr(se, se.get());
    SINVARIANT(e->extent_source_offset == -1); 
    SINVARIANT(e == f);
    SINVARIANT(se.use_count() == 0);
    SINVARIANT(se == NULL);

    // smart -> raw -> smart
    se.reset(new Extent(type));
    SINVARIANT(se.get() != e);
    Extent *g = se.get();
    Extent *h = releaseExtentSharedPtr(se, se.get());
    SINVARIANT(g == h);
    SINVARIANT(se.use_count() == 0);
    SINVARIANT(se == NULL);
    se.reset(g);

    SINVARIANT(e->extent_source_offset == -1); 
    SINVARIANT(g->extent_source_offset == -1);
    delete e;
    se.reset();
    SINVARIANT(e->extent_source_offset == -2); // Read of freed memory!
    SINVARIANT(g->extent_source_offset == -2); // Read of freed memory!
    
    cout << "passed.\n";
}

void testConvertInvalid() {
    cout << "testing convert-invalid...";
    ExtentTypeLibrary lib;
    const ExtentType &type(lib.registerTypeR(extent_type));

    boost::shared_ptr<Extent> se1(new Extent(type));
    boost::shared_ptr<Extent> se2 = se1;
    TEST_INVARIANT_MSG1(releaseExtentSharedPtr(se2, se2.get()),
                        "Attempting to convert a shared pointer back to a native"
                        " pointer only works with use count 1; likely you need getExtentShared()");
    cout << "passed.\n";
}

class FakeSourceBare : public DataSeriesModule {
public:
    FakeSourceBare(uint32_t num) : num(num), lib(), type(lib.registerTypeR(extent_type)) { }

    virtual Extent *getExtent() {
        if (num == 0) {
            return NULL;
        } else {
            --num;
            return new Extent(type);
        }
    }

    uint32_t num;
    ExtentTypeLibrary lib;
    const ExtentType &type;
};

class FakeSourceShared : public DataSeriesModule {
public:
    FakeSourceShared(uint32_t num) : num(num), lib(), type(lib.registerTypeR(extent_type)) { }

    virtual Extent::Ptr getSharedExtent() {
        if (num == 0) {
            return Extent::Ptr();
        } else {
            --num;
            Extent::Ptr ret(new Extent(type));
            ret->extent_source_offset = num;
            return ret;
        }
    }

    uint32_t num;
    ExtentTypeLibrary lib;
    const ExtentType &type;
};

class PassThroughBare : public DataSeriesModule {
public:
    PassThroughBare(DataSeriesModule &src) : src(src) { }

    virtual Extent *getExtent() {
        return src.getExtent();
    }

    DataSeriesModule &src;
};

class PassThroughShared : public DataSeriesModule {
public:
    PassThroughShared(DataSeriesModule &src) : src(src) { }

    virtual Extent::Ptr getSharedExtent() {
        return src.getSharedExtent();
    }

    DataSeriesModule &src;
};

class KeeperShared : public DataSeriesModule {
public:
    KeeperShared(DataSeriesModule &src) : src(src) { }

    virtual Extent::Ptr getSharedExtent() {
        Extent::Ptr e(src.getSharedExtent());
        if (e != NULL) {
            vec.push_back(e);
        }
        return e;
    }

    vector<Extent::Ptr> vec;
    DataSeriesModule &src;
};

void addRandomPassthrough(SequenceModule &s, MersenneTwisterRandom &rng, uint32_t n) {
    for (; n > 0; --n) {
        if (rng.randBool()) {
            s.addModule(new PassThroughBare(s.tail()));
        } else {
            s.addModule(new PassThroughShared(s.tail()));
        }
    }
}

void testPipelines() {
    SequenceModule seq1(new FakeSourceBare(10)), seq2(new FakeSourceShared(10));

    MersenneTwisterRandom rng;
    cout << format("pipeline testing with seed %d\n") % rng.seed_used;
    seq1.addModule(new PassThroughShared(seq1.tail()));
    seq1.addModule(new PassThroughBare(seq1.tail()));
    seq1.addModule(new PassThroughShared(seq1.tail()));
    seq1.addModule(new PassThroughBare(seq1.tail()));

    addRandomPassthrough(seq1, rng, 20);
    addRandomPassthrough(seq2, rng, 20);

    cout << "  bare source...";
    seq1.getAndDelete();
    cout << "passed.\n  shared source...";
    seq2.getAndDeleteShared();
    cout << "passed.\n";
    
    SequenceModule seq3(new FakeSourceShared(10));
    vector< shared_ptr<KeeperShared> > keepers;
    for (uint32_t i = 0; i < 10;++i) {
        shared_ptr<KeeperShared> k(new KeeperShared(seq3.tail()));
        seq3.addModule(k);
        keepers.push_back(k);
        seq3.addModule(new PassThroughShared(seq3.tail()));
    }

    cout << "  keeper...";
    seq3.getAndDeleteShared();

    SINVARIANT(keepers[0]->vec.size() == 10);
    for (uint32_t i = 0; i < 10; ++i) {
        SINVARIANT(keepers[0]->vec[i]->extent_source_offset == (9 - i));
    }
    BOOST_FOREACH(shared_ptr<KeeperShared> p, keepers) {
        SINVARIANT(p->vec.size() == 10);
        SINVARIANT(p->vec == keepers[0]->vec);
    }
    cout << "passed.\n";
}

int main(int, char **) {
    testConvertValid();
    testConvertInvalid();
    testPipelines();
    return 0;
}

    
