/*
   (c) Copyright 2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <iostream>

#include <boost/format.hpp>

#include <Lintel/MersenneTwisterRandom.hpp>

#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>

using namespace std;
using namespace dataseries;
using boost::format;

const string fixed_width_types_xml = 
"<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"fixed-width-types\" version=\"1.0\" >\n"
"  <field type=\"bool\" name=\"bool\" />\n"
"  <field type=\"byte\" name=\"byte\" />\n"
"  <field type=\"int32\" name=\"int32\" />\n"
"  <field type=\"int64\" name=\"int64\" />\n"
"  <field type=\"double\" name=\"double\" />\n"
"  <field type=\"variable32\" name=\"variable32\" />\n"
"  <field type=\"fixedwidth\" name=\"fw7\" size=\"7\" />\n"
"  <field type=\"fixedwidth\" name=\"fw20\" size=\"20\" />\n"

"  <field type=\"bool\" name=\"n-bool\" opt_nullable=\"yes\" />\n"
"  <field type=\"byte\" name=\"n-byte\" opt_nullable=\"yes\" />\n"
"  <field type=\"int32\" name=\"n-int32\" opt_nullable=\"yes\" />\n"
"  <field type=\"int64\" name=\"n-int64\" opt_nullable=\"yes\" />\n"
"  <field type=\"double\" name=\"n-double\" opt_nullable=\"yes\" />\n"
"  <field type=\"variable32\" name=\"n-variable32\" opt_nullable=\"yes\" />\n"
"  <field type=\"fixedwidth\" name=\"n-fw13\" size=\"13\" opt_nullable=\"yes\" />\n"
"</ExtentType>\n";

template<class T>
class NullableField : public T {
public:
    NullableField(ExtentSeries &series, const std::string &field)
        : T(series, field, Field::flag_nullable)
    { }
};

template<typename T, typename FT> T randomVal(FT &field, MersenneTwisterRandom &rng) {
    return static_cast<T>(rng.randInt());
}

template<> string randomVal(Variable32Field &field, MersenneTwisterRandom &rng) {
    string ret;
    ret.resize(rng.randInt(256));
    for (size_t i=0; i < ret.size(); ++i) {
        ret[i] = rng.randInt(256);
    }
    return ret;
}

template<> string randomVal(NullableField<Variable32Field> &field, MersenneTwisterRandom &rng) {
    string ret;
    ret.resize(rng.randInt(256));
    for (size_t i=0; i < ret.size(); ++i) {
        ret[i] = rng.randInt(256);
    }
    return ret;
}

template<> vector<uint8_t> randomVal(FixedWidthField &field, MersenneTwisterRandom &rng) {
    vector<uint8_t> ret;
    ret.resize(field.size());
    for (size_t i=0; i < ret.size(); ++i) {
        ret[i] = rng.randInt(256);
    }
    return ret;
}

template<> vector<uint8_t> 
randomVal(NullableField<FixedWidthField> &field, MersenneTwisterRandom &rng) {
    vector<uint8_t> ret;
    ret.resize(field.size());
    for (size_t i=0; i < ret.size(); ++i) {
        ret[i] = rng.randInt(256);
    }
    return ret;
}

template<typename T, typename FT> void 
fillSEP_RowOffset(ExtentSeries &s, FT &field, vector<SEP_RowOffset> &o,
                  vector<T> &r, bool nullable) {
    MersenneTwisterRandom rng;

    for (uint32_t i = 0; i < 1000; ++i) {
        s.newRecord();
        T val = randomVal<T>(field, rng);
        if (nullable && rng.randInt(4) == 0) {
            val = T();
            field.setNull();
        } else {
            field.set(val);
        }
        o.push_back(s.getRowOffset());
        r.push_back(val);
    }
}

template<typename T> T transform(const T &update, const T &offset) {
    return update + offset;
}

template<> bool transform(const bool &update, const bool &offset) {
    return update ^ offset;
}

template<> vector<uint8_t> transform(const vector<uint8_t> &update, const vector<uint8_t> &offset) {
    vector<uint8_t> ret(update);
    if (update.empty()) {
        ret = offset;
    } else {
        SINVARIANT(update.size() == offset.size());
        for (size_t i=0;i < update.size(); ++i) {
            ret[i] += offset[i];
        }
    }
    return ret;
}

// TODO: add field::value_type so that we can write FT::value_type getVal(...)
// will also remove the need to write getVal<type>
template<typename T, typename FT> T getVal(const FT &field, Extent &e, const SEP_RowOffset offset) {
    return field.val(e, offset);
}

template<typename T, typename FT> T getOp(const FT &field, Extent &e, const SEP_RowOffset offset) {
    return field(e, offset);
}

template<> string getVal(const Variable32Field &field, Extent &e, const SEP_RowOffset offset) {
    return field.stringval(e, offset);
}

template<> string getOp(const Variable32Field &field, Extent &e, const SEP_RowOffset offset) {
    return string(reinterpret_cast<const char *>(field(e, offset)), field.size(e, offset));
}

template<> string 
getVal(const NullableField<Variable32Field> &field, Extent &e, const SEP_RowOffset offset) {
    return field.stringval(e, offset);
}

template<> string 
getOp(const NullableField<Variable32Field> &field, Extent &e, const SEP_RowOffset offset) {
    return string(reinterpret_cast<const char *>(field(e, offset)), field.size(e, offset));
}

template<> vector<uint8_t>
getVal(const FixedWidthField &field, Extent &e, const SEP_RowOffset offset) {
    return field.arrayVal(e, offset);
}

template<> vector<uint8_t>
getOp(const FixedWidthField &field, Extent &e, const SEP_RowOffset offset) {
    return vector<uint8_t>(field.val(e, offset), field.val(e, offset) + field.size());
}

template<> vector<uint8_t>
getVal(const NullableField<FixedWidthField> &field, Extent &e, const SEP_RowOffset offset) {
    return field.arrayVal(e, offset);
}

template<> vector<uint8_t>
getOp(const NullableField<FixedWidthField> &field, Extent &e, const SEP_RowOffset offset) {
    if (field.isNull(e, offset)) {
        return vector<uint8_t>();
    } else {
        return vector<uint8_t>(field.val(e, offset), field.val(e, offset) + field.size());
    }
}

template<typename FT, typename T> void 
updateVal(MersenneTwisterRandom &rng, FT &field, Extent &e, const SEP_RowOffset offset, 
          vector<T> &update, size_t i, bool nullable) {
    if (nullable && rng.randInt(2) == 0 && !field.isNull(e, offset)) { 
        // 50% chance to swap from not-null to null
        field.setNull(e, offset);
        update[i] = T();
    } else {
        T update_by = randomVal<T>(field, rng);
    
        T update_val = update[i]; // hack round vector<bool>[i] != bool type
        update[i] = transform(update_val, update_by);
        if (rng.randInt(2) == 0) {
            field.set(e, offset, transform(getOp<T>(field, e, offset), update_by));
        } else {
            field.set(e, offset, transform(getVal<T>(field, e, offset), update_by));
        }
    }
}

namespace std {
ostream &operator <<(ostream &to, const vector<uint8_t> &v) {
    to << "(unimplemented)";
    return to;
}
}

template<typename T, typename FT> void testOneSEP_RowOffset(const string &field_name) {
    cout << format("testing on field %s\n") % field_name;
    ExtentTypeLibrary lib;
    const ExtentType &t(lib.registerTypeR(fixed_width_types_xml));

    ExtentSeries s;
    // TODO-eric: make test with these being const.
    Extent e1(t), e2(t);
    vector<SEP_RowOffset> o1, o2;
    vector<T> r1, r2;

    FT field(s, field_name);

    bool nullable = t.getNullable(field_name);

    s.setExtent(e1);
    SEP_RowOffset first_e1(s.getRowOffset());
    fillSEP_RowOffset(s, field, o1, r1, nullable);
    s.setExtent(e2);
    fillSEP_RowOffset(s, field, o2, r2, nullable);
    s.clearExtent(); // leaves the type alone

    SINVARIANT(r1.size() == r2.size() && !r1.empty());

    SEP_RowOffset mid_e1(first_e1, r1.size()/2, e1), last_e1(first_e1, r1.size(), e1);

    SINVARIANT(first_e1 != mid_e1 && mid_e1 != last_e1 && first_e1 != last_e1);
    MersenneTwisterRandom rng;

    // uses both the field.get() and field() variants to test both, normally this would be
    // weird style.
    for (size_t i = 0; i < r1.size(); ++i) {
        INVARIANT(getVal<T>(field, e1, o1[i]) == r1[i],
                  format("%s != %s") % getVal<T>(field, e1, o1[i]) % r1[i]);
        SINVARIANT(getOp<T>(field, e2, o2[i]) == r2[i]);

        updateVal(rng, field, e1, o1[i], r1, i, nullable);
        updateVal(rng, field, e2, o2[i], r2, i, nullable);

        SINVARIANT(first_e1 <= o1[i] && o1[i] < last_e1);

        SINVARIANT((first_e1 == o1[i]) == (i == 0));
        SINVARIANT((first_e1 == o1[i]) != (first_e1 != o1[i]));

        SINVARIANT((mid_e1 < o1[i] || mid_e1 == o1[i]) == (mid_e1 <= o1[i]));
        SINVARIANT((mid_e1 > o1[i] || mid_e1 == o1[i]) == (mid_e1 >= o1[i]));

        SINVARIANT((mid_e1 < o1[i]) == (o1[i] > mid_e1));
        SINVARIANT((mid_e1 <= o1[i]) == (o1[i] >= mid_e1));

        if (mid_e1 == o1[i]) {
            SINVARIANT(!(mid_e1 < o1[i]) && !(mid_e1 > o1[i]));
        } else {
            SINVARIANT((mid_e1 < o1[i]) != (mid_e1 > o1[i]));
        }
    }

    // Verify all the updates happened.
    for (size_t i = 0; i < r1.size(); ++i) {
        INVARIANT(getOp<T>(field, e1, o1[i]) == r1[i], 
                  format("%d: %s != %s") % i % getOp<T>(field, e1, o1[i]) % r1[i]);
        INVARIANT(getVal<T>(field, e2, o2[i]) == r2[i],
                  format("%d: %s != %s") % i % getOp<T>(field, e2, o2[i]) % r2[i]);
    }
}
       
template<typename T> void testOneSEP_RowOffset(const string &field_name) {
    testOneSEP_RowOffset<T, TFixedField<T> >(field_name);
}


void testSEP_RowOffset() {
    // Template fields
    testOneSEP_RowOffset<uint8_t>("byte");
    testOneSEP_RowOffset<int32_t>("int32");
    testOneSEP_RowOffset<int64_t>("int64");
    testOneSEP_RowOffset<double>("double");

    // Normal fields
    testOneSEP_RowOffset<bool, BoolField>("bool");
    testOneSEP_RowOffset<ExtentType::byte, ByteField>("byte");
    testOneSEP_RowOffset<ExtentType::int32, Int32Field>("int32");
    testOneSEP_RowOffset<ExtentType::int64, Int64Field>("int64");
    testOneSEP_RowOffset<double, DoubleField>("double");
    testOneSEP_RowOffset<string, Variable32Field>("variable32");
    testOneSEP_RowOffset<vector<uint8_t>, FixedWidthField>("fw7");
    testOneSEP_RowOffset<vector<uint8_t>, FixedWidthField>("fw20");

    // Nullable fields
    testOneSEP_RowOffset<bool, NullableField<BoolField> >("n-bool");
    testOneSEP_RowOffset<ExtentType::byte, NullableField<ByteField> >("n-byte");
    testOneSEP_RowOffset<ExtentType::int32, NullableField<Int32Field> >("n-int32");
    testOneSEP_RowOffset<ExtentType::int64, NullableField<Int64Field> >("n-int64");
    testOneSEP_RowOffset<ExtentType::int32, NullableField<DoubleField> >("n-double");
    testOneSEP_RowOffset<string, NullableField<Variable32Field> >("n-variable32");
    testOneSEP_RowOffset<vector<uint8_t>, NullableField<FixedWidthField> >("n-fw13");
}

int main() {
    dataseries::checkSubExtentPointerSizes();
    testSEP_RowOffset();
    return 0;
}
