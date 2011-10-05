/*
   (c) Copyright 2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <iostream>

#include <boost/format.hpp>

#include <Lintel/MersenneTwisterRandom.hpp>

#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>
#include <DataSeries/GeneralField.hpp>

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

class GFWrapper : boost::noncopyable {
public:
    GFWrapper(ExtentSeries &series, const std::string &column) 
        : gf(GeneralField::create(series, column)) 
    { }

    ~GFWrapper() {
        delete gf;
    }

    void setNull() {
        gf->setNull();
    }

    void setNull(Extent &e, const SEP_RowOffset &offset) {
        gf->setNull(e, offset);
    }

    bool isNull() {
        return gf->isNull();
    }

    bool isNull(Extent &e, const SEP_RowOffset &offset) {
        return gf->isNull(e, offset);
    }

    void set(const GeneralValue &v) {
        gf->set(v);
    }

    void set(Extent &e, const SEP_RowOffset &offset, const GeneralValue &v) {
        gf->set(e, offset, v);
    }

    GeneralValue val() const {
        return gf->val();
    }

    GeneralValue operator()() const {
        return val();
    }

    GeneralValue val(const Extent &e, const SEP_RowOffset &offset) const {
        return gf->val(e, offset);
    }

    GeneralValue operator()(const Extent &e, const SEP_RowOffset &offset) const {
        return val(e, offset);
    }

    ExtentType::fieldType getType() {
        return gf->getType();
    }

    int32_t fixedWidthSize() {
        GF_FixedWidth *fw = reinterpret_cast<GF_FixedWidth *>(gf);
        SINVARIANT(fw != NULL);
        return fw->size();
    }
private:
    GeneralField *gf;
};

template<typename T, typename FT> T randomVal(FT &field, MersenneTwisterRandom &rng) {
    return static_cast<T>(rng.randInt());
}

string randomString(MersenneTwisterRandom &rng, size_t size) {
    string ret;
    ret.resize(size);
    for (size_t i=0; i < ret.size(); ++i) {
        ret[i] = rng.randInt(256);
    }
    return ret;
}

template<> string randomVal(Variable32Field &field, MersenneTwisterRandom &rng) {
    return randomString(rng, rng.randInt(256));
}

template<> string randomVal(NullableField<Variable32Field> &field, MersenneTwisterRandom &rng) {
    return randomString(rng, rng.randInt(256));
}

vector<uint8_t> randomArray(MersenneTwisterRandom &rng, size_t size) {
    vector<uint8_t> ret;
    ret.resize(size);
    for (size_t i=0; i < ret.size(); ++i) {
        ret[i] = rng.randInt(256);
    }
    return ret;
}

template<> vector<uint8_t> randomVal(FixedWidthField &field, MersenneTwisterRandom &rng) {
    return randomArray(rng, field.size());
}

template<> vector<uint8_t> 
randomVal(NullableField<FixedWidthField> &field, MersenneTwisterRandom &rng) {
    return randomArray(rng, field.size());
}

template<> GeneralValue randomVal(GFWrapper &field, MersenneTwisterRandom &rng) {
    GeneralValue ret;
    switch(field.getType())
        {
        case ExtentType::ft_unknown: FATAL_ERROR("?"); break;
        case ExtentType::ft_bool: ret.setBool(rng.randInt(2) ? true : false); break;
        case ExtentType::ft_byte: ret.setByte(rng.randInt(256)); break;
        case ExtentType::ft_int32: ret.setInt32(rng.randInt()); break;
        case ExtentType::ft_int64: ret.setInt64(rng.randInt()); break;
        case ExtentType::ft_double: ret.setDouble(rng.randInt() / 10.0); break;
        case ExtentType::ft_variable32: 
            ret.setVariable32(randomString(rng, rng.randInt(256))); break;
        case ExtentType::ft_fixedwidth: 
            ret.setFixedWidth(randomString(rng, field.fixedWidthSize())); break;
        }
    return ret;
}

// TODO: add field::value_type so that we can write FT::value_type getVal(...)
// will also remove the need to write getVal<type>
template<typename T, typename FT> T 
getVal(const FT &field, const Extent &e, const SEP_RowOffset offset) {
    return field.val(e, offset);
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
            SINVARIANT(field.isNull());
            INVARIANT(getVal<T>(field, *s.getExtent(), s.getRowOffset()) == T(),
                      format("%d: %s != %s") % i 
                      % getVal<T>(field, *s.getExtent(), s.getRowOffset()) % T());
        } else {
            field.set(val);
            if (nullable) {
                SINVARIANT(!field.isNull());
            }
            SINVARIANT(getVal<T>(field, *s.getExtent(), s.getRowOffset()) == val);
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

template<> GeneralValue transform(const GeneralValue &update, const GeneralValue &offset) {
    GeneralValue ret;

    switch (offset.getType())
        {
        case ExtentType::ft_unknown: FATAL_ERROR("?");
        case ExtentType::ft_bool: ret.setBool(update.valBool() ^ offset.valBool()); break;
        case ExtentType::ft_byte: ret.setByte(update.valByte() + offset.valByte()); break;
        case ExtentType::ft_int32: ret.setInt32(update.valInt32() + offset.valInt32()); break;
        case ExtentType::ft_int64: ret.setInt64(update.valInt64() + offset.valInt64()); break;
        case ExtentType::ft_double: ret.setDouble(update.valDouble() + offset.valDouble()); break;
        case ExtentType::ft_variable32: 
            ret.setVariable32(update.valString() + offset.valString()); break;
        case ExtentType::ft_fixedwidth: {
            string u = update.valString();
            string o = offset.valString();
            if (u.empty()) {
                ret.setFixedWidth(o);
            } else {
                SINVARIANT(u.size() == o.size());
                for (size_t i=0; i < u.size(); ++i) {
                    u[i] += o[i];
                }
                ret.setFixedWidth(u);
            }
        }
            break;
        default: FATAL_ERROR("?");
        }
    return ret;
}

template<typename T, typename FT> T 
getOp(const FT &field, const Extent &e, const SEP_RowOffset offset) {
    return field(e, offset);
}

template<> string 
getVal(const Variable32Field &field, const Extent &e, const SEP_RowOffset offset) {
    return field.stringval(e, offset);
}

template<> string 
getOp(const Variable32Field &field, const Extent &e, const SEP_RowOffset offset) {
    return string(reinterpret_cast<const char *>(field.val(e, offset)), field.size(e, offset));
}

template<> string 
getVal(const NullableField<Variable32Field> &field, const Extent &e, const SEP_RowOffset offset) {
    return field.stringval(e, offset);
}

template<> string 
getOp(const NullableField<Variable32Field> &field, const Extent &e, const SEP_RowOffset offset) {
    return string(reinterpret_cast<const char *>(field.val(e, offset)), field.size(e, offset));
}

template<> vector<uint8_t>
getVal(const FixedWidthField &field, const Extent &e, const SEP_RowOffset offset) {
    return field.arrayVal(e, offset);
}

template<> vector<uint8_t>
getOp(const FixedWidthField &field, const Extent &e, const SEP_RowOffset offset) {
    return vector<uint8_t>(field.val(e, offset), field.val(e, offset) + field.size());
}

template<> vector<uint8_t>
getVal(const NullableField<FixedWidthField> &field, const Extent &e, const SEP_RowOffset offset) {
    return field.arrayVal(e, offset);
}

template<> vector<uint8_t>
getOp(const NullableField<FixedWidthField> &field, const Extent &e, const SEP_RowOffset offset) {
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

template<typename T, typename FT> 
void checkUpdates(FT &field, const Extent &e1, const vector<SEP_RowOffset> &o1, const vector<T> &r1,
                  const Extent &e2, const vector<SEP_RowOffset> &o2, const vector<T> &r2) {
    // Verify all the updates happened; deliberately tested with everything const in the parameters.
    for (size_t i = 0; i < r1.size(); ++i) {
        INVARIANT(getOp<T>(field, e1, o1[i]) == r1[i], 
                  format("%d: %s != %s") % i % getOp<T>(field, e1, o1[i]) % r1[i]);
        INVARIANT(getVal<T>(field, e2, o2[i]) == r2[i],
                  format("%d: %s != %s") % i % getOp<T>(field, e2, o2[i]) % r2[i]);
    }
}

template<typename T, typename FT> void testOneSEP_RowOffset(const string &field_name) {
    cout << format("testing on field %s\n") % field_name;
    ExtentTypeLibrary lib;
    const ExtentType &t(lib.registerTypeR(fixed_width_types_xml));

    ExtentSeries s;
    Extent e1(t), e2(t);
    vector<SEP_RowOffset> o1, o2;
    vector<T> r1, r2;

    s.setExtent(e1);

    FT field(s, field_name); // need to make after series so that type is defined for general fields
    bool nullable = t.getNullable(field_name);

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

    checkUpdates(field, e1, o1, r1, e2, o2, r2);
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
    testOneSEP_RowOffset<double, NullableField<DoubleField> >("n-double");
    testOneSEP_RowOffset<string, NullableField<Variable32Field> >("n-variable32");
    testOneSEP_RowOffset<vector<uint8_t>, NullableField<FixedWidthField> >("n-fw13");

    // General fields
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("bool");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("n-bool");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("byte");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("n-byte");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("int32");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("n-int32");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("int64");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("n-int64");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("double");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("n-double");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("variable32");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("n-variable32");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("fw7");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("fw20");
    testOneSEP_RowOffset<GeneralValue, GFWrapper>("n-fw13");
}

int main() {
    dataseries::checkSubExtentPointerSizes();
    testSEP_RowOffset();
    return 0;
}
