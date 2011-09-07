/*
   (c) Copyright 2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <Lintel/MersenneTwisterRandom.hpp>

#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>

using namespace std;
using namespace dataseries;

const string fixed_width_types_xml = 
"<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"fixed-width-types\" version=\"1.0\" >\n"
"  <field type=\"byte\" name=\"byte\" />\n"
"  <field type=\"int32\" name=\"int32\" />\n"
"  <field type=\"int64\" name=\"int64\" />\n"
"  <field type=\"double\" name=\"double\" />\n"
"</ExtentType>\n";

template<typename T> void 
fillSEP_RowOffset(ExtentSeries &s, TFixedField<T> &field, vector<SEP_RowOffset> &o,
                  vector<T> &r) {
    MersenneTwisterRandom rng;

    for (uint32_t i = 0; i < 1000; ++i) {
        s.newRecord();
        T val = static_cast<T>(rng.randInt());
        field.set(val);
        o.push_back(s.getRowOffset());
        r.push_back(val);
    }
}

template<typename T> void testOneSEP_RowOffset(const string &field_name) {
    ExtentTypeLibrary lib;
    const ExtentType &t(lib.registerTypeR(fixed_width_types_xml));

    ExtentSeries s;
    Extent e1(t), e2(t);
    vector<SEP_RowOffset> o1, o2;
    vector<T> r1, r2;

    TFixedField<T> field(s, field_name);

    s.setExtent(e1);
    fillSEP_RowOffset(s, field, o1, r1);
    s.setExtent(e2);
    fillSEP_RowOffset(s, field, o2, r2);
    s.clearExtent(); // leaves the type alone

    SINVARIANT(r1.size() == r2.size() && !r1.empty());

    MersenneTwisterRandom rng;

    // uses both the field.get() and field() variants to test both, normally this would be
    // weird style.
    for (size_t i = 0; i < r1.size(); ++i) {
        SINVARIANT(field.val(e1, o1[i]) == r1[i]);
        SINVARIANT(field(e2, o2[i]) == r2[i]);
        T offset = static_cast<T>(rng.randInt());
        field.set(e1, o1[i], field(e1, o1[i]) + offset);
        field.set(e2, o2[i], field.val(e2, o2[i]) + offset);
        r1[i] += offset;
        r2[i] += offset;
    }

    // Verify all the updates happened.
    for (size_t i = 0; i < r1.size(); ++i) {
        SINVARIANT(field(e1, o1[i]) == r1[i]);
        SINVARIANT(field.val(e2, o2[i]) == r2[i]);
    }
}
        
void testSEP_RowOffset() {
    testOneSEP_RowOffset<uint8_t>("byte");
    testOneSEP_RowOffset<int32_t>("int32");
    testOneSEP_RowOffset<int64_t>("int64");
    testOneSEP_RowOffset<double>("double");
}

int main() {
    dataseries::checkSubExtentPointerSizes();
    testSEP_RowOffset();
    return 0;
}
