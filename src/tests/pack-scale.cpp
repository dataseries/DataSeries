// -*-C++-*-
/*
   (c) Copyright 2012, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    test program for DataSeries
*/

#include <boost/bind.hpp>

#include <Lintel/LintelLog.hpp>

#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>

using namespace std;
using boost::format;

const string test_xml =
"<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"scale-tests\" version=\"1.0\" >\n"
"  <field type=\"double\" name=\"s-1\" pack_scale=\"1.0\" />\n"
"  <field type=\"double\" name=\"s-1-nowarn\" pack_scale=\"1.0\" pack_scale_warn=\"no\" />\n"
"  <field type=\"double\" name=\"s-0.1\" pack_scale=\"0.1\" pack_scale_warn=\"yes\" />\n"
"</ExtentType>\n";

vector<string> log_msgs;
void saveAppender(const std::string &msg, const LintelLog::LogType type) {
    SINVARIANT(type == LintelLog::Warn);
    log_msgs.push_back(msg);
}

void testSimple() {
    const ExtentType::Ptr type(ExtentTypeLibrary::sharedExtentTypePtr(test_xml));

    Extent::Ptr e(new Extent(type));
    ExtentSeries s(e);
    DoubleField scale_1(s, "s-1");
    DoubleField scale_1_nowarn(s, "s-1-nowarn");
    DoubleField scale_0_1(s, "s-0.1");

    s.newRecord(); // round down | exact
    scale_1.set(0.2);
    scale_1_nowarn.set(0.2);
    scale_0_1.set(0.2);

    s.newRecord(); // round up | exact
    scale_1.set(1.8);
    scale_1_nowarn.set(1.8);
    scale_0_1.set(1.8);

    s.newRecord(); // round warning on 0.1 scaling
    scale_1.set(2.08);
    scale_1_nowarn.set(2.08);
    scale_0_1.set(2.08);
    
    LintelLog::addAppender(boost::bind(saveAppender, _1, _2));
    Extent::ByteArray packed_data;
    e->packData(packed_data, Extent::compress_lzf, 9, NULL, NULL, NULL);
    SINVARIANT(log_msgs.size() == 2);
    SINVARIANT(log_msgs[0] == "Warning, while packing field s-1 of record 1, error was > 10%:\n"
               "  (0.2 / 1 = 0.20, round() = 0)\n");
    SINVARIANT(log_msgs[1] == "Warning, while packing field s-0.1 of record 3, error was > 10%:\n"
               "  (2.08 / 0.1 = 20.80, round() = 21)\n");

    e->unpackData(packed_data, false);

    s.setExtent(e);
    SINVARIANT(Double::eq(scale_1(), 0));
    SINVARIANT(Double::eq(scale_1_nowarn(), 0));
    SINVARIANT(Double::eq(scale_0_1(), 0.2));

    s.next();
    SINVARIANT(Double::eq(scale_1(), 2));
    SINVARIANT(Double::eq(scale_1_nowarn(), 2));
    SINVARIANT(Double::eq(scale_0_1(), 1.8));

    s.next();
    SINVARIANT(Double::eq(scale_1(), 2));
    SINVARIANT(Double::eq(scale_1_nowarn(), 2));
    SINVARIANT(Double::eq(scale_0_1(), 2.1));
    cout << "Simple test passed.\n";
}

int main() {
    testSimple();
    return 0;
}
