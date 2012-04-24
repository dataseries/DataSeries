// -*-C++-*-
/*
   (c) Copyright 2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Test the pad-record options
*/

#include <iostream>

#include <DataSeries/ExtentType.hpp>

using namespace std;
using boost::format;

void expectRecordSize(const string &type_in, unsigned original,	unsigned max_column_size) {
    string a_type((format(type_in) % "original").str());
    const ExtentType::Ptr a(ExtentTypeLibrary::sharedExtentTypePtr(a_type));
    SINVARIANT(a->fixedrecordsize() == original);

    string b_type((format(type_in) % "max_column_size").str());
    const ExtentType::Ptr b(ExtentTypeLibrary::sharedExtentTypePtr(b_type));
    SINVARIANT(b->fixedrecordsize() == max_column_size);
}

void testExtentTypeSize() {
    string bool_max_xml("<ExtentType name=\"Test::PackPadRecord\" pack_pad_record=\"%s\" >\n"
			"  <field type=\"bool\" name=\"bool1\" />\n"
			"  <field type=\"bool\" name=\"bool2\" />\n"
			"</ExtentType>\n");

    expectRecordSize(bool_max_xml, 8, 1);

    string byte_max_xml("<ExtentType name=\"Test::PackPadRecord\" pack_pad_record=\"%s\" >\n"
			"  <field type=\"bool\" name=\"bool1\" />\n"
			"  <field type=\"bool\" name=\"bool2\" />\n"
			"  <field type=\"byte\" name=\"byte1\" />\n"
			"  <field type=\"byte\" name=\"byte2\" />\n"
			"</ExtentType>\n");

    expectRecordSize(byte_max_xml, 8, 3);

    string int1_max_xml("<ExtentType name=\"Test::PackPadRecord\" pack_pad_record=\"%s\" >\n"
			"  <field type=\"bool\" name=\"bool1\" />\n"
			"  <field type=\"bool\" name=\"bool2\" />\n"
			"  <field type=\"byte\" name=\"byte1\" />\n"
			"  <field type=\"byte\" name=\"byte2\" />\n"
			"  <field type=\"int32\" name=\"int1\" />\n"
			"</ExtentType>\n");

    expectRecordSize(int1_max_xml, 8, 8);

    string int2_max_xml("<ExtentType name=\"Test::PackPadRecord\" pack_pad_record=\"%s\" >\n"
			"  <field type=\"bool\" name=\"bool1\" />\n"
			"  <field type=\"bool\" name=\"bool2\" />\n"
			"  <field type=\"byte\" name=\"byte1\" />\n"
			"  <field type=\"byte\" name=\"byte2\" />\n"
			"  <field type=\"int32\" name=\"int1\" />\n"
			"  <field type=\"variable32\" name=\"var1\" />\n"
			"</ExtentType>\n");

    expectRecordSize(int2_max_xml, 16, 12);

    string dbl1_max_xml("<ExtentType name=\"Test::PackPadRecord\" pack_pad_record=\"%s\" >\n"
			"  <field type=\"bool\" name=\"bool1\" />\n"
			"  <field type=\"bool\" name=\"bool2\" />\n"
			"  <field type=\"byte\" name=\"byte1\" />\n"
			"  <field type=\"byte\" name=\"byte2\" />\n"
			"  <field type=\"int32\" name=\"int1\" />\n"
			"  <field type=\"variable32\" name=\"var1\" />\n"
			"  <field type=\"double\" name=\"dbl1\" />\n"
			"</ExtentType>\n");

    expectRecordSize(dbl1_max_xml, 24, 24);
}

int main() {
    testExtentTypeSize();
    cout << "Passed pack_pad_record tests.\n";
    return 0;
}

