// -*-C++-*-
/*
   (c) Copyright 2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Test the pad-record options
*/

#include <DataSeries/Extent.H>
#include <DataSeries/ExtentField.H>

using namespace std;

void testSimpleCheckOffsets(const string &type_xml, int off_bool, int off_byte,
			    int off_i32, int off_v32, int off_i64, 
			    int off_dbl, unsigned recordsize)
{
    ExtentType &a(ExtentTypeLibrary::sharedExtentType(type_xml));

    SINVARIANT(a.getOffset("bool") == off_bool);
    SINVARIANT(a.getOffset("byte") == off_byte);
    SINVARIANT(a.getOffset("i32") == off_i32);
    SINVARIANT(a.getOffset("v32") == off_v32);
    SINVARIANT(a.getOffset("i64") == off_i64);
    SINVARIANT(a.getOffset("dbl") == off_dbl);
    SINVARIANT(a.fixedrecordsize() == recordsize);
}

void testSimple()
{
    // packed as bool, byte, pad2, i32, var32, pad4, i64, dbl
    string test1_xml("<ExtentType name=\"Test::FieldOrder\" pack_field_ordering=\"small_to_big_sep_var32\">\n"
		     "  <field type=\"int64\" name=\"i64\" />\n"
		     "  <field type=\"variable32\" name=\"v32\" />\n"
		     "  <field type=\"bool\" name=\"bool\" />\n"
		     "  <field type=\"int32\" name=\"i32\" />\n"
		     "  <field type=\"double\" name=\"dbl\" />\n"
		     "  <field type=\"byte\" name=\"byte\" />\n"
		     "</ExtentType>\n");

    testSimpleCheckOffsets(test1_xml, 0, 1, 4, 8, 16, 24, 32);

    // packed as dbl, i64, i32, v32, byte, bool, pad6
    string test2_xml("<ExtentType name=\"Test::FieldOrder\" pack_field_ordering=\"big_to_small_sep_var32\">\n"
		     "  <field type=\"variable32\" name=\"v32\" />\n"
		     "  <field type=\"bool\" name=\"bool\" />\n"
		     "  <field type=\"int32\" name=\"i32\" />\n"
		     "  <field type=\"byte\" name=\"byte\" />\n"
		     "  <field type=\"double\" name=\"dbl\" />\n"
		     "  <field type=\"int64\" name=\"i64\" />\n"
		     "</ExtentType>\n");

    testSimpleCheckOffsets(test2_xml, 25,24,16,20,8,0, 32);
}

int main()
{
    testSimple();
    cout << "Passed pack_field_ordering tests\n";
    return 0;
}
