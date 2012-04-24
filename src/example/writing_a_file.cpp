// -*-C++-*-
/*
   (c) Copyright 2008 Harvey Mudd College

   See the file named COPYING for license details
*/

#include <boost/regex.hpp>
#include <boost/cstdint.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>

#include <DataSeries/ExtentType.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/DataSeriesModule.hpp>

const char* original_records[] = {
    "read 1 100 100",
    "write 2 50",
    "read 3 12 12",
    "write 4 32768",
    "write 5 523",
    "read 6 70 59",
    "write 7 50",
    "write 8 50"
};

boost::regex record_regex("(read (\\d+) (\\d+) (\\d+))|(write (\\d+) (\\d+))");
namespace submatches {
    enum {
        full,
        read,
        read_timestamp,
        read_requested,
        read_actual,
        write,
        write_timestamp,
        write_bytes
    };
}

boost::int64_t get_field(const boost::cmatch& match, int id) {
    return(boost::lexical_cast<boost::int64_t>(match[id].str()));
}

int main() {
    /*  First we need to create XML descriptions of the Extents that we mean to use.  For this
        example we will use two ExtentTypes--one for reads and one for writes.  DataSeries doesn't
        like to mix different types of records, so we need separate types for reads and writes if
        we want each field to be non-null. */

    const char* read_xml =
        "<ExtentType name=\"ExtentType1\">"
        "  <field name=\"timestamp\" type=\"int64\" />"
        "  <field name=\"requested_bytes\" type=\"int64\" />"
        "  <field name=\"actual_bytes\" type=\"int64\" />"
        "</ExtentType>\n";

    const char* write_xml =
        "<ExtentType name=\"ExtentType2\">"
        "  <field name=\"timestamp\" type=\"int64\" />"
        "  <field name=\"bytes\" type=\"int64\" />"
        "</ExtentType>\n";

    /*  Next we need to put both of these in an ExtentType library which
        will be the first thing written to the file. */

    ExtentTypeLibrary types_for_file;
    const ExtentType::Ptr read_type = types_for_file.registerTypePtr(read_xml);
    const ExtentType::Ptr write_type = types_for_file.registerTypePtr(write_xml);

    /*  Then we open a file to write the records to. This will overwrite an existing file.
        DataSeries files can not be updated once closed. */

    DataSeriesSink output_file("writing_a_file.ds");
    output_file.writeExtentLibrary(types_for_file);

    /*  Now, we create structures for writing the individual
        types.  Note that each type of extent is stored separately.
        The ExtentSeries will allow us to set the fields in each
        record that we create.  We'll split the records into
        approximately 1024 byte chunks uncompressed. */

    ExtentSeries read_series;
    OutputModule read_output(output_file, read_series, read_type, 1024);
    ExtentSeries write_series;
    OutputModule write_output(output_file, write_series, write_type, 1024);

    /*  These are handles to the fields in the "current record" of
        each ExtentSeries. */

    Int64Field read_timestamp(read_series, "timestamp");
    Int64Field read_requested_bytes(read_series, "requested_bytes");
    Int64Field read_actual_bytes(read_series, "actual_bytes");

    Int64Field write_timestamp(write_series, "timestamp");
    Int64Field write_bytes(write_series, "bytes");

    BOOST_FOREACH(const char* record, original_records) {
        boost::cmatch match;
        boost::regex_match(record, record_regex);
        if(match[submatches::read].matched) {
            /*  Create a new record and set its fields. */
            read_output.newRecord();
            read_timestamp.set(get_field(match, submatches::read_timestamp));
            read_requested_bytes.set(get_field(match, submatches::read_requested));
            read_actual_bytes.set(get_field(match, submatches::read_actual));
        } else if(match[submatches::write].matched) {
            /*  Create a new record and set its fields. */
            write_output.newRecord();
            write_timestamp.set(get_field(match, submatches::write_timestamp));
            write_bytes.set(get_field(match, submatches::write_bytes));
        }
    }

    /*  At this point the destructors will kick in.
        - the OutputModules will flush the Extents that
          they are currently holding
        - the DataSeriesFile will be closed. */
}
