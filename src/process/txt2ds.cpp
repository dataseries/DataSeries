#include <iostream>
#include <fstream>
#include <string>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/commonargs.hpp>

using namespace std;
using boost::format;

const string text_xml(
  "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Text\" version=\"1.0\" pack_pad_record=\"max_column_size\" pack_field_ordering=\"big_to_small_sep_var32\" comment=\"\" >\n"
  "  <field type=\"variable32\" name=\"line\" comment=\"one line of a text file\" />\n"
  "</ExtentType>"
  );

ExtentSeries series;
OutputModule *outmodule;
Variable32Field line(series, "line");

int
main(int argc,char *argv[])
{
	commonPackingArgs packing_args;
	getPackingArgs(&argc, argv, &packing_args);

	INVARIANT(argc == 3 || argc == 4,
			format("Usage: %s [ds-common-args] inname outdsname [copies]; - valid for inname")
			% argv[0]);

	// how many copies of the text do we want?
	// multiple copies are useful for synthetically creating large DS files
	int copies = (argc == 4) ? atoi(argv[3]) : 1;

	ifstream infile(argv[1]);
	INVARIANT(infile.is_open(), format("Unable to open file %s") % argv[1]);

	DataSeriesSink outds(argv[2],
				packing_args.compress_modes,
				packing_args.compress_level);
	ExtentTypeLibrary library;
	const ExtentType *extent_type = library.registerType(text_xml);
	series.setType(*extent_type);
	outmodule = new OutputModule(outds, series, extent_type,
			packing_args.extent_size);
	outds.writeExtentLibrary(library);

	int64_t line_count = 0;
	string str;

	cout << "Multiplying the content by " << copies << endl;

	for (int i = 0; i < copies; ++i) {
		cout << "Processing copy #" << i << " (starting at line #" << line_count << ")" << endl;
		while (!infile.eof()) {
			getline(infile, str); // read next line from file
			outmodule->newRecord();
			line.set(str); // create a record in data series for the line
			++line_count;
		}
		infile.clear(); // clear the eof state
		infile.seekg(0); // start from the beginning again
	}

	delete outmodule;
	outds.close();

	cout << "Finished writing " << line_count << " records/lines" << endl;

	infile.close();
    return 0;
}

