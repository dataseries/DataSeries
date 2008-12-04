// -*-C++-*-
/*
   (c) Copyright 2003-2006, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Select subset of fields from a collection of traces, generate a new trace
*/

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <Lintel/AssertBoost.hpp>
#include <Lintel/ProgramOptions.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/commonargs.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/DSExpr.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

using namespace std;
using boost::format;

lintel::ProgramOption<string> where_arg
  ("where", "expression controlling which lines to select, man DSExpr for details");

int main(int argc, char *argv[]) {
    Extent::setReadChecksFromEnv(true); // going to be compressing, may as well check
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);
    lintel::programOptionsHelp("[common-args] [options] type-prefix field,field,...\n"
			       "    input-filenames... output-filename\n"
			       "common-args include:\n");
    lintel::programOptionsHelp(packingOptions());

    vector<string> extra_args = lintel::parseCommandLine(argc, argv, true);
    if (extra_args.size() < 4) {
	lintel::programOptionsUsage(argv[0]);
	exit(0);
    }

    string type_prefix(extra_args[0]);
    string fieldlist(extra_args[1]);
    
    vector<string> fields;

    split(fieldlist,",",fields);

    {
	struct stat buf;
	INVARIANT(stat(extra_args.back().c_str(), &buf) != 0, 
		  format("Refusing to overwrite existing file %s.")
		  % extra_args.back());
    }
    DataSeriesSink output(extra_args.back(), packing_args.compress_modes, 
			  packing_args.compress_level);

    // to get the complete typename and type information...
    DataSeriesSource first_file(extra_args[2]);
    const ExtentType *intype = first_file.mylibrary.getTypeByPrefix(type_prefix);
    first_file.closefile();
    INVARIANT(intype != NULL, boost::format("can not find a type matching prefix %s")
	      % type_prefix);

    TypeIndexModule source(intype->getName());
    for(unsigned i=2; i < (extra_args.size()-1); ++i) {
	source.addSource(extra_args[i]);
    }
    source.startPrefetching();

    ExtentSeries inputseries(ExtentSeries::typeLoose);
    ExtentSeries outputseries(ExtentSeries::typeLoose);
    vector<GeneralField *> infields, outfields;

    // TODO: figure out how to handle pack_relative options that are
    // specified relative to a field that was not selected.  Right
    // now, there is no way to do this selection; try replacing
    // enter_driver with leave_driver in
    // src/Makefile.am:ran.check-dsselect. Also, give an option for the
    // xml description to be specified on the command line.

    string xmloutdesc(str(format("<ExtentType name=\"%s\" namespace=\"%s\" version=\"%d.%d\">\n")
			  % intype->getName() % intype->getNamespace() % intype->majorVersion()
			  % intype->minorVersion()));
    inputseries.setType(*intype);
    for(vector<string>::iterator i = fields.begin();
	i != fields.end();++i) {
	cout << format("%s -> %s\n") % *i % intype->xmlFieldDesc(*i);
	xmloutdesc.append(str(format("  %s\n") % intype->xmlFieldDesc(*i)));
	infields.push_back(GeneralField::create(NULL,inputseries,*i));
    }
    xmloutdesc.append("</ExtentType>\n");
    cout << xmloutdesc << "\n";

    ExtentTypeLibrary library;
    const ExtentType *outputtype = library.registerType(xmloutdesc);
    output.writeExtentLibrary(library);
    INVARIANT(outputtype != NULL, "bad");
    outputseries.setType(*outputtype);
    for(vector<string>::iterator i = fields.begin();
	i != fields.end();++i) {
	outfields.push_back(GeneralField::create(NULL,outputseries,*i));
    }
    DSExpr *where = NULL;
    if (where_arg.used()) {
	string tmp = where_arg.get();
	where = DSExpr::make(inputseries, tmp);
    }

    OutputModule outmodule(output,outputseries,outputtype,
			   packing_args.extent_size);
    uint64_t input_row_count = 0, output_row_count = 0;
    while(true) {
	Extent *inextent = source.getExtent();
	if (inextent == NULL) 
	    break;
	for(inputseries.setExtent(inextent);inputseries.pos.morerecords(); ++inputseries.pos) {
	    ++input_row_count;
	    if (where && !where->valBool()) {
		continue;
	    }
	    ++output_row_count;
	    outmodule.newRecord();
	    for(unsigned int i=0;i<infields.size();++i) {
		outfields[i]->set(infields[i]);
	    }
	}
	delete inextent;
    }
    outmodule.flushExtent();
    outmodule.close();
    
    GeneralField::deleteFields(infields);
    GeneralField::deleteFields(outfields);
    delete where;

    cout << format("%d input rows, %d output rows\n") % input_row_count % output_row_count;
    return 0;
}

    
