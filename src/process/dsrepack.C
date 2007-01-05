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

#include <boost/format.hpp>

#include <Lintel/StringUtil.H>
#include <Lintel/AssertBoost.H>

#include <DataSeries/commonargs.H>
#include <DataSeries/DataSeriesFile.H>
#include <DataSeries/GeneralField.H>
#include <DataSeries/DataSeriesModule.H>
#include <DataSeries/TypeIndexModule.H>
#include <DataSeries/PrefetchBufferModule.H>

static const bool debug = true;

using namespace std;

struct PerTypeWork {
    OutputModule *output_module;
    ExtentSeries inputseries, outputseries;
    vector<GeneralField *> infields, outfields;
    PerTypeWork(DataSeriesSink &output, unsigned extent_size, ExtentType *t) 
	: inputseries(t), outputseries(t) {
	for(unsigned i = 0; i < t->getNFields(); ++i) {
	    const string &s = t->getFieldName(i);
	    infields.push_back(GeneralField::create(NULL, inputseries, s));
	    outfields.push_back(GeneralField::create(NULL, outputseries, s));
	}
	output_module = new OutputModule(output, outputseries, t, 
					 extent_size);
    }
};

int
main(int argc, char *argv[])
{
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    AssertAlways(argc > 2,
		 ("Usage: %s <common-args> input-filename... output-filename\n",argv[0]));
    
    {
	struct stat buf;
	AssertAlways(stat(argv[argc-1],&buf) != 0, 
		     ("Refusing to overwrite existing file %s.\n", argv[argc-1]));
    }
    TypeIndexModule source("");
    ExtentTypeLibrary library;
    map<string, PerTypeWork *> per_type_work;

    DataSeriesSink output(argv[argc-1], packing_args.compress_modes,
			  packing_args.compress_level);

    uint32_t extent_count = 0;
    for(int i=1;i<(argc-1);++i) {
	source.addSource(argv[i]);

	// Nothing helping the fact that we have to open all of the
	// files to verify type identicalness before we can re-pack
	// things.  Luckily people should only end up doing this
	// infrequently when they've just retrieved bz2 compressed
	// extents and want to make them larger and faster, or to do
	// the reverse for distribution.

	DataSeriesSource f(argv[i]);

	for(map<const string, ExtentType *>::iterator j = f.mylibrary.name_to_type.begin();
	    j != f.mylibrary.name_to_type.end(); ++j) {
	    if (j->first == "DataSeries: ExtentIndex" ||
		j->first == "DataSeries: XmlType") {
		continue;
	    }
	    ++extent_count;
	    ExtentType *tmp = library.getTypeByName(j->first, true);
	    INVARIANT(tmp == NULL || tmp == j->second,
		      boost::format("XML types for type '%s' differ between file %s and an earlier file")
		      % j->first % argv[i]);
	    if (tmp == NULL) {
		if (debug) {
		    cout << "Registering type of name " << j->first << endl;
		}
		ExtentType *t = library.registerType(j->second->xmldesc);
		per_type_work[j->first] = 
		    new PerTypeWork(output, packing_args.extent_size, t);
	    }
	    DEBUG_INVARIANT(per_type_work[j->first] != NULL, "internal");
	}
    }

    DataSeriesModule *from = &source;
    if (getenv("DISABLE_PREFETCHING") == NULL) {
	from = new PrefetchBufferModule(source, 64*1024*1024);
    }
    output.writeExtentLibrary(library);

    uint32_t extent_num = 0;
    while(true) {
	Extent *inextent = from->getExtent();
	if (inextent == NULL)
	    break;
	
	if (inextent->type->name == "DataSeries: ExtentIndex" ||
	    inextent->type->name == "DataSeries: XmlType") {
	    continue;
	}
	++extent_num;
	if (debug) {
	    cout << boost::format("Processing extent #%d/%d of type %s")
		 % extent_num % extent_count % inextent->type->name
		 << endl;
	}
	PerTypeWork *ptw = per_type_work[inextent->type->name];
	INVARIANT(ptw != NULL, "internal");
	for(ptw->inputseries.setExtent(inextent);
	    ptw->inputseries.pos.morerecords();
	    ++ptw->inputseries.pos) {
	    ptw->output_module->newRecord();
	    for(unsigned int i=0; i<ptw->infields.size(); ++i) {
		ptw->outfields[i]->set(ptw->infields[i]);
	    }
	}
	delete inextent;
    }

    for(map<string, PerTypeWork *>::iterator i = per_type_work.begin();
	i != per_type_work.end(); ++i) {
	i->second->output_module->flushExtent();
    }

    return 0;
}

    
