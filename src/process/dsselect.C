/* -*-C++-*-
*******************************************************************************
*
* File:         dsselect.C
* RCS:          $Header: /mount/cello/cvs/DataSeries/src/process/dsselect.C,v 1.3 2004/09/07 21:29:30 anderse Exp $
* Description:  Select subset of fields from a collection of traces, generate a new trace
* Author:       Eric Anderson
* Created:      Mon Aug  4 22:05:21 2003
* Modified:     Thu Jul  8 20:35:19 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#include <StringUtil.H>

#include <commonargs.H>
#include <DataSeriesFile.H>
#include <GeneralField.H>
#include <DataSeriesModule.H>

#ifndef HPUX_ACC
using namespace std;
#endif

int
main(int argc, char *argv[])
{
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    AssertAlways(argc > 4,
		 ("Usage: %s <common-args> type-prefix field,field,field,... input-filename... output-filename\n",argv[0]));
    string type_prefix(argv[1]);
    string fieldlist(argv[2]);
    
    vector<string> fields;

    split(fieldlist,",",fields);

    DataSeriesSink output(argv[argc-1],packing_args.compress_modes,packing_args.compress_level);
    SourceModule source;
    for(int i=3;i<(argc-1);++i) {
	source.addSource(argv[i]);
    }
    ExtentType *intype = source.curSource()->mylibrary.getTypeByPrefix(type_prefix);
    AssertAlways(intype != NULL,
		 ("can't find a type matching prefix %s",type_prefix.c_str()));
    ExtentSeries inputseries(ExtentSeries::typeLoose);
    ExtentSeries outputseries(ExtentSeries::typeLoose);
    vector<GeneralField *> infields, outfields;
    string xmloutdesc("<ExtentType name=\"");
    xmloutdesc.append(type_prefix);
    xmloutdesc.append("\">\n");
    inputseries.setType(intype);
    for(vector<string>::iterator i = fields.begin();
	i != fields.end();++i) {
	xmloutdesc.append("  ");
	printf("%s -> %s\n",i->c_str(),intype->xmlFieldDesc(*i).c_str());
	xmloutdesc.append(intype->xmlFieldDesc(*i));
	xmloutdesc.append("\n");
	infields.push_back(GeneralField::create(NULL,inputseries,*i));
    }
    xmloutdesc.append("</ExtentType>\n");
    printf("%s\n",xmloutdesc.c_str());

    ExtentTypeLibrary library;
    ExtentType *outputtype = library.registerType(xmloutdesc);
    output.writeExtentLibrary(library);
    outputseries.setType(outputtype);
    for(vector<string>::iterator i = fields.begin();
	i != fields.end();++i) {
	outfields.push_back(GeneralField::create(NULL,outputseries,*i));
    }
    FilterModule filtermodule(source,type_prefix);
    OutputModule outmodule(output,outputseries,outputtype,
			   packing_args.extent_size);
    while(true) {
	Extent *inextent = filtermodule.getExtent();
	if (inextent == NULL)
	    break;
	for(inputseries.setExtent(inextent);inputseries.pos.morerecords();
	    ++inputseries.pos) {
	    outmodule.newRecord();
	    for(unsigned int i=0;i<infields.size();++i) {
		outfields[i]->set(infields[i]);
	    }
	}
	delete inextent;
    }
    outmodule.flushExtent();
    return 0;
}

    
