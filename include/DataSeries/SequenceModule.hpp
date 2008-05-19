// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A linear series of modules
*/

#ifndef __SERIES_MODULE_H
#define __SERIES_MODULE_H

#include <vector>

#include <DataSeries/DataSeriesModule.hpp>

class SequenceModule : public DataSeriesModule {
public:
    // head should be dynaically allocated
    SequenceModule(DataSeriesModule *head);
    virtual ~SequenceModule();

    DataSeriesModule &tail();
    // mod should be connected to the previous tail, and will become the new
    // tail of the series; mod should have been allocated with new, and will
    // be deleted when this module is torn down.
    void addModule(DataSeriesModule *mod); 
    virtual Extent *getExtent(); // will getExtent on tail.
    unsigned size() { return modules.size(); }

    typedef std::vector<DataSeriesModule *>::iterator iterator;
    iterator begin() { return modules.begin(); }
    iterator end() { return modules.end(); }
private:
    std::vector<DataSeriesModule *> modules;
};

#endif

	
