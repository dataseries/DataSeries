/* -*-C++-*-
*******************************************************************************
*
* File:         SequenceModule.H
* RCS:          $Header: /mount/cello/cvs/DataSeries/include/DataSeries/SequenceModule.H,v 1.1 2003/10/27 03:46:32 anderse Exp $
* Description:  A linear series of modules
* Author:       Eric Anderson
* Created:      Thu Oct  9 13:55:00 2003
* Modified:     Sat Oct 11 18:51:16 2003 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#ifndef __SERIES_MODULE_H
#define __SERIES_MODULE_H

#include <vector>

#include <DataSeriesModule.H>

class SequenceModule : public DataSeriesModule {
public:
    // head should be dynaically allocated
    SequenceModule(DataSeriesModule &head);
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

	
