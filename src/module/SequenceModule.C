/* -*-C++-*-
*******************************************************************************
*
* File:         SequenceModule.C
* RCS:          $Header: /mount/cello/cvs/DataSeries/src/module/SequenceModule.C,v 1.2 2004/04/02 22:07:29 anderse Exp $
* Description:  implementation
* Author:       Eric Anderson
* Created:      Thu Oct  9 13:57:30 2003
* Modified:     Tue Mar  9 16:01:00 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#include <SequenceModule.H>

SequenceModule::SequenceModule(DataSeriesModule &head)
{
    modules.push_back(&head);
}

SequenceModule::SequenceModule(DataSeriesModule *head)
{
    AssertAlways(head != NULL,("invalid argument"));
    modules.push_back(head);
}

SequenceModule::~SequenceModule()
{
    // later modules could depend on earlier ones
    for(std::vector<DataSeriesModule *>::iterator i = modules.end() - 1;
	i >= modules.begin();--i) {
	delete *i;
    }
}

DataSeriesModule &
SequenceModule::tail()
{
    DataSeriesModule *tail = modules.back();
    AssertAlways(tail != NULL,("internal error"));
    return *tail;
}

void
SequenceModule::addModule(DataSeriesModule *mod)
{
    AssertAlways(mod != NULL,("invalid argument"));
    modules.push_back(mod);
}

Extent *
SequenceModule::getExtent()
{
    return tail().getExtent();
}
