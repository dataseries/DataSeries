// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <DataSeries/SequenceModule.hpp>

SequenceModule::SequenceModule(DataSeriesModule *head)
{
    INVARIANT(head != NULL, "invalid argument");
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
    SINVARIANT(tail != NULL);
    return *tail;
}

void
SequenceModule::addModule(DataSeriesModule *mod)
{
    SINVARIANT(mod != NULL);
    modules.push_back(mod);
}

Extent *
SequenceModule::getExtent()
{
    return tail().getExtent();
}
