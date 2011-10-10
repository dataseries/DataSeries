// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <DataSeries/SequenceModule.hpp>

SequenceModule::SequenceModule(DataSeriesModule *head)
{
    addModule(head);
}

SequenceModule::SequenceModule(DsmPtr head)
{
    INVARIANT(head != NULL, "invalid argument");
    modules.push_back(head);
}

SequenceModule::~SequenceModule() {
    // later modules could depend on earlier ones, so reset in reverse order.
    for(std::vector<DsmPtr>::reverse_iterator i = modules.rbegin(); i != modules.rend(); ++i) {
	i->reset();
    }
}

DataSeriesModule &SequenceModule::tail() {
    DsmPtr tail = modules.back();
    SINVARIANT(tail != NULL);
    return *tail;
}

void SequenceModule::addModule(DataSeriesModule *mod) {
    SINVARIANT(mod != NULL);
    DsmPtr p_mod(mod);
    modules.push_back(p_mod);
}

void SequenceModule::addModule(DsmPtr mod) {
    SINVARIANT(mod != NULL);
    modules.push_back(mod);
}

Extent *SequenceModule::getExtent() {
    return tail().getExtent();
}
