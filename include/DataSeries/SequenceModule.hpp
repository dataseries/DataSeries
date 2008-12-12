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

/** \brief Holds a linear sequence of modules.
  *
  * Used as in: \dotfile "doxygen-figures/sequence-module.dot" "A sequence module with sub-modules"
  **/
class SequenceModule : public DataSeriesModule {
public:
    /** head should be dynaically allocated with new.  The @c SequenceModule
        takes ownershipof it.  head is the source from which the other
        modules added by \link SequenceModule::addModule addModule \endlink
        will ultimately get the \link Extent Extents \endlink to process. */
    SequenceModule(DataSeriesModule *head);
    /** Destroys all owned modules in reverse order of addition. */
    virtual ~SequenceModule();

    DataSeriesModule &tail();
    /** mod should be connected to the previous tail, and will become the new
        tail of the series; mod should have been allocated with new.  The
        @c ExtentSeries takes ownership of it.

        \code

        SequenceModule all_modules(source module);
        //...
        all_modules.addModule(new MyModule(all_modules.tail(), more arguments));

        \endcode

        */
    void addModule(DataSeriesModule *mod);
    /** calls \link DataSeriesModule::getExtent getExtent \endlink on
        the tail.  Presumably, this will call
        \link DataSeriesModule::getExtent getExtent \endlink on
        all the \link DataSeriesModule modules \endlink added.*/
    virtual Extent *getExtent();
    /** Returns the number of Modules added, including the one passed to the
        constructor. */
    unsigned size() { return modules.size(); }

    typedef std::vector<DataSeriesModule *>::iterator iterator;
    /** Modules appear in the order in which they were added */
    iterator begin() { return modules.begin(); }
    /** Modules appear in the order in which they were added */
    iterator end() { return modules.end(); }
private:
    std::vector<DataSeriesModule *> modules;
};

#endif

	
