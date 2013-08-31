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

#include <boost/shared_ptr.hpp>

#include <DataSeries/DataSeriesModule.hpp>

/** \brief Holds a linear sequence of modules.
 *
 * The sequence module holds a linear sequence of modules, which is primarily
 * used for running a set of extents through a collection of modules.
 *
 * As shown in: \dotfile "doxygen-figures/sequence-module.dot" "A sequence module with sub-modules"
 **/
class SequenceModule : public DataSeriesModule {
  public:
    typedef boost::shared_ptr<DataSeriesModule> DsmPtr;

    /** head should be dynamically allocated with new.  The @c SequenceModule
        takes ownership of it.  @param head is the source from which the other
        modules added by \link SequenceModule::addModule addModule \endlink
        will ultimately get the \link Extent Extents \endlink to process. */
    SequenceModule(DataSeriesModule *head);

    /** Construct a sequence module starting with a shared pointer. */
    SequenceModule(DsmPtr head);

    /** Destroys all owned modules in reverse order of addition if they have no other
        pointers to them. */
    virtual ~SequenceModule();

    /** get the last data series module in the sequence for connecting in to the next module
        in the sequence.  Usually used like seq_mod.addModule(new Module(seq_mod.tail())) */
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

    /** Like addModule with a pointer, but with a shared pointer */
    void addModule(DsmPtr mod);

    /** calls \link DataSeriesModule::getExtent getExtent \endlink on
        the tail.  Assuming the modules were set up properly, this will call
        \link DataSeriesModule::getExtent getExtent \endlink on
        all the \link DataSeriesModule modules \endlink added.*/
    virtual Extent *getExtent();

    /** calls \link DataSeriesModule::getSharedExtent getSharedExtent \endlink on
        the tail.  Assuming the modules were set up properly, this will call
        \link DataSeriesModule::getSharedExtent getSharedExtent \endlink on
        all the \link DataSeriesModule modules \endlink added.*/
    virtual Extent::Ptr getSharedExtent();

    /** Returns the number of Modules added, including the one passed to the
        constructor. */
    unsigned size() { return modules.size(); }

    typedef std::vector<DsmPtr>::iterator iterator;

    /** Modules appear in the order in which they were added */
    iterator begin() { return modules.begin(); }

    /** Modules appear in the order in which they were added */
    iterator end() { return modules.end(); }
  private:
    std::vector<DsmPtr> modules;
};

#endif

        
