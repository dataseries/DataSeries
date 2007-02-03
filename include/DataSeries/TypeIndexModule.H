// -*-C++-*-
/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    A module which uses the type index in each file to select 
    the extents to return.
*/

#ifndef __DATASERIES_TYPEINDEXMODULE_H
#define __DATASERIES_TYPEINDEXMODULE_H

#include <DataSeries/IndexSourceModule.H>

/** \brief Source module that returns extents matching a particular type

 * Each DataSeries file contains an index that tells the type and
 * offset of every extent in that file.  This source module takes an
 * extent type prefix and returns all of the extents which match that
 * prefix from all of the files. 
 */

class TypeIndexModule : public IndexSourceModule {
public:
    TypeIndexModule(const std::string &type_prefix = "");

    virtual ~TypeIndexModule();
    void setPrefix(const std::string &type_prefix);
    /// Ugly temporary hack until the larger rewrite to handle name rewriting
    void setSecondPrefix(const std::string &type_prefix);

    void addSource(const std::string &filename);
    bool haveSources() { return !inputFiles.empty(); }
protected:
    std::string type_prefix, second_type_prefix;
    ExtentSeries indexSeries;
    Int64Field extentOffset;
    Variable32Field extentType;

    virtual void lockedResetModule();
    virtual compressedPrefetch *lockedGetCompressedExtent();

private:
    unsigned int cur_file;
    DataSeriesSource *cur_source;
    std::vector<std::string> inputFiles;
};

#endif
