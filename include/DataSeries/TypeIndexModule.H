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

#include <IndexSourceModule.H>

/** \brief Source module that returns extents matching a particular type

 * Each DataSeries file contains an index that tells the type and
 * offset of every extent in that file.  This source module takes an
 * extent type prefix and returns all of the extents which match that
 * prefix from all of the files. 
 */

class TypeIndexModule : public IndexSourceModule {
public:
    TypeIndexModule(const std::string &type_prefix = "",
		    SourceList *sources = NULL);

    virtual ~TypeIndexModule();
    void setPrefix(const std::string &type_prefix);

protected:
    std::string type_prefix;
    ExtentSeries indexSeries;
    Int64Field extentOffset;
    Variable32Field extentType;

    virtual void lockedResetModule();
    virtual compressedPrefetch *lockedGetCompressedExtent();
};

#endif
