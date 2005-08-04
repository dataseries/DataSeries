/* -*-C++-*-
*******************************************************************************
*
* File:         TypeIndexModule.H
* RCS:          $Header: /mount/cello/cvs/DataSeries/include/DataSeries/TypeIndexModule.H,v 1.2 2004/09/07 21:29:30 anderse Exp $
* Description:  A module which uses the type index in each file to select 
                the extents to return.
* Author:       Eric Anderson
* Created:      Thu May 27 22:57:23 2004
* Modified:     Thu Jul  8 15:30:59 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2004, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
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
