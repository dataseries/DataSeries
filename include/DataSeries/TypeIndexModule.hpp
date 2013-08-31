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

#include <DataSeries/IndexSourceModule.hpp>

/** \brief Source module that returns extents matching a particular type

 * Each DataSeries file contains an index that tells the type and
 * offset of every extent in that file.  This source module takes an
 * extent type match; if the match type is empty, this returns all of
 * the extents, and otherwise, chooses a type using
 * ExtentTypeLibrary::getTypeMatch, and returns all of the extents
 * which have the same type. */
class TypeIndexModule : public IndexSourceModule {
  public:
    typedef boost::shared_ptr<TypeIndexModule> Ptr;
    TypeIndexModule(const std::string &type_match = "");

    static Ptr make(const std::string &type_match = "") {
        return Ptr(new TypeIndexModule(type_match));
    }

    virtual ~TypeIndexModule();
    void setMatch(const std::string &type_match);

    /// Ugly temporary hack until the larger rewrite to handle name rewriting
    void setSecondMatch(const std::string &type_match);

    void addSource(const std::string &filename);
    bool haveSources() { return !inputFiles.empty(); }

    void sameInputFiles(TypeIndexModule &from) {
        inputFiles = from.inputFiles;
    }

    const ExtentType *getType() FUNC_DEPRECATED {
        return my_type.get();
    }

    const ExtentType::Ptr getTypePtr() {
        return my_type;
    }
  protected:
    std::string type_match, second_type_match;
    ExtentSeries indexSeries;
    Int64Field extentOffset;
    Variable32Field extentType;

    virtual void lockedResetModule();
    virtual PrefetchExtent *lockedGetCompressedExtent();

  private:
    const ExtentType::Ptr matchType(); // May return NULL

    unsigned int cur_file;
    DataSeriesSource *cur_source;
    std::vector<std::string> inputFiles;
    ExtentType::Ptr my_type;
};

#endif
