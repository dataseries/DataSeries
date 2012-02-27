/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/
/** @file
    A module which uses a provided Filter class along with the type
    index in each file to select the extents to return.
*/

#ifndef __DATASERIES_TYPEFILTERMODULE_HPP
#define __DATASERIES_TYPEFILTERMODULE_HPP

#include <string>
#include <vector>
#include <DataSeries/IndexSourceModule.hpp>

/** \brief A Filter class for the TypeFilterModule that returns any type matching a given prefix.

  * Uses the constructor's argument to choose types who are a
  * prefix-match for the provided string. */
class PrefixFilter {
public:
    PrefixFilter(const std::string &p) : prefix(p) { }

    bool operator()(const std::string &type) {
        return (type.substr(0, prefix.size()) == prefix);
    }

private:
    std::string prefix;
};

/** \brief Source module that returns extents matching types as determined by the provided Filter.

  * Each DataSeries file contains an index that tells the type and
  * offset of every extent in that file.  This source module takes a
  * Filter class as a template parameter which specifies which types
  * to return and which to ignore. */
template<class Filter>
class TypeFilterModule : public IndexSourceModule {
public:
    TypeFilterModule(Filter &f)
        : IndexSourceModule(), filter(f), index_series(ExtentSeries::typeExact),
          extent_offset(index_series, "offset"), extent_type(index_series, "extenttype"),
          cur_file(0), cur_source(NULL)
    { }

    void addSource(const std::string &filename) {
        input_files.push_back(filename);
    }

    virtual void lockedResetModule() {
        index_series.clearExtent();
        cur_file = 0;
    }

    virtual PrefetchExtent *lockedGetCompressedExtent() {
        while(true) {
            if (!index_series.hasExtent()) {
                if (cur_file == input_files.size()) {
                    INVARIANT(!input_files.empty(), "type index module had no input files??");
                    return NULL;
                }
                cur_source = new DataSeriesSource(input_files[cur_file]);
                INVARIANT(cur_source->index_extent != NULL,
                          "can't handle source with null index extent\n");
                index_series.setExtent(cur_source->index_extent);
            }
            for (; index_series.morerecords(); ++index_series) {
                if (filter(extent_type.stringval())) {
                    off64_t v = extent_offset.val();
                    PrefetchExtent *ret = readCompressed(cur_source, v, extent_type.stringval());
                    ++index_series;
                    return ret;
                }
            }
            if (index_series.morerecords() == false) {
                index_series.clearExtent();
                delete cur_source;
                cur_source = NULL;
                ++cur_file;
            }
        }
    }

protected:
    Filter &filter;

    ExtentSeries index_series;
    Int64Field extent_offset;
    Variable32Field extent_type;

private:
    unsigned int cur_file;
    DataSeriesSource *cur_source;
    std::vector<std::string> input_files;
};

typedef TypeFilterModule<PrefixFilter> PrefixFilterModule;

#endif
