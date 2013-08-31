// -*-C++-*-
/*
  (c) Copyright 2003-2008, Hewlett-Packard Development Company, LP

  See the file named COPYING for license details
*/

/** @file
    int64 field class
*/

#ifndef DATASERIES_INT64FIELD_HPP
#define DATASERIES_INT64FIELD_HPP

#include <DataSeries/FixedField.hpp>

/** \brief Accessor for int64 fields. */
class Int64Field : public dataseries::detail::SimpleFixedField<int64_t> {
  public:
    Int64Field(ExtentSeries &dataseries, const std::string &field, 
               int flags = 0, int64_t default_value = 0, bool auto_add = true)
            : dataseries::detail::SimpleFixedField<int64_t>(dataseries, field, flags, default_value)
    { 
        if (auto_add) {
            dataseries.addField(*this);
        }
    }
    friend class GF_Int64;
};

#endif
