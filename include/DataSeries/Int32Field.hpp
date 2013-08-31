// -*-C++-*-
/*
  (c) Copyright 2003-2009, Hewlett-Packard Development Company, LP

  See the file named COPYING for license details
*/

/** @file
    int32 field class
*/

#ifndef DATASERIES_INT32FIELD_HPP
#define DATASERIES_INT32FIELD_HPP

/** \brief Accessor for int32 fields. */
class Int32Field : public dataseries::detail::SimpleFixedField<int32_t> {
  public:
    Int32Field(ExtentSeries &dataseries, const std::string &field, 
               int flags = 0, int32_t default_value = 0, bool auto_add = true)
            : dataseries::detail::SimpleFixedField<int32_t>(dataseries, field, flags, default_value)
    { 
        if (auto_add) {
            dataseries.addField(*this);
        }
    }
    friend class GF_Int32;
};

#endif


