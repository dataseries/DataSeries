// -*-C++-*-
/*
  (c) Copyright 2003-2011, Hewlett-Packard Development Company, LP

  See the file named COPYING for license details
*/

/** @file
    double field class
*/

#ifndef DATASERIES_DOUBLEFIELD_HPP
#define DATASERIES_DOUBLEFIELD_HPP

#include <Lintel/CompilerMarkup.hpp>

#include <DataSeries/FixedField.hpp>

/** \brief Accessor for double fields. */
class DoubleField : public dataseries::detail::SimpleFixedField<double> {
  public:
    /// flag_allownonzerobase is deprecated.  It seemed like a good
    /// idea when we initially created it, but in practice it just
    /// makes writing analysis really difficult.  It was intended to
    /// deal with time fields, and the newer Int64TimeField deals with
    /// that much better.
    static const int flag_allownonzerobase = 1024;

    DoubleField(ExtentSeries &dataseries, const std::string &field, 
                int flags = 0, double default_value = 0, bool auto_add = true)
            : dataseries::detail::SimpleFixedField<double>(dataseries, field, flags, default_value),
              base_val(Double::NaN)
    { 
        if (auto_add) {
            dataseries.addField(*this);
        }
    }

    // TODO: actually deprecate the *abs* functions
    double absval() const { 
        if (isNull()) {
            // 2006-06-30 EricAnderson, used to return just
            // default_value, but that seems inconsistent with what is
            // being done below, and different than how val() would
            // work.  Correct behavior seems to be unclear
            return default_value + base_val; 
        } else {
            return val() + base_val;
        }
    }

    void setabs(double val) {
        *(double *)(rowPos() + offset) = val - base_val;
        setNull(false);
    }
  
    double base_val;
    virtual void newExtentType();

    friend class GF_Double;
};

#endif
