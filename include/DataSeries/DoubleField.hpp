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

#include <DataSeries/FixedField.hpp>

/** \brief Accessor for double fields. */
class DoubleField : public FixedField {
public:
    /// flag_allownonzerobase is deprecated.  It seemed like a good
    /// idea when we initially created it, but in practice it just
    /// makes writing analysis really difficult.  It was intended to
    /// deal with time fields, and the newer Int64TimeField deals with
    /// that much better.
    static const int flag_allownonzerobase = 1024;

    DoubleField(ExtentSeries &_dataseries, const std::string &field, 
		int flags = 0, double default_value = 0, bool auto_add = true);

    /** Returns the value of the field in the @c ExtentSeries' current record.
        If the field is null returns the default value.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record. */
    double val() const { 
        if (isNull()) {
            return default_value;
        } else {
            return *(double *)rawval();
        }
    }
  
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

    /** Sets the value of the field in the @c ExtentSeries' current record.
        The field will never be null immediately after a call to set(),
        regardless of whether the argument is the same as the default value.

        Preconditions:
            - The name of the Field must have been set and the associated
              @c ExtentSeries must have a current record. */
    void set(double val) {
        *(double *)rawval() = val;
        setNull(false);
    }
  
    void setabs(double val) {
        *(double *)rawval() = val - base_val;
        setNull(false);
    }
  
    double val(Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        INVARIANT(!nullable, "unimplemented");
        return *reinterpret_cast<double *>(rawval(e, row_offset));
    }

    double operator ()(Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        return val(e, row_offset);
    }

    void set(Extent &e, const dataseries::SEP_RowOffset &row_offset, double val) {
        INVARIANT(!nullable, "unimplemented");
        *reinterpret_cast<double *>(rawval(e, row_offset)) = val;
    }

    double default_value;
    double base_val;
    virtual void newExtentType();
};

#endif
