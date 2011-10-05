// -*-C++-*-
/*
   (c) Copyright 2003-2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    double field class
*/

#ifndef DATASERIES_BOOLFIELD_HPP
#define DATASERIES_BOOLFIELD_HPP

#include <DataSeries/FixedField.hpp>

namespace dataseries { namespace detail {
/** \brief Accessor for boolean fields.
  *
  */
class BoolFieldImpl : public FixedField {
public:
    /** Creates a new @c BoolField associated with the specified series.
        An empty string as the field name means that the name of the
        field will be specified later.  It must be set via
        @c Field::setFieldName before the field is used. flags should be
        either @c Field::flag_nullable or 0. If set to
        @c Field::flag_nullable, indicates that the field may be nullable.
        default_value specifies a value to be returned by @c BoolField::val()
        for null fields.

        Preconditions:
            - The ExtentType of the series must have a bool field with the
              specified name. If the type of the series is not known, this
              check will be delayed until the type is set. */
    BoolFieldImpl(ExtentSeries &dataseries, const std::string &field, 
                  int flags, bool default_value) 
        : FixedField(dataseries, field, ExtentType::ft_bool, flags),
          default_value(default_value), bit_mask(0)
    { }

    /** The value returned by val for null fields. */
    bool default_value;

    virtual void newExtentType();
protected:
    bool val(const Extent &e, uint8_t *row_pos) const {
        DEBUG_SINVARIANT(&e != NULL);
        uint8_t *byte_pos = row_pos + offset;
        DEBUG_SINVARIANT(e.insideExtentFixed(byte_pos));
        if (nullable && isNull(e, row_pos)) {
            return default_value;
        } else {
            return *byte_pos & bit_mask ? true : false; 
        }
    }

    void set(const Extent &e, uint8_t *row_pos, bool val) {
        DEBUG_SINVARIANT(&e != NULL);
        uint8_t *byte_pos = row_pos + offset;
        DEBUG_SINVARIANT(e.insideExtentFixed(byte_pos));
        if (val) {
            *byte_pos = *byte_pos | bit_mask;
        } else {
            *byte_pos = *byte_pos & ~bit_mask;
        }
        setNull(e, row_pos, false);
    }

private:
    byte bit_mask;
};
}}

class BoolField 
    : public dataseries::detail::CommonFixedField<bool, dataseries::detail::BoolFieldImpl> {
public:
    /** Creates a new @c BoolField associated with the specified series.
        An empty string as the field name means that the name of the
        field will be specified later.  It must be set via
        @c Field::setFieldName before the field is used. flags should be
        either @c Field::flag_nullable or 0. If set to
        @c Field::flag_nullable, indicates that the field may be nullable.
        default_value specifies a value to be returned by @c BoolField::val()
        for null fields.

        Preconditions:
            - The ExtentType of the series must have a bool field with the
              specified name. If the type of the series is not known, this
              check will be delayed until the type is set. */
    BoolField(ExtentSeries &dataseries, const std::string &field, 
              int flags = 0, bool default_value = false, bool auto_add = true)
        : dataseries::detail::CommonFixedField<bool, BoolFieldImpl>
                  (dataseries, field, flags, default_value)
    {
        if (auto_add) {
            dataseries.addField(*this);
        }
    }

    friend class GF_Bool;
};

#endif
