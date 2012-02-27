// -*-C++-*-
/*
   (c) Copyright 2003-2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    fixed size data base field
*/

#ifndef DATASERIES_FIXEDFIELD_HPP
#define DATASERIES_FIXEDFIELD_HPP

#include <DataSeries/Field.hpp>

/** \brief Base class for fixed size fields. */
class FixedField : public Field {
public:
    typedef ExtentType::byte byte;
    
protected:
    FixedField(ExtentSeries &dataseries, const std::string &field, 
	       ExtentType::fieldType ft, int flags);

    virtual void newExtentType();

    // -1 is used as a flag for uninitialized, otherwise would be uint32_t
    int32_t field_size, offset;
    ExtentType::fieldType fieldtype;
};

namespace dataseries { namespace detail {

template<typename T, class PT> class CommonFixedField : public PT {
public:
    CommonFixedField(ExtentSeries &series, const std::string &field,
                     int flags, T default_value)
        : PT(series, field, flags, default_value) { }

    /** Returns the value of the field in the @c ExtentSeries' current record.
        If the field is null returns the default value.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record. */
    T val() const { 
        return this->PT::val(this->dataseries.getExtentRef(), this->rowPos());
    }

    /** Returns the value of the field in the @c ExtentSeries' current record.
        If the field is null returns the default value.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record. */
    T operator() () const {
	return val();
    }

    /** Sets the value of the field in the @c ExtentSeries' current record.
        The field will never be null immediately after a call to set(),
        regardless of whether the argument is the same as the default value.

        Preconditions:
            - The name of the Field must have been set and the associated
              @c ExtentSeries must have a current record. */
    void set(T val) {
        this->PT::set(this->dataseries.getExtentRef(), this->rowPos(), val);
    }

    /** Returns the value of the field in the specified extent and row_offset.
        If the field is null returns the default value.

        Preconditions:
        - The name of the Field must have been set. */
    T val(const Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        return this->PT::val(e, this->rowPos(e, row_offset));
    }

    /** Returns the value of the field in the specified extent and row_offset.
        If the field is null returns the default value.

        Preconditions:
        - The name of the Field must have been set. */
    T operator ()(const Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        return val(e, row_offset);
    }

    /** Sets the value of the field in the specified extent and row_offset.
        Sets the field to not null.

        Preconditions:
        - The name of the Field must have been set. */
    void set(Extent &e, const dataseries::SEP_RowOffset &row_offset, T val) {
        this->PT::set(e, this->rowPos(e, row_offset), val);
    }
};

template<typename T>
struct CppTypeToFieldType { 
    //	    static const ExtentType::fieldType ft() 
};

template<>
struct CppTypeToFieldType<uint8_t> {
    static const ExtentType::fieldType ft = ExtentType::ft_byte;
};
template<>
struct CppTypeToFieldType<int32_t> {
    static const ExtentType::fieldType ft = ExtentType::ft_int32;
};
template<>
struct CppTypeToFieldType<int64_t> {
    static const ExtentType::fieldType ft = ExtentType::ft_int64;
};
template<>
struct CppTypeToFieldType<double> {
    static const ExtentType::fieldType ft = ExtentType::ft_double;
};

template<typename T> class SimpleFixedFieldImpl : public FixedField {
public:
    SimpleFixedFieldImpl(ExtentSeries &series, const std::string &field,
                         int flags, T default_value) 
        : FixedField(series, field, CppTypeToFieldType<T>::ft, flags), default_value(default_value)
    { }

    T default_value;
    
protected:
    T val(const Extent &e, uint8_t *row_pos) const {
        DEBUG_SINVARIANT(&e != NULL);
        uint8_t *byte_pos = row_pos + offset;
        DEBUG_SINVARIANT(e.insideExtentFixed(byte_pos));
        if (nullable && isNull(e, row_pos)) {
            return default_value;
        } else {
            return *reinterpret_cast<T *>(byte_pos);
        }
    }

    void set(Extent &e, uint8_t *row_pos, T val) {
        DEBUG_SINVARIANT(&e != NULL);
        uint8_t *byte_pos = row_pos + offset;
        DEBUG_SINVARIANT(e.insideExtentFixed(byte_pos));
        *reinterpret_cast<T *>(byte_pos) = val;
        setNull(e, row_pos, false);
    }
};

template<typename T> class SimpleFixedField 
    : public CommonFixedField<T, SimpleFixedFieldImpl<T> > {
public:
    SimpleFixedField(ExtentSeries &series, const std::string &field,
                     int flags, T default_value) 
        : CommonFixedField<T, SimpleFixedFieldImpl<T> >(series, field, flags, default_value)
    { }

    // nset is used for data formats where a sentintal value was used as "null" or "default". Note
    // that in the typical use case, the sentinal value is not typically the same as the value you
    // would want to use when blindly reading the value.  That is, the default_value is not going
    // to be the same as the null sentinal.
    void nset(T val, T null_val = static_cast<T>(-1)) {
	if (val == null_val) {
	    this->setNull(true);
	} else {
	    this->set(val);
	}
    }
};

}} // namespace

#endif
