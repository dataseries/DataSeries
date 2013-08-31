// -*-C++-*-
/*
  (c) Copyright 2003-2011, Hewlett-Packard Development Company, LP

  See the file named COPYING for license details
*/

/** @file
    variable32 field class
*/

#ifndef DATASERIES_VARIABLE32FIELD_HPP
#define DATASERIES_VARIABLE32FIELD_HPP

#include <DataSeries/FixedField.hpp>

/** \brief Accessor for variable32 fields. */
class Variable32Field : public Field {
  public:
    typedef ExtentType::byte byte;
    typedef ExtentType::int32 int32;
    static const std::string empty_string;

    Variable32Field(ExtentSeries &_dataseries, const std::string &field, 
                    int flags = 0, 
                    const std::string &default_value = empty_string,
                    bool auto_add = true);

    const byte *val() const {
        return val(dataseries.getExtentRef(), rowPos());
    }

    int32 size() const {
        return size(dataseries.getExtentRef(), rowPos());
    }

    std::string stringval() const {
        return stringval(dataseries.getExtentRef(), rowPos());
    }

    const byte *val(const Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        return val(e, rowPos(e, row_offset));
    }

    int32 size(const Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        return size(e, rowPos(e, row_offset));
    }

    std::string stringval(const Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        return stringval(e, rowPos(e, row_offset));
    }

    /// Allocate, data_size bytes of space.  The space may not be
    /// initialized.  This function is intended to be used with
    /// partialSet in order to efficiently put together multiple parts
    /// into a single variable32 value.
    void allocateSpace(uint32_t data_size) {
        allocateSpace(dataseries.getExtentRef(), rowPos(), data_size);
    }

    /// Allocate, data_size bytes of space in Extent e for row row_offset.  The space may not be
    /// initialized.  This function is intended to be used with partialSet in order to efficiently
    /// put together multiple parts into a single variable32 value.
    void allocateSpace(Extent &e, const dataseries::SEP_RowOffset &row_offset, uint32_t data_size) {
        allocateSpace(e, rowPos(e, row_offset), data_size);
    }

    /// Overwrite @param data_size bytes at offset @param offset with
    /// the bytes @param data  Invalid to call with data_size +
    /// offset > currently allocated space.
    /// 
    /// @param data The data to set into the field.
    /// @param data_size Number of bytes to copy from data into the field.
    /// @param offset Offset in bytes from the start of the field for the first copied byte.
    void partialSet(const void *data, uint32_t data_size, uint32_t offset) {
        partialSet(dataseries.getExtentRef(), rowPos(), data, data_size, offset);
    }

    /// Overwrite @param data_size bytes at offset @param offset with
    /// the bytes @param data  Invalid to call with data_size +
    /// offset > currently allocated space.
    /// 
    /// @param e The extent to update.
    /// @param row_offset The row to update.
    /// @param data The data to set into the field.
    /// @param data_size Number of bytes to copy from data into the field.
    /// @param offset Offset in bytes from the start of the field for the first copied byte.
    void partialSet(Extent &e, const dataseries::SEP_RowOffset &row_offset,
                    const void *data, uint32_t data_size, uint32_t offset) {
        partialSet(e, rowPos(e, row_offset), data, data_size, offset);
    }

    void set(const void *data, int32 data_size) {
        set(dataseries.getExtentRef(), rowPos(), data, data_size);
    }

    void set(Extent &e, const dataseries::SEP_RowOffset &row_offset,
             const void *data, uint32_t data_size) {
        set(e, rowPos(e, row_offset), data, data_size);
    }

    void set(const std::string &data) { // note this doesn't propagate the C '\0' at the end (neither does a C++ string)
        set(data.data(), data.size());
    }

    void set(Extent &e, const dataseries::SEP_RowOffset &row_offset, const std::string &val) {
        set(e, row_offset, val.data(), val.size());
    }

    void set(Variable32Field &from) {
        set(from.val(),from.size());
    }

    // TODO-reviewer: should we also have a set(source_e, source_row_offset, from, from_e,
    // from_row_offset)?  This one is ambiguious as to whether e & row_offset apply to from.
    // We then also get set(from, from_e, from_row_offset);  Same question applies to
    // GeneralField::set(GeneralField &from)
    void set(Extent &e, const dataseries::SEP_RowOffset &row_offset, Variable32Field &from) {
        set(e, row_offset, from.val(), from.size());
    }

    void nset(const std::string &val, const std::string &null_val) {
        if (val == null_val) {
            setNull(true);
        } else {
            set(val);
        }
    }

    void nset(Extent &e, const dataseries::SEP_RowOffset &row_offset,
              const std::string &val, const std::string &null_val) {
        if (val == null_val) {
            setNull(e, row_offset, true);
        } else {
            set(e, row_offset, val);
        }
    }

    void clear() {
        DEBUG_SINVARIANT(dataseries.hasExtent());
        clear(dataseries.getExtentRef(), rowPos());
    }

    void clear(Extent &e, const dataseries::SEP_RowOffset &row_offset) {
        clear(e, rowPos(e, row_offset));
    }

    bool equal(const std::string &to) {
        if (isNull()) {
            return false;
        }
        if (to.size() != static_cast<size_t>(size())) {
            return false;
        }
        return memcmp(to.data(),val(),to.size()) == 0;
    }

    bool equal(const Variable32Field &to) {
        if (isNull()) {
            return to.isNull();
        }
        if (to.isNull()) {
            return false;
        }
        return size() == to.size() && memcmp(val(), to.val(), size()) == 0;
    }
    std::string default_value;
  protected:
    friend class Extent;
    friend class GF_Variable32;

    void clear(Extent &e, uint8_t *row_offset) {
        byte *fixed_data_ptr = row_offset + offset_pos;
        DEBUG_SINVARIANT(e.insideExtentFixed(fixed_data_ptr));
        *reinterpret_cast<int32_t *>(fixed_data_ptr) = 0;
        DEBUG_SINVARIANT(*reinterpret_cast<int32_t *>(e.variabledata.begin()) == 0);
        setNull(e, row_offset, false);
    }        

    void set(Extent &e, uint8_t *row_pos, const void *data, uint32_t data_size) {
        allocateSpace(e, row_pos, data_size);
        partialSet(e, row_pos, data, data_size, 0);
    }

    void allocateSpace(Extent &e, uint8_t *row_pos, uint32_t data_size);
    void partialSet(Extent &e, uint8_t *row_pos, 
                    const void *data, uint32_t data_size, uint32_t offset);

    virtual void newExtentType();
    static void *vardata(const Extent::ByteArray &varbytes, int32 offset) {
        return (void *)(varbytes.begin() + offset);
    }
    static int32 size(const Extent::ByteArray &varbytes, int32 varoffset) {
        return *(int32 *)(varbytes.begin() + varoffset);
    }            
    static byte *val(const Extent::ByteArray &varbytes, int32 varoffset) {
        return (byte *)vardata(varbytes,varoffset+4);
    }
    static int32 getVarOffset(const byte *record, int offset) {
        return *(const int32 *)(record + offset);
    }

    int32_t getVarOffset(const Extent &e, uint8_t *row_pos) const {
        DEBUG_SINVARIANT(&e != NULL);
        DEBUG_SINVARIANT(offset_pos >= 0);
        DEBUG_SINVARIANT(e.insideExtentFixed(row_pos + offset_pos));
        int32 var_offset = getVarOffset(row_pos, offset_pos);
        IF_LINTEL_DEBUG(selfcheck(e.variabledata, var_offset));
        return var_offset;
    }
        
    std::string stringval(const Extent &e, uint8_t *row_pos) const {
        DEBUG_SINVARIANT(&e != NULL);
        if (nullable && isNull(e, row_pos)) {
            return default_value;
        } else {
            return std::string(reinterpret_cast<const char *>(val(e, row_pos)), size(e, row_pos));
        }
    }

    void selfcheck() const {
        DEBUG_SINVARIANT(dataseries.hasExtent());
        dataseries.checkOffset(offset_pos);
        int32 varoffset = getVarOffset(recordStart(), offset_pos);
        selfcheck(varoffset);
    }
    void selfcheck(int32 varoffset) const {
        selfcheck(dataseries.getExtentRef().variabledata,varoffset);
    }
    static void selfcheck(const Extent::ByteArray &varbytes, int32 varoffset);

    static int32 roundupSize(int32 size) {
        return size + (12 - (size % 8)) % 8;
    }
    int offset_pos;
    bool unique;

  private:
    const byte *val(const Extent &e, uint8_t *row_pos) const {
        DEBUG_SINVARIANT(&e != NULL);
        if (nullable && isNull(e, row_pos)) {
            return reinterpret_cast<const byte *>(default_value.data());
        } else {
            return val(e.variabledata, getVarOffset(e, row_pos));
        }
    }

    int32_t size(const Extent &e, uint8_t *row_pos) const {
        DEBUG_SINVARIANT(&e != NULL);
        if (nullable && isNull(e, row_pos)) {
            return default_value.size();
        } else {
            return size(e.variabledata, getVarOffset(e, row_pos));
        }
    }        
};

#endif
