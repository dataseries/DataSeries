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
        DEBUG_INVARIANT(dataseries.extent() != NULL && offset_pos >= 0,
                        "internal error; extent not set or field not ready");
        if (isNull()) {
            return reinterpret_cast<const byte *>(default_value.data());
        } else {
            return val(dataseries.extent()->variabledata, getVarOffset());
        }
    }
    int32 size() const {
        DEBUG_INVARIANT(dataseries.extent() != NULL && offset_pos >= 0,
                        "internal error; extent not set or field not ready");
        if (isNull()) {
            return default_value.size();
        } else {
            // getVarOffset() has checked that the size is valid.
            return size(dataseries.extent()->variabledata,getVarOffset());
        }
    }
    std::string stringval() const {
        if (isNull()) {
            return default_value;
        } else {
            return std::string((char *)val(),size());
        }
    }

    const byte *val(const Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        if (isNull(e, row_offset)) {
            return reinterpret_cast<const byte *>(default_value.data());
        } else {
            return val(e.variabledata, getVarOffset(e, row_offset));
        }
    }

    int32 size(const Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        if (isNull(e, row_offset)) {
            return default_value.size();
        } else {
            return size(e.variabledata, getVarOffset(e, row_offset));
        }
    }

    const byte *operator ()(const Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        return val(e, row_offset);
    }

    std::string stringval(Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        if (isNull(e, row_offset)) {
            return default_value;
        } else {
            return std::string(reinterpret_cast<const char *>(val(e, row_offset)), 
                               size(e, row_offset));
        }
    }

    // TODO-eric: refactor properly
    void set(Extent &e, const dataseries::SEP_RowOffset &row_offset, const std::string &val) {
        allocateSpace(e, e.fixeddata.begin() + row_offset.row_offset + offset_pos, val.size());
        partialSet(e, row_offset, val.data(), val.size(), 0);
        setNull(e, row_offset, false);
    }

    /// Allocate, data_size bytes of space.  The space may not be
    /// initialized.  This function is intended to be used with
    /// partialSet in order to efficiently put together multiple parts
    /// into a single variable32 value.
    void allocateSpace(uint32_t data_size);

    /// Overwrite @param data_size bytes at offset @param offset with
    /// the bytes @param data  Invalid to call with data_size +
    /// offset > currently allocated space.
    /// 
    /// @param data The data to set into the field.
    /// @param data_size Number of bytes to copy from data into the field.
    /// @param offset Offset in bytes from the start of the field for the first copied byte.
    void partialSet(const void *data, uint32_t data_size, uint32_t offset);

    void partialSet(Extent &e, const dataseries::SEP_RowOffset &row_offset,
                    const void *data, uint32_t data_size, uint32_t offset);

    void set(const void *data, int32 datasize) {
	allocateSpace(datasize);
	partialSet(data, datasize, 0);
    }

    void set(const std::string &data) { // note this doesn't propagate the C '\0' at the end (neither does a C++ string)
        set(data.data(),data.size());
    }
    void nset(const std::string &val, const std::string &null_val) {
        if (val == null_val) {
            setNull(true);
        } else {
            set(val);
        }
    }
    void set(Variable32Field &from) {
        set(from.val(),from.size());
    }
    void clear() {
        DEBUG_INVARIANT(dataseries.extent() != NULL,  "internal error; extent not set");
        clear(*dataseries.extent(), recordStart() + offset_pos);
	setNull(false);
    }

    void clear(Extent &e, const dataseries::SEP_RowOffset &row_offset) {
        clear(e, e.fixeddata.begin() + row_offset.row_offset + offset_pos);
        setNull(e, row_offset, false);
    }

    bool equal(const std::string &to) {
        if ((int)to.size() != size())
            return false;
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
    void allocateSpace(Extent &e, byte *fixed_data_ptr, uint32_t data_size);
    void clear(Extent &e, byte *fixed_data_ptr) {
        DEBUG_SINVARIANT(e.insideExtentFixed(fixed_data_ptr));
        *reinterpret_cast<int32_t *>(fixed_data_ptr) = 0;
	DEBUG_SINVARIANT(*reinterpret_cast<int32_t *>(e.variabledata.begin()) == 0);
    }        
    

    virtual void newExtentType();
    friend class Extent;
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

    int32 getVarOffset() const {
        DEBUG_SINVARIANT(dataseries.extent() != NULL);
#if LINTEL_DEBUG
        dataseries.checkOffset(offset_pos);
#endif
        int32 varoffset = getVarOffset(recordStart(), offset_pos);
#if LINTEL_DEBUG
        selfcheck(varoffset);
#endif
        return varoffset;
    }

    int32 getVarOffset(const Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        DEBUG_SINVARIANT(&e.getType() == dataseries.getType());
        byte *record_start = e.fixeddata.begin() + row_offset.row_offset;
        IF_LINTEL_DEBUG(e.insideExtentFixed(record_start + offset_pos));
        int32 var_offset = getVarOffset(record_start, offset_pos);
        IF_LINTEL_DEBUG(selfcheck(e.variabledata, var_offset));
        return var_offset;
    }

    void selfcheck() const {
        DEBUG_SINVARIANT(dataseries.extent() != NULL);
        dataseries.checkOffset(offset_pos);
        int32 varoffset = getVarOffset(recordStart(), offset_pos);
        selfcheck(varoffset);
    }
    void selfcheck(int32 varoffset) const {
        selfcheck(dataseries.extent()->variabledata,varoffset);
    }
    static void selfcheck(const Extent::ByteArray &varbytes, int32 varoffset);

    static int32 roundupSize(int32 size) {
        return size + (12 - (size % 8)) % 8;
    }
    int offset_pos;
    bool unique;
};

#endif
