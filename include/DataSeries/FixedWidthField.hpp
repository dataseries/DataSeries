// -*-C++-*-
/*
   (c) Copyright 2003-2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Byte array class
*/

// TODO: need to have tests written for this class.
#ifndef DATASERIES_FIXEDWIDTHFIELD_HPP
#define DATASERIES_FIXEDWIDTHFIELD_HPP

#include <vector>

#include <DataSeries/FixedField.hpp>

/** \brief Accessor for byte array fields. */
class FixedWidthField : public FixedField {
public:
    FixedWidthField(ExtentSeries &_dataseries, const std::string &field,
                    int flags = 0, bool auto_add = true);

    /** Returns the value of the field in the @c ExtentSeries' current record.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record. */
    const byte *val() const {
        DEBUG_SINVARIANT(dataseries.extent() != NULL);
        return val(*dataseries.extent(), rowPos());
    }

    /** Returns the value of the field in the @c ExtentSeries' current record.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record. */
    const byte *val(const Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        return val(e, rowPos(e, row_offset));
    }

    /** Returns the size of the field (in bytes). */
    int32_t size() const {
        return field_size;
    }

    /** Returns the value of the field in the @c ExtentSeries' current record.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record. */
    std::vector<uint8_t> arrayVal() const {
        if (isNull()) {
            return std::vector<uint8_t>();
        } else {
            const uint8_t *v = val();
            return std::vector<uint8_t>(v, v + size());
        }
    }

    /** Returns the value of the field in the @c ExtentSeries' current record.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record. */
    std::vector<uint8_t>
    arrayVal(const Extent &e, const dataseries::SEP_RowOffset &row_offset) const {
        if (isNull(e, row_offset)) {
            return std::vector<uint8_t>();
        } else {
            const uint8_t *v = val(e, row_offset);
            return std::vector<uint8_t>(v, v + size());
        }
    }

    /** Sets the value of the field in the @c ExtentSeries' current record.
        Note that @param val must be the correct size (or NULL)

        Preconditions:
            - The name of the Field must have been set and the associated
              @c ExtentSeries must have a current record. 

	@param val source value for the copy
	@param val_size size of the value
    */
    void set(const void *val, uint32_t val_size = 0) {
        DEBUG_SINVARIANT(dataseries.extent() != NULL);
        set(*dataseries.extent(), rowPos(), val, val_size);
    }

    /** Sets the value of the field in the @c ExtentSeries' current record.
        Note that @param val must be the correct size (or NULL)

        Preconditions:
            - The name of the Field must have been set and the associated
              @c ExtentSeries must have a current record. 

        @param e Extent in which set the value
        @param row_offset Offset of the row to be set
	@param val source value for the copy
	@param val_size size of the value
    */
    void set(Extent &e, const dataseries::SEP_RowOffset &row_offset,
             const void *val, uint32_t val_size = 0) {
        set(e, rowPos(e, row_offset), val, val_size);
    }

    /** Sets the value of the field in the @c ExtentSeries' current record.
        Note that @param val must be the correct size.

        Preconditions:
            - The name of the Field must have been set and the associated
              @c ExtentSeries must have a current record. 

	@param val source value for the copy
    */
    void set(const std::vector<uint8_t> &val) {
        set(&val[0], val.size());
    }

    /** Sets the value of the field in the @c ExtentSeries' current record.
        Note that @param val must be the correct size.

        Preconditions:
            - The name of the Field must have been set and the associated
              @c ExtentSeries must have a current record. 

        @param e The extent to update
        @param row_offset The row to update
	@param val source value for the copy
    */
    void set(Extent &e, const dataseries::SEP_RowOffset &row_offset,
             const std::vector<uint8_t> &val) {
        set(e, row_offset, &val[0], val.size());
    }

    // TODO: should implment nset as well

    friend class GF_FixedWidth;
private:
    void set(const Extent &e, uint8_t *row_pos, const void *val, uint32_t val_size) {
        DEBUG_SINVARIANT(&e != NULL);
        if (val == NULL) {
            setNull(e, row_pos, true);
            return;
        }
	DEBUG_SINVARIANT(val_size == static_cast<uint32_t>(field_size));
	(void)val_size;

        uint8_t *byte_pos = row_pos + offset;
        DEBUG_SINVARIANT(e.insideExtentFixed(byte_pos));
        
        memmove(byte_pos, val, field_size);
        setNull(e, row_pos, false);
    }

    const byte *val(const Extent &e, uint8_t *row_pos) const {
        DEBUG_SINVARIANT(&e != NULL);
        if (nullable && isNull(e, row_pos)) {
            return NULL;
        } else {
            uint8_t *byte_pos = row_pos + offset;
            DEBUG_SINVARIANT(e.insideExtentFixed(byte_pos));
            return byte_pos;
        }
    }
};

#endif
