// -*-C++-*-
/*
   (c) Copyright 2003-2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Byte array class
*/

#ifndef DATASERIES_FIXEDWIDTHFIELD_HPP
#define DATASERIES_FIXEDWIDTHFIELD_HPP

/** \brief Accessor for byte array fields. */
class FixedWidthField : public FixedField {
public:
    FixedWidthField(ExtentSeries &_dataseries, const std::string &field,
                    int flags = 0, bool auto_add = true);

    /** Returns the value of the field in the @c ExtentSeries' current record.

        Preconditions:
            - The name of the Field must have been set and the
              @c ExtentSeries must have a current record. */
    byte* val() const {
        if (isNull()) {
            return NULL;
        }
        return rawval();
    }

    /** Returns the size of the field (in bytes). */
    int size() const {
        return _size;
    }

    /** Sets the value of the field in the @c ExtentSeries' current record.
        Note that @param val must be of sufficient size (or NULL).

        Preconditions:
            - The name of the Field must have been set and the associated
              @c ExtentSeries must have a current record. */
    void set(byte *val) {
        if (val == NULL) {
            setNull(true);
            return;
        }
        memcpy(rawval(), val, _size);
        setNull(false);
    }
};

#endif
