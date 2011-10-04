// -*-C++-*-
/*
   (c) Copyright 2003-2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    byte field class
*/

#ifndef DATASERIES_BYTEFIELD_HPP
#define DATASERIES_BYTEFIELD_HPP

/** \brief Accessor for byte fields
  */
class ByteField : public dataseries::detail::SimpleFixedField<uint8_t> {
public:
    ByteField(ExtentSeries &dataseries, const std::string &field, 
	      int flags = 0, byte default_value = 0, bool auto_add = true)
        : dataseries::detail::SimpleFixedField<uint8_t>(dataseries, field, flags, default_value)
    { 
        if (auto_add) {
            dataseries.addField(*this);
        }
    }

    friend class GF_Byte;
};

#endif
