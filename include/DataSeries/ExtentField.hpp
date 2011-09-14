// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    accessor classes for fields in an extent; going to become a
    meta include file for all the field files.
*/

#ifndef EXTENT_FIELD_H
#define EXTENT_FIELD_H

#include <Lintel/DebugFlag.hpp>

#include <DataSeries/Field.hpp>

// TODO: make a private function in the various fields that accesses
// a field directly; then use it in Extent::packData rather than
// duplicating the code there to convert raw to real data.  Might be able
// to use the new SubExtentPointer work for this goal.

#include <DataSeries/FixedField.hpp>

#include <DataSeries/BoolField.hpp>
#include <DataSeries/ByteField.hpp>
#include <DataSeries/Int32Field.hpp>
#include <DataSeries/Int64Field.hpp>
#include <DataSeries/Int64TimeField.hpp>
#include <DataSeries/DoubleField.hpp>
#include <DataSeries/FixedWidthField.hpp>
#include <DataSeries/Variable32Field.hpp>

#include <DataSeries/TFixedField.hpp>

#endif
