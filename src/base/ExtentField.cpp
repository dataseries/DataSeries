/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <Lintel/Double.hpp>
#include <DataSeries/ExtentField.hpp>

using namespace std;
using boost::format;

Field::Field(ExtentSeries &_dataseries, const std::string &_fieldname, 
	     uint32_t _flags)
    : nullable(0),null_offset(0), null_bit_mask(0), 
      dataseries(_dataseries), flags(_flags), fieldname(_fieldname) 
{ }

Field::~Field() {
    dataseries.removeField(*this, false);
}

void Field::setFieldName(const string &new_name) {
    INVARIANT(!new_name.empty(), "Can't set field name to empty");
    fieldname = new_name;
    if (dataseries.getType() != NULL) {
	newExtentType();
    }
}

void Field::newExtentType() {
    SINVARIANT(!fieldname.empty());
    if (flags & flag_nullable) {
	nullable = dataseries.type->getNullable(fieldname);
	if (nullable) {
	    std::string nullfieldname = ExtentType::nullableFieldname(fieldname);
	    SINVARIANT(dataseries.type->getFieldType(nullfieldname) == ExtentType::ft_bool);
	    null_offset = dataseries.type->getOffset(nullfieldname);
	    SINVARIANT(null_offset >= 0);
	    int bitpos = dataseries.type->getBitPos(nullfieldname);
	    SINVARIANT(bitpos >= 0);
	    null_bit_mask = 1 << bitpos;
	} else {
	    null_offset = 0;
	    null_bit_mask = 0;
	}
    } else {
	INVARIANT(dataseries.type->getNullable(fieldname) == false,
		  format("field %s accessor doesn't support nullable fields")
		  % getName());
    }
}

FixedField::FixedField(ExtentSeries &_dataseries, const std::string &field, 
		       ExtentType::fieldType ft, int flags)
    : Field(_dataseries,field, flags), field_size(-1), offset(-1),
      fieldtype(ft)
{
    INVARIANT(dataseries.getType() == NULL || field.empty() || 
	      dataseries.getType()->getFieldType(field) == ft,
	      format("mismatch on field types for field %s in type %s")
	      % field % dataseries.getType()->getName());
}

void FixedField::newExtentType() {
    if (getName().empty())
	return; // Don't have a name yet.
    Field::newExtentType();
    field_size = dataseries.getType()->getSize(getName());
    offset = dataseries.getType()->getOffset(getName());
    INVARIANT(dataseries.getType()->getFieldType(getName()) == fieldtype,
	      format("mismatch on field types for field named %s in type %s")
	      % getName() % dataseries.getType()->getName());
}

namespace dataseries { namespace detail {

void BoolFieldImpl::newExtentType() {
    FixedField::newExtentType();
    if (getName().empty())
	return; // Not ready yet
    int bitpos = dataseries.getType()->getBitPos(getName());
    bit_mask = (byte)(1 << bitpos);
}

}}

FixedWidthField::FixedWidthField(ExtentSeries &_dataseries, const std::string &field,
                                 int flags, bool auto_add)
    : FixedField(_dataseries, field, ExtentType::ft_fixedwidth, flags)
{
    if (auto_add) {
        dataseries.addField(*this);
    }
}

void DoubleField::newExtentType() {
    if (getName().empty()) {
	return;
    }
    FixedField::newExtentType();
    base_val = dataseries.getType()->getDoubleBase(getName());
    INVARIANT(flags & flag_allownonzerobase || base_val == 0,
	      format("accessor for field %s doesn't support non-zero base")
	      % getName());
}

const std::string Variable32Field::empty_string("");

Variable32Field::Variable32Field(ExtentSeries &_dataseries, 
				 const std::string &field, int flags, 
				 const std::string &_default_value,
				 bool auto_add) 
    : Field(_dataseries,field,flags), default_value(_default_value), 
      offset_pos(-1), unique(false)
{ 
    if (auto_add) {
	dataseries.addField(*this);
    }
}

void Variable32Field::newExtentType() {
    Field::newExtentType();
    offset_pos = dataseries.getType()->getOffset(getName());
    unique = dataseries.getType()->getUnique(getName());
    INVARIANT(dataseries.getType()->getFieldType(getName()) 
	      == ExtentType::ft_variable32,
	      format("mismatch on field types for field named %s in type %s")
	      % getName() % dataseries.getType()->getName());
}

void Variable32Field::allocateSpace(Extent *e, uint8_t *row_pos, uint32_t data_size) {
    DEBUG_SINVARIANT(e != NULL);
    DEBUG_SINVARIANT(offset_pos >= 0);
    uint8_t *fixed_data_ptr = row_pos + offset_pos;
    DEBUG_SINVARIANT(e->insideExtentFixed(fixed_data_ptr));
    SINVARIANT(data_size <= static_cast<uint32_t>(numeric_limits<int32_t>::max()));

    if (data_size == 0) {
	clear(*e, row_pos);
	return;
    }
    int32_t roundup = roundupSize(data_size);
    DEBUG_SINVARIANT((roundup+4) % 8 == 0);
		    
    // TODO: we can eventually decide to support overwrites at some
    // point, but it doesn't seem worth it now since I (Eric) don't
    // think that feature is used at all -- pack_unique is effectively
    // the default.  See revs before 2009-05-19 for the version that
    // supports overwrite.

    // need to repack at the end of the variable data
    int32_t varoffset = e->variabledata.size();
    e->variabledata.resize(varoffset + 4 + roundup);
    *reinterpret_cast<int32_t *>(fixed_data_ptr) = varoffset;

    int32_t *var_data = reinterpret_cast<int32_t *>(vardata(e->variabledata, varoffset));
					      
    *var_data = data_size;

#if LINTEL_DEBUG
    // we get to avoid zeroing since it happens automatically for us
    // when we resize the bytearray
    for(++var_data; roundup > 0; roundup -= 4) {
	SINVARIANT(*var_data == 0);
    }

    selfcheck(e->variabledata,varoffset);
#endif

    setNull(*e, row_pos, false);
}    

void Variable32Field::partialSet(Extent *e, uint8_t *row_pos, 
                                 const void *data, uint32_t data_size, uint32_t offset) {
    DEBUG_SINVARIANT(e != NULL);
    if (data_size == 0) {
	return; // occurs on set("");
    }
    SINVARIANT(data_size <= static_cast<uint32_t>(numeric_limits<int32_t>::max()));
    SINVARIANT(offset <= static_cast<uint32_t>(numeric_limits<int32_t>::max()));

    // TODO: this is almost like rawval() in fixedfield; think about unifying?
    int32_t varoffset = *reinterpret_cast<int32_t *>(row_pos + offset_pos);
    int32_t *var_data = reinterpret_cast<int32_t *>(vardata(e->variabledata, varoffset));
    
    uint32_t cur_size = *var_data;
    INVARIANT(offset < cur_size && data_size <= cur_size && offset + data_size <= cur_size,
	      boost::format("%d + %d > %d") % offset % data_size % cur_size);
    uint8_t *var_bits = reinterpret_cast<uint8_t *>(var_data + 1); // + 1 as it's int32_t
    memcpy(var_bits + offset, data, data_size);
}


void Variable32Field::selfcheck(const Extent::ByteArray &varbytes, int32 varoffset) {
    INVARIANT(varoffset >= 0 && (uint32_t)varoffset <= (varbytes.size() - 4),
	      format("Internal error, bad variable offset %d") % varoffset);
    if (varoffset == 0) {
	// special case this check, as it is slightly different
	INVARIANT(*(int32 *)varbytes.begin() == 0, "Whoa, zero string got a size");
	return;
    }
    INVARIANT((varoffset % 4) == 0, "Internal error, bad variable offset");
    INVARIANT(varoffset == 0 || ((varoffset + 4) % 8) == 0,
	      "Internal error, bad variable offset");
    int32 size = *(int32 *)(vardata(varbytes, varoffset));
    INVARIANT(size >= 0, "Internal error, bad variable size");
    int32 roundup = roundupSize(size);
    INVARIANT((unsigned int)(varoffset + 4 + roundup) <= varbytes.size(),
	      "Internal error, bad variable offset");
    for(int32 i=size;i<roundup;++i) {
	INVARIANT(*(byte *)vardata(varbytes,varoffset + 4 + i) == 0,
		  "Internal error, bad padding");
    }
}
