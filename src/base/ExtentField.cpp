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
{
}

Field::~Field()
{
    // could be not present if the constructor failed somehow.  TODO:
    // think about how best to handle this since the call in the
    // type-specific fields that registers the field is wrong if the
    // series already has a type.  If we can figure out that side,
    // then we can also fix this side.
    dataseries.removeField(*this, false);
}

void
Field::setFieldName(const string &new_name)
{
    INVARIANT(!new_name.empty(), "Can't set field name to empty");
    fieldname = new_name;
    if (dataseries.getType() != NULL) {
	newExtentType();
    }
}

void 
Field::newExtentType()
{
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
    : Field(_dataseries,field, flags), size(-1), offset(-1),
      fieldtype(ft)
{
    INVARIANT(dataseries.type == NULL || field.empty() || 
	      dataseries.type->getFieldType(field) == ft,
	      format("mismatch on field types for field %s in type %s")
	      % field % dataseries.type->name);
}

FixedField::~FixedField()
{
}

void
FixedField::newExtentType()
{
    if (getName().empty())
	return; // Don't have a name yet.
    Field::newExtentType();
    size = dataseries.type->getSize(getName());
    offset = dataseries.type->getOffset(getName());
    INVARIANT(dataseries.type->getFieldType(getName()) == fieldtype,
	      format("mismatch on field types for field named %s in type %s")
	      % getName() % dataseries.type->getName());
}

BoolField::BoolField(ExtentSeries &_dataseries, const std::string &field, 
		     int flags, bool _default_value, bool auto_add)
    : FixedField(_dataseries,field, ExtentType::ft_bool,flags),
      default_value(_default_value), bit_mask(0)
{ 
    if (auto_add) {
	dataseries.addField(*this);
    }
}

void 
BoolField::newExtentType()
{
    FixedField::newExtentType();
    if (getName().empty())
	return; // Not ready yet
    int bitpos = dataseries.type->getBitPos(getName());
    bit_mask = (byte)(1 << bitpos);
}

ByteField::ByteField(ExtentSeries &_dataseries, const std::string &field, 
		     int flags, byte _default_value, bool auto_add)
    : FixedField(_dataseries,field,ExtentType::ft_byte,flags),
      default_value(_default_value)
{ 
    if (auto_add) {
	dataseries.addField(*this);
    }
}

Int32Field::Int32Field(ExtentSeries &_dataseries, const std::string &field, 
		       int flags, int32_t _default_value, bool auto_add)
    : FixedField(_dataseries,field, ExtentType::ft_int32, flags),
      default_value(_default_value)
{ 
    if (auto_add) {
	dataseries.addField(*this);
    }
}

Int64Field::Int64Field(ExtentSeries &_dataseries, const std::string &field, 
		       int flags, int64_t _default_value, bool auto_add)
    : FixedField(_dataseries,field, ExtentType::ft_int64, flags),
      default_value(_default_value)
{ 
    if (auto_add) {
	dataseries.addField(*this);
    }
}

Int64Field::~Int64Field()
{
}

DoubleField::DoubleField(ExtentSeries &_dataseries, const std::string &field,
			 int flags, double _default_value, bool auto_add)
    : FixedField(_dataseries,field, ExtentType::ft_double, flags),
      default_value(_default_value), base_val(Double::NaN)
{ 
    if (auto_add) {
	dataseries.addField(*this);
    }
}

void 
DoubleField::newExtentType()
{
    if (getName().empty())
	return;
    FixedField::newExtentType();
    base_val = dataseries.type->getDoubleBase(getName());
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

void 
Variable32Field::newExtentType()
{
    Field::newExtentType();
    offset_pos = dataseries.type->getOffset(getName());
    unique = dataseries.type->getUnique(getName());
    INVARIANT(dataseries.type->getFieldType(getName()) 
	      == ExtentType::ft_variable32,
	      format("mismatch on field types for field named %s in type %s")
	      % getName() % dataseries.type->name);
}

void
Variable32Field::dosetandguard(byte *vardatapos, 
			       const void *data, int32 datasize,
			       int32 roundup)
{
    *(int32 *)vardatapos = datasize;
    byte *i = vardatapos + 4;
    SINVARIANT((unsigned long)i % 8 == 0);
    memcpy(i,data,datasize);
    i += datasize;
    switch(roundup - datasize) 
	{ // all cases fall through...
	case 7: *i = 0; ++i;
	case 6: *i = 0; ++i;
	case 5: *i = 0; ++i;
	case 4: *i = 0; ++i;
	case 3: *i = 0; ++i;
	case 2: *i = 0; ++i;
	case 1: *i = 0; ++i;
	case 0: 
	    break;
	default: FATAL_ERROR("internal error");
	}
    INVARIANT((unsigned long)(i + 4) % 8 == 0,
	      format("internal error %lx %lx %lx\n")
	      % (unsigned long)vardatapos 
	      % (unsigned long)(vardatapos + 4 + datasize)
	      % (unsigned long)(vardatapos + 4 + datasize 
				+ (roundup - datasize)));
}

void
Variable32Field::set(const void *data, int32 datasize)
{
    int32 varoffset = getVarOffset();
    INVARIANT(datasize >= 0, format("invalid datasize, %d < 0") % datasize);
    if (datasize == 0) {
	*(int32 *)(dataseries.pos.record_start() + offset_pos) = 0;
	SINVARIANT(*(int32 *)dataseries.extent()->variabledata.begin() == 0)
	setNull(false);
	return;
    }
    int32 roundup = roundupSize(datasize);
    SINVARIANT((roundup+4) % 8 == 0);
    int32 old_size = size(dataseries.extent()->variabledata,
			  varoffset);
    if (old_size != 0 && datasize < roundupSize(old_size) && unique == false) {
	// can repack into the same location 
    } else {
	// need to repack at the end of the variable data
	varoffset = dataseries.extent()->variabledata.size();
	dataseries.extent()->variabledata.resize(varoffset + 4 + roundup);
	*(int32 *)(dataseries.pos.record_start() + offset_pos) = varoffset;
    }
    SINVARIANT(data != NULL);
    dosetandguard((byte *)vardata(dataseries.extent()->variabledata,varoffset),
		  data,datasize,roundup);
#if defined(COMPILE_DEBUG) || defined(DEBUG)
    selfcheck(dataseries.extent()->variabledata,varoffset);
#endif
    setNull(false);
}

void
Variable32Field::selfcheck(Extent::ByteArray &varbytes, int32 varoffset)
{
    INVARIANT(varoffset >= 0 
	      && (uint32_t)varoffset <= (varbytes.size() - 4),
	      format("Internal error, bad variable offset %d") % varoffset);
    if (varoffset == 0) {
	// special case this check, as it is slightly different
	INVARIANT(*(int32 *)varbytes.begin() == 0,
		  "Whoa, zero string got a size");
	return;
    }
    INVARIANT((varoffset % 4) == 0,
	      "Internal error, bad variable offset");
    INVARIANT(varoffset == 0 || ((varoffset + 4) % 8) == 0,
	      "Internal error, bad variable offset");
    int32 size = *(int32 *)(vardata(varbytes,varoffset));
    INVARIANT(size >= 0,"Internal error, bad variable size");
    int32 roundup = roundupSize(size);
    INVARIANT((unsigned int)(varoffset + 4 + roundup) <= varbytes.size(),
	      "Internal error, bad variable offset");
    for(int32 i=size;i<roundup;++i) {
	INVARIANT(*(byte *)vardata(varbytes,varoffset + 4 + i) == 0,
		  "Internal error, bad padding");
    }
}
