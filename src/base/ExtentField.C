/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <Lintel/Double.H>
#include <DataSeries/ExtentField.H>

Field::Field(ExtentSeries &_dataseries, const std::string &_fieldname, int _flags)
    : nullable(0),null_offset(0), null_bit_mask(0), 
    dataseries(_dataseries), fieldname(_fieldname), flags(_flags) 
{
}

Field::~Field()
{
    dataseries.removeField(*this);
}

void 
Field::newExtentType()
{
    if (flags & flag_nullable) {
	nullable = dataseries.type->getNullable(fieldname);
	if (nullable) {
	    std::string nullfieldname = ExtentType::nullableFieldname(fieldname);
	    AssertAlways(dataseries.type->getFieldType(nullfieldname) == ExtentType::ft_bool,("internal error\n"));
	    null_offset = dataseries.type->getOffset(nullfieldname);
	    int bitpos = dataseries.type->getBitPos(nullfieldname);
	    null_bit_mask = 1 << bitpos;
	} else {
	    null_offset = 0;
	    null_bit_mask = 0;
	}
    } else {
	AssertAlways(dataseries.type->getNullable(fieldname) == false,
		     ("field %s accessor doesn't support nullable fields\n",
		      fieldname.c_str()));
    }
}

FixedField::FixedField(ExtentSeries &_dataseries, const std::string &field, 
		       ExtentType::fieldType ft, int flags)
    : Field(_dataseries,field, flags), size(-1), offset(-1),
      fieldtype(ft)
{
    AssertAlways(dataseries.type == NULL ||
		 dataseries.type->getFieldType(field) == ft,
		 ("mismatch on field types for field %s in type %s\n",
		  field.c_str(), dataseries.type->name.c_str()));
}

void
FixedField::newExtentType()
{
    Field::newExtentType();
    size = dataseries.type->getSize(fieldname);
    offset = dataseries.type->getOffset(fieldname);
    AssertAlways(dataseries.type->getFieldType(fieldname) == fieldtype,
		 ("mismatch on field types for field named %s in type %s\n",
		  fieldname.c_str(),dataseries.type->name.c_str()));
}

BoolField::BoolField(ExtentSeries &_dataseries, const std::string &field, int flags,
		     bool _default_value)
    : FixedField(_dataseries,field, ExtentType::ft_bool,flags),
      default_value(_default_value), bit_mask(0)
{ 
    dataseries.addField(*this); 
}

void 
BoolField::newExtentType()
{
    FixedField::newExtentType();
    int bitpos = dataseries.type->getBitPos(fieldname);
    bit_mask = (byte)(1 << bitpos);
}

ByteField::ByteField(ExtentSeries &_dataseries, const std::string &field, int flags,
		     byte _default_value)
    : FixedField(_dataseries,field,ExtentType::ft_byte,flags),
      default_value(_default_value)
{ 
    dataseries.addField(*this); 
}

Int32Field::Int32Field(ExtentSeries &_dataseries, const std::string &field, int flags,
		       int32 _default_value)
    : FixedField(_dataseries,field, ExtentType::ft_int32, flags),
      default_value(_default_value)
{ 
    dataseries.addField(*this); 
}

Int64Field::Int64Field(ExtentSeries &_dataseries, const std::string &field, int flags,
		       int64 _default_value)
    : FixedField(_dataseries,field, ExtentType::ft_int64, flags),
      default_value(_default_value)
{ 
    dataseries.addField(*this); 
}

DoubleField::DoubleField(ExtentSeries &_dataseries, const std::string &field,
			 int flags, double _default_value)
    : FixedField(_dataseries,field, ExtentType::ft_double, flags),
      default_value(_default_value), base_val(Double::NaN)
{ 
    dataseries.addField(*this); 
}

void 
DoubleField::newExtentType()
{
    FixedField::newExtentType();
    base_val = dataseries.type->getDoubleBase(fieldname);
    AssertAlways(flags & flag_allownonzerobase || base_val == 0,
		 ("accessor for field %s doesn't support non-zero base\n",
		  fieldname.c_str()));
}

const std::string Variable32Field::empty_string("");

Variable32Field::Variable32Field(ExtentSeries &_dataseries, 
				 const std::string &field, 
				 int flags, const std::string &_default_value) 
    : Field(_dataseries,field,flags), default_value(_default_value), 
    offset_pos(-1), unique(false)
{ 
    dataseries.addField(*this); 
}

void 
Variable32Field::newExtentType()
{
    Field::newExtentType();
    offset_pos = dataseries.type->getOffset(fieldname);
    unique = dataseries.type->getUnique(fieldname);
    AssertAlways(dataseries.type->getFieldType(fieldname) == ExtentType::ft_variable32,
		 ("mismatch on field types for field named %s in type %s\n",
		  fieldname.c_str(),dataseries.type->name.c_str()));
}

void
Variable32Field::dosetandguard(byte *vardatapos, 
			       const void *data, int32 datasize,
			       int32 roundup)
{
    *(int32 *)vardatapos = datasize;
    byte *i = vardatapos + 4;
    AssertAlways((unsigned long)i % 8 == 0,("internal error\n"));
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
	default: AssertFatal(("internal error\n"));
	}
    AssertAlways((unsigned long)(i + 4) % 8 == 0,
		 ("internal error %lx %lx %lx\n",(unsigned long)vardatapos,
		  (unsigned long)(vardatapos + 4 + datasize),
		  (unsigned long)(vardatapos + 4 + datasize + (roundup - datasize))));
}

void
Variable32Field::set(const void *data, int32 datasize)
{
    int32 varoffset = getVarOffset();
    AssertAlways(datasize >= 0,("invalid datasize, %d < 0\n",datasize));
    if (datasize == 0) {
	*(int32 *)(dataseries.pos.record_start() + offset_pos) = 0;
	AssertAlways(*(int32 *)dataseries.extent()->variabledata.begin() == 0,
		     ("internal error\n"));
	setNull(false);
	return;
    }
    int32 roundup = roundupSize(datasize);
    AssertAlways((roundup+4) % 8 == 0,("internal error\n"));
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
    dosetandguard((byte *)vardata(dataseries.extent()->variabledata,varoffset),
		  data,datasize,roundup);
    selfcheck(dataseries.extent()->variabledata,varoffset);
    setNull(false);
}

void
Variable32Field::selfcheck(Extent::ByteArray &varbytes, int32 varoffset)
{
    AssertAlways(varoffset >= 0 && (unsigned int)varoffset <= (varbytes.size() - 4),
		 ("Internal error, bad variable offset %d\n",varoffset));
    if (varoffset == 0) {
	// special case this check, as it is slightly different
	AssertAlways(*(int32 *)varbytes.begin() == 0,
		     ("Whoa, zero string got a size\n"));
	return;
    }
    AssertAlways((varoffset % 4) == 0,
		 ("Internal error, bad variable offset\n"));
    AssertAlways(varoffset == 0 || ((varoffset + 4) % 8) == 0,
		 ("Internal error, bad variable offset\n"));
    int32 size = *(int32 *)(vardata(varbytes,varoffset));
    AssertAlways(size >= 0,("Internal error, bad variable size\n"));
    int32 roundup = roundupSize(size);
    AssertAlways((unsigned int)(varoffset + 4 + roundup) <= varbytes.size(),
		 ("Internal error, bad variable offset\n"));
    for(int32 i=size;i<roundup;++i) {
	AssertAlways(*(byte *)vardata(varbytes,varoffset + 4 + i) == 0,
		     ("Internal error, bad padding\n"));
    }
}
