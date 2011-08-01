/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <Lintel/AssertBoost.hpp>

#include <DataSeries/ExtentSeries.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>

using namespace std;

ExtentSeries::ExtentSeries(Extent *e, typeCompatibilityT _tc)
    : typeCompatibility(_tc)
{
    if (e == NULL) {
	type = NULL;
	my_extent = NULL;
    } else {
	type = &e->type;
	INVARIANT(type != NULL,"bad initialization");
	my_extent = e;
	pos.reset(e);
    }
}

ExtentSeries::~ExtentSeries()
{
    INVARIANT(my_fields.size() == 0, 
	      boost::format("You still have fields such as %s live on a series over type %s.  You have to delete dynamically allocated fields before deleting the ExtentSeries.  Class member variables and static ones are automatically deleted in the proper order.")
	      % my_fields[0]->getName() 
	      % (type == NULL ? "unset type" : type->getName()));
}

void
ExtentSeries::setType(const ExtentType &_type)
{
    INVARIANT(&_type != NULL,"you made a NULL reference grr");
    if (type == &_type) {
	return; // nothing to do, a valid but useless call
    }
    switch(typeCompatibility)
	{
	case typeExact: 
	    INVARIANT(type == NULL || type->getXmlDescriptionString() 
		      != _type.getXmlDescriptionString(), 
		      "internal -- same xmldesc should get same type");
	    INVARIANT(type == NULL,
		      boost::format("Unable to change type with typeExact compatibility\nType 1:\n%s\nType 2:\n%s\n")
		      % type->getXmlDescriptionString() % _type.getXmlDescriptionString());
	    break;
	case typeLoose:
	    break;
	default:
	    FATAL_ERROR(boost::format("unrecognized type compatibility option %d") % typeCompatibility);
	}

    type = &_type;
    for(vector<Field *>::iterator i = my_fields.begin();
	i != my_fields.end();++i) {
	SINVARIANT(&(**i).dataseries == this)
	(**i).newExtentType();
    }
}

void ExtentSeries::setExtent(Extent *e) {
    my_extent = e;
    pos.reset(my_extent);
    if (e != NULL && &e->type != type) {
	setType(e->type);
    }
}

void
ExtentSeries::addField(Field &field)
{
    if (type != NULL) {
	field.newExtentType();
    }
    my_fields.push_back(&field);
}

void ExtentSeries::removeField(Field &field, bool must_exist)
{
    bool found = false;
    for(vector<Field *>::iterator i = my_fields.begin(); 
	i != my_fields.end(); ++i) {
	if (*i == &field) {
	    found = true;
	    my_fields.erase(i);
	    break;
	}
    }
    SINVARIANT(!must_exist || found);
}

void
ExtentSeries::iterator::setPos(const void *_new_pos)
{
    const byte *new_pos = static_cast<const byte *>(_new_pos);
    byte *cur_begin = cur_extent->fixeddata.begin();
    unsigned recnum = (new_pos - cur_begin) / recordsize;
    INVARIANT(cur_extent != NULL, "no current extent?");
    INVARIANT(new_pos >= cur_begin, 
	      "new pos before start");
    INVARIANT(new_pos <= cur_extent->fixeddata.end(),
	      "new pos after end");
    size_t offset = new_pos - cur_begin;
    INVARIANT(recnum * recordsize == offset,
	      "new position not aligned to record boundary");
    cur_pos = cur_begin + offset;
}

void
ExtentSeries::iterator::update(Extent *e)
{
    if (e->type.fixedrecordsize() == recordsize) {
	int offset = cur_pos - cur_extent->fixeddata.begin();
	byte *begin_pos = cur_extent->fixeddata.begin();
	cur_pos = begin_pos + offset;
    } else {
	unsigned recnum = (cur_pos - cur_extent->fixeddata.begin()) / recordsize;
	size_t offset = cur_pos - cur_extent->fixeddata.begin();
	INVARIANT(recnum * recordsize == offset,
		  ("whoa, pointer not on a record boundary?!\n"));
	recordsize = e->type.fixedrecordsize();
	byte *begin_pos = cur_extent->fixeddata.begin();
	cur_pos = begin_pos + recnum * recordsize;
    }
}


void
ExtentSeries::iterator::forceCheckOffset(long offset)
{
    INVARIANT(cur_extent != NULL, 
	      "internal error, current extent is NULL");
    INVARIANT(cur_pos + offset >= cur_extent->fixeddata.begin() &&
	      cur_pos + offset < cur_extent->fixeddata.end(),
	      boost::format("internal error, %p + %d = %p not in [%p..%p]\n") 
	      % reinterpret_cast<void *>(cur_pos) % offset
	      % reinterpret_cast<void *>(cur_pos+offset)
	      % reinterpret_cast<void *>(cur_extent->fixeddata.begin())
	      % reinterpret_cast<void *>(cur_extent->fixeddata.end()));
}
