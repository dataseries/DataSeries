/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <Lintel/AssertBoost.H>

#include <DataSeries/ExtentSeries.H>
#include <DataSeries/Extent.H>
#include <DataSeries/ExtentField.H>

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

void
ExtentSeries::setType(const ExtentType &_type)
{
    INVARIANT(&_type != NULL,"you made a NULL reference grr");
    switch(typeCompatibility)
	{
	case typeExact: 
	    INVARIANT(type == NULL || type->xmldesc != _type.xmldesc, 
		      "internal -- same xmldesc should get same type");
	    INVARIANT(type == NULL,
		      boost::format("Unable to change type with typeExact compatibility\nType 1:\n%s\nType 2:\n%s\n")
		      % type->xmldesc % _type.xmldesc);
	    break;
	case typeLoose:
	    break;
	default:
	    AssertFatal(("internal error\n"));
	}

    type = &_type;
    for(std::vector<Field *>::iterator i = my_fields.begin();
	i != my_fields.end();++i) {
	AssertAlways(&(**i).dataseries == this,
		     ("Internal error\n"));
	(**i).newExtentType();
    }
}

void
ExtentSeries::setExtent(Extent *e)
{
    AssertAlways(e != NULL,("setExtent(NULL) invalid\n"));
    pos.reset(e);
    bool newtype = &e->type != type;
    my_extent = e;
    if (newtype) {
	setType(e->type);
    }
}

void
ExtentSeries::clearExtent()
{
    my_extent = NULL;
    pos.cur_extent = NULL;
    pos.cur_pos = NULL;
    pos.recordsize = 0;
}

void
ExtentSeries::addField(Field &field)
{
    if (type != NULL) {
	field.newExtentType();
    }
    my_fields.push_back(&field);
}

void
ExtentSeries::iterator::setpos(byte *new_pos)
{
    unsigned recnum = (new_pos - cur_extent->fixeddata.begin()) / recordsize;
    INVARIANT(cur_extent != NULL, "no current extent?");
    INVARIANT(new_pos >= cur_extent->fixeddata.begin(), 
	      "new pos before start");
    INVARIANT(new_pos <= cur_extent->fixeddata.end(),
	      "new pos after end");
    size_t offset = new_pos - cur_extent->fixeddata.begin();
    INVARIANT(recnum * recordsize == offset,
	      "new position not aligned to record boundary");
    cur_pos = new_pos;
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
    AssertAlways(cur_extent != NULL, 
		 ("internal error, current extent is NULL"));
    INVARIANT(cur_pos + offset >= cur_extent->fixeddata.begin() &&
	      cur_pos + offset < cur_extent->fixeddata.end(),
	      boost::format("internal error, %p + %d = %p not in [%p..%p]\n") 
	      % reinterpret_cast<void *>(cur_pos) % offset
	      % reinterpret_cast<void *>(cur_pos+offset)
	      % reinterpret_cast<void *>(cur_extent->fixeddata.begin())
	      % reinterpret_cast<void *>(cur_extent->fixeddata.end()));
}
