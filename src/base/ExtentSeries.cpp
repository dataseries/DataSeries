/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <algorithm>

#include <boost/foreach.hpp>
#include <boost/bind.hpp>

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
	    INVARIANT(type == NULL || type->xmldesc != _type.xmldesc,
		      "internal -- same xmldesc should get same type");
	    INVARIANT(type == NULL,
		      boost::format("Unable to change type with typeExact compatibility\nType 1:\n%s\nType 2:\n%s\n")
		      % type->xmldesc % _type.xmldesc);
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

void
ExtentSeries::setExtent(Extent *e)
{
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
	      % reinterpret_cast<const void *>(cur_pos) % offset
	      % reinterpret_cast<const void *>(cur_pos+offset)
	      % reinterpret_cast<const void *>(cur_extent->fixeddata.begin())
	      % reinterpret_cast<const void *>(cur_extent->fixeddata.end()));
}

void ExtentSeries::setRecordIndex(size_t index) {
    setExtentPosition(indices[index]);
}

void ExtentSeries::setExtentPosition(const ExtentPosition &extentPosition) {
    setExtent(extentPosition.extent);
    setCurPos(extentPosition.position);
}

ExtentSeries::ExtentPosition ExtentSeries::getExtentPosition() {
    ExtentPosition extentPosition;
    extentPosition.extent = getExtent();
    extentPosition.position = getCurPos();
    return extentPosition;
}

size_t ExtentSeries::getRecordCount() {
    return indices.size();
}

void ExtentSeries::setExtents(const vector<Extent*> &extents) {
    // special case for no extents so that we don't have to worry about this case anymore
    if (extents.size() == 0) {
        setExtent(NULL);
        return;
    }

    // calculate the total number of records in all of the provided extents
    size_t byteCount = 0;
    BOOST_FOREACH(Extent *extent, extents) {
        byteCount += extent->fixeddata.end() - extent->fixeddata.begin();
    }
    size_t recordCount = byteCount / extents[0]->type.fixedrecordsize();

    // prepare the indices vector so that we can have fast random access
    indices.resize(recordCount);
    size_t lastRecord = 0;
    BOOST_FOREACH(Extent *extent, extents) {
        setExtent(extent);
        for (; pos.morerecords(); ++pos) {
            indices[lastRecord].position = pos.getPos();
            indices[lastRecord].extent = extent;
            ++lastRecord;
        }
    }

    SINVARIANT(lastRecord == indices.size()); // hopefully our calculation was right!
}

void ExtentSeries::sortRecords(IExtentPositionComparator *comparator) {
    ExtentPosition savedPosition = getExtentPosition();
    sort(indices.begin(), indices.end(), bind(&IExtentPositionComparator::compare, comparator, *_1, *_2));
    setExtentPosition(savedPosition);
}
