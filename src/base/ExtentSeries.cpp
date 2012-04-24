/*
   (c) Copyright 2003-2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <Lintel/AssertBoost.hpp>

#define DS_RAW_EXTENT_PTR_DEPRECATED /* allowed */

#include <DataSeries/ExtentSeries.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>

using namespace std;

ExtentSeries::ExtentSeries(Extent *e, typeCompatibilityT tc)
    : typeCompatibility(tc)
{
    if (e == NULL) {
	type.reset();
	my_extent = NULL;
    } else {
	type = e->type->shared_from_this();
	INVARIANT(type != NULL,"bad initialization");
	my_extent = e;
	pos.reset(e);
    }
}

ExtentSeries::ExtentSeries(Extent::Ptr e, typeCompatibilityT tc)
    : typeCompatibility(tc)
{
    if (e == NULL) {
	type.reset();
	my_extent = NULL;
    } else {
	type = e->type->shared_from_this();
	INVARIANT(type != NULL,"bad initialization");
        shared_extent = e;
	my_extent = e.get();
	pos.reset(e.get());
    }
}

ExtentSeries::~ExtentSeries() {
    INVARIANT(my_fields.size() == 0, 
	      boost::format("You still have fields such as %s live on a series over type %s."
                            " You have to delete dynamically allocated fields before deleting"
                            " the ExtentSeries. Class member variables and static ones are"
                            " automatically deleted in the proper order.")
	      % my_fields[0]->getName() % (type == NULL ? "unset type" : type->getName()));
}

void ExtentSeries::setType(const ExtentType::Ptr in_type) {
    INVARIANT(in_type != NULL,"you made a NULL reference grr");
    if (type == in_type) {
	return; // nothing to do, a valid but useless call
    }
    switch(typeCompatibility)
	{
	case typeExact: 
	    INVARIANT(type == NULL || type->getXmlDescriptionString() 
		      != in_type->getXmlDescriptionString(), 
		      "internal -- same xmldesc should get same type");
	    INVARIANT(type == NULL,
		      boost::format("Unable to change type with typeExact compatibility\n"
                                    "Type 1:\n%s\nType 2:\n%s\n")
		      % type->getXmlDescriptionString() % in_type->getXmlDescriptionString());
	    break;
	case typeLoose:
	    break;
	default:
	    FATAL_ERROR(boost::format("unrecognized type compatibility option %d") 
                        % typeCompatibility);
	}

    type = in_type;
    for(vector<Field *>::iterator i = my_fields.begin();
	i != my_fields.end();++i) {
	SINVARIANT(&(**i).dataseries == this)
	(**i).newExtentType();
    }
}

void ExtentSeries::setExtent(Extent *e) {
    if (shared_extent.get() == e) {
        // ok, via setExtent(Extent::Ptr)
    } else {
        shared_extent.reset();
        if (e != NULL) {
            try {
                Extent::Ptr se = e->shared_from_this();
                FATAL_ERROR("setExtent(raw ptr) called on a shared extent");
            } catch (std::exception &) {
                // ok
            }
        }
    }
        
    my_extent = e;
    pos.reset(my_extent);
    if (e != NULL && e->type != type) {
	setType(e->type);
    }
}

void ExtentSeries::setExtent(Extent::Ptr e) {
    shared_extent = e;
    setExtent(e.get());
}

void ExtentSeries::setExtent(Extent &e) {
    setExtent(&e);
}

void ExtentSeries::start(Extent *e) {
    setExtent(e);
}

void ExtentSeries::newExtent() {
    SINVARIANT(getType() != NULL);
    Extent::Ptr e(new Extent(*getType()));
    setExtent(e);
}

void ExtentSeries::addField(Field &field) {
    if (type != NULL) {
	field.newExtentType();
    }
    my_fields.push_back(&field);
}

void ExtentSeries::removeField(Field &field, bool must_exist) {
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

void ExtentSeries::iterator::setPos(const void *in_new_pos) {
    const byte *new_pos = static_cast<const byte *>(in_new_pos);
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

void ExtentSeries::iterator::update(Extent *e) {
    if (e->type->fixedrecordsize() == recordsize) {
	int offset = cur_pos - cur_extent->fixeddata.begin();
	byte *begin_pos = cur_extent->fixeddata.begin();
	cur_pos = begin_pos + offset;
    } else {
	unsigned recnum = (cur_pos - cur_extent->fixeddata.begin()) / recordsize;
	size_t offset = cur_pos - cur_extent->fixeddata.begin();
	INVARIANT(recnum * recordsize == offset,
		  ("whoa, pointer not on a record boundary?!\n"));
	recordsize = e->type->fixedrecordsize();
	byte *begin_pos = cur_extent->fixeddata.begin();
	cur_pos = begin_pos + recnum * recordsize;
    }
}


void ExtentSeries::iterator::forceCheckOffset(long offset) {
    INVARIANT(cur_extent != NULL, "internal error, current extent is NULL");
    INVARIANT(cur_extent->insideExtentFixed(cur_pos + offset),
	      boost::format("internal error, %p + %d = %p not in [%p..%p]\n") 
	      % reinterpret_cast<void *>(cur_pos) % offset
	      % reinterpret_cast<void *>(cur_pos+offset)
	      % reinterpret_cast<void *>(cur_extent->fixeddata.begin())
	      % reinterpret_cast<void *>(cur_extent->fixeddata.end()));
}
