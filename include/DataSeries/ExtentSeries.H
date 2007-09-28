/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/


#ifndef __EXTENT_SERIES_H
#define __EXTENT_SERIES_H
// This class allows us to have all the fields share a single
// dataseries so that you don't have to keep updating the extent
// associated with each field separately.  If you want to have fields
// use different extents, they need to use different data series.

class Field;
class Extent;

#include <DataSeries/ExtentType.H>
#include <DataSeries/Extent.H>

class ExtentSeries {
public:
    // the types for the extents must match:
    //   typeExact: same ExtentLibrary, same type
    //   typeXMLIdentical: identical XML type descriptions
    //   typeFieldMatch: all fields must occur in the same order with the same type (including base for Doubles?)
    //   typeLoose: possibly reordered fields must match types (just the signature) for used fields
    enum typeCompatibilityT { typeExact, typeXMLIdentical, typeFieldMatch, typeLoose };

    ExtentSeries(typeCompatibilityT _tc = typeExact) 
	: type(NULL), my_extent(NULL), typeCompatibility(_tc) {
    }

    ExtentSeries(const ExtentType &_type,
		 typeCompatibilityT _tc = typeExact)
	: type(&_type), my_extent(NULL), typeCompatibility(_tc) {
    }
    ExtentSeries(const ExtentType *_type,
		 typeCompatibilityT _tc = typeExact)
	: type(_type), my_extent(NULL), typeCompatibility(_tc) {
    }
    ExtentSeries(ExtentTypeLibrary &library, std::string type_name,
		 typeCompatibilityT _tc = typeExact)
	: type(library.getTypeByName(type_name)), my_extent(NULL),
	  typeCompatibility(_tc) {
    }
    ExtentSeries(Extent *e, 
		 typeCompatibilityT _tc = typeExact);
    ExtentSeries(const std::string &xmltype,
		 typeCompatibilityT _tc = typeExact)
	: type(ExtentTypeLibrary::sharedExtentType(xmltype)),
	  my_extent(NULL), typeCompatibility(_tc) { 
    }
    void setType(const ExtentType *type);
    // setExtent will automatically reset the pos iterator to the
    // beginning of the extent.
    void setExtent(Extent *e);
    void setExtent(Extent &e) {
	setExtent(&e);
    }
    void clearExtent();

    Extent *extent() { 
	return my_extent;
    }
    const ExtentType *type;
    void addField(Field &field);

    // if you have other iterators, you must! call reset on them 
    // after calling either newRecord() or createRecords()
    void newRecord() { // sets the current position to the new record
	AssertAlways(my_extent != NULL,
		     ("must set extent for data series before calling newRecord()\n"));
	int offset = my_extent->fixeddata.size();
	my_extent->createRecords(1);
	pos.cur_pos = my_extent->fixeddata.begin() + offset;
    }
    void createRecords(int nrecords) {
	// leaves the current record position unchanged
	AssertAlways(my_extent != NULL,
		     ("must set extent for data series before calling newRecord()\n"));
	int offset = pos.cur_pos - my_extent->fixeddata.begin();
	my_extent->createRecords(nrecords);
	pos.cur_pos = my_extent->fixeddata.begin() + offset;
    }
    // TODO: make this class go away, it doesn't actually make sense since
    // each of the fields are tied to the ExtentSeries, not to the iterator
    // within it.  Only use seems to be within Extent.C which is using the
    // raw access that the library has anyway.
    class iterator {
    public:
	iterator() : cur_extent(NULL), cur_pos(NULL), recordsize(-1) { }
	iterator(Extent *e) { reset(e); }
	typedef ExtentType::byte byte;
	iterator &operator++() { cur_pos += recordsize; return *this; }
	byte *record_start() { return cur_pos; }
	void reset(Extent *e) { 
	    if (e == NULL) {
		cur_extent = NULL;
		cur_pos = NULL;
		recordsize = 0;
	    } else {
		cur_extent = e;
		cur_pos = e->fixeddata.begin();
		recordsize = e->type->fixedrecordsize();
	    }
	}
	void setpos(byte *new_pos);

	int currecnum() {
	    int recnum = (cur_pos - cur_extent->fixeddata.begin()) / recordsize;
	    checkOffset(cur_pos - cur_extent->fixeddata.begin());
	    return recnum;
	}
	// You need to call update on any of your iterators after you
	// call Extent::createRecords() or ExtentSeries::newRecord().
	// newRecord() will update the series pos iterator. update()
	// keeps the current position at the same relative record as before
	void update(Extent *e);

	bool morerecords() {
	    return cur_extent != NULL && cur_pos < cur_extent->fixeddata.end();
	}
	void checkOffset(long offset) {
#ifdef COMPILE_DEBUG
	    forceCheckOffset(offset);
#else
	    (void)offset; // eliminate compilation warning
#endif
	}
	void forceCheckOffset(long offset);
    private:
	friend class ExtentSeries;
	Extent *cur_extent;
	byte *cur_pos;
	unsigned recordsize;
    };

    iterator pos;

    bool morerecords() {
	return pos.morerecords();
    }
    iterator &operator++() { ++pos; return pos; }

    typeCompatibilityT getTypeCompat() { return typeCompatibility; }
    const Extent *curExtent() { return my_extent; }
private:
    Extent *my_extent;
    const typeCompatibilityT typeCompatibility;
    std::vector<Field *> my_fields;
};

#endif
