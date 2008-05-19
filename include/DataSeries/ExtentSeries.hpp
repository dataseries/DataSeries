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

#include <DataSeries/ExtentType.hpp>
#include <DataSeries/Extent.hpp>

class ExtentSeries {
public:
    // the types for the extents must match:
    //   typeExact: identical XML type descriptions
    //   TODO: typeMajorVersion: major version matches, may want to have
    //         a required major and a minimal minor specified also
    //   typeLoose: possibly reordered fields must match types (just
    //              the signature) for used fields
    enum typeCompatibilityT { typeExact, typeLoose };

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
	: type(&ExtentTypeLibrary::sharedExtentType(xmltype)),
	  my_extent(NULL), typeCompatibility(_tc) { 
    }

    ~ExtentSeries();

    /// Next three are the most common way of using a series; may
    /// eventually decide to deprecate as many of the other options as
    /// possible after investigating to see if they matter.
    ///
    /// Standard loop ends up being: 
    /// for(series.start(e); series.more(); series.next()) { ... }

    void start(Extent *e) {
	setExtent(e);
    }
    bool more() {
	return pos.morerecords();
    }
    void next() {
	++pos;
    }
    /// For saving and restoring the current position of an extent in a series
    const void *getCurPos() {
	return pos.getPos();
    }
    /// For saving and restoring the current position of an extent in a series;
    /// this function may be slow.
    void setCurPos(const void *position) {
	pos.setPos(position);
    }
    
    void setType(const ExtentType &type);
    const ExtentType *getType() { return type; }
    // setExtent will automatically reset the pos iterator to the
    // beginning of the extent; it is valid to call with NULL, that is
    // equivalent to a call to clearExtent()
    void setExtent(Extent *e);
    void setExtent(Extent &e) {
	setExtent(&e);
    }
    void clearExtent() { setExtent(NULL); }

    Extent *extent() { 
	return my_extent;
    }
    Extent *getExtent() {
	return my_extent;
    }
    const ExtentType *type;
    void addField(Field &field);
    void removeField(Field &field, bool must_exist = true);

    // if you have other iterators, you must! call reset on them 
    // after calling either newRecord() or createRecords()
    void newRecord() { // sets the current position to the new record
	INVARIANT(my_extent != NULL,
		  "must set extent for data series before calling newRecord()");
	int offset = my_extent->fixeddata.size();
	my_extent->createRecords(1);
	pos.cur_pos = my_extent->fixeddata.begin() + offset;
    }
    void createRecords(int nrecords) {
	// leaves the current record position unchanged
	INVARIANT(my_extent != NULL,
		  "must set extent for data series before calling newRecord()\n");
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
	iterator() : cur_extent(NULL), cur_pos(NULL), recordsize(0) { }
	iterator(Extent *e) { reset(e); }
	typedef ExtentType::byte byte;
	iterator &operator++() { cur_pos += recordsize; return *this; }
	void reset(Extent *e) { 
	    if (e == NULL) {
		cur_extent = NULL;
		cur_pos = NULL;
		recordsize = 0;
	    } else {
		cur_extent = e;
		cur_pos = e->fixeddata.begin();
		recordsize = e->type.fixedrecordsize();
	    }
	}
	
	/// old api
	void setpos(byte *new_pos) {
	    setPos(new_pos);
	}
	/// old api
	byte *record_start() { return cur_pos; }

	void setPos(const void *new_pos);
	const void *getPos() {
	    return cur_pos;
	}
	uint32_t currecnum() {
	    SINVARIANT(cur_extent != NULL);
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
#if defined(COMPILE_DEBUG) || defined(DEBUG)
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
