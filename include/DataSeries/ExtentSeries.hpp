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

#include <Lintel/DebugFlag.hpp>

#include <DataSeries/ExtentType.hpp>
#include <DataSeries/Extent.hpp>

/** \brief Encapsulates iteration over a group of similar Extents.
  *
  * Standard loop ends up being:
  * @code
  * for(series.start(e); series.more(); series.next()) { ... }
  * @endcode
  */
class ExtentSeries {
public:
    // the types for the extents must match:
    //   typeExact: identical XML type descriptions
    //   TODO: typeMajorVersion: major version matches, may want to have
    //         a required major and a minimal minor specified also
    //   typeLoose: possibly reordered fields must match types (just
    //              the signature) for used fields
    /** Describes what variations in the types of Extents are allowed. */
    enum typeCompatibilityT {
        /** The ExtentSeries can only hold Extents of a single type. */
        typeExact,
        /** The ExtentSeries can hold Extents of any type which is 
            compatible with all of its fields.  This effectively means
            that it doesn't matter what the ExtentSeries held previously. */
        typeLoose
    };

    /** This constructor leaves the type unknown. The type can be set
        later either  explicitly using setType or implicitly using setExtent.

        Postconditions:
            - getType() == 0 and getExtent() == 0 */
    explicit ExtentSeries(typeCompatibilityT _tc = typeExact) 
	: type(NULL), my_extent(NULL), typeCompatibility(_tc) {
    }

    /** Sets the type held by the ExtentSeries to the specified type.
        Note that even when typeLoose is given, this can be useful,
        because it allows Fields to be checked before an Extent is
        specified.

        Postconditions:
            - getType() == &_type and getExtent() == 0 */
    explicit ExtentSeries(const ExtentType &_type,
		 typeCompatibilityT _tc = typeExact)
	: type(&_type), my_extent(NULL), typeCompatibility(_tc) {
    }
    /** Sets the type held by the ExtentSeries to the specified type.
        If the type is null, then this is equivalent to the default
        constructor.

        Postconditions:
            - getType() == _type and getExtent() == 0 */
    explicit ExtentSeries(const ExtentType *_type,
		 typeCompatibilityT _tc = typeExact)
	: type(_type), my_extent(NULL), typeCompatibility(_tc) {
    }
    /** Sets the type held by the ExtentSeries by looking it up
        in an @c ExtentTypeLibrary

        Preconditions:
            - An ExtentType with the specified name has
            been registered with the library.

        Postconditions:
            - getType() == library.getTypeByName(type name)
            - getExtent() == 0 */
    ExtentSeries(ExtentTypeLibrary &library, std::string type_name,
		 typeCompatibilityT _tc = typeExact)
	: type(library.getTypeByName(type_name)), my_extent(NULL),
	  typeCompatibility(_tc) {
    }
    /** Initializes with the specified @c Extent. If it is null then this
        is equivalent to the default constructor. Otherwise sets the
        type to the type of the @c Extent. */
    explicit ExtentSeries(Extent *e, 
		 typeCompatibilityT _tc = typeExact);
    /** Initialize using the @c ExtentType corresponding to the given XML. */
    explicit ExtentSeries(const std::string &xmltype,
		 typeCompatibilityT _tc = typeExact)
	: type(&ExtentTypeLibrary::sharedExtentType(xmltype)),
	  my_extent(NULL), typeCompatibility(_tc) { 
    }

    /** Preconditions:
            - All the attached fields must have been destroyed first. */
    ~ExtentSeries();

    // Next three are the most common way of using a series; may
    // eventually decide to deprecate as many of the other options as
    // possible after investigating to see if they matter.

    /** Sets the current extent being processed. If e is null, clears
        the current Extent.  If the type has already been set, requires
        that the type of the new Extent be compatible with the existing
        type, as specified by the typeCompatibilityT argument of all the
        constructors. */
    void start(Extent *e) {
	setExtent(e);
    }
    /** Returns true iff the current extent is not null and has more records. */
    bool more() {
	return pos.morerecords();
    }
    /** Advances to the next record in the current Extent.

        Preconditions:
            - more() */
    void next() {
	++pos;
    }

    /** Together with @c setCurPos, this function allows saving and restoring
        the current position of an extent in a series.
        Returns an opaque handle to the current position in an Extent. The only
        thing that can be done with the result is to pass it to setCurPos. The
        handle is valid for any ExtentSeries which uses the same Extent. It is
        invalidated by any operation which changes the size of the Extent.

        Preconditions:
            - getExtent() != 0 */
    const void *getCurPos() {
	return pos.getPos();
    }
    /** Restores the current position to a saved state.  position must be the
        the result of a previous call to @c getCurPos() with the current
        @c Extent and must not have been invalidated.
        
        This function may be slow. (Slow means that it does a handful of
        arithmetic and comparisons to verify that the call leaves that
        the @c ExtentSeries in a valid state.) */
    void setCurPos(const void *position) {
	pos.setPos(position);
    }
    
    /** Sets the type of Extents. If the type has already been set in any way,
        requires that the new type be compatible with the existing type as
        specified by the typeCompatibilityT of all the constructors. */
    void setType(const ExtentType &type);
    /** Returns a pointer to the current type. If no type has been set, the
        result will be null.
        Invariants:
            - If getExtent() is not null, then the result is the
              same as getExtent()->type*/
    const ExtentType *getType() { return type; }
    /** Sets the current extent being processed. If e is null, clears the
        current Extent.  If the type has already been set, requires that
        the type of the new Extent be compatible with the existing type.
        it is valid to call this with NULL which is equivalent to a call 
        to clearExtent()
        
        Postconditions:
            - getExtent() = e
            - the current position will be set to the beginning of the
              new @c Extent. */
    void setExtent(Extent *e);
    /** Equivalent to @c setExtent(&e) */
    void setExtent(Extent &e) {
	setExtent(&e);
    }
    /** Clears the current Extent. Note that this only affects the
        Extent, the type is left unchanged.  Exactly equivalent to
        setExtent(NULL) */
    void clearExtent() { setExtent(NULL); }

    /** Returns the current extent. */
    Extent *extent() { 
	return my_extent;
    }
    Extent *getExtent() {
	return my_extent;
    }
    /// \cond INTERNAL_ONLY
    const ExtentType *type;
    /// \endcond

    /** Registers the specified field. This should only be called from
        constructor of classes derived from @c Field.  Users should not need
        to call it. */
    void addField(Field &field);
    /** Removes a field. Never call this directly, the @c Field destructor
        handles this automatically.*/
    void removeField(Field &field, bool must_exist = true);

    /** Appends a record to the end of the current Extent. Invalidates any
        other @c ExtentSeries operating on the same Extent. i.e. you must
        restart the Extent in every such series.  The new record will contain
        all zeros. Variable32 fields will be empty. Nullable fields will be
        initialized just like non-nullable field--They will not be set to null.
        After this function returns, the current position will point to the
        new record.

        Preconditions:
            - The current Extent is not null. 

        \todo are multiple @c ExtentSeries even supported?
        \todo should nullable fields be set to null? */
    void newRecord() {
	INVARIANT(my_extent != NULL,
		  "must set extent for data series before calling newRecord()");
	int offset = my_extent->fixeddata.size();
	my_extent->createRecords(1);
	pos.cur_pos = my_extent->fixeddata.begin() + offset;
    }
    /** Appends a specified number of records onto the end of the current
        @c Extent.  The same cautions apply as for @c newRecord. The current
        record will be the first one inserted.

        Preconditions:
            - The current extent cannot be null */
    void createRecords(int nrecords) {
	// leaves the current record position unchanged
	INVARIANT(my_extent != NULL,
		  "must set extent for data series before calling newRecord()\n");
	int offset = pos.cur_pos - my_extent->fixeddata.begin();
	my_extent->createRecords(nrecords);
	pos.cur_pos = my_extent->fixeddata.begin() + offset;
    }
    /// \cond INTERNAL_ONLY
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
#if LINTEL_DEBUG
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

    /// \endcond

    /** Returns true iff the current Extent is not null and we are not at the
        end of it. */
    bool morerecords() {
	return pos.morerecords();
    }

    /// \cond INTERNAL_ONLY
    iterator &operator++() { ++pos; return pos; }
    /// \endcond

    /** Indicates how the ExtentSeries handles different Extent types. */
    typeCompatibilityT getTypeCompat() { return typeCompatibility; }
    /** Returns the current Extent. */
    const Extent *curExtent() { return my_extent; }
private:
    Extent *my_extent;
    const typeCompatibilityT typeCompatibility;
    std::vector<Field *> my_fields;
};

#endif
