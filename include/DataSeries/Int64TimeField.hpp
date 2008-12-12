// -*-C++-*-
/*
   (c) Copyright 2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#ifndef DATASERIES_INT64TIMEFIELD_HPP
#define DATASERIES_INT64TIMEFIELD_HPP

#include <time.h>

#include <DataSeries/Int64Field.hpp>


/** \brief Special field for dealing with different time representations that 
    have a base type of an int64.
    
    May eventually be extended to deal with
    underlying representations that are int32's.  Currently enforces the
    restriction that all accessed extents have the same units and epoch.
    
    Int64Field is intentionally protected so that you can't get at the
    various base-class operations without doing something special.
*/
class Int64TimeField : protected Int64Field {
public:
    // TODO: calculate functions that will tell us how much error we
    // introduced by doing a conversion; probably in both directions.

    // TODO: figure out how we are going to deal with epoch
    // conversions; it turns out that there are two uses for
    // converting a time: 1) converting the units, for example to
    // calculate the distance between two times (durations), and 2)
    // converting the full epoch to get an absolute time.  Really need
    // to get some non-unix epoch data so we can play with this; only
    // ran into the issues with double time by using it (not enough
    // precision for unix epoch absolute times), and similarly with
    // unsigned time (can't represent differences), and with fixed
    // point time (can't do stddev because we need to calculate v^2)

    /// If you use the Raw typedef, then you will be able to switch to
    /// a Int128TimeField or Int32TimeField, once those are written
    /// with much less difficulty.
    typedef int64_t Raw;

    /// lock down the bit width; various timespec implementations may
    /// differ; will probably put precision loss information in here
    /// eventually; hence a structure rather than just a pair.  With
    /// infinite precision floating point time t, seconds =
    /// floor(t/1.0e9), and nanoseconds = t - seconds * 1e9.  This
    /// means that -.1s is representated as (-1, 900 * 1000 * 1000).
    struct SecNano {
	int32_t seconds;
	uint32_t nanoseconds;
	SecNano() : seconds(0), nanoseconds(0) { }
	SecNano(int32_t a, uint32_t b)
	    : seconds(a), nanoseconds(b) { }
	bool operator ==(const SecNano &b) const {
	    return seconds == b.seconds && nanoseconds == b.nanoseconds;
	}
    };

    /// Standard field constructor
    Int64TimeField(ExtentSeries &series, const std::string &field,
		   unsigned flags = 0, int64_t default_value = 0);
    virtual ~Int64TimeField();

    /// Set the raw time value at current series position
    void setRaw(Raw raw) {
	Int64Field::set(raw);
    }

    /// Raw time value at current series position
    Raw valRaw() const {
	return Int64Field::val();
    }

    /// Current value at series position in units of 2^-32 seconds and
    /// unix epoch time.  Note this is slightly different than 
    /// Lintel Clock::Tfrac as that one is unsigned.
    int64_t valFrac32() const {
	return rawToFrac32(valRaw());
    }
    /// (seconds, nanoseconds) with unix epoch time for value at
    /// current series position.
    SecNano valSecNano() const {
	return rawToSecNano(valRaw());
    }

    /// seconds with unix epoch for time at current series position.
    int64_t valSec() const {
	// TODO: make this more efficient.
	return valSecNano().seconds;
    }
	
    /// Return the seconds.nanoseconds string value for the current
    /// series position.
    std::string valStrSecNano() const {
	return rawToStrSecNano(valRaw());
    }

    /// convert a raw value into tfrac (2^-32s units, unix epoch); an
    /// error if the value would be out of bounds, or if we don't yet
    /// have the epoch
    int64_t rawToFrac32(Raw raw) const;
    /// convert a tfrac value back into a Raw value; an error if the
    /// value would be out of bounds.
    Raw frac32ToRaw(int64_t frac32) const;

    /// Convert a raw value into a second, nanosecond pair; unix epoch
    SecNano rawToSecNano(Raw raw) const;
    /// Convert seconds and nanoseconds into the Raw format
    Raw secNanoToRaw(int32_t seconds, uint32_t nanoseconds = 0) const;
    /// Convert a raw value into a seconds.nanoseconds string value
    std::string rawToStrSecNano(Raw raw) const;

    /// Convert a raw value into a double with units of seconds. Note
    /// this is dangerous with large values, so the conversion
    /// verifies that a precision of at least nanoseconds will be
    /// preserved after the conversion unless precision_check is set
    /// to false.  Skipping the precision check is more safe if you
    /// are converting a sum of times for turning into a mean,
    /// although there you could be running the risk of overflowing
    /// the raw units.
    double rawToDoubleSeconds(Raw raw, bool precision_check = true) const;

    /// Some old files may not include the necessary units and epoch
    /// fields.  This function provides a back-door for specifying
    /// these values.  A call to this will override any specification
    /// in a file.
    static void registerUnitsEpoch(const std::string &field_name,
				   const std::string &type_name,
				   const std::string &name_space,
				   const uint32_t major_version,
				   const std::string &units,
				   const std::string &epoch);
    /// Really mostly for testing purposes; you can use it, but you then
    /// have to get it right because it's an error to change it.
    void setUnitsEpoch(const std::string &units, 
		       const std::string &epoch);

    // TODO: figure out how to make this more general
    enum TimeType { Unknown, UnixFrac32, UnixNanoSec, UnixMicroSec };
    /// Useful for verifying multiple fields have the same type.
    TimeType getType() const {
	return time_type;
    }

    /// allow people to call this function
    const std::string &getName() const {
	return Field::getName();
    }

    /// allow people to call this function
    void setFieldName(const std::string &new_name) {
	Field::setFieldName(new_name);
    }

private:
    virtual void newExtentType();

    static void frac32ToSecNano(int64_t tfrac, SecNano &secnano);
    static int64_t secNanoToFrac32(const SecNano &secnano);
    static void splitSecNano(int64_t isecnano, SecNano &osecnano);
    static inline int64_t joinSecNano(int32_t sec, uint32_t nsec) {
	DEBUG_SINVARIANT(nsec < 1000 * 1000 * 1000);
	return static_cast<int64_t>(sec) * 1000 * 1000 * 1000 
	    + nsec;
    }
    static inline int64_t joinSecNano(const SecNano &secnano) {
	return joinSecNano(secnano.seconds, secnano.nanoseconds);
    }

    static TimeType convertUnitsEpoch(const std::string &units,
				      const std::string &epoch,
				      const std::string &field_name);
    TimeType time_type;
    
    int64_t val() const; // unimplemented; no accidental use
};

#endif
