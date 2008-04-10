// -*-C++-*-
/*
   (c) Copyright 2008, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <Lintel/HashMap.hpp>
#include <Lintel/PThread.hpp>

#include <DataSeries/Int64TimeField.hpp>

using namespace std;
using boost::format;

struct RegisteredInfo {
    string type_name, name_space;
    uint32_t major_version;
    bool operator==(const RegisteredInfo &a) const {
	return a.major_version == major_version &&
	    a.type_name == type_name &&
	    a.name_space == name_space;
    }
    RegisteredInfo(const string &a, const string &b, uint32_t c)
	: type_name(a), name_space(b), major_version(c) 
    { }
};

template <>
struct HashMap_hash<const RegisteredInfo> {
    uint32_t operator()(const RegisteredInfo &r) const {
	uint32_t a = HashTable_hashbytes(r.type_name.data(),
					 r.type_name.size());
	uint32_t b = HashTable_hashbytes(r.name_space.data(),
					 r.name_space.size());
	return BobJenkinsHashMix3(a,b,r.major_version);
    }
};

static PThreadMutex registered_mutex;
static HashMap<RegisteredInfo, Int64TimeField::TimeType> registered_type_info;

Int64TimeField::Int64TimeField(ExtentSeries &series, const std::string &field,
			       unsigned flags, int64_t default_value)
    : Int64Field(series, field, flags, default_value), 
      time_type(Unknown)
{
}

Int64TimeField::~Int64TimeField()
{ 
}

int64_t Int64TimeField::rawToFrac32(Raw raw) const
{
    if (time_type == UnixFrac32) {
	return raw;
    } else if (time_type == UnixNanoSec) {
	SecNano secnano;
	splitSecNano(raw, secnano);
	return secNanoToFrac32(secnano);
    } else if (time_type == Unknown) {
	FATAL_ERROR("time type has not been set yet; no extent?");
    } else {
	FATAL_ERROR("internal error");
    }
}

Int64TimeField::Raw Int64TimeField::frac32ToRaw(int64_t frac32) const
{
    if (time_type == UnixFrac32) {
	return frac32;
    } else if (time_type == UnixNanoSec) {
	SecNano secnano;
	frac32ToSecNano(frac32, secnano);
	return joinSecNano(secnano);
    } else if (time_type == Unknown) {
	FATAL_ERROR("time type has not been set yet; no extent?");
    } else {
	FATAL_ERROR("internal error");
    }
}

Int64TimeField::SecNano Int64TimeField::rawToSecNano(Raw raw) const
{
    SecNano ret;
    if (time_type == UnixFrac32) {
	frac32ToSecNano(raw, ret);
    } else if (time_type == UnixNanoSec) {
	splitSecNano(raw, ret);
    } else if (time_type == Unknown) {
	FATAL_ERROR("time type has not been set yet; no extent?");
    } else {
	FATAL_ERROR("internal error");
    }
    return ret;
}

Int64TimeField::Raw 
Int64TimeField::secNanoToRaw(int32_t seconds, uint32_t nanoseconds) const
{
    SINVARIANT(nanoseconds < (1000*1000*1000));
    if (time_type == UnixFrac32) {
	return secNanoToFrac32(SecNano(seconds, nanoseconds));
    } else if (time_type == UnixNanoSec) {
	return joinSecNano(seconds, nanoseconds);
    } else if (time_type == Unknown) {
	FATAL_ERROR("time type has not been set yet; no extent?");
    } else {
	FATAL_ERROR("internal error");
    }
}

string Int64TimeField::rawToStrSecNano(Raw raw) const
{
    SecNano v(rawToSecNano(raw));
    
    if (v.seconds >= 0) {
	return (boost::format("%d.%09d") % v.seconds % v.nanoseconds).str();
    } else if (v.nanoseconds == 0) {
	return (boost::format("%d.%09d") % v.seconds % 0).str();
    } else {
	return (boost::format("%d.%09d") % (v.seconds+1)
		% (1000*1000*1000 - v.nanoseconds)).str();
    }
}

void Int64TimeField::newExtentType()
{
    Int64Field::newExtentType();
    DEBUG_SINVARIANT(dataseries.getType() != NULL);
    RegisteredInfo ri(dataseries.getType()->getName(),
		      dataseries.getType()->getNamespace(),
		      dataseries.getType()->majorVersion());
    {
	PThreadScopedLock lock(registered_mutex);
	TimeType *t = registered_type_info.lookup(ri);
	if (t != NULL) {
	    SINVARIANT(time_type == *t || time_type == Unknown);
	    time_type = *t;
	    return;
	}
    }
    // not registered
    xmlNodePtr ftype = dataseries.getType()->xmlNodeFieldDesc(getName());
    string units = ExtentType::strGetXMLProp(ftype, "units");
    string epoch = ExtentType::strGetXMLProp(ftype, "epoch");
    setUnitsEpoch(units, epoch);
}

// The first two functions convert without range problems because the
// seconds part is always 32 bits; but the conversion in each
// direction has some amount of precision loss because tfrac can
// represent ~250 picoseconds, but does so in non-decimal units.

void 
Int64TimeField::frac32ToSecNano(int64_t tfrac, SecNano &secnano)
{
    static const double ns_conversion
	= (1000.0 * 1000 * 1000) / (4.0 * 1024 * 1024 * 1024);
    secnano.seconds = tfrac >> 32;
    uint32_t tfrac_lower = (tfrac & 0xFFFFFFFF);
    double nanoseconds = round(tfrac_lower * ns_conversion);
    DEBUG_SINVARIANT(nanoseconds >= 0 && nanoseconds < 1000 * 1000 * 1000);
    secnano.nanoseconds = static_cast<int32_t>(nanoseconds);
}

int64_t Int64TimeField::secNanoToFrac32(const SecNano &secnano) 
{
    // Can't use Lintel/Clock routines because they deal with
    // unsigned conversions.
    DEBUG_SINVARIANT(secnano.nanoseconds >= 0 
		     && secnano.nanoseconds < 1000 * 1000 * 1000);
    int64_t tfrac = static_cast<int64_t>(secnano.seconds) << 32;
    // Can't precalculate the 2^32/10^9 constant; there isn't enough
    // precision in some rare cases; see special-a in tests/time-field.cpp
    double tfrac_ns = (secnano.nanoseconds * 4.0 * 1024 * 1024 * 1024)
	/ (1000.0 * 1000 * 1000);
    tfrac_ns = round(tfrac_ns);
    DEBUG_SINVARIANT(tfrac_ns < 4.0*1024*1024*1024 && tfrac_ns >= 0);
    return tfrac + static_cast<int64_t>(tfrac_ns);
}

void 
Int64TimeField::splitSecNano(int64_t isecnano, SecNano &osecnano)
				  
{
    // convert to double and floor rounds badly in the 2^32-1 * 1e9 case
    // straight integer division seems to round toward 0.
    int64_t tmp = isecnano / (1000 * 1000 * 1000);
    if ((tmp*1000*1000*1000) > isecnano) {
	tmp -= 1;
	SINVARIANT(tmp*1000*1000*1000 < isecnano);
    }
    SINVARIANT(tmp >= numeric_limits<int32_t>::min() &&
	       tmp <= numeric_limits<int32_t>::max());
    osecnano.seconds = static_cast<int32_t>(tmp);
    int64_t tmp2 = isecnano - static_cast<int64_t>(tmp) * 1000 * 1000 * 1000;
    SINVARIANT(tmp2 >= 0 && tmp2 <= 999999999);
    osecnano.nanoseconds = static_cast<uint32_t>(tmp2);
}

void Int64TimeField::registerUnitsEpoch(const std::string &type_name,
					const std::string &name_space,
					const uint32_t major_version,
					const std::string &units,
					const std::string &epoch)
{
    FATAL_ERROR("unimplemented");
}

void Int64TimeField::setUnitsEpoch(const std::string &units,
				   const std::string &epoch)
{
    TimeType new_type = convertUnitsEpoch(units,epoch);
    INVARIANT(time_type == Unknown || time_type == new_type,
	      format("invalid to change the time type after it is set (%d != %d)")
	      % new_type % time_type);
    time_type = new_type;
}

static string str_unix("unix");
static string str_tfrac_seconds("2^-32 seconds");
static string str_nanoseconds("nanoseconds");
static string str_microseconds("microseconds");

Int64TimeField::TimeType 
Int64TimeField::convertUnitsEpoch(const std::string &units,
				  const std::string &epoch)
{
    INVARIANT(epoch == str_unix,
	      format("only handle unix epoch, not '%s' for field %s")
	      % epoch % getName());
    if (units == str_tfrac_seconds) {
	return UnixFrac32;
    } else if (units == str_nanoseconds) {
	return UnixNanoSec;
    } else if (units == str_microseconds) {
	return UnixMicroSec;
    } else {
	FATAL_ERROR(format("unrecognized time units %s") % units);
    }
}
