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
    string field_name, type_name, name_space;
    uint32_t major_version;
    bool operator==(const RegisteredInfo &a) const {
        return a.major_version == major_version && a.field_name == field_name 
                && a.type_name == type_name && a.name_space == name_space;
    }
    RegisteredInfo() : major_version(0) { }
    RegisteredInfo(const string &a, const string &b, const string &c, 
                   uint32_t d)
            : field_name(a), type_name(b), name_space(c), major_version(d) 
    { }
};

template <>
struct HashMap_hash<const RegisteredInfo> {
    uint32_t operator()(const RegisteredInfo &r) const {
        uint32_t a = lintel::hashBytes(r.field_name.data(), 
                                       r.field_name.size(), r.major_version);
        a = lintel::hashBytes(r.type_name.data(), r.type_name.size(), a);
        a = lintel::hashBytes(r.name_space.data(), r.name_space.size(), a);
        return a;
    }
};

static PThreadMutex registered_mutex;
static HashMap<RegisteredInfo, Int64TimeField::TimeType> registered_type_info;

Int64TimeField::Int64TimeField(ExtentSeries &series, const std::string &field,
                               unsigned flags, TimeType _time_type,
                               int64_t default_value, bool auto_add)
: Int64Field(series, field, flags, default_value, false), 
    time_type(_time_type)
{
    if (auto_add) {
        series.addField(*this);
    }
}

Int64TimeField::~Int64TimeField() { }

int64_t Int64TimeField::rawToFrac32(Raw raw) const {
    switch (time_type)
    {
        case UnixFrac32: return raw;
        case UnixNanoSec: {
            SecNano secnano;
            splitSecNano(raw, secnano);
            return secNanoToFrac32(secnano);
        }
        case UnixMicroSec: {
            SecNano secnano;
            splitSecMicro(raw, secnano);
            return secNanoToFrac32(secnano);
        }
        case Unknown:
            FATAL_ERROR("time type has not been set yet; no extent?");
        default:
            FATAL_ERROR(format("internal error, unhandled time type %d") % time_type);
    }
}

Int64TimeField::Raw Int64TimeField::frac32ToRaw(int64_t frac32) const {
    switch(time_type)
    {
        case UnixFrac32: return frac32;
        case UnixNanoSec: {
            SecNano secnano;
            frac32ToSecNano(frac32, secnano);
            return joinSecNano(secnano);
        }
        case Unknown:
            FATAL_ERROR("time type has not been set yet; no extent?");
        default:
            FATAL_ERROR("internal error");
    }
}

Int64TimeField::SecNano Int64TimeField::rawToSecNano(Raw raw) const {
    SecNano ret;
    switch (time_type)
    {
        case UnixFrac32: 
            frac32ToSecNano(raw, ret);
            break;
        case UnixNanoSec:
            splitSecNano(raw, ret);
            break;
        case UnixMicroSec:
            splitSecMicro(raw, ret);
            break;
        case Unknown:
            FATAL_ERROR("time type has not been set yet; no extent?");
        default:
            FATAL_ERROR(format("internal error, unhandled time type %d") % time_type);
    }

    return ret;
}

Int64TimeField::Raw Int64TimeField::secNanoToRaw(int32_t seconds, uint32_t nanoseconds) const {
    SINVARIANT(nanoseconds < (1000*1000*1000));
    switch(time_type)
    {
        case UnixFrac32: return secNanoToFrac32(SecNano(seconds, nanoseconds));
        case UnixNanoSec: return joinSecNano(seconds, nanoseconds);
        case Unknown: FATAL_ERROR("time type has not been set yet; no extent?");
        default: FATAL_ERROR("internal error");
    }
}

string Int64TimeField::rawToStrSecNano(Raw raw) const {
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

double Int64TimeField::rawToDoubleSeconds(Raw raw, bool precision_check) const {
    // max_seconds = floor(2^52/(1000^3)), using 52 bits to give us 1
    // bit of extra precision to achive nanosecond resolution on the
    // return value.
    static const int32_t max_seconds = 4503599;
    switch (time_type) 
    {
        case UnixFrac32: {
            int32_t seconds = raw >> 32;
            SINVARIANT(!precision_check || (seconds <= max_seconds && seconds >= -max_seconds));
            return Clock::int64TfracToDouble(raw);
        }
        case UnixNanoSec: {
            SecNano tmp;
            splitSecNano(raw, tmp);
            SINVARIANT(!precision_check 
                       || (tmp.seconds <= max_seconds && tmp.seconds >= -max_seconds));
            return tmp.seconds + tmp.nanoseconds / (1.0e9);
        }
        case Unknown: FATAL_ERROR("time type has not been set yet; no extent?");
        default: FATAL_ERROR("internal error");
    }
}

Int64TimeField::Raw Int64TimeField::doubleSecondsToRaw(double seconds) const {
    SINVARIANT(seconds > numeric_limits<int32_t>::min() && 
               seconds < numeric_limits<int32_t>::max());
    int32_t i_seconds = static_cast<int32_t>(floor(seconds));
    uint32_t nanosec = static_cast<uint32_t>(round(1.0e9 * (seconds - i_seconds)));
    if (nanosec == 1000000000) {
        ++i_seconds;
        nanosec = 0;
    }
    return secNanoToRaw(i_seconds, nanosec);
}

void Int64TimeField::newExtentType() {
    Int64Field::newExtentType();
    DEBUG_SINVARIANT(dataseries.getTypePtr() != NULL);
    RegisteredInfo ri(getName(),
                      dataseries.getTypePtr()->getName(),
                      dataseries.getTypePtr()->getNamespace(),
                      dataseries.getTypePtr()->majorVersion());
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
    xmlNodePtr ftype = dataseries.getTypePtr()->xmlNodeFieldDesc(getName());
    string units = ExtentType::strGetXMLProp(ftype, "units");
    string epoch = ExtentType::strGetXMLProp(ftype, "epoch");
    setUnitsEpoch(units, epoch);
}

// The first two functions convert without range problems because the
// seconds part is always 32 bits; but the conversion in each
// direction has some amount of precision loss because tfrac can
// represent ~250 picoseconds, but does so in non-decimal units.

void Int64TimeField::frac32ToSecNano(int64_t tfrac, SecNano &secnano) {
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

void Int64TimeField::splitSecNano(int64_t isecnano, SecNano &osecnano) {
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

void Int64TimeField::splitSecMicro(int64_t isecmicro, SecNano &osecnano) {
    int64_t tmp = isecmicro / (1000 * 1000);
    if ((tmp*1000*1000) > isecmicro) {
        tmp -= 1;
        SINVARIANT(tmp*1000*1000 < isecmicro);
    }
    SINVARIANT(tmp >= numeric_limits<int32_t>::min() &&
               tmp <= numeric_limits<int32_t>::max());
    osecnano.seconds = static_cast<int32_t>(tmp);
    int64_t tmp2 = isecmicro - static_cast<int64_t>(tmp) * 1000 * 1000;
    SINVARIANT(tmp2 >= 0 && tmp2 <= 999999);
    osecnano.nanoseconds = static_cast<uint32_t>(tmp2) * 1000;
}

void Int64TimeField::registerUnitsEpoch(const std::string &field_name,
                                        const std::string &type_name,
                                        const std::string &name_space,
                                        const uint32_t major_version,
                                        const std::string &units,
                                        const std::string &epoch) {
    TimeType time_type = convertUnitsEpoch(units, epoch, field_name);

    RegisteredInfo ri(field_name, type_name, name_space, major_version);

    PThreadScopedLock lock(registered_mutex);
    if (registered_type_info.exists(ri)) {
        SINVARIANT(registered_type_info[ri] == time_type);
    } else {
        registered_type_info[ri] = time_type;
    }
}

void Int64TimeField::setUnitsEpoch(const std::string &units,
                                   const std::string &epoch) {
    TimeType new_type = convertUnitsEpoch(units, epoch, getName(), true);
    if (new_type == Unknown) {
        INVARIANT(time_type != Unknown,
                  format("Can not figure out time type for field %s, in type %s:"
                         " units '%s', epoch '%s'") 
                  % getName() % (dataseries.hasExtent() 
                                 ? dataseries.getTypePtr()->getName() : "unknown")
                  % units % epoch);
    } else {
        INVARIANT(time_type == Unknown  || time_type == new_type, 
                  format("invalid to change the time type after it is set (%d != %d)")
                  % new_type % time_type);
        time_type = new_type;
    }
}

static string str_unix("unix");
static string str_unknown("unknown");
static string str_tfrac_seconds("2^-32 seconds");
static string str_nanoseconds("nanoseconds");
static string str_microseconds("microseconds");

Int64TimeField::TimeType Int64TimeField::convertUnitsEpoch(const std::string &units,
                                                           const std::string &epoch,
                                                           const std::string &field_name,
                                                           bool unknown_return_ok) {
    if (epoch != str_unix && unknown_return_ok) {
        return Unknown;
    }
    INVARIANT(epoch == str_unix || epoch == str_unknown,
              format("only handle unix epoch, not '%s' for field %s")
              % epoch % field_name);
    if (units == str_tfrac_seconds) {
        return UnixFrac32;
    } else if (units == str_nanoseconds) {
        return UnixNanoSec;
    } else if (units == str_microseconds) {
        return UnixMicroSec;
    } else {
        INVARIANT(unknown_return_ok, 
                  format("unrecognized time units %s") % units);
        return Unknown;
    }
}
