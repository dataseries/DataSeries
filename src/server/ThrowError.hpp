#ifndef DATASERIES_THROWERROR_HPP
#define DATASERIES_THROWERROR_HPP

#include <Lintel/LintelLog.hpp>

#include "gen-cpp/DataSeriesServer.h"

class ThrowError {
public:
    void requestError(const std::string &msg) {
        LintelLog::warn(boost::format("request error: %s") % msg);
        throw dataseries::RequestError(msg);
    }

    void requestError(const boost::format &fmt) {
        requestError(str(fmt));
    }

    void invalidTableName(const std::string &table, const std::string &msg) {
        LintelLog::warn(boost::format("invalid table name '%s': %s") % table % msg);
        throw dataseries::InvalidTableName(table, msg);
    }
};

// TODO: make this throw an error, and eventually figure out the right thing to have
// in AssertBoost.  Note that we want to be able to specify the class, have it automatically
// pick up file, line, expression, and any optional parameters (message, values, etc).
#define TINVARIANT(x) SINVARIANT(x)

#endif
