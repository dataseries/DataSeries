/*
 * grepanalysis.cpp
 *
 *  Created on: May 5, 2009
 *      Author: shirant
 */


#include <string>
#include <algorithm>

#include <boost/foreach.hpp>
#include <boost/format.hpp>

#include <Lintel/BoyerMooreHorspool.hpp>
#include <Lintel/LintelLog.hpp>

#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/GrepModule.hpp>
#include <DataSeries/Extent.hpp>

using namespace std;

class StringFieldMatcher {
public:
    StringFieldMatcher(const string &needle) : matcher(needle.c_str(), needle.size()) {
    }

    bool operator()(const Variable32Field &field) {
        return matcher.matches((const char*)field.val(), field.size());
    }

private:
    BoyerMooreHorspool matcher;
};

int main(int argc, const char *argv[]) {
    LintelLog::parseEnv();
    const char *inputFile = argv[1];
    const char *outputFile = argv[2];
    const char *needle = argv[3];
    bool countOnly = true;

    LintelLogDebug("grepanalysis", "Starting grep analysis");

    TypeIndexModule inputModule("Text");
    inputModule.addSource(inputFile);

    StringFieldMatcher fieldMatcher(needle);
    string fieldName("line");
    GrepModule grepModule(inputModule, fieldName, fieldMatcher);

    size_t count = 0;
    if (!countOnly) {
        Extent *extent = grepModule.getExtent(); // the first extent
        if (extent != NULL) {
            DataSeriesSink sink(outputFile, Extent::compress_none, 0);
            ExtentTypeLibrary library;
            library.registerType(extent->getType());
            sink.writeExtentLibrary(library);

            while (extent != NULL) {
                sink.writeExtent(*extent, NULL);
                count += extent->getRecordCount();

                delete extent;
                extent = grepModule.getExtent();
            }
            sink.close();
        }
    } else {
        Extent *extent = NULL;
        while ((extent = grepModule.getExtent()) != NULL) {
            count += extent->getRecordCount();
            delete extent;
        }
    }
    LintelLogDebug("grepanalysis", boost::format("Found %d occurrence(s) of the string '%s'") % count % needle);

}
