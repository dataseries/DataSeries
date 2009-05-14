/*
 * sortanalysis.cpp
 *
 *  Created on: Apr 30, 2009
 *      Author: shirant
 */

#include <string>
#include <algorithm>

#include <Lintel/LintelLog.hpp>

#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/MemorySortModule.hpp>
#include <DataSeries/SortModule.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentWriter.hpp>

class StringFieldComparator {
public:
    // simply returning field0.stringval() < field1.stringval() would also work
    // but increases the running time by 4x!
    bool operator()(const Variable32Field &field0, const Variable32Field &field1) {
        //return field0.val()[0] < field1.val()[1];
        int result = memcmp(field0.val(), field1.val(), std::min(field0.size(), field1.size()));
        return result == 0 ? (field0.size() < field1.size()) : (result < 0);
    }
};

int main(int argc, const char *argv[]) {
    LintelLog::parseEnv();

    TypeIndexModule inputModule("Text");
    inputModule.addSource(argv[1]);

    SortModule<Variable32Field, StringFieldComparator>
              sortModule(inputModule, "line", StringFieldComparator(), 1000 * 1000, 10 * 1000 * 1000);

    bool writeOutput = false;
    if (writeOutput) {
        DataSeriesSink sink(argv[2], Extent::compress_none, 0);
        bool wroteLibrary = false;
        Extent *extent = NULL;
        while ((extent = sortModule.getExtent()) != NULL) {
            if (!wroteLibrary) {
                ExtentTypeLibrary library;
                library.registerType(extent->getType());
                sink.writeExtentLibrary(library);
                wroteLibrary = true;
            }
            sink.writeExtent(*extent, NULL);
            delete extent;
        }
        sink.close();
    } else {
        Extent *extent = NULL;
        while ((extent = sortModule.getExtent()) != NULL) {
            delete extent;
        }
    }
}
