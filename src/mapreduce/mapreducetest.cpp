// TODO-tomer: fix copyright.
/*
 * mapreducetest.cpp
 *
 *  Created on: Apr 30, 2009
 *      Author: shirant
 */

// TODO-tomer: include this in examples directory and brad may hug you.

#include <string>
#include <algorithm>

#include <Lintel/LintelLog.hpp>

#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/MemorySortModule.hpp>
#include <DataSeries/SortModule.hpp>
#include <DataSeries/Extent.hpp>

class StringFieldComparator {
public:
    // simply returning field0.stringval() < field1.stringval() would also work
    // but increases the running time by 4x!
    bool operator()(const Variable32Field &field0, const Variable32Field &field1) {
        const ExtentType::byte *val0 = field0.val();
        const ExtentType::byte *val1 = field1.val();
        int size = std::min(field0.size(), field1.size());
        for (int i = 0; i < size; ++i) {
            if (*val0 < *val1) return true;
            if (*val0 > *val1) return false;
            ++val0;
            ++val1;
        }

        // they were equal all the way until the minimum length so the shorter wins
        return size == field0.size();
    }
};

int main(int argc, const char *argv[]) {
    LintelLog::parseEnv();
    LintelLogDebug("mapreducedemo", "Hi!");

    TypeIndexModule inputModule("Text");
    inputModule.addSource(argv[1]);

    //MemorySortModule<Variable32Field> sortModule(inputModule, "line", StringFieldComparator(), 1 << 20);
    //SortModule<Variable32Field> sortModule(inputModule, "line",
    //StringFieldComparator(), 1 << 20, 1 << 30, "/tmp/sort");
    // TODO-tomer: at least use default values. 
    SortModule<Variable32Field> sortModule(inputModule, "line", StringFieldComparator(), 1000 * 1000, 1000 * 1000 * 1000, "/tmp/sort");

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
}
