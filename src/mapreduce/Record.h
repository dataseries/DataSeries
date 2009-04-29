/*
 * Record.h
 *
 *  Created on: Apr 24, 2009
 *      Author: shirant
 */

#ifndef RECORD_H_
#define RECORD_H_

#include <string>
#include <memory>
#include <vector>

#include <DataSeries/DataSeriesModule.hpp>

class TypeIndexModule;

class Record : public ExtentSeries::IExtentPositionComparator {
public:
    Record();
    virtual ~Record();

    virtual const char* getTypeName() const = 0;
    virtual const char* getTypeXml() const = 0;
    ExtentSeries& getSeries();

    void attachInput(const std::string &file, size_t bytesPerFetch = 0, bool sort = false);
    void attachOutput(const std::string &file, int extentSize = 64 * 1024);

    bool read();
    void create();

    void close();

    virtual bool compare(const ExtentSeries::ExtentPosition &lhsExtentPosition,
            const ExtentSeries::ExtentPosition &rhsExtentPosition) { return false; }

protected:
    ExtentSeries series;

private:
    void fetchExtents();

    std::auto_ptr<TypeIndexModule> inputModule;
    std::vector<Extent*> extents;

    std::auto_ptr<OutputModule> outputModule;
    std::auto_ptr<DataSeriesSink> sink;


    bool started;
    size_t bytesPerFetch;
    size_t extentsPerFetch; // will be calculated after first extent is read
    bool sort;
    size_t index;
};

#endif /* RECORD_H_ */
