#include <string>

#include <boost/format.hpp>
#include <boost/foreach.hpp>

#include <Lintel/LintelLog.hpp>
#include <DataSeries/TypeIndexModule.hpp>

#include "Mapper.h"
#include "Reducer.h"

using namespace std;
using boost::format;

class DemoInputRecord : public Record {
public:
    DemoInputRecord(): line(series, "line") {}
    virtual ~DemoInputRecord() {}

    virtual const char* getTypeName() const { return "Text"; }
    virtual const char* getTypeXml() const {
        return
            "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Text\">"
            "  <field name=\"line\" type=\"variable32\"/>"
            "</ExtentType>\n";
    }

    Variable32Field line;
};


class DemoMapRecord : public Record {
public:
    DemoMapRecord(): line(series, "line") {}
    virtual ~DemoMapRecord() {}

    virtual const char* getTypeName() const { return "DemoMapType"; }
    virtual const char* getTypeXml() const {
        return
            "<ExtentType name=\"DemoMapType\">"
            "  <field name=\"line\" type=\"variable32\"/>"
            "</ExtentType>\n";
    }

    Variable32Field line;

    virtual bool compare(const ExtentSeries::ExtentPosition &lhsExtentPosition,
                const ExtentSeries::ExtentPosition &rhsExtentPosition) {
        // grab rhs string
        series.setExtentPosition(rhsExtentPosition);
        string rhsVal(line.stringval());

        // grab lhs string
        series.setExtentPosition(lhsExtentPosition);
        string lhsVal(line.stringval());

        return lhsVal < rhsVal;
    }

    virtual int compare(Record *rhsRecord) {
        DemoMapRecord *record = static_cast<DemoMapRecord*>(rhsRecord);
        string lhsVal(line.stringval());
        string rhsVal(record->line.stringval());
        if (lhsVal < rhsVal) return -1;
        if (lhsVal == rhsVal) return 0;
        return 1;
    }
};


class DemoOutputRecord : public Record {
public:
    DemoOutputRecord(): line(series, "line") {}
    virtual ~DemoOutputRecord() {}

    virtual const char* getTypeName() const { return "DemoOutputType"; }
    virtual const char* getTypeXml() const {
        return
            "<ExtentType name=\"DemoOutputType\">"
            "  <field name=\"line\" type=\"variable32\"/>"
            "</ExtentType>\n";
    }

    Variable32Field line;
};


class DemoMapper : public Mapper {
public:
    DemoMapper() {}
    virtual ~DemoMapper() {}

    virtual void map(Record *inputRecord, Record *outputRecord) {
        DemoInputRecord *input = static_cast<DemoInputRecord*>(inputRecord);
        DemoMapRecord *output = static_cast<DemoMapRecord*>(outputRecord);
        output->create();
        output->line.set(input->line.val(), input->line.size());
    }
};


class DemoReducer : public Reducer {
public:
    DemoReducer() {}
    virtual ~DemoReducer() {}

    virtual void reduce(const vector<Record*> &inputRecords, Record *outputRecord) {
        DemoOutputRecord *output = static_cast<DemoOutputRecord*>(outputRecord);

        BOOST_FOREACH(Record *inputRecord, inputRecords) {
            DemoMapRecord *input = static_cast<DemoMapRecord*>(inputRecord);
            output->create();
            output->line.set(input->line.val(), input->line.size());
        }
    }
private:
    const Field *keyField;
};

void map(Mapper &mapper, Record &inputRecord, Record &mapOutputRecord) {
    LintelLogDebug("mapreduce", "Starting map");

    while (inputRecord.read()) {
        mapper.map(&inputRecord, &mapOutputRecord);
    }
}

void reduce(Reducer &reducer, const vector<Record*> &mapInputRecords, Record &outputRecord) {
    // read all the records in mapInputRecord, sort them, and call outputRecord
    LintelLogDebug("mapreduce", "Starting reduce");

    // TODO: reduce is totally wrong in terms of logic!!


    while (mapInputRecords[0]->read()) {
        reducer.reduce(mapInputRecords, &outputRecord);
    }
}

int main(int argc, const char *argv[]) {
    LintelLog::parseEnv();
    LintelLogDebug("mapreducedemo", "Hi!");

    DemoInputRecord inputRecord;
    inputRecord.attachInput(argv[1]);

    DemoMapRecord mapOutputRecord;
    mapOutputRecord.attachOutput(argv[2]);

    DemoMapRecord mapInputRecord0;
    mapInputRecord0.attachInput(argv[2], 1 << 20, true); // read and sort approximately 1 MB of records at a time

    vector<Record*> mapInputRecords;
    mapInputRecords.push_back(&mapInputRecord0);

    DemoOutputRecord outputRecord;
    outputRecord.attachOutput(argv[3]);

    DemoMapper mapper;
    DemoReducer reducer;

    ::map(mapper, inputRecord, mapOutputRecord);
    inputRecord.close();
    mapOutputRecord.close();

    ::reduce(reducer, mapInputRecords, outputRecord);
    mapInputRecord0.close();
    outputRecord.close();

    return 0;
}
