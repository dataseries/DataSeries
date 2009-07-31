#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/uio.h>

#include <algorithm>
#include <iostream>
#include <string>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/random.hpp>
#include <boost/foreach.hpp>

#include <Lintel/PriorityQueue.hpp>
#include <Lintel/ProgramOptions.hpp>

using namespace std;

#define HEAD_BYTE_COUNT (2)
#define HEAD_COUNT (1 << (HEAD_BYTE_COUNT * 8))

struct Buffer {
    uint8_t *data;
    size_t size;
    uint8_t *end;
};

struct Record {
    uint8_t key[10];
    uint8_t value[90];
};

struct Head {
    Head() : count(0) {}
    vector<Record*> records;
    size_t count;
};

struct Comparator {
    bool operator()(Record *lhs, Record *rhs) {
        return memcmp(lhs->key, rhs->key, 10) < 0;
    }
};

Buffer readFileToBuffer(string filename) {
    struct stat file_stat;
    stat(filename.c_str(), &file_stat);

    Buffer buffer;
    buffer.size = file_stat.st_size;
    buffer.data = new uint8_t[buffer.size];
    buffer.end = buffer.data + buffer.size;

    FILE *file = fopen(filename.c_str(), "rb");
    size_t elements_read = fread(buffer.data, buffer.size, 1, file);
    SINVARIANT(elements_read == 1);
    return buffer;
}

int main(int argc, char *argv[]) {
    SINVARIANT(argc > 1);
    Buffer buffer = readFileToBuffer(argv[1]);

    Head heads[HEAD_COUNT];
    memset(heads, 0, sizeof(heads));

    Record *record = reinterpret_cast<Record*>(buffer.data);
    Record *end = reinterpret_cast<Record*>(buffer.end);

    while (record < end) {
        ++heads[*reinterpret_cast<uint16_t*>(record->key)].count;
        ++record;
    }

    // START PARALLELIZATION
    for (uint32_t i = 0; i < HEAD_COUNT; ++i) {
        heads[i].records.reserve(heads[i].count);
    }
    // END PARALLELIZATION

    record = reinterpret_cast<Record*>(buffer.data);
    while (record < end) {
        heads[*reinterpret_cast<uint16_t*>(record->key)].records.push_back(record);
        ++record;
    }

    // START PARALLELIZATION
    Comparator comparator;
    for (uint32_t i = 0; i < HEAD_COUNT; ++i) {
        sort(heads[i].records.begin(), heads[i].records.end(), comparator);
    }
    // END PARALLELIZATION

    for (uint32_t i = 0; i < HEAD_COUNT; ++i) {
        BOOST_FOREACH(Record *record, heads[i].records) {
            record = NULL;
        }
    }

    delete [] buffer.data;
}
