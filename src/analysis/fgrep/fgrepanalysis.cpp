#include <iostream>
#include <string>
#include <algorithm>
#include <string.h>

#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SimpleSourceModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/ExtentField.hpp>

#include "BoyerMooreHorspool.hpp"

using namespace std;

class FgrepAnalysisModule: public RowAnalysisModule {
public:
    FgrepAnalysisModule(DataSeriesModule &source, const string &pattern, bool print_matches=false);

    virtual void processRow() {
        ++line_number;
        const char *val = (const char*)line.val(); // not NULL-terminated
        int32_t size = line.size();

        if (matcher.matches(val, size)) {
            ++match_count;
            if (print_matches) {
                cout << std::string((char*)val, size)  << endl;
            }
        }
    }

    virtual void printResult() {
        cout << "*** Found " << match_count << ((match_count == 1) ? " match " : " matches") << " in " << line_number << " lines (" << extent_count << " extents)" << endl;
    }

    virtual ~FgrepAnalysisModule();

    virtual void newExtentHook(const Extent &e);

private:
        Variable32Field line;

        string pattern;
        int64_t line_number;
        int32_t match_count;
        int32_t extent_count;
        bool print_matches;
        BoyerMooreHorspool matcher;
};

FgrepAnalysisModule::FgrepAnalysisModule(DataSeriesModule &source, const string &pattern, bool print_matches) :
        RowAnalysisModule(source),
        line(series, "line"),
        pattern(pattern),
        line_number(0),
        match_count(0),
        extent_count(0),
        print_matches(print_matches),
        matcher(pattern.c_str(), pattern.size()) {
}

FgrepAnalysisModule::~FgrepAnalysisModule() {
}

void FgrepAnalysisModule::newExtentHook(const Extent &e) {
    ++extent_count;
}

void printUsage(const char *command) {
    cerr << "Usage: " << command << " [--no-memcpy] [--count] pattern filename" << endl <<
            "    --no-memcpy: Use the specialized SimpleSourceModule module for reading the (uncompressed) file" << endl <<
            "    --count: Suppress normal output; instead print a count of matching lines" << endl <<
            "    pattern: A fixed string that the fgrep module will search for" << endl <<
            "    filename: A DS file that was created via txt2ds (and --compress-none if --no-memcpy is used)" << endl;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        printUsage(argv[0]);
        return 1;
    }

    bool no_memcpy = false;
    bool count = false;
    const char *pattern = argv[argc - 2];
    const char *filename = argv[argc - 1];

    for (int i = 1; i < argc - 2; ++i) {
        string argument(argv[i]);
        if (argument == "--no-memcpy") {
            no_memcpy = true;
        } else if (argument == "--count") {
            count = true;
        } else {
            printUsage(argv[0]);
            return 2;
        }
    }

    DataSeriesModule *source = NULL;
    if (no_memcpy) {
        source = new SimpleSourceModule(filename);
        cout << "Reading with SimpleSourceModule (memcpy avoided)" << endl;
    } else {
        source = new TypeIndexModule("Text"); // "Text" is the type name used by ds2text
        static_cast<TypeIndexModule*>(source)->addSource(filename);
        cout << "Reading with TypeIndexModule (memcpy required)" << endl;
    }

    FgrepAnalysisModule analysis(*source, pattern, !count);
    analysis.getAndDelete();
    analysis.printResult();
    delete source;

    return 0;
}
