#include <iostream>
#include <string>
//#include <pcre.h>
#include <algorithm>
#include <string.h>

#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SimpleSourceModule.hpp>
#include <DataSeries/ExtentField.hpp>

using namespace std;

class BoyerMooreHorspool {
public:
    BoyerMooreHorspool(const char *needle, int32_t needle_length);
    ~BoyerMooreHorspool();

    bool matches(const char *haystack, int32_t haystack_length);

private:
    int32_t bad_char_shift[CHAR_MAX + 1];
    char *needle;
    int32_t needle_length;
    int32_t last;
};

BoyerMooreHorspool::BoyerMooreHorspool(const char *needle, int32_t needle_length) :
        needle_length(needle_length), last(needle_length - 1) {
    // initialize the bad character shift array
    for (int32_t i = 0; i <= CHAR_MAX; ++i) {
        bad_char_shift[i] = needle_length;
    }

    for (int32_t i = 0; i < last; i++) {
        bad_char_shift[(int8_t)needle[i]] = last - i;
    }

    this->needle = new char[needle_length];
    memcpy(this->needle, needle, needle_length);
}

BoyerMooreHorspool::~BoyerMooreHorspool() {
    delete [] needle;
}

bool BoyerMooreHorspool::matches(const char *haystack, int32_t haystack_length) {
    while (haystack_length >= needle_length) {
        int32_t i;
        for (i = last; haystack[i] == needle[i]; --i) {
            if (i == 0) { // first char matches so it's a match!
                return true;
            }
        }

        int32_t skip = bad_char_shift[(int8_t)haystack[last]];
        haystack_length -= skip;
        haystack += skip;
    }
    return false;
}

class FgrepAnalysisModule: public RowAnalysisModule {
public:
    FgrepAnalysisModule(DataSeriesModule &source, const string &pattern);

    virtual void processRow() {
        ++line_number;
        const char *val = (const char*)line.val(); // not NULL-terminated
        int32_t size = line.size();

        //if (pcre_exec(re, NULL, (const char*)val, size, 0, 0, NULL, 0) == 0) {
        if (matcher.matches(val, size)) {
            ++match_count;
            //cout << std::string((char*)val, size)  << endl;
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

        BoyerMooreHorspool matcher;
        //pcre *re;
};

FgrepAnalysisModule::FgrepAnalysisModule(DataSeriesModule &source, const string &pattern) :
        RowAnalysisModule(source),
        line(series, "line"),
        pattern(pattern),
        line_number(0),
        match_count(0),
        extent_count(0),
        matcher(pattern.c_str(), pattern.size()) {


        //const char *error;
        //int erroffset;
        //re = pcre_compile(pattern.c_str(), 0, &error, &erroffset, NULL);
        //INVARIANT(re != NULL, boost::format("Unable to create the regular expression"));
}

FgrepAnalysisModule::~FgrepAnalysisModule() {
        //pcre_free(re);
}

void FgrepAnalysisModule::newExtentHook(const Extent &e) {
    ++extent_count;
}

int main(int argc, char* argv[]) {
    cout << "Starting fgrep analysis..." << endl;


    INVARIANT(argc >= 3 && strcmp(argv[1], "-h") != 0,
                boost::format("Usage: %s pattern <file...>\n") % argv[0]);

    SimpleSourceModule source(argv[2]);
    FgrepAnalysisModule analysis(source, argv[1]);
    analysis.getAndDelete();
    analysis.printResult();

    return 0;
}
