#include <iostream>
#include <string>
#include <pcre.h>

#include <string.h>

#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/ExtentField.hpp>

using namespace std;

class FgrepAnalysisModule: public RowAnalysisModule {
public:
    FgrepAnalysisModule(DataSeriesModule &source, const string &pattern);

    bool find(const char *line, int32_t line_length, const char *substr, int32_t substr_length) {
    	return false;/*
    	// line is not NULL-terminated
    	int32_t last_possible_position = line_length - substr_length;
    	for (int32_t i = 0; i < last_possible_position; ++i) {
    		const char *line_position = line;
    		const char *substr_position = substr;
    		int32_t j = 0;
    		for (j = 0; j < substr_length; j++) {
    			if (*line_position != *substr_position) break;
    			++line_position;
    			++substr_position;
    		}
    		if (j == substr_length) return true;
    		++line;
    	}
    	return false;*/
    }

    virtual void processRow() {
    	++line_number;
    	const char *val = (const char*)line.val(); // not NULL-terminated
    	int32_t size = line.size();


    	//if (size > 0) {
    	//    if (*val == *pattern.c_str()) {
    	//        ++match_count;
    	//    }
    	//}
    	//const char *pattern_val = pattern.c_str();
    	//int32_t pattern_size_plus_one = pattern.size() + 1;

		//if (pattern_size_plus_one >= size) return;


    	//if (pcre_exec(re, NULL, (const char*)val, size, 0, 0, NULL, 0) == 0) {
    	if (*val == *pattern.c_str()) {
            ++match_count;
            //cout << std::string((char*)val, size)  << endl;

    	}
    	/*for (int32_t i = size - pattern_size_plus_one; i != 0; --i) {
    		const char *temp_val = val;
    		const char *temp_pattern_val = pattern_val;
    		int32_t j;
    		for (j = pattern_size_plus_one; j != 0; --j) {
    			if (*temp_pattern_val != *temp_val) break;
    			++temp_pattern_val; ++temp_val;
    		}
    		if (j == 0) {
    			++match_count;
    			break;
    		}
    		++val;
    	}*/
    	//if (false) {
    	//	cout << line_number << ": " << std::string((char*)val, size) << endl;
    	//	++match_count;
    	//}
    }

    virtual void printResult() {
        cout << "*** Found " << match_count << ((match_count == 1) ? " match " : " matches") << " in " << line_number << " lines (" << extent_count <<" extents)" << endl;
    }

    virtual ~FgrepAnalysisModule();

    virtual void newExtentHook(const Extent &e);

private:
	Variable32Field line;

	string pattern;
	uint64_t line_number;
	uint32_t match_count;
	uint32_t extent_count;
	pcre *re;
};

FgrepAnalysisModule::FgrepAnalysisModule(DataSeriesModule &source, const string &pattern) :
	RowAnalysisModule(source),
	line(series, "line"),
	pattern(pattern),
	//regular_expression(pattern),
	line_number(0),
	match_count(0),
        extent_count(0) {

	const char *error;
	int erroffset;
	re = pcre_compile(pattern.c_str(), 0, &error, &erroffset, NULL);
	INVARIANT(re != NULL, boost::format("Unable to create the regular expression"));
}

FgrepAnalysisModule::~FgrepAnalysisModule() {
	pcre_free(re);
}

void FgrepAnalysisModule::newExtentHook(const Extent &e) {
    ++extent_count;
}

int main(int argc, char* argv[]) {
    cout << "Starting fgrep analysis..." << endl;
    TypeIndexModule source("Text");

    INVARIANT(argc >= 3 && strcmp(argv[1], "-h") != 0,
    		boost::format("Usage: %s pattern <file...>\n") % argv[0]);

    for (int i = 2; i < argc; ++i) {
        source.addSource(argv[i]);
    }

    FgrepAnalysisModule analysis(source, argv[1]);
    analysis.getAndDelete();
    analysis.printResult();

    return 0;
}
