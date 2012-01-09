#include "DSSModule.hpp"

class TeeModule : public RowAnalysisModule {
public:
    TeeModule(DataSeriesModule &source_module, const string &output_path)
	: RowAnalysisModule(source_module), output_path(output_path), 
	  output_series(), output(output_path, Extent::compress_lzf, 1),
	  output_module(NULL), copier(series, output_series), row_count(0), first_extent(false)
    { }

    virtual ~TeeModule() {
	delete output_module;
    }

    virtual void firstExtent(const Extent &e) {
	series.setType(e.getType());
	output_series.setType(e.getType());
        SINVARIANT(output_module == NULL);
	output_module = new OutputModule(output, output_series, e.getType(), 96*1024);
	copier.prep();
	ExtentTypeLibrary library;
	library.registerType(e.getType());
	output.writeExtentLibrary(library);
        first_extent = true;
    }

    virtual void processRow() {
	++row_count;
	output_module->newRecord();
	copier.copyRecord();
    }

    virtual void completeProcessing() {
        if (!first_extent) {
            SINVARIANT(row_count == 0);
            LintelLog::warn(format("no rows in %s") % output_path);
            ExtentTypeLibrary library;
            output.writeExtentLibrary(library);
        }
    }

    void close() {
        delete output_module;
        output_module = NULL;
        output.close();
    }

    const string output_path;
    ExtentSeries output_series;
    DataSeriesSink output;
    OutputModule *output_module;
    ExtentRecordCopy copier;
    uint64_t row_count;
    bool first_extent;
};

DataSeriesModule::Ptr dataseries::makeTeeModule(DataSeriesModule &source_module, 
                                                const string &output_path) {
    return DataSeriesModule::Ptr(new TeeModule(source_module, output_path));
}

