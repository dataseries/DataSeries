#include <Lintel/StatsQuantile.hpp>

#include <DataSeries/ExtentField.hpp>
#include <DataSeries/RowAnalysisModule.hpp>

using namespace std;
using dataseries::TFixedField;

class ReadWriteExtentAnalysis : public RowAnalysisModule {
public:
    ReadWriteExtentAnalysis(DataSeriesModule &source)
        : RowAnalysisModule(source),
          filehandle(series, "filehandle"),
          is_read(series, ""),
          offset(series, "offset"),
          bytes(series, "bytes"),
	  read_size(0.005, static_cast<uint64_t>(1.0e10))
    {
    }

    virtual ~ReadWriteExtentAnalysis() { }

    void newExtentHook(const Extent &e) {
	if (series.getType() != NULL) {
	    return; // already did this
	}
	const ExtentType &type = e.getType();
	if (type.getName() == "NFS trace: read-write") {
	    SINVARIANT(type.getNamespace() == "" &&
		       type.majorVersion() == 0 &&
		       type.minorVersion() == 0);
	    is_read.setFieldName("is-read");
	} else if (type.getName() == "Trace::NFS::read-write"
		   && type.versionCompatible(1,0)) {
	    is_read.setFieldName("is-read");
	} else if (type.getName() == "Trace::NFS::read-write"
		   && type.versionCompatible(2,0)) {
	    is_read.setFieldName("is_read");
	} else {
	    FATAL_ERROR("?");
	}
    }

    virtual void processRow() {
	if (is_read.val()) {
	    read_size.add(bytes.val());
	} else {
	    write_size.add(bytes.val());
	}
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	
	cout << "Read size:\n";
	read_size.printTextRanges(cout, 100);
	read_size.printTextTail(cout);
	cout << "Write size:\n";
	write_size.printTextRanges(cout, 100);
	write_size.printTextTail(cout);
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }

private:
    Variable32Field filehandle;
    BoolField is_read;
    TFixedField<int64_t> offset;
    TFixedField<int32_t> bytes;

    StatsQuantile read_size, write_size;
};

namespace NFSDSAnalysisMod {
    RowAnalysisModule *
    newReadWriteExtentAnalysis(DataSeriesModule &prev) {
	return new ReadWriteExtentAnalysis(prev);
    }
}

