#include <Lintel/StatsQuantile.hpp>

#include <DataSeries/ExtentField.hpp>
#include <DataSeries/RowAnalysisModule.hpp>

using namespace std;
using boost::format;
using dataseries::TFixedField;

class ReadWriteExtentAnalysis : public RowAnalysisModule {
  public:
    ReadWriteExtentAnalysis(DataSeriesModule &source)
    : RowAnalysisModule(source),
      filehandle(series, "filehandle"),
      is_read(series, ""),
      offset(series, "offset"),
      bytes(series, "bytes"),
      read_size(0.005, static_cast<uint64_t>(1.0e10)),
      write_size(0.005, static_cast<uint64_t>(1.0e10)),
      read_offset(0.001, static_cast<uint64_t>(1.0e10)),
      write_offset(0.001, static_cast<uint64_t>(1.0e10))
    {
    }

    virtual ~ReadWriteExtentAnalysis() { }

    void newExtentHook(const Extent &e) {
        if (series.getTypePtr() != NULL) {
            return; // already did this
        }
        const ExtentType::Ptr type = e.getTypePtr();
        if (type->getName() == "NFS trace: read-write") {
            SINVARIANT(type->getNamespace() == "" &&
                       type->majorVersion() == 0 &&
                       type->minorVersion() == 0);
            is_read.setFieldName("is-read");
        } else if (type->getName() == "Trace::NFS::read-write"
                   && type->versionCompatible(1,0)) {
            is_read.setFieldName("is-read");
        } else if (type->getName() == "Trace::NFS::read-write"
                   && type->versionCompatible(2,0)) {
            is_read.setFieldName("is_read");
        } else {
            FATAL_ERROR("?");
        }
    }

    virtual void processRow() {
        if (is_read.val()) {
            read_size.add(bytes.val());
            read_offset.add(offset.val());
        } else {
            write_size.add(bytes.val());
            write_offset.add(offset.val());
        }
    }

    virtual void printResult() {
        cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;
        
        cout << "Read size:\n";
        read_size.printTextRanges(cout, 100);
        read_size.printTextTail(cout);

        cout << "Read offset:\n";
        read_offset.printTextRanges(cout, 500);

        cout << "Write size:\n";
        write_size.printTextRanges(cout, 100);
        write_size.printTextTail(cout);

        cout << "Write offset:\n";
        write_offset.printTextRanges(cout, 500);
        cout << format("End-%s\n") % __PRETTY_FUNCTION__;
    }

  private:
    Variable32Field filehandle;
    BoolField is_read;
    TFixedField<int64_t> offset;
    TFixedField<int32_t> bytes;

    StatsQuantile read_size, write_size;
    StatsQuantile read_offset, write_offset;
};

namespace NFSDSAnalysisMod {
    RowAnalysisModule *
    newReadWriteExtentAnalysis(DataSeriesModule &prev) {
        return new ReadWriteExtentAnalysis(prev);
    }
}

