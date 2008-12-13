#include <Lintel/ConstantString.hpp>
#include <Lintel/HashMap.hpp>
#include <Lintel/HashUnique.hpp>
#include <Lintel/StatsQuantile.hpp>

#include <DataSeries/RowAnalysisModule.hpp>

#include <analysis/nfs/common.hpp>

using namespace std;
using boost::format;
using dataseries::TFixedField;

class UniqueFileHandles : public RowAnalysisModule {
public:
    UniqueFileHandles(DataSeriesModule &source)
        : RowAnalysisModule(source),
          filehandle(series, "filehandle"),
          lookup_dir_filehandle(series, "", Field::flag_nullable),
	  file_size(series, ""),
	  last_size_report(0)
    {
    }

    virtual ~UniqueFileHandles() { }

    void newExtentHook(const Extent &e) {
	if (fh_to_size.size() > last_size_report + 1000000) {
	    cout << format("UniqueFileHandles interim count: %d\n")
		% fh_to_size.size();
	    last_size_report = fh_to_size.size();
	    INVARIANT(last_size_report < 2000000000, 
		      "HashMap about to overflow");
	}
	if (series.getType() != NULL) {
	    return; // already did this
	}
	const ExtentType &type = e.getType();
	if (type.getName() == "NFS trace: attr-ops") {
	    SINVARIANT(type.getNamespace() == "" &&
		       type.majorVersion() == 0 &&
		       type.minorVersion() == 0);
	    lookup_dir_filehandle.setFieldName("lookup-dir-filehandle");
	    file_size.setFieldName("file-size");
	} else if (type.getName() == "Trace::NFS::attr-ops"
		   && type.versionCompatible(1,0)) {
	    lookup_dir_filehandle.setFieldName("lookup-dir-filehandle");
	    file_size.setFieldName("file-size");
	} else if (type.getName() == "Trace::NFS::attr-ops"
		   && type.versionCompatible(2,0)) {
	    lookup_dir_filehandle.setFieldName("lookup_dir_filehandle");
	    file_size.setFieldName("file_size");
	} else {
	    FATAL_ERROR("?");
	}
    }

#define USE_MD5 1

    void addEntry(Variable32Field &f) {
#if USE_MD5
	int64_t &size = fh_to_size[md5FileHash(f)];
	size = max(size, file_size.val());
#else
#error "no"
	ConstantString tmp(f.val(), f.size());
	unique_filehandles.add(tmp);
#endif
    }

    virtual void processRow() {
	addEntry(filehandle);
	if (!lookup_dir_filehandle.isNull()) {
	    addEntry(lookup_dir_filehandle);
	}
    }

    virtual void printResult() {
	StatsQuantile file_size_stat(0.01/2, fh_to_size.size()+1);
	for(HashMap<uint64_t, int64_t>::iterator i = fh_to_size.begin(); 
	    i != fh_to_size.end(); ++i) {
	    file_size_stat.add(i->second);
	}

	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;
	cout << format("found %d unique filehandles\n") 
	    % fh_to_size.size();
	cout << format("file size quantiles:\n");
	file_size_stat.printTextRanges(cout, 100);
	cout << format("End-%s\n") % __PRETTY_FUNCTION__;
    }

private:
    Variable32Field filehandle;
    Variable32Field lookup_dir_filehandle;
    TFixedField<int64_t> file_size;
#if USE_MD5
    HashMap<uint64_t, int64_t> fh_to_size;
#else
    HashUnique<ConstantString> unique_filehandles;
#endif
    size_t last_size_report;
};

namespace NFSDSAnalysisMod {
    RowAnalysisModule *
    newUniqueFileHandles(DataSeriesModule &prev) {
	return new UniqueFileHandles(prev);
    }
}
