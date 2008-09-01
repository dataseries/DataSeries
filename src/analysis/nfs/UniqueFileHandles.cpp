#include <Lintel/ConstantString.hpp>
#include <Lintel/HashUnique.hpp>

#include <DataSeries/RowAnalysisModule.hpp>

using namespace std;
using boost::format;

class UniqueFileHandles : public RowAnalysisModule {
public:
    UniqueFileHandles(DataSeriesModule &source)
        : RowAnalysisModule(source),
          filehandle(series, "filehandle"),
          lookup_dir_filehandle(series, "", Field::flag_nullable)
    {
    }

    virtual ~UniqueFileHandles() { }

    void newExtentHook(const Extent &e) {
	if (series.getType() != NULL) {
	    return; // already did this
	}
	const ExtentType &type = e.getType();
	if (type.getName() == "NFS trace: attr-ops") {
	    SINVARIANT(type.getNamespace() == "" &&
		       type.majorVersion() == 0 &&
		       type.minorVersion() == 0);
	    lookup_dir_filehandle.setFieldName("lookup-dir-filehandle");
	} else if (type.getName() == "Trace::NFS::attr-ops"
		   && type.versionCompatible(1,0)) {
	    lookup_dir_filehandle.setFieldName("lookup-dir-filehandle");
	} else if (type.getName() == "Trace::NFS::attr-ops"
		   && type.versionCompatible(2,0)) {
	    lookup_dir_filehandle.setFieldName("lookup_dir_filehandle");
	} else {
	    FATAL_ERROR("?");
	}
    }

    void addEntry(Variable32Field &f) {
	ConstantString tmp(f.val(), f.size());
	unique_filehandles.add(tmp);
    }

    virtual void processRow() {
	addEntry(filehandle);
	if (!lookup_dir_filehandle.isNull()) {
	    addEntry(lookup_dir_filehandle);
	}
    }

    virtual void printResult() {
	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;
	cout << format("found %d unique filehandles\n") 
	    % unique_filehandles.size();
	cout << format("End-%s\n") % __PRETTY_FUNCTION__;
    }

private:
    Variable32Field filehandle;
    Variable32Field lookup_dir_filehandle;
    HashUnique<ConstantString> unique_filehandles;
};

namespace NFSDSAnalysisMod {
    RowAnalysisModule *
    newUniqueFileHandles(DataSeriesModule &prev) {
	return new UniqueFileHandles(prev);
    }
}
