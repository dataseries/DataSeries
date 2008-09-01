#include <openssl/md5.h>

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
          lookup_dir_filehandle(series, "", Field::flag_nullable),
	  last_size_report(0)
    {
    }

    virtual ~UniqueFileHandles() { }

    void newExtentHook(const Extent &e) {
	if (unique_filehandles.size() > last_size_report + 1000000) {
	    cout << format("UniqueFileHandles interim count: %d\n")
		% unique_filehandles.size();
	    last_size_report = unique_filehandles.size();
	    INVARIANT(last_size_report < 2000000000, 
		      "HashUnique about to overflow");
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

#define USE_MD5 1

#if USE_MD5
    union MD5Union {
	unsigned char digest[16];
	uint64_t u64Digest[2];
    };
#endif

    void addEntry(Variable32Field &f) {
#if USE_MD5
	MD5_CTX ctx;
	MD5Union tmp;
	MD5_Init(&ctx);
	MD5_Update(&ctx, f.val(), f.size());
	MD5_Final(tmp.digest, &ctx);
	
	unique_filehandles.add(tmp.u64Digest[0]);
#else
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
	cout << format("Begin-%s\n") % __PRETTY_FUNCTION__;
	cout << format("found %d unique filehandles\n") 
	    % unique_filehandles.size();
	cout << format("End-%s\n") % __PRETTY_FUNCTION__;
    }

private:
    Variable32Field filehandle;
    Variable32Field lookup_dir_filehandle;
#if USE_MD5
    HashUnique<uint64_t> unique_filehandles;
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
