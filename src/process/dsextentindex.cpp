// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Select subset of fields from a collection of traces, generate a new trace
*/

#include <sys/types.h>
#include <sys/stat.h>

#include <boost/foreach.hpp>
#include <boost/filesystem.hpp>

#include <Lintel/AssertBoost.hpp>
#include <Lintel/HashMap.hpp>
#include <Lintel/LintelLog.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/commonargs.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/RowAnalysisModule.hpp>

using namespace std;
using boost::format;
namespace bf = boost::filesystem;

static LintelLog::Category debug_min_max_output("MinMaxOutput");

struct IndexValues {
    string filename;
    int64_t offset;
    vector<GeneralValue> mins, maxs;
    vector<bool> hasnulls;
    int64_t rowcount;
    IndexValues() : offset(-1), rowcount(0) { }

    void reset(int64_t o, int64_t r = 0) {
        mins.clear();
        maxs.clear();
        hasnulls.clear();

        rowcount = r;
        offset = o;
    }

    void reset(int64_t o, int64_t r, const std::string &f) {
        reset(o, r);
        filename = f;
    }
};

vector<string> fields;

static const string str_min("min:");
static const string str_max("max:");
static const string str_hasnull("hasnull:");

vector<ExtentType::fieldType> infieldtypes;

const string modifytype_xml = 
"<ExtentType namespace=\"dataseries.hpl.hp.com\" name=\"DSIndex::Extent::ModifyTimes\" version=\"1.0\" >\n"
"  <field type=\"variable32\" name=\"filename\" />\n"
"  <field type=\"int64\" name=\"modify-time\" />\n"
"</ExtentType>\n";

typedef HashMap<string, ExtentType::int64> ModifyTimesT;
struct ModifyTimesByFilename {
    bool operator() (const ModifyTimesT::HashTableT::hte &a, 
		     const ModifyTimesT::HashTableT::hte &b) const {
	return a.data.first < b.data.first;
    }
};

const string indexinfo_xml =
"<ExtentType namespace=\"dataseries.hpl.hp.com\" name=\"DSIndex::Extent::Info\" version=\"1.0\">\n"
"  <field type=\"variable32\" name=\"type-prefix\" />\n"
"  <field type=\"variable32\" name=\"fields\" />\n"
"</ExtentType>\n";


// reads in existing DSIndex::Extent::ModifyTimes extent(s) and creates 
// a hash table for efficient checking if a file has changed.
class ModTimesModule : public RowAnalysisModule {
public:
    ModTimesModule(DataSeriesModule &source, ModifyTimesT &times)
        : RowAnalysisModule(source), times(times),
          filename(series, "filename"), mtime(series, "modify-time")
    { }

    void processRow() {
        times[filename.stringval()] = mtime.val();
    }

    ModifyTimesT &times; // needed by others so make it public

private:
    Variable32Field filename;
    Int64Field mtime;
};


// reads an existing index, manages the creation of a new DSIndex file
class MinMaxOutput {
public:
    MinMaxOutput(const commonPackingArgs &packing_args)
        : packing_args(packing_args), is_open(false), is_finished(false), type_namespace(NULL)
    { }

    ~MinMaxOutput() {
        minmaxmodule->flushExtent();

        if(!is_finished) {
            finish();
        }
    }

    void finish() {
	SINVARIANT(!is_finished);
        // write modify extents
        ExtentSeries modifyseries(modifytype);
        Variable32Field modifyfilename(modifyseries,"filename");
        Int64Field modifytime(modifyseries,"modify-time");
        OutputModule *modifymodule = new OutputModule(*output, modifyseries, modifyseries.type,
                                                      packing_args.extent_size);

	// sort so we get consistent output for regression testing.
        typedef ModifyTimesT::HashTableT::hte_vectorT mt_vectorT;
        mt_vectorT mt_rawtable = modify.getHashTable().unsafeGetRawDataVector();
        sort(mt_rawtable.begin(), mt_rawtable.end(), ModifyTimesByFilename());
        for (mt_vectorT::iterator i = mt_rawtable.begin(); i != mt_rawtable.end(); ++i) {
            modifymodule->newRecord();
            modifyfilename.set(i->data.first);
            modifytime.set(i->data.second);
        }

        modifymodule->flushExtent();
        delete modifymodule;

        // clean up the fields
        GeneralField::deleteFields(mins);
        GeneralField::deleteFields(maxs);
        for (vector<BoolField *>::iterator i = hasnulls.begin(); i != hasnulls.end(); ++i) {
            delete *i;
        }

        delete rowcount;
        delete extent_offset;
        delete filename;
        delete minmaxseries;
        delete minmaxmodule;

        // fsync() and rename
        if(!old_index.empty()) {
            output->close(true);
            rename(index_filename.c_str(), old_index.c_str());
        }
        delete output;

        is_finished = true;
    }

    const string &typePrefix() {
        return type_prefix;
    }

    void newIndex(const char *index_filename, const string &type_prefix, const string &fieldlist) {
        this->type_prefix = type_prefix;
        this->fieldlist = fieldlist;
        this->index_filename = index_filename;
    }

    void openIndex(const char *index) {
        index_filename = index;
        index_filename += ".new.ds";
        old_index = index;

        // read the mtimes from the old index
        TypeIndexModule modTimes("DSIndex::Extent::ModifyTimes");
        modTimes.addSource(old_index);

        ModTimesModule modModule(modTimes, modify);
        modModule.getAndDelete();

        // read the type info from the old index
        TypeIndexModule info_mod("DSIndex::Extent::Info");
        info_mod.addSource(old_index);

        Extent *e = info_mod.getExtent();
        INVARIANT(e != NULL, format("must have an DSIndex::Extent::Info extent"
                                    " in index %s!") % old_index);
        ExtentSeries infoseries(e);
        Variable32Field info_type_prefix(infoseries,"type-prefix");
        Variable32Field info_fields(infoseries,"fields");
        INVARIANT(infoseries.pos.morerecords(),
                  "must have at least one rows in info extent");
        this->type_prefix = info_type_prefix.stringval();
        this->fieldlist = info_fields.stringval();
	split(this->fieldlist,",",fields);

        ++infoseries.pos;
        INVARIANT(infoseries.pos.morerecords() == false,
                  "must have at most one row in info extent");
        e = info_mod.getExtent();
        INVARIANT(e == NULL, format("must have only one DSIndex::Extent::Info in"
                                    " index %s!") % old_index);

        // update the namespace/version information
        string minmax_typename("DSIndex::Extent::MinMax::");
        minmax_typename.append(type_prefix);

        DataSeriesSource source(old_index);
        const ExtentType *type = source.getLibrary().getTypeByName(minmax_typename);
        updateNamespaceVersions(type);
    }

    void add(IndexValues &v) {
	// TODO-future: pre-determine the infieldtypes during the type
        // library scans and do the open when you start indexing

        // Note: it is not safe to open the file until we know the
        // infieldtypes, which are unknown until we open the first
        // file.  This is guaranteed to happen after that.
        if(!is_open) {
            open();
        }

	if (LintelLog::wouldDebug(debug_min_max_output)) {
	    for (unsigned i = 0; i < fields.size(); ++i) {
		LintelLog::debug(format("  field %1% min'%2%' max '%3%' rowcount %4%\n")
				 % fields[i] % v.mins[i] % v.maxs[i] % v.rowcount);
	    }
	}

        // create a new min/max index record
	minmaxmodule->newRecord();

	filename->set(v.filename);
	extent_offset->set(v.offset);
	rowcount->set(v.rowcount);
        LintelLogDebug("MinMaxOutput", format("%s:%d %d rows\n")
                       % v.filename % v.offset % v.rowcount);

	for (unsigned i = 0; i < fields.size(); ++i) {
	    mins[i]->set(v.mins[i]);
	    maxs[i]->set(v.maxs[i]);
	    hasnulls[i]->set(v.hasnulls[i]);
            LintelLogDebug("MinMaxOutput", format("  field %1% min '%2%' max '%3%'\n")
                           % fields[i] % v.mins[i] % v.maxs[i]);
	}
    }

    // defined below
    void indexFiles(const vector<string> &files);

protected:
    // update the namespace/version info from an extent type
    void updateNamespaceVersions(const ExtentType *type) {
        if (type->getNamespace().empty()) {
            INVARIANT(type_namespace == NULL, 
		      "invalid to index some extents with a namespace and some without");
        } else {
            if (type_namespace == NULL) {
                type_namespace = new string(type->getNamespace());
                major_version = type->majorVersion();
                minor_version = type->minorVersion();
                LintelLogDebug("MinMaxOutput",
                               boost::format("Using namespace %s, major version %d")
			       % *type_namespace % major_version);
            }
            INVARIANT(*type_namespace == type->getNamespace(),
                      boost::format("conflicting namespaces, found both '%s' and '%s'")
                      % *type_namespace % type->getNamespace());
            INVARIANT(major_version == type->majorVersion(),
                      boost::format("conflicting major versions, found both %d and %d")
                      % major_version % minor_version);
            if (type->minorVersion() < minor_version) {
                minor_version = type->minorVersion();
            }
        } 
    }

    // create the DSIndex::Extent::MinMax::* xml string
    string generateMinMaxType(const string &type_prefix) {
        string minmaxtype_xml = "<ExtentType";
        if (type_namespace != NULL) {
            minmaxtype_xml += (format(" namespace=\"%s\" version=\"%d.%d\"")
                               % *type_namespace % major_version % minor_version).str();
        }
        minmaxtype_xml += (format(" name=\"DSIndex::Extent::MinMax::%s\">\n") % type_prefix).str();
        minmaxtype_xml += "  <field type=\"variable32\" name=\"filename\" />\n";
        minmaxtype_xml += "  <field type=\"int64\" name=\"extent_offset\" />\n";
        minmaxtype_xml += "  <field type=\"int32\" name=\"rowcount\" />\n";
        INVARIANT(fields.size() == infieldtypes.size(),
                  format("internal %d %d") % fields.size() % infieldtypes.size());
        for (unsigned i = 0; i < fields.size(); ++i) {
            minmaxtype_xml += (format("  <field type=\"%1%\" name=\"min:%2%\" />\n"
                                      "  <field type=\"%1%\" name=\"max:%2%\" />\n"
                                      "  <field type=\"bool\" name=\"hasnull:%2%\" />\n")
                               % ExtentType::fieldTypeString(infieldtypes[i]) % fields[i]).str();
        }
        minmaxtype_xml += "</ExtentType>\n";

        LintelLogDebug("MinMaxOutput", boost::format("final MinMaxXML\n%s\n") % minmaxtype_xml);
        return minmaxtype_xml;
    }

    void setFieldList(const string &fieldlist) {
        // write info extents -- one row
        ExtentSeries infoseries(infotype);
        Variable32Field info_type_prefix(infoseries, "type-prefix");
        Variable32Field info_fields(infoseries, "fields");
        OutputModule infomodule(*output, infoseries, infotype, packing_args.extent_size);

        infomodule.newRecord();
        info_type_prefix.set(type_prefix);
        info_fields.set(fieldlist);
        infomodule.flushExtent();

        for(unsigned i = 0; i < fields.size(); ++i) {
            mins.push_back(GeneralField::create(NULL, *minmaxseries, str_min + fields[i]));
            maxs.push_back(GeneralField::create(NULL, *minmaxseries, str_max + fields[i]));
            hasnulls.push_back(new BoolField(*minmaxseries, str_hasnull + fields[i]));
        }
    }

    void open() {
	SINVARIANT(!is_open);
        is_open = true;

        infotype = library.registerType(indexinfo_xml);
        minmaxtype = library.registerType(generateMinMaxType(type_prefix));
        modifytype = library.registerType(modifytype_xml);

        output = new DataSeriesSink(index_filename,
                                    packing_args.compress_modes,
                                    packing_args.compress_level);

        minmaxseries = new ExtentSeries(minmaxtype);

        filename = new Variable32Field(*minmaxseries, "filename");
        extent_offset = new Int64Field(*minmaxseries, "extent_offset");
        rowcount = new Int32Field(*minmaxseries, "rowcount");

        minmaxmodule = new OutputModule(*output, *minmaxseries, minmaxtype,
                                        packing_args.extent_size);

        output->writeExtentLibrary(library);

        setFieldList(fieldlist);
    }

private:
    commonPackingArgs packing_args;

    ExtentTypeLibrary library;
    const ExtentType *infotype;
    const ExtentType *minmaxtype;
    const ExtentType *modifytype;
    DataSeriesSink *output;

    ExtentSeries *minmaxseries;
    vector<GeneralField *> mins, maxs;
    vector<BoolField *> hasnulls;

    Variable32Field *filename;
    Int64Field *extent_offset;
    Int32Field *rowcount;

    OutputModule *minmaxmodule;
    bool is_open;
    bool is_finished;
    string index_filename, old_index, type_prefix, fieldlist;

    ModifyTimesT modify;

    const string *type_namespace;
    unsigned major_version, minor_version;
};

class IndexFileModule : public RowAnalysisModule {
public:
    IndexFileModule(DataSeriesModule &source, const string &filename, MinMaxOutput *minMaxOutput)
        : RowAnalysisModule(source, ExtentSeries::typeLoose), minMaxOutput(minMaxOutput)
    {
        iv.filename = filename; // set the filename in the index values
    }

    virtual ~IndexFileModule() {
        // write the final row
        if (iv.offset >= 0) {
            minMaxOutput->add(iv);
        }

        GeneralField::deleteFields(infields);
    }

    void prepareForProcessing() {
        // set up the fields to index
        for (unsigned i = 0; i < fields.size(); ++i) {
            GeneralField *f = GeneralField::create(NULL, series, fields[i]);
            infields.push_back(f);
            if (infieldtypes.size() < infields.size()) {
                infieldtypes.push_back(f->getType());
            } else {
                SINVARIANT((infieldtypes[i]) == f->getType());
            }
        }

        // mark the offset of this extent
        iv.offset = series.extent()->extent_source_offset;
    }

    virtual void newExtentHook(const Extent &e) {
        // if we have an offset, update the file
        if (iv.offset >= 0) {
            minMaxOutput->add(iv);
        }

        iv.reset(e.extent_source_offset);

        LintelLogDebug("IndexFileModule",
                       format("index extent %s:%d\n") % iv.filename % iv.offset);
    }

    virtual void processRow() {
        // update the index value as appropriate
        if (iv.rowcount) {
            for (unsigned i = 0; i < infields.size(); ++i) {
                if (infields[i]->isNull()) {
                    iv.hasnulls[i] = true;
                } else {
                    GeneralValue v(infields[i]);
                    if (iv.mins[i] > v) {
                        iv.mins[i] = v;
                    } 
                    if (iv.maxs[i] < v) {
                        iv.maxs[i] = v;
                    }
                }
            }
        } else {
            for (unsigned i = 0; i < infields.size(); ++i) {
                GeneralValue v(infields[i]);
                iv.mins.push_back(v);
                iv.maxs.push_back(v);
                iv.hasnulls.push_back(infields[i]->isNull());
            }
        }
        ++iv.rowcount;
    }

private:
    vector<GeneralField *> infields;
    IndexValues iv;
    MinMaxOutput *minMaxOutput;
};


int64_t mtimens(const std::string &filename) {
    struct stat statbuf;
    INVARIANT(stat(filename.c_str(),&statbuf)==0, format("stat failed: %s") % strerror(errno));

#ifdef __HP_aCC
    // don't know how to get ns time on HPUX
    return ((int64_t)statbuf.st_mtime * (int64_t)1000000000);
#else    
    return ((int64_t)statbuf.st_mtime * (int64_t)1000000000 + statbuf.st_mtim.tv_nsec);
#endif
}


class OldIndexModule : public DataSeriesModule {
public:
    OldIndexModule(DataSeriesModule *source, MinMaxOutput *minMaxOutput,
                   ModifyTimesT &modify, const vector<string> &files,
                   ExtentSeries::typeCompatibilityT type_compatibility = ExtentSeries::typeExact)
        : source(source), minMaxOutput(minMaxOutput), series(type_compatibility),
          filename(series, "filename"), extent_offset(series, "extent_offset"),
          rowcount(series, "rowcount"), modify(modify), filePos(0), files(files)
    { }

    Extent *getExtent() {
        if (source) {
            // setting to NULL here just means skipping the index processing loop
            series.setExtent(source->getExtent());

            if (series.getExtent() != NULL) {
                processIndex();
            }
        }

        // process any remaining files that weren't in the old index
        while (filePos < files.size()) {
            processCurrentFile();
            ++filePos;
        }

        return NULL;
    }

protected:
    bool nextRow() {
        ++series.pos;
        if(!series.pos.morerecords()) {
            Extent *e = source->getExtent();
            if(e == NULL) {
                series.clearExtent();
                return false;
            }
            series.setExtent(e);
        }
        return true;
    }

    void processIndex() {
        // prepare the fields
        for (unsigned i = 0; i < fields.size(); ++i) {
            GeneralField *f = GeneralField::create(NULL, series,
                                                   str_min + fields[i]);
            mins.push_back(f);
            maxs.push_back(GeneralField::create(NULL, series,
                                                str_max + fields[i]));
            hasnulls.push_back(new BoolField(series, str_hasnull + fields[i]));

            if (infieldtypes.size() < mins.size()) {
                infieldtypes.push_back(f->getType());
            } else {
                SINVARIANT((infieldtypes[i]) == f->getType());
            }
        }

        // go through the old index
        while (series.getExtent() != NULL) {
            curName = filename.stringval();
                
            // index any missing files at the appropriate place
            while (filePos < files.size() && files[filePos] < curName) {
                processCurrentFile();
                ++filePos;
            }

            // process the current file
            int64_t modify_time = mtimens(curName);
            int64_t *stored = modify.lookup(curName);
            if(stored && (*stored) == modify_time) {
                cout << curName << " already indexed.\n";

                // pull the index data from the existing index
                IndexValues iv;
                do {
                    iv.reset(extent_offset.val(), rowcount.val(), filename.stringval());
                        
                    LintelLogDebug("OldIndexModule", format("%s:%d %d rows\n")
                                   % iv.filename % iv.offset % iv.rowcount);

                    for (unsigned i = 0; i < fields.size(); ++i) {
                        iv.mins.push_back(GeneralValue(mins[i]));
                        iv.maxs.push_back(GeneralValue(maxs[i]));
                        iv.hasnulls.push_back(hasnulls[i]->val());

                        LintelLogDebug("OldIndexModule",
                                       format("  field %1% min '%2%' max '%3%'\n")
                                       % fields[i] % iv.mins[i] % iv.maxs[i]);
                    }

                    minMaxOutput->add(iv);
                } while (nextRow() && curName == filename.stringval());

            } else {
                if (stored && modify_time < (*stored)) {
                    cerr << (format("warning, curName %s has gone backards in time"
                                    ", now %.2f, was %.2f\n") % curName
                             % ((double)modify_time/1.0e9) % ((double)modify[curName]/1.0e9));
                }

                // re-index the file
                processFile(curName, modify_time);

                // skip the data in the old index
                while (nextRow() && curName == filename.stringval()) { }
            }

            // now that we've processed the file, skip ahead if it was requested
            if (filePos < files.size() && files[filePos] == curName) {
                ++filePos;
            }
        }

        // clear the fields
        GeneralField::deleteFields(mins);
        GeneralField::deleteFields(maxs);
        for (vector<BoolField *>::iterator i = hasnulls.begin(); i != hasnulls.end(); ++i) {
            delete *i;
        }
    }

    void processFile(const std::string &file, int64_t modify_time) {
        modify[file] = modify_time;

        TypeIndexModule module(minMaxOutput->typePrefix());
        module.addSource(file);
        IndexFileModule index(module, file, minMaxOutput);
        index.getAndDelete();
    }

    void processCurrentFile() {
        int64_t modify_time = mtimens(files[filePos]);
        processFile(files[filePos], modify_time);
    }

private:
    DataSeriesModule *source;
    MinMaxOutput *minMaxOutput;
    ExtentSeries series;
    Variable32Field filename;
    Int64Field extent_offset;
    Int32Field rowcount;
    vector<GeneralField *> mins, maxs;
    vector<BoolField *> hasnulls;

    ModifyTimesT &modify;
    string curName;
    unsigned int filePos;
    const vector<string> &files;
};

void MinMaxOutput::indexFiles(const vector<string> &files) {
    // update the namespace/version information
    BOOST_FOREACH(const string &file, files) {
        ExtentType::int64 *time = modify.lookup(file);
        if(!time || mtimens(file) != *time) {
            DataSeriesSource source(file);
            const ExtentType *type = source.getLibrary().getTypeMatch(type_prefix);
            updateNamespaceVersions(type);
        }
    }

    // merge with the old index (if it exists)
    string minmax_typename("DSIndex::Extent::MinMax::");
    minmax_typename.append(type_prefix);
    DataSeriesModule *source = NULL;
    TypeIndexModule minmax_mod(minmax_typename);
    if(!old_index.empty()) {
        minmax_mod.addSource(old_index);
        source = &minmax_mod;
    }

    OldIndexModule old(source, this, modify, files);
    old.getAndDelete();
}


int main(int argc, char *argv[]) {
    Extent::setReadChecksFromEnv(true); // want to make sure everything is ok
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    MinMaxOutput minMaxOutput(packing_args);

    INVARIANT(argc >= 3, 
	      format("Usage: %s <common-args>"
		     " [--new type-prefix field,field,field,...]"
		     " index-dataseries input-filename...") % argv[0]);
    int files_start= -1;
    const char *index_filename = NULL;
    if (strcmp(argv[1],"--new") == 0) {
	INVARIANT(argc > 5, "--new needs more arguments");
	string type_prefix = argv[2];
	string fieldlist = argv[3];
	
	split(fieldlist,",",fields);
	files_start = 5;
	index_filename = argv[4];
	struct stat statbuf;
	int ret = stat(index_filename,&statbuf);
	INVARIANT(ret == -1 && errno == ENOENT,
		  format("refusing to run with existing index dataseries %s"
			 "in --new mode (%d,%s)") % index_filename % errno
		  % strerror(errno));

        minMaxOutput.newIndex(index_filename, type_prefix, fieldlist);
    } else {
	index_filename = argv[1];
	files_start = 2;

        minMaxOutput.openIndex(index_filename);
    }

    vector<string> files;
    INVARIANT(files_start < argc, "missing input files?");
    for(int i = files_start; i < argc; ++i) {
        files.push_back(argv[i]);
    }
    sort(files.begin(), files.end());

    minMaxOutput.indexFiles(files);

//     printf("indexed %d extents over %d files with %d files already indexed\n",
// 	   indexed_extents,argc - files_start,already_indexed_files);
//     printf("%d total extents indexed in file\n",ivHashTable.size());

    return 0;
}
