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

#include <Lintel/AssertBoost.hpp>
#include <Lintel/HashMap.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/commonargs.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/GeneralField.hpp>
#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>
#include <DataSeries/RowAnalysisModule.hpp>

using namespace std;
using boost::format;

struct IndexValues {
    string filename;
    ExtentType::int64 offset;
    vector<GeneralValue> mins, maxs;
    vector<bool> hasnulls;
    int rowcount;
    IndexValues() : offset(-1), rowcount(0) { }

    void reset() {
        mins.clear();
        maxs.clear();
        hasnulls.clear();
        rowcount = 0;
    }
};

vector<string> fields;

static const bool verbose_index = false;
static const bool verbose_read = false;
static const bool verbose_results = false;

static const string str_min("min:");
static const string str_max("max:");
static const string str_hasnull("hasnull:");

vector<ExtentType::fieldType> infieldtypes;

const string modifytype_xml = 
"<ExtentType namespace=\"dataseries.hpl.hp.com\" name=\"DSIndex::Extent::ModifyTimes\" version=\"1.0\" >\n"
"  <field type=\"variable32\" name=\"filename\" />\n"
"  <field type=\"int64\" name=\"modify-time\" />\n"
"</ExtentType>\n";

typedef HashMap<string, ExtentType::int64> modifyTimesT;
struct ModifyTimesByFilename {
    bool operator() (const modifyTimesT::HashTableT::hte &a, const modifyTimesT::HashTableT::hte &b) const {
	return a.data.first < b.data.first;
    }
};

const string indexinfo_xml =
"<ExtentType namespace=\"dataseries.hpl.hp.com\" name=\"DSIndex::Extent::Info\" version=\"1.0\">\n"
"  <field type=\"variable32\" name=\"type-prefix\" />\n"
"  <field type=\"variable32\" name=\"fields\" />\n"
"</ExtentType>\n";


// creates a hash table from a DSIndex::Extent::ModifyTimes extent
class ModTimesModule : public RowAnalysisModule {
private:
    Variable32Field filenameF;
    Int64Field mtimeF;

public:
    modifyTimesT &times;

public:
    ModTimesModule(DataSeriesModule &source,
                   modifyTimesT &times)
        : RowAnalysisModule(source),
          filenameF(series, "filename"),
          mtimeF(series, "modify-time"),
          times(times)
    { }

    void processRow() {
        times[filenameF.stringval()] = mtimeF.val();
    }
};


// manages the creation of a new DSIndex file
class MinMaxOutput {
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

    Variable32Field *filenameF;
    Int64Field *extent_offsetF;
    Int32Field *rowcountF;

    OutputModule *minmaxmodule;
    bool is_open;
    const char *index_filename;
    string oldIndex, type_prefix, fieldlist;

    modifyTimesT modify;

    const string *type_namespace;
    unsigned major_version, minor_version;

    // update the namespace/version info from an extent type
    void updateNamespaceVersions(const ExtentType *type) {
        if (type->getNamespace().empty()) {
            INVARIANT(type_namespace == NULL, "invalid to index some extents with a namespace and some without");
        } else {
            if (type_namespace == NULL) {
                type_namespace = new string(type->getNamespace());
                major_version = type->majorVersion();
                minor_version = type->minorVersion();
                if (false) {
                    cout << "Using namespace " << *type_namespace << ", major version " << major_version << "\n";
                }
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
            minmaxtype_xml.append((boost::format(" namespace=\"%s\" version=\"%d.%d\"")
                                   % *type_namespace % major_version % minor_version).str());
        }
        minmaxtype_xml.append(" name=\"DSIndex::Extent::MinMax::");
        minmaxtype_xml.append(type_prefix);
        minmaxtype_xml.append("\">\n");
        minmaxtype_xml.append("  <field type=\"variable32\" name=\"filename\" />\n");
        minmaxtype_xml.append("  <field type=\"int64\" name=\"extent_offset\" />\n");
        minmaxtype_xml.append("  <field type=\"int32\" name=\"rowcount\" />\n");
        INVARIANT(fields.size() == infieldtypes.size(),
                  boost::format("internal %d %d") % fields.size()
                  % infieldtypes.size());
        for(unsigned i = 0;i<fields.size();++i) {
            minmaxtype_xml.append("  <field type=\"");
            minmaxtype_xml.append(ExtentType::fieldTypeString(infieldtypes[i]));
            minmaxtype_xml.append("\" name=\"min:");
            minmaxtype_xml.append(fields[i]);
            minmaxtype_xml.append("\" />\n");
            minmaxtype_xml.append("  <field type=\"");
            minmaxtype_xml.append(ExtentType::fieldTypeString(infieldtypes[i]));
            minmaxtype_xml.append("\" name=\"max:");
            minmaxtype_xml.append(fields[i]);
            minmaxtype_xml.append("\" />\n");
            minmaxtype_xml.append("  <field type=\"bool\" name=\"hasnull:");
            minmaxtype_xml.append(fields[i]);
            minmaxtype_xml.append("\" />\n");
        }
        minmaxtype_xml.append("</ExtentType>\n");
        if (false) printf("XX\n%s\n",minmaxtype_xml.c_str());

        return minmaxtype_xml;
    }

    void
    setFieldList(const string &fieldlist)
    {
        // write info extents -- one row
        ExtentSeries infoseries(infotype);
        Variable32Field info_type_prefix(infoseries, "type-prefix");
        Variable32Field info_fields(infoseries, "fields");
        OutputModule infomodule(*output, infoseries, infotype,
                                packing_args.extent_size);

        infomodule.newRecord();
        info_type_prefix.set(type_prefix);
        info_fields.set(fieldlist);
        infomodule.flushExtent();

        for(unsigned i = 0; i < fields.size(); ++i) {
            mins.push_back(GeneralField::create(NULL, *minmaxseries,
                                                str_min + fields[i]));
            maxs.push_back(GeneralField::create(NULL, *minmaxseries,
                                                str_max + fields[i]));
            hasnulls.push_back(new BoolField(*minmaxseries,
                                             str_hasnull + fields[i]));
        }
    }

    void
    open()
    {
        is_open = true;

        infotype = library.registerType(indexinfo_xml);
        minmaxtype = library.registerType(generateMinMaxType(type_prefix));
        modifytype = library.registerType(modifytype_xml);

        output = new DataSeriesSink(index_filename,
                                    packing_args.compress_modes,
                                    packing_args.compress_level);

        minmaxseries = new ExtentSeries(minmaxtype);

        filenameF = new Variable32Field(*minmaxseries, "filename");
        extent_offsetF = new Int64Field(*minmaxseries, "extent_offset");
        rowcountF = new Int32Field(*minmaxseries, "rowcount");

        minmaxmodule = new OutputModule(*output, *minmaxseries, minmaxtype,
                                        packing_args.extent_size);

        output->writeExtentLibrary(library);

        setFieldList(fieldlist);
    }

public:
    MinMaxOutput(const commonPackingArgs &packing_args)
        : packing_args(packing_args),
          is_open(false),
          type_namespace(NULL)
    { }

    ~MinMaxOutput() {
        minmaxmodule->flushExtent();

        // write modify extents ...

        ExtentSeries modifyseries(modifytype);
        Variable32Field modifyfilename(modifyseries,"filename");
        Int64Field modifytime(modifyseries,"modify-time");
        OutputModule *modifymodule =
            new OutputModule(*output, modifyseries, modifyseries.type,
                             packing_args.extent_size);

        typedef modifyTimesT::HashTableT::hte_vectorT mt_vectorT;
        mt_vectorT mt_rawtable = modify.getHashTable().unsafeGetRawDataVector();
        sort(mt_rawtable.begin(), mt_rawtable.end(), ModifyTimesByFilename());
        for(mt_vectorT::iterator i = mt_rawtable.begin(); i != mt_rawtable.end(); ++i) {
            modifymodule->newRecord();
            modifyfilename.set(i->data.first);
            modifytime.set(i->data.second);
        }

        modifymodule->flushExtent();
        delete modifymodule;

        GeneralField::deleteFields(mins);
        GeneralField::deleteFields(maxs);
        for(vector<BoolField *>::iterator i = hasnulls.begin();
            i != hasnulls.end(); ++i) {
            delete *i;
        }

        delete rowcountF;
        delete extent_offsetF;
        delete filenameF;
        delete minmaxseries;
        delete minmaxmodule;
        delete output;
    }

    const string &typePrefix() {
        return type_prefix;
    }

    void newIndex(const char *index_filename,
                  const string &type_prefix,
                  const string &fieldlist) {
        this->type_prefix = type_prefix;
        this->index_filename = index_filename;
        this->fieldlist = fieldlist;
    }

    void openIndex(const char *index_filename) {
        this->index_filename = index_filename;

        // save the existing index
        oldIndex = index_filename;
        oldIndex += ".old.ds";

        // rename
        link(index_filename, oldIndex.c_str());
        unlink(index_filename);

        // read the mtimes from the old index
        TypeIndexModule modTimes("DSIndex::Extent::ModifyTimes");
        modTimes.addSource(oldIndex);

        ModTimesModule modModule(modTimes, modify);
        modModule.getAndDelete();

        // read the type info from the old index
        TypeIndexModule info_mod("DSIndex::Extent::Info");
        info_mod.addSource(oldIndex);

        Extent *e = info_mod.getExtent();
        INVARIANT(e != NULL, format("must have an DSIndex::Extent::Info extent"
                                    " in index %s!") % oldIndex);
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
        INVARIANT(e == NULL,
                  format("must have only one DSIndex::Extent::Info in"
                         " index %s!") % oldIndex);

        // update the namespace/version information
        string minmax_typename("DSIndex::Extent::MinMax::");
        minmax_typename.append(type_prefix);

        DataSeriesSource source(oldIndex);
        const ExtentType *type =
            source.getLibrary().getTypeByName(minmax_typename);
        updateNamespaceVersions(type);
    }

    void add(IndexValues &v) {
        // open the file if its not already open
        if(!is_open) {
            open();
        }

        // print if the indexing is verbose
        if (verbose_index) {
            for(unsigned i = 0; i < fields.size(); ++i) {
                printf("  field %s min '", fields[i].c_str());
                v.mins[i].write(stdout);
                printf("' max '");
                v.maxs[i].write(stdout);
                printf("' rowcount %d\n", v.rowcount);
            }
            printf("\n");
        }

        // create a new min/max index record
	minmaxmodule->newRecord();

	filenameF->set(v.filename);
	extent_offsetF->set(v.offset);
	rowcountF->set(v.rowcount);
	if (verbose_results) {
	    cout << format("%s:%d %d rows\n") % v.filename
		% v.offset % v.rowcount;
	}

	for(unsigned i = 0; i < fields.size(); ++i) {
	    mins[i]->set(v.mins[i]);
	    maxs[i]->set(v.maxs[i]);
	    hasnulls[i]->set(v.hasnulls[i]);
	    if (verbose_results) {
		printf("  field %s min '",fields[i].c_str());
		v.mins[i].write(stdout);
		printf("' max '");
		v.maxs[i].write(stdout);
		printf("'\n");
	    }
	}
    }

    // defined below
    void indexFiles(const vector<string> &files);
};

class IndexFileModule : public RowAnalysisModule {
private:
    vector<GeneralField *> infields;
    IndexValues iv;
    MinMaxOutput *minMaxOutput;

public:
    IndexFileModule(DataSeriesModule &source,
                    const string &filename,
                    MinMaxOutput *minMaxOutput)
        : RowAnalysisModule(source, ExtentSeries::typeExact),
          minMaxOutput(minMaxOutput)
    {
        // set the filename in the index values
        iv.filename = filename;
    }

    virtual ~IndexFileModule() {
        // write the final row
        if(iv.offset >= 0) {
            minMaxOutput->add(iv);
        }

        GeneralField::deleteFields(infields);
    }

    void prepareForProcessing() {
        // set up the fields to index
        for(unsigned i = 0; i < fields.size(); ++i) {
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
        if(iv.offset >= 0) {
            minMaxOutput->add(iv);
        }

        // clear the input values
        iv.reset();

        // mark the offset of this extent
        iv.offset = e.extent_source_offset;

        if (verbose_index) {
            cout << format("index extent %s:%d\n") % iv.filename % iv.offset;
        }
    }

    virtual void processRow() {
        // update the index value as appropriate
        if(iv.rowcount) {
            for(unsigned i = 0; i < infields.size(); ++i) {
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
            for(unsigned i = 0; i < infields.size(); ++i) {
                GeneralValue v(infields[i]);
                iv.mins.push_back(v);
                iv.maxs.push_back(v);
                iv.hasnulls.push_back(infields[i]->isNull());
            }
        }
        ++iv.rowcount;
    }
};


ExtentType::int64 
mtimens(const char *filename)
{
    struct stat statbuf;
    INVARIANT(stat(filename,&statbuf)==0,
              format("stat failed: %s") % strerror(errno));

#ifdef __HP_aCC
    // don't know how to get ns time on HPUX
    return ((ExtentType::int64)statbuf.st_mtime *
            (ExtentType::int64)1000000000);
#else    
    return ((ExtentType::int64)statbuf.st_mtime *
            (ExtentType::int64)1000000000 + statbuf.st_mtim.tv_nsec);
#endif
}


class OldIndexModule : public RowAnalysisModule {
private:
    MinMaxOutput *minMaxOutput;
    Variable32Field filenameF;
    Int64Field extent_offsetF;
    Int32Field rowcountF;
    vector<GeneralField *> mins, maxs;
    vector<BoolField *> hasnulls;

    modifyTimesT &modify;
    string curName;
    bool reprocessFile;
    unsigned int filePos;
    const vector<string> &files;

public:
    OldIndexModule(DataSeriesModule &source,
                   MinMaxOutput *minMaxOutput,
                   modifyTimesT &modify,
                   const vector<string> &files)
        : RowAnalysisModule(source),
          minMaxOutput(minMaxOutput),
          filenameF(series, "filename"),
          extent_offsetF(series, "extent_offset"),
          rowcountF(series, "rowcount"),
          modify(modify),
          filePos(0),
          files(files)
    { }

    ~OldIndexModule() {
        GeneralField::deleteFields(mins);
        GeneralField::deleteFields(maxs);
        for(vector<BoolField *>::iterator
                i = hasnulls.begin(); i != hasnulls.end(); ++i) {
            delete *i;
        }
    }

    void prepareForProcessing() {
        for(unsigned i = 0; i < fields.size(); ++i) {
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
    }

    void processRow() {
        if(filenameF.stringval() != curName) {
            // set the new filename
            curName = filenameF.stringval();

            // index any missing files
            while(filePos < files.size() && files[filePos] < curName) {
                ExtentType::int64 modify_time = mtimens(files[filePos].c_str());
                modify[files[filePos]] = modify_time;

                TypeIndexModule module(minMaxOutput->typePrefix());
                module.addSource(files[filePos]);
                IndexFileModule index(module, files[filePos], minMaxOutput);
                index.getAndDelete();

                ++filePos;
            }

            // new filename... check the mtime
            ExtentType::int64 modify_time = mtimens(curName.c_str());
            ExtentType::int64 *stored = modify.lookup(curName);
            if(stored && (*stored) == modify_time) {
                // ++already_indexed_files;
                cout << "already indexed.\n";
                reprocessFile = false;
            } else {
                if (stored && modify_time < (*stored)) {
                    fprintf(stderr,"warning, curName %s has gone backards in time, now %.2f, was %.2f\n",
                            curName.c_str(), (double)modify_time/1.0e9, (double)modify[curName]/1.0e9);
                }

                modify[curName] = modify_time;
                reprocessFile = true;

                // re-index the file
                TypeIndexModule module(minMaxOutput->typePrefix());
                module.addSource(curName);
                IndexFileModule index(module, curName, minMaxOutput);
                index.getAndDelete();
            }
        }

        if(!reprocessFile) {
            IndexValues iv;

            iv.filename = filenameF.stringval();
            iv.offset = extent_offsetF.val();
            iv.rowcount = rowcountF.val();
            iv.mins.clear();
            iv.maxs.clear();
            iv.hasnulls.clear();
            for(unsigned i = 0; i < fields.size(); ++i) {
                iv.mins.push_back(GeneralValue(mins[i]));
                iv.maxs.push_back(GeneralValue(maxs[i]));
                iv.hasnulls.push_back(hasnulls[i]->val());
            }
            if (verbose_read) {
                cout << format("%s:%d %d rows\n") % iv.filename
                    % iv.offset % iv.rowcount;
                for(unsigned i = 0; i < fields.size(); ++i) {
                    printf("  field %s min '",fields[i].c_str());
                    iv.mins[i].write(stdout);
                    printf("' max '");
                    iv.maxs[i].write(stdout);
                    printf("'\n");
                }
            }

            minMaxOutput->add(iv);
        }
    }

    void processRemaining() {
        while(filePos < files.size()) {
            ExtentType::int64 modify_time = mtimens(files[filePos].c_str());
            modify[files[filePos]] = modify_time;

            TypeIndexModule module(minMaxOutput->typePrefix());
            module.addSource(files[filePos]);
            IndexFileModule index(module, files[filePos], minMaxOutput);
            index.getAndDelete();

            ++filePos;
        }
    }
};

void
MinMaxOutput::indexFiles(const vector<string> &files)
{
    // update the namespace/version information
    BOOST_FOREACH(const string &file, files) {
        ExtentType::int64 *time = modify.lookup(file);
        if(!time || mtimens(file.c_str()) != *time) {
            DataSeriesSource source(file);
            const ExtentType *type =
                source.getLibrary().getTypeByName(type_prefix);
            updateNamespaceVersions(type);
        }
    }

    if(oldIndex.empty()) {
        // no old index, index each file
        BOOST_FOREACH(const string &file, files) {
            modify[file] = mtimens(file.c_str());

            TypeIndexModule module(type_prefix);
            module.addSource(file);

            IndexFileModule index(module, file, this);
            index.getAndDelete();
        }
    } else {
        // merge with the old index
        string minmax_typename("DSIndex::Extent::MinMax::");
        minmax_typename.append(type_prefix);
        TypeIndexModule minmax_mod(minmax_typename);
        minmax_mod.addSource(oldIndex);

        OldIndexModule old(minmax_mod, this, modify, files);
        old.getAndDelete();
        old.processRemaining();

        // remove the old index
        unlink(oldIndex.c_str());
    }
}


int
main(int argc, char *argv[])
{
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
