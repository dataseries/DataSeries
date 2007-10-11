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

#include <Lintel/AssertBoost.H>
#include <Lintel/HashMap.H>
#include <Lintel/StringUtil.H>

#include <DataSeries/commonargs.H>
#include <DataSeries/DataSeriesFile.H>
#include <DataSeries/GeneralField.H>
#include <DataSeries/DataSeriesModule.H>
#include <DataSeries/TypeIndexModule.H>

#ifndef __HP_aCC
using namespace std;
#endif

HashMap<string, vector<ExtentType::int64> > filenameToOffsets;

struct IndexValues {
    string filename;
    ExtentType::int64 offset;
    vector<GeneralValue> mins, maxs;
    vector<bool> hasnulls;
    int rowcount;
    IndexValues() { }
    IndexValues(const char *_filename, ExtentType::int64 _offset)
	: filename(_filename), offset(_offset), rowcount(0) { }
};

struct ivHash {
    unsigned int operator()(const IndexValues &k) {
	return BobJenkinsHash(BobJenkinsHashMixULL(k.offset),k.filename.data(),
			      k.filename.size());
    }
};

struct ivEqual {
    bool operator()(const IndexValues &a, const IndexValues &b) {
	return a.offset == b.offset && a.filename == b.filename;
    }
};

typedef HashTable<IndexValues,ivHash,ivEqual> ivHashTableT;

struct IndexValuesByFilenameOffset {
    bool operator() (const ivHashTableT::hte &a, const ivHashTableT::hte &b) {
	if (a.data.filename == b.data.filename) {
	    return a.data.offset < b.data.offset;
	} else {
	    return a.data.filename < b.data.filename;
	}
    }
};

ivHashTableT ivHashTable;

string type_prefix;
vector<string> fields;

static const bool verbose_index = false;
static const bool verbose_read = false;
static const bool verbose_results = false;

ExtentSeries inseries(ExtentSeries::typeLoose);
vector<GeneralField *> infields;
vector<ExtentType::fieldType> infieldtypes;

const string *type_namespace;
unsigned major_version, minor_version;

const string modifytype_xml = 
"<ExtentType namespace=\"dataseries.hpl.hp.com\" name=\"DSIndex::Extent::ModifyTimes\" version=\"1.0\" >\n"
"  <field type=\"variable32\" name=\"filename\" />\n"
"  <field type=\"int64\" name=\"modify-time\" />\n"
"</ExtentType>\n";

typedef HashMap<string, ExtentType::int64> modifyTimesT;
modifyTimesT modifytimes;

struct ModifyTimesByFilename {
    bool operator() (const modifyTimesT::HashTableT::hte &a, const modifyTimesT::HashTableT::hte &b) {
	return a.data.first < b.data.first;
    }
};

const string indexinfo_xml =
"<ExtentType namespace=\"dataseries.hpl.hp.com\" name=\"DSIndex::Extent::Info\" version=\"1.0\">\n"
"  <field type=\"variable32\" name=\"type-prefix\" />\n"
"  <field type=\"variable32\" name=\"fields\" />\n"
"</ExtentType>\n";

void
updateNamespaceVersions(Extent &e)
{
    if (e.type->getNamespace().empty()) {
	INVARIANT(type_namespace == NULL, "invalid to index some extents with a namespace and some without");
    } else {
	if (type_namespace == NULL) {
	    type_namespace = new string(e.type->getNamespace());
	    major_version = e.type->majorVersion();
	    minor_version = e.type->minorVersion();
	    if (false) {
		cout << "Using namespace " << *type_namespace << ", major version " << major_version << "\n";
	    }
	}
	INVARIANT(*type_namespace == e.type->getNamespace(),
		  boost::format("conflicting namespaces, found both '%s' and '%s'")
		  % *type_namespace % e.type->getNamespace());
	INVARIANT(major_version == e.type->majorVersion(),
		  boost::format("conflicting major versions, found both %d and %d")
		  % major_version % minor_version);
	if (e.type->minorVersion() < minor_version) {
	    minor_version = e.type->minorVersion();
	}
    } 
}

void
indexExtent(DataSeriesSource &source, const string &filename, 
	    ExtentType::int64 offset)
{
    if (verbose_index) 
	printf("index extent %s:%lld\n",filename.c_str(),offset);
    // copy offset as preadExtent updates offset to offset of next extent
    off64_t tmp_offset = offset; 
    Extent *e = source.preadExtent(tmp_offset);
    updateNamespaceVersions(*e);
    inseries.setExtent(e);
    AssertAlways(inseries.pos.morerecords(),("internal"));

    if (infields.size() == 0) {
	// do things this way so we store the types for generating the
	// output dataseries, and so that we guarentee that all of the
	// types are the same across the extents.
	for(unsigned i = 0; i<fields.size();++i) {
	    GeneralField *f = GeneralField::create(NULL,inseries,fields[i]);
	    infields.push_back(f);
	    if (infieldtypes.size() < infields.size()) {
		infieldtypes.push_back(f->getType());
	    } else {
		AssertAlways((infieldtypes[i]) == f->getType(),("internal"));
	    }
	}
    }
    IndexValues indexvals(filename.c_str(),offset);
    for(vector<GeneralField *>::iterator i = infields.begin();i != infields.end();++i) {
	indexvals.mins.push_back(GeneralValue(**i));
	indexvals.maxs.push_back(GeneralValue(**i));
	indexvals.hasnulls.push_back(false);
    }

    for(;inseries.pos.morerecords();++inseries.pos) {
	for(unsigned i = 0; i < infields.size(); ++i) {
	    if (infields[i]->isNull()) {
		indexvals.hasnulls[i] = true;
	    } else {
		GeneralValue v(infields[i]);
		if (indexvals.mins[i] > v) {
		    indexvals.mins[i] = v;
		} 
		if (indexvals.maxs[i] < v) {
		    indexvals.maxs[i] = v;
		}
		if (false) {
		    indexvals.mins[i].write(stdout);
		    printf(" <= ");
		    v.write(stdout);
		    printf(" <= ");
		    indexvals.maxs[i].write(stdout);
		    printf("\n");
		}
	    }
	}
	++indexvals.rowcount;
    }
    for(unsigned i = 0; i < infields.size(); ++i) {
	if (verbose_index) {
	    printf("  field %s min '",fields[i].c_str());
	    indexvals.mins[i].write(stdout);
	    printf("' max '");
	    indexvals.maxs[i].write(stdout);
	    printf("' rowcount %d\n",indexvals.rowcount);
	}
    }
    if (verbose_index) {
	printf("\n");
    }
    ivHashTable.remove(indexvals,false);
    ivHashTable.add(indexvals);
    delete e;
}

ExtentType::int64 
mtimens(const char *filename)
{
    struct stat statbuf;
    AssertAlways(stat(filename,&statbuf)==0,("stat failed: %s\n",strerror(errno)));

#ifdef __HP_aCC
    // don't know how to get ns time on HPUX
    return (ExtentType::int64)statbuf.st_mtime * (ExtentType::int64)1000000000;
#else    
    return (ExtentType::int64)statbuf.st_mtime * (ExtentType::int64)1000000000 + statbuf.st_mtim.tv_nsec;
#endif
}

int already_indexed_files = 0;
int indexed_extents = 0;

void
indexFile(const string &filename)
{
    cout << "indexing " << filename << " ...";
    cout.flush();
    AssertAlways(filename.size() > 0,("empty filename?!"));
    if (filename[0] != '/') {
	fprintf(stderr,"warning, filename %s is relative, not absolute\n",filename.c_str());
    }
    ExtentType::int64 modify_time = mtimens(filename.c_str());
    if (modifytimes.lookup(filename) != NULL) {
	if (modifytimes[filename] == modify_time) {
	    ++already_indexed_files;
	    cout << "already indexed.\n";
	    return;
	}
	if (modify_time < modifytimes[filename]) {
	    fprintf(stderr,"warning, filename %s has gone backards in time, now %.2f, was %.2f\n",
		    filename.c_str(), (double)modify_time/1.0e9, (double)modifytimes[filename]/1.0e9);
	}
    }
    modifytimes[filename] = modify_time;

    // delete any existing entries
    IndexValues iv;
    iv.filename = filename;
    vector<ExtentType::int64> &offsets = filenameToOffsets[filename];
    for(vector<ExtentType::int64>::iterator i = offsets.begin();
	i != offsets.end();++i) {
	iv.offset = *i;
	ivHashTable.remove(iv);
    }

    DataSeriesSource source(filename);

    // TODO: re-do this with TypeIndexModule.
    ExtentSeries s(source.indexExtent);
    Variable32Field extenttype(s,"extenttype");
    Int64Field offset(s,"offset");

    for(;s.pos.morerecords();++s.pos) {
	cout << "."; cout.flush();
	if (ExtentType::prefixmatch(extenttype.stringval(),type_prefix)) {
	    ++indexed_extents;
	    indexExtent(source,filename,offset.val());
	}
    }
    cout << "\n";
}

void
readExistingIndex(const char *index_filename, string &fieldlist)
{
    cout << "reading existing index " << index_filename << "..."; 
    cout.flush();
    TypeIndexModule info_mod("DSIndex::Extent::Info");
    TypeIndexModule modifytimes_mod("DSIndex::Extent::ModifyTimes");
    info_mod.addSource(index_filename);
    modifytimes_mod.addSource(index_filename);

    Extent *e = info_mod.getExtent();
    AssertAlways(e != NULL,("must have an DSIndex::Extent::Info extent in index %s!",
			    index_filename));
    ExtentSeries infoseries(e);
    Variable32Field info_type_prefix(infoseries,"type-prefix");
    Variable32Field info_fields(infoseries,"fields");
    AssertAlways(infoseries.pos.morerecords(),("must have at least one rows in info extent"));
    
    type_prefix = info_type_prefix.stringval();
    fieldlist = info_fields.stringval();
    ++infoseries.pos;
    AssertAlways(infoseries.pos.morerecords() == false,
		 ("must have at most one row in info extent"));
    e = info_mod.getExtent();
    AssertAlways(e == NULL,("must have only one DSIndex::Extent::Info in index %s!",
			    index_filename));

    string minmax_typename("DSIndex::Extent::MinMax::");
    minmax_typename.append(type_prefix);
    TypeIndexModule minmax_mod(minmax_typename);
    minmax_mod.addSource(index_filename);

    int modifytimes_count = 0;
    while(true) {
	e = modifytimes_mod.getExtent();
	if (e == NULL) break;
	++modifytimes_count;
	ExtentSeries modifyseries(e);
	Variable32Field modifyfilename(modifyseries,"filename");
	Int64Field modifytime(modifyseries,"modify-time");
	for(;e != NULL; e = modifytimes_mod.getExtent()) {
	    for(modifyseries.setExtent(e);modifyseries.pos.morerecords();
		++modifyseries.pos) {
		modifytimes[modifyfilename.stringval()] = modifytime.val();
	    }
	}
    }
    AssertAlways(modifytimes_count > 0,
		 ("must have modifytimes extent in index %s!",
		  index_filename));

    cout << "."; 
    cout.flush();
    split(fieldlist,",",fields);

    // get extent to define type
    e = minmax_mod.getExtent();
    AssertAlways(e != NULL,("must have at least one minmax extent"));
    ExtentSeries minmaxseries(e);
    // TODO: check type.
    vector<GeneralField *> mins, maxs;
    vector<BoolField *> hasnulls;
    const string str_min("min:"), str_max("max:"), str_hasnull("hasnull:");
    
    Variable32Field filenameF(minmaxseries,"filename");
    Int64Field extent_offsetF(minmaxseries,"extent_offset");
    Int32Field rowcountF(minmaxseries,"rowcount");

    for(unsigned i = 0;i<fields.size();++i) {
	mins.push_back(GeneralField::create(NULL, minmaxseries, str_min + fields[i]));
	maxs.push_back(GeneralField::create(NULL, minmaxseries, str_max + fields[i]));
	hasnulls.push_back(new BoolField(minmaxseries, str_hasnull + fields[i]));
    }

    IndexValues iv;
    do {
	cout << ".";
	cout.flush();
	updateNamespaceVersions(*e);
	for(minmaxseries.setExtent(e);minmaxseries.pos.morerecords();++minmaxseries.pos) {
	    iv.filename = filenameF.stringval();
	    iv.offset = extent_offsetF.val();
	    iv.rowcount = rowcountF.val();
	    iv.mins.clear();
	    iv.maxs.clear();
	    iv.hasnulls.clear();
	    for(unsigned i=0;i<fields.size();++i) {
		iv.mins.push_back(GeneralValue(mins[i]));
		iv.maxs.push_back(GeneralValue(maxs[i]));
		iv.hasnulls.push_back(hasnulls[i]->val());
	    }
	    if (verbose_read) {
		printf("%s:%lld %d rows\n",iv.filename.c_str(),iv.offset,iv.rowcount);
		for(unsigned i = 0; i < fields.size(); ++i) {
		    printf("  field %s min '",fields[i].c_str());
		    iv.mins[i].write(stdout);
		    printf("' max '");
		    iv.maxs[i].write(stdout);
		    printf("'\n");
		}
	    }
	    ivHashTable.add(iv);
	    filenameToOffsets[iv.filename].push_back(iv.offset);
	}
	// get extent at end of loop because we got it earlier to define the type
	e = minmax_mod.getExtent();
    } while (e != NULL);

    cout << "\n";
    for(unsigned i = 0;i<fields.size();++i) {
	infieldtypes.push_back(mins[i]->getType());
	delete mins[i];
	delete maxs[i];
	delete hasnulls[i];
    }
}

int
main(int argc, char *argv[])
{
    Extent::setReadChecksFromEnv(true); // want to make sure everything is ok
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    AssertAlways(argc >= 3,
		 ("Usage: %s <common-args> [--new type-prefix field,field,field,...] index-dataseries input-filename...\n",argv[0]));
    int files_start= -1;
    const char *index_filename = NULL;
    string fieldlist;
    if (strcmp(argv[1],"--new") == 0) {
	AssertAlways(argc > 5,("--new needs more arguments"));
	type_prefix = argv[2];
	fieldlist = argv[3];
	
	split(fieldlist,",",fields);
	files_start = 5;
	index_filename = argv[4];
	struct stat statbuf;
	int ret = stat(index_filename,&statbuf);
	AssertAlways(ret == -1 && errno == ENOENT,
		     ("refusing to run with existing index dataseries %s in --new mode (%d,%s)",
		      index_filename,errno,strerror(errno)));
    } else {
	index_filename = argv[1];
	files_start = 2;
	readExistingIndex(index_filename,fieldlist);
    }

    AssertAlways(files_start < argc,("missing input files?"));
    for(int i=files_start;i<argc;++i) {
	char *filename = argv[i];
	indexFile(filename);
    }
    printf("indexed %d extents over %d files with %d files already indexed\n",
	   indexed_extents,argc - files_start,already_indexed_files);
    printf("%d total extents indexed in file\n",ivHashTable.size());

    if (indexed_extents == 0) {
	cout << "No new extents; not updating file.\n";
	exit(0);
    }
 
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

    ExtentTypeLibrary library;
    ExtentType *infotype = library.registerType(indexinfo_xml);
    ExtentType *minmaxtype = library.registerType(minmaxtype_xml);
    ExtentType *modifytype = library.registerType(modifytype_xml);

    DataSeriesSink output(index_filename,packing_args.compress_modes,
			  packing_args.compress_level);

    output.writeExtentLibrary(library);

    // write info extents -- one row

    ExtentSeries infoseries(infotype);
    Variable32Field info_type_prefix(infoseries,"type-prefix");
    Variable32Field info_fields(infoseries,"fields");
    OutputModule infomodule(output,infoseries,infotype,
			    packing_args.extent_size);
    infomodule.newRecord();
    info_type_prefix.set(type_prefix);
    info_fields.set(fieldlist);
    infomodule.flushExtent();

    // write output extents ...

    ExtentSeries minmaxseries(minmaxtype);
    vector<GeneralField *> mins, maxs;
    vector<BoolField *> hasnulls;
    const string str_min("min:"), str_max("max:"), str_hasnull("hasnull:");
    
    Variable32Field filenameF(minmaxseries,"filename");
    Int64Field extent_offsetF(minmaxseries,"extent_offset");
    Int32Field rowcountF(minmaxseries,"rowcount");

    for(unsigned i = 0;i<fields.size();++i) {
	mins.push_back(GeneralField::create(NULL, minmaxseries, str_min + fields[i]));
	maxs.push_back(GeneralField::create(NULL, minmaxseries, str_max + fields[i]));
	hasnulls.push_back(new BoolField(minmaxseries, str_hasnull + fields[i]));
    }

//    infields can have no entries if we didn't actually index any new extents
//    AssertAlways(infields.size() == infieldtypes.size() && infields.size() > 0,
//		   ("internal error %d %d", infields.size(), infieldtypes.size()));

    OutputModule minmaxmodule(output,minmaxseries,minmaxtype,
			      packing_args.extent_size);
    if (false) printf("LL %d\n",ivHashTable.size());
    INVARIANT(ivHashTable.dense(), "need to implement the densify hash table function");

    // This sort and the next one are both here to make the regression
    // tests work out, not because they are needed in any way by the
    // MinMaxIndexModule.
    ivHashTableT::hte_vectorT iv_rawtable = ivHashTable.unsafeGetRawDataVector();
    sort(iv_rawtable.begin(), iv_rawtable.end(), IndexValuesByFilenameOffset());
    for(ivHashTableT::hte_vectorT::iterator j = iv_rawtable.begin(); j != iv_rawtable.end(); ++j) {
	IndexValues &v(j->data);
	minmaxmodule.newRecord();
	filenameF.set(v.filename);
	extent_offsetF.set(v.offset);
	rowcountF.set(v.rowcount);
	if (verbose_results) printf("%s:%lld %d rows\n",v.filename.c_str(),v.offset,v.rowcount);
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

    minmaxmodule.flushExtent();

    // write modify extents ...

    ExtentSeries modifyseries(modifytype);
    Variable32Field modifyfilename(modifyseries,"filename");
    Int64Field modifytime(modifyseries,"modify-time");
    OutputModule *modifymodule = new OutputModule(output,modifyseries,
						  modifyseries.type,
						  packing_args.extent_size);

    typedef modifyTimesT::HashTableT::hte_vectorT mt_vectorT;
    mt_vectorT mt_rawtable = modifytimes.getHashTable().unsafeGetRawDataVector();
    sort(mt_rawtable.begin(), mt_rawtable.end(), ModifyTimesByFilename());
    for(mt_vectorT::iterator i = mt_rawtable.begin(); i != mt_rawtable.end(); ++i) {
	modifymodule->newRecord();
	modifyfilename.set(i->data.first);
	modifytime.set(i->data.second);
    }

    modifymodule->flushExtent();
    return 0;
}

    
