/* -*-C++-*-
*******************************************************************************
*
* File:         textindex.C
* Description:  Text indexing tool
* Author:       Eric Anderson
*
* (C) Copyright 2006, Hewlett-Packard Development Company, L.P., all rights reserved.
*
*******************************************************************************
*/

#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdio.h>

#include <vector>
#include <string>

#include <Lintel/StringUtil.H>
#include <Lintel/HashMap.H>
#include <Lintel/HashUnique.H>

#include <DataSeries/commonargs.H>
#include <DataSeries/DataSeriesFile.H>
#include <DataSeries/DataSeriesModule.H>
#include <DataSeries/RowAnalysisModule.H>
#include <DataSeries/TypeIndexModule.H>

static const bool debug_word_find = false;
static const bool debug_search_found = false;

using namespace std;

const string text_entries_xml = 
"<ExtentType name=\"TextIndex::Entries\">\n"
"  <field type=\"int32\" name=\"id\" comment=\"only unique within a single file\" pack_relative=\"id\" />\n"
"  <field type=\"variable32\" name=\"text\" print_style=\"text\" />\n"
"</ExtentType>";

const string textindex_word_xml = 
"<ExtentType name=\"TextIndex::Word\">\n"
"  <field type=\"variable32\" name=\"filename\" pack_unique=\"yes\" />\n"
"  <field type=\"int64\" name=\"offset\" pack_relative=\"offset\" />\n"
"  <field type=\"int32\" name=\"id\" pack_relative=\"id\" />\n"
"  <field type=\"variable32\" name=\"type\" pack_unique=\"yes\" comment=\"what type of word is this, e.g. for email, from, to, subject, body; for html, title, header, body\" />\n"
"  <field type=\"variable32\" name=\"word\" pack_unique=\"yes\" print_style=\"text\" />\n"
"</ExtentType>";

void
check_file_missing(const char *filename)
{
    struct stat statbuf;
    int ret = stat(filename,&statbuf);
    AssertAlways(ret == -1 && errno == ENOENT,
		 ("refusing to run with existing file %s (%d,%s)",
		  filename,errno,strerror(errno)));
}

string
lowerCaseString(const string &src)
{
    string ret = src;
    for(unsigned i = 0; i<ret.size(); ++i) {
	ret[i] = tolower(ret[i]);
    }
    return ret;
}

class Indexer {
public:
    Indexer(const string &index_filename, commonPackingArgs &packing_args) 
    : id_out(word_series,"id"), type_out(word_series,"type"), word_out(word_series,"word"),
      id_in(text_entries_series,"id"), text_in(text_entries_series,"text"),
      filename_out(word_series, "filename"), offset_out(word_series, "offset") {
	check_file_missing(index_filename.c_str());
	word_index_type = library.registerType(textindex_word_xml);
	
	output = new DataSeriesSink(index_filename, 
				    packing_args.compress_modes,
				    packing_args.compress_level);
	output->writeExtentLibrary(library);

	word_series.setType(*word_index_type);
	word_module = new OutputModule(*output, word_series, word_index_type,
				       packing_args.extent_size);
    }

    virtual ~Indexer() {
	delete word_module;
	delete output;
    }

    void processArgs(vector<string> &args, int filename_start) {
	for(unsigned i=filename_start; i<args.size(); ++i) {
	    cout << "indexing " << args[i] << "..."; cout.flush();
	    DataSeriesSource source(args[i]);

	    ExtentSeries s(source.indexExtent);
	    Variable32Field extenttype(s,"extenttype");
	    Int64Field offset(s,"offset");
	
	    for(;s.pos.morerecords();++s.pos) {
		if (extenttype.stringval() == "TextIndex::Entries") {
		    cout << "!"; cout.flush();
		    indexExtent(source,args[i],offset.val());
		}
	    }
	    cout << "\n";
	}
    }

    void indexExtent(DataSeriesSource &source, const string &filename,
		     off64_t offset) {
	off64_t tmp_offset = offset; 
	Extent *e = source.preadExtent(tmp_offset); // tmp_offset will be auto-updated to next extent
	for(text_entries_series.setExtent(e);
	    text_entries_series.morerecords();
	    ++text_entries_series) {

	    cout << "."; cout.flush(); 
	    indexRow(filename, offset);
	}	    
	delete e;
    }

    virtual void indexRow(const string &filename, int64_t offset) = 0;

protected:
    ExtentTypeLibrary library;
    const ExtentType *word_index_type;
    DataSeriesSink *output;

    ExtentSeries word_series;
    Int32Field id_out;
    Variable32Field type_out;
    Variable32Field word_out;

    ExtentSeries text_entries_series;
    Int32Field id_in;
    Variable32Field text_in;

    OutputModule *word_module;

    Variable32Field filename_out;
    Int64Field offset_out;
};

class emailIndexer : public Indexer {
public:
    emailIndexer(const string &index_filename, commonPackingArgs &packing_args) 
	: Indexer(index_filename, packing_args) {
    }
    
    virtual ~emailIndexer() { }

    string nextLine(const string &msg, unsigned &offset) {
	string ret;
	while(1) {
	    AssertAlways(offset < msg.size(),("bad"));
	    char tmp = msg[offset];
	    ret.push_back(tmp);
	    ++offset;
	    if (tmp == '\n') 
		break;
	}
	return ret;
    }
	
    bool nextHeaderLine(const string &msg, unsigned &offset, string &headerline) {
	// TODO: collapse multi-line headers to single line stripping
	// newline and tab and replace with space.

	headerline = nextLine(msg,offset);
	while('\t' == msg[offset]) {
	    headerline.append(nextLine(msg,offset));
	}
	return headerline.size() > 1; // > 1 ==> more headers possible
    }
	
    bool nextWord(const string &msg, unsigned &offset, string &word) {
	while(offset < msg.size() && isspace(msg[offset])) {
	    ++offset;
	}
	if (offset == msg.size()) {
	    return false;
	}
	word.clear();
	// TODO: Return HTML blobs as single bit somehow detecting
	// this properly, might need the MIME decoding to detect the
	// HTML.
	while(offset < msg.size() && !isspace(msg[offset])) {
	    word.push_back(msg[offset]);
	    ++offset;
	}
	return true;
    }
	    
    virtual void indexRow(const string &filename, int64_t file_offset) {
	string msg = text_in.stringval();
	
	string headerline;
	unsigned offset = 0;
	while(nextHeaderLine(msg,offset,headerline)) {
	    if (prefixequal(headerline,"From: ") ||
		prefixequal(headerline,"To: ") ||
		prefixequal(headerline,"Subject: ")) {
		// TODO: work out how to split these a bit.
		unsigned tmp_offset = 0;
		string header_type;
		AssertAlways(nextWord(headerline, tmp_offset, header_type) == true,
			     ("bad"));
		AssertAlways(header_type[header_type.size()-1] == ':',
			     ("bad"));
		AssertAlways(headerline[headerline.size()-1] == '\n',("bad"));
		// strip off header type and newline
		headerline = headerline.substr(header_type.size()+1,
					       headerline.size()-(header_type.size() + 2));
		header_type = lowerCaseString(header_type.substr(0,header_type.size()-1));

		// TODO: consider finding a way to sort the words so
		// that the same word in different documents shows up
		// in a row.  This will make search use less memory
		// since it can just spin through words that don't
		// match any of the substrings.

		word_module->newRecord();
		filename_out.set(filename);
		offset_out.set(file_offset);
		id_out.set(id_in.val());
		type_out.set(header_type);
		word_out.set(headerline);
		// printf("  HEADER: %s",headerline.c_str());
	    }
	}
	// TODO: MIME decoding...
	string word;

	HashMap<string,bool> seen;
	while(nextWord(msg,offset,word)) {
	    // TODO: Prune out HTML tags
	    if (word.size() < 3 || word.size() > 20 ||
		seen[word]) {
		continue;
	    }
	    seen[word] = true;
	    // printf("    WORD: %s\n",word.c_str());
	    word_module->newRecord();
	    filename_out.set(filename);
	    offset_out.set(file_offset);
	    id_out.set(id_in.val());
	    type_out.set("body");
	    word_out.set(word);
	}
    }
};

void
usage(const char *argv0, const char *msg)
{
    fprintf(stderr,
	    "%sUsage: %s\n"
	    "%s"
	    "   [--email-entries <new-ds-file> <source-file>...]\n"
	    "   [--email-index <textindex-ds-file> <ds-file...>\n"
	    "   [--search-and <substrings...> -- <textindex-ds-file>\n"
	    "   [--search-and-case-insensitive <substrings...> -- <textindex-ds-file>\n",
	    msg,argv0,packingOptions().c_str());
    exit(1);
}

string
readLine(FILE *f)
{
    string ret;
    char buf[1024];

    while(true) {
	buf[0] = '\0';
	buf[1023] = '\0';
	fgets(buf,1023,f);
	AssertAlways('\0' == buf[1023],("internal"));
	if ('\0' == buf[0]) {
	    AssertAlways(ret.size() == 0,("internal"));
	    return "";
	}
	ret.append(buf);
	if (ret[ret.size()-1] == '\n') {
	    return ret;
	}
    }
}

void
email_entries(vector<string> &args, commonPackingArgs &packing_args)
{
    if (args.size() < 4) {
	usage(args[0].c_str(),"missing arguments for --email-entries");
    }

    const char *entries_filename = args[2].c_str();

    check_file_missing(entries_filename);

    ExtentTypeLibrary library;
    const ExtentType *entries_type = library.registerType(text_entries_xml);

    DataSeriesSink output(entries_filename, packing_args.compress_modes,
			  packing_args.compress_level);

    output.writeExtentLibrary(library);

    ExtentSeries entries_series(entries_type);
    Int32Field id(entries_series, "id");
    Variable32Field text(entries_series, "text");
    OutputModule entries_module(output, entries_series, entries_type,
				packing_args.extent_size);
    int cur_id = 0;
    for(unsigned i=3;i<args.size();++i) {
	FILE *f = fopen(args[i].c_str(), "r");
	cout << "open " << args[i] << ".\n";
	cout << "importing...";
	cout.flush();
	string message;
	string line;
	while(!ferror(f)) {
	    line = readLine(f);
	    if ((feof(f) || line.substr(0,5) == "From ") &&
		message.size() > 0) { // reached end of previous message
		++cur_id;
		cout << ".";
		cout.flush();
		entries_module.newRecord();
		id.set(cur_id);
		text.set(message);
		message.clear();
	    }
	    if (feof(f)) {
		break;
	    }
	    message.append(line);
	}
	cout << "\n";
    }
}

void
indexEmailExtent(DataSeriesSource &source, const string &filename,
		 int64_t offset);

void
email_index(vector<string> &args, commonPackingArgs &packing_args)
{
    if (args.size() < 4) {
	usage(args[0].c_str(),"missing arguments for --email-index");
    }
    
    emailIndexer indexer(args[2],packing_args);
    indexer.processArgs(args,3);
    cout << "compressing index extents...\n";
}

class SearchWordAndModule : public RowAnalysisModule {
public:
    SearchWordAndModule(DataSeriesModule &source,
			vector<string> &_substring_types, vector<string> &_substrings,
			bool _case_insensitive)
	: RowAnalysisModule(source),
	  filename(series, "filename"), offset(series, "offset"),
	  id(series,"id"), type(series,"type"), word(series,"word"), 
	  substring_types(_substring_types), substrings(_substrings),
	  case_insensitive(_case_insensitive) {
	if (case_insensitive) {
	    for(unsigned i=0; i<substrings.size(); ++i) {
		substrings[i] = lowerCaseString(substrings[i]);
	    }
	}
    }

    void processRow() {
	// TODO: use more efficient storage of type+word bits
	string tmp = type.stringval();
	tmp.push_back(':');
	string word_s = word.stringval();
	if (case_insensitive) {
	    word_s = lowerCaseString(word_s);
	}
	tmp.append(word_s);
	vector<int> matches;
	if (wordMatch_cache.exists(tmp)) {
	    int match = wordMatch_cache[tmp];
	    if (match == -1) {
		return; // no match
	    }
	    matches.push_back(match);
	} else {
	    for(unsigned i = 0; i<substrings.size(); ++i) {
		if (substring_types[i].size() > 0 && 
		    !type.equal(substring_types[i])) {
		    continue; // search has a type and we don't match
		}
		if(word_s.find(substrings[i],0) < word_s.size()) {
		    // match
		    if (debug_word_find) {
			printf("match %s(%d) in %s\n",substrings[i].c_str(),i,word_s.c_str());
		    }
		    matches.push_back(i);
		}
	    }
	    if (matches.size() == 0) {
		wordMatch_cache[tmp] = -1;
		return;
	    } else if (matches.size() == 1) {
		wordMatch_cache[tmp] = matches[0];
	    } else {
		// print warning?  Will be correct but potentially slower
	    }
	}

	AssertAlways(filename.stringval().size() > 0,("bad"));
	vector<bool> &found = found_list[filename.stringval()][id.val()];
	if (found.size() == 0) {
	    found.resize(substrings.size());
	}
	for(vector<int>::iterator i = matches.begin(); i != matches.end(); ++i) {
	    int substring_num = *i;
	    AssertAlways(substring_num >= 0 && found.size() > static_cast<unsigned>(substring_num),("bad"));
	    found[substring_num] = true;
	    if (debug_word_find) {
		printf("Found %d (%s:%s) in %d via %s\n",substring_num, substring_types[substring_num].c_str(),
		       substrings[substring_num].c_str(), id.val(), word.stringval().c_str());
	    }
	}
	filename_id_extentoffset[filename.stringval()][id.val()] = offset.val();
    }

    Variable32Field filename;
    Int64Field offset;
    Int32Field id;
    Variable32Field type;
    Variable32Field word;
    
    vector<string> substring_types;
    vector<string> substrings;
    bool case_insensitive;

    HashMap<string, int> wordMatch_cache;
    HashMap<string, HashMap<int, vector<bool> > > found_list;
    HashMap<string, HashMap<int, int64_t> > filename_id_extentoffset;
};

void
search_and(vector<string> &args, bool case_insensitive)
{
    if (args.size() < 4) {
	usage(args[0].c_str(),"missing arguments for --search-and");
    }
    
    vector<string> substring_types;
    vector<string> substrings;
    unsigned args_offset = 2;
    for(;args_offset < args.size() && args[args_offset] != "--"; ++args_offset) {
	unsigned colon_offset = args[args_offset].find(':',0);
	if (colon_offset < args[args_offset].size()) {
	    substring_types.push_back(lowerCaseString(args[args_offset].substr(0,colon_offset)));
	    substrings.push_back(args[args_offset]
				 .substr(colon_offset+1,
					 args[args_offset].size() - (colon_offset+1)));
	} else {
	    substring_types.push_back("");
	    substrings.push_back(args[args_offset]);
	}
    }
    AssertAlways(args_offset < args.size() -1 && args[args_offset] == "--",("bad"));
    ++args_offset;

    for(;args_offset < args.size(); ++args_offset) {
	TypeIndexModule word_source("TextIndex::Word");
	word_source.addSource(args[args_offset]);
	SearchWordAndModule search(word_source, substring_types, substrings, case_insensitive);
	DataSeriesModule::getAndDelete(search);
	HashMap<string, HashUnique<int> > wanted_ids;
	HashMap<string, HashUnique<int64_t> > wanted_extents;

	for(HashMap<string, HashMap<int, vector<bool> > >::iterator i = search.found_list.begin();
	    i != search.found_list.end(); ++i) {
	    AssertAlways(i->first.size() > 0, ("bad"));
	    for(HashMap<int, vector<bool> >::iterator j = i->second.begin();
		j != i->second.end(); ++j) {
		bool all_found = true;
		vector<bool> &found = j->second;
		AssertAlways(found.size() == substrings.size(),("bad"));
		for(unsigned k=0; k<found.size(); ++k) {
		    if (false == found[k]) {
			all_found = false;
			break;
		    }
		}
		if (all_found) {
		    wanted_ids[i->first].add(j->first);
		    int64_t wanted_offset = search.filename_id_extentoffset[i->first][j->first];
		    wanted_extents[i->first].add(wanted_offset);
		}
	    }
	}
	for(HashMap<string, HashUnique<int> >::iterator i = wanted_ids.begin();
	    i != wanted_ids.end(); ++i) {

	    AssertAlways(i->first.size() > 0,("bad"));
	    if (debug_search_found) {
		printf("in %s:\n",i->first.c_str());
	    }
	    if (debug_search_found) {
		for(HashUnique<int>::iterator j = i->second.begin();
		    j != i->second.end(); ++j) {
		    printf("  want id %d\n",*j);
		}
	    }
	    HashUnique<int64_t> &extents = wanted_extents[i->first];
	    vector<int64_t> extent_offset_list;
	    for(HashUnique<int64_t>::iterator j = extents.begin();
		j != extents.end(); ++j) {
		extent_offset_list.push_back(*j);
		if (debug_search_found) {
		    printf("  want extent at offset %lld\n",*j);
		}
	    }
	    sort(extent_offset_list.begin(), extent_offset_list.end());
	    DataSeriesSource source(i->first);
	    for(vector<int64_t>::iterator j = extent_offset_list.begin();
		j != extent_offset_list.end(); ++j) {
		off64_t offset = *j;
		Extent *e = source.preadExtent(offset);
		ExtentSeries s;
		Int32Field id(s,"id");
		Variable32Field text(s,"text");
		for(s.setExtent(e); s.morerecords(); ++s) {
		    if (i->second.exists(id.val())) {
			cout << text.stringval();
		    }
		}
		delete e;
	    }		
	}
    }
}
    

int
main(int argc, char *argv[]) 
{
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    vector<string> args;

    for(int i=0;i<argc;++i) {
	string tmp(argv[i]);
	args.push_back(tmp);
    }

    if (args.size() < 2) usage(argv[0],"");
    if (args[1] == "--email-entries") {
	email_entries(args, packing_args);
    } else if (args[1] == "--email-index") {
	email_index(args, packing_args);
    } else if (args[1] == "--search-and") {
	search_and(args,false);
    } else if (args[1] == "--search-and-case-insensitive") {
	search_and(args,true);
    } else {
	printf("Unknown command '%s'\n",args[1].c_str());
	usage(argv[0],"");
    }
}

