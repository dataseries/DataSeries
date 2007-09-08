/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <errno.h>
#include <stdio.h>

#include <ostream>

#include <DataSeries/DataSeriesFile.H>
#include <DataSeries/commonargs.H>

using namespace std;

const string pstrace_xml
(
 "<ExtentType name=\"Process sample\" >\n"
 "  <field type=\"double\" name=\"sampletime\" pack_scale=\"1\" pack_relative=\"sampletime\" />\n"
 "  <field type=\"variable32\" name=\"username\" pack_unique=\"yes\" />\n"
 "  <field type=\"int32\" name=\"pid\" />\n"
 "  <field type=\"int32\" name=\"ppid\" />\n"
 "  <field type=\"double\" name=\"cputime\" pack_scale=\"1\" />\n"
 "  <field type=\"variable32\" name=\"command\" pack_unique=\"yes\" />\n"
 "  <field type=\"variable32\" name=\"args\" pack_unique=\"yes\" />\n"
 "</ExtentType>\n"
);

string
extract_field(const string &buffer, int *bufpos)
{
    unsigned int field_end = *bufpos;
    for(;field_end < buffer.size();field_end++) {
	if (buffer[field_end] == ' ')
	    break;
    }
    AssertAlways(field_end < buffer.size(),("bad input line %s\n",buffer.c_str()));
    string ret = buffer.substr(*bufpos,field_end - *bufpos);
    *bufpos = field_end + 1;
    return ret;
}

void
isinteger(const string &s)
{
    for(unsigned int i=0;i<s.size();++i) {
	AssertAlways(isdigit(s[i]),("%s is supposed to be an integer\n",s.c_str()));
    }
}

void
readString(FILE *in, string &ret)
{
#if 0
    // this doesn't work on popened files or something like that :(
    const int bufsize = 4096;
    char buf[bufsize];
    fgets(buf,bufsize,in);
    buf[bufsize-1] = '\0';
    int len = strlen(buf);
    if (len == 0) {
	AssertAlways(feof(in),("got 0 bytes but not at eof?!\n"));
	return;
    }
    AssertAlways(len < bufsize - 1,("Internal error, string too long in input\n"));
    AssertAlways(len > 10,("Internal error, string too short in input\n"));
    AssertAlways(buf[len-1] == '\n',("Internal error, string '%s' doesn't end with newline %d\n",buf,feof(in)));
    buf[len-1] = '\0';
    ret.resize(0);
    ret.append(buf,len-1);
#endif
    vector<char> foo_str; // Linux & HP-UX STL differ enough that I
    // can't find a common way to append a single character to a string, 
    // so either construct in a vector and convert, or build a char * out 
    // of each character and append that way :(
    while(true) {
	int c = fgetc(in);
	if (c == EOF) {
	    AssertAlways(foo_str.size() == 0,("whoa, hit eof partway through string?!\n"));
	    return;
	}
	if (c == '\n') {
	    AssertAlways(foo_str.size() > 10,("Internal error, string too short in input\n"));
	    ret.assign(&foo_str[0],foo_str.size());
	    return;
	}
	foo_str.push_back(c);
    }
}

int
main(int argc, char *argv[])
{
    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    double first_ps_record_time = -Double::Inf;
    if (argc == 4) {
	first_ps_record_time = atof(argv[3]);
	argc = 3;
	printf("skipping ps samples before %.0f\n",first_ps_record_time);
    }
    AssertAlways(argc == 3,
		 ("Usage: %s in-file out-file [skip-records-before] '- allowed for stdin/stdout, .gz, .bz2 inputs will be automatically unpacked'\n",
		  argv[0]));
    FILE *infile;
    int len = strlen(argv[1]);
    bool popened = false;
    if (strcmp(argv[1],"-")==0) {
	infile = stdin;
    } else if (len >= 3 && strcmp(argv[1] + len - 3,".gz")==0) {
	char buf[1024];
	AssertAlways(len < 200,("no\n"));
	snprintf(buf,1024,(char *)"gunzip -c %s",argv[1]);
	infile = popen(buf,"r");
	popened = true;
    } else if (len >= 4 && strcmp(argv[1] + len - 4,".bz2")==0) {
	char buf[1024];
	AssertAlways(len < 200,("no\n"));
	snprintf(buf,1024,(char *)"bunzip2 -c %s",argv[1]);
	infile = popen(buf,"r");
	popened = true;
    } else {
	infile = fopen(argv[1],"r");
    }
    AssertAlways(infile != NULL,
		 ("Failure opening %s: %s\n",argv[1],strerror(errno)));
    
    DataSeriesSink psdsout(argv[2],packing_args.compress_modes,packing_args.compress_level);
    ExtentTypeLibrary library;
    ExtentType *pstype = library.registerType(pstrace_xml);
    ExtentSeries psseries(*pstype);
    psdsout.writeExtentLibrary(library);
    DoubleField curtime(psseries,"sampletime");
    Variable32Field username(psseries,"username");
    Int32Field pid(psseries,"pid");
    Int32Field ppid(psseries,"ppid");
    DoubleField cputime(psseries,"cputime");
    Variable32Field command(psseries,"command");
    Variable32Field args(psseries,"args");

    Extent *psextent = new Extent(psseries);
    int nrecords = 0;
    int nread = 0;
    string buffer;
    readString(infile,buffer);
    AssertAlways(buffer == "#curtime user pid ppid time command args...",
		 ("Bad first line for ps trace buffer '%s'\n",buffer.c_str()));
    while(1) {
	readString(infile,buffer);
	++nread;
	if ((int)(psextent->extentsize()+buffer.size()) > packing_args.extent_size ||
	    feof(infile)) {
	    psdsout.writeExtent(*psextent, NULL);
	    psextent->clear();
	}
	
	if (feof(infile))
	    break;
	int bufpos = 0;
	string s_curtime = extract_field(buffer,&bufpos);
	isinteger(s_curtime);
	if ((nread % 20000) == 0) {
	    printf("%d records read so far, current time %.0f\n",nread,atof(s_curtime.c_str()));
	}
	if (atof(s_curtime.c_str()) < first_ps_record_time) {
	    continue;
	}

	string s_username = extract_field(buffer,&bufpos);
	string s_pid = extract_field(buffer,&bufpos);
	string s_ppid = extract_field(buffer,&bufpos);
	string s_cputime = extract_field(buffer,&bufpos);
	string s_command = extract_field(buffer,&bufpos);
	string s_args = buffer.substr(bufpos);

	isinteger(s_pid);
	isinteger(s_ppid);
	isinteger(s_cputime);
	AssertAlways(s_username.size() > 0,("bad username\n"));
	AssertAlways(s_command.size() > 0,("bad command\n"));
	
	++nrecords;
	psseries.newRecord();
	curtime.set(atoi(s_curtime.c_str()));
	username.set(s_username);
	pid.set(atoi(s_pid.c_str()));
	ppid.set(atoi(s_ppid.c_str()));
	cputime.set(atoi(s_cputime.c_str()));
	command.set(s_command);
	args.set(s_args);
    }
    if (popened) {
	AssertAlways(pclose(infile) == 0,("Error on pclose: %s\n",strerror(errno)));
    } else {
	AssertAlways(fclose(infile) == 0,("Error on fclose: %s\n",strerror(errno)));
    }

    psdsout.close();
    psdsout.getStats().printText(cout, pstype->name);
}
