/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <errno.h>
#include <stdio.h>

#include <DataSeries/DataSeriesFile.H>
#include <DataSeries/commonargs.H>

#include "cryptutil.H"

const std::string logfu_xml
(
 "<ExtentType name=\"TiColi trace\" >\n"
 "  <field type=\"variable32\" name=\"timestamp\" pack_unique=\"yes\" />\n"
 "  <field type=\"variable32\" name=\"oper\" pack_unique=\"yes\" />\n"
 "  <field type=\"variable32\" name=\"path\" pack_unique=\"yes\" />\n"
 "  <field type=\"int32\" name=\"offset\" />\n"
 "  <field type=\"int32\" name=\"length\" />\n"
 "</ExtentType>\n"
);

std::string
extract_to_end(const std::string &buffer, int *bufpos)
{
    unsigned int field_end = buffer.size();
    std::string ret = buffer.substr(*bufpos,field_end - *bufpos);
    *bufpos = field_end + 1;
    return ret;
}

std::string
extract_field(const std::string &buffer, int *bufpos)
{
    unsigned int field_end = *bufpos;
    for(;field_end < buffer.size();field_end++) {
	if (buffer[field_end] == ' ')
	    break;
    }
    std::string ret = buffer.substr(*bufpos,field_end - *bufpos);
    *bufpos = field_end + 1;
    return ret;
}

void
isinteger(const std::string &s)
{
    for(unsigned int i=0;i<s.size();++i) {
	AssertAlways(isdigit(s[i]),("%s is supposed to be an integer\n",s.c_str()));
    }
}

void
readString(FILE *in, std::string &ret)
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
    std::vector<char> foo_str; // Linux & HP-UX STL differ enough that I
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

    double first_logfu_record_time = -Double::Inf;
    AssertAlways(argc == 3,
		 ("Usage: %s in-file out-file '- allowed for stdin/stdout, .gz, .bz2 inputs will be automatically unpacked'\n",
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
    
    DataSeriesSink logfudsout(argv[2],packing_args.compress_modes,packing_args.compress_level);
    ExtentTypeLibrary library;
    ExtentType *logfutype = library.registerType(logfu_xml);
    ExtentSeries logfuseries(*logfutype);
    logfudsout.writeExtentLibrary(library);

    Variable32Field timestamp(logfuseries,"timestamp");
    Variable32Field oper(logfuseries,"oper");
    Variable32Field path(logfuseries,"path");
    Int32Field offset(logfuseries,"offset");
    Int32Field length(logfuseries,"length");

    prepareEncrypt("01234567890123456789", "01234567890123456789");

    Extent *logfuextent = new Extent(logfuseries);
    int nrecords = 0;
    int nread = 0;
    std::string buffer;
    while(1) {
	readString(infile,buffer);
	++nread;
	if ((int)(logfuextent->extentsize()+buffer.size()) > packing_args.extent_size ||
	    feof(infile)) {
	    logfudsout.writeExtent(logfuextent);
	    logfuextent->clear();
	}
	
	if (feof(infile))
	    break;
	int bufpos = 0;
	std::string s_timestamp = extract_field(buffer,&bufpos);
	AssertAlways(s_timestamp.size() > 0,("bad timestamp\n"));
	std::string s_oper = extract_field(buffer,&bufpos);
	AssertAlways(s_oper.size() > 0,("bad operation\n"));
	std::string s_path;
	std::string s_offset = "0";
	std::string s_length = "0";

	if (s_oper == "generic") {
	    std::string s_zero = extract_field(buffer,&bufpos);
	    s_path = extract_to_end(buffer,&bufpos);
	} else 
	    s_path = encryptString(extract_field(buffer,&bufpos));

	if ((s_oper == "readdir") || (s_oper == "start") || (s_oper == "readlink") || (s_oper == "open")) {
	    s_offset = extract_field(buffer,&bufpos);
	    isinteger(s_offset);
	}

	if ((s_oper == "read") || (s_oper == "write")) {
	    s_offset = extract_field(buffer,&bufpos);
	    isinteger(s_offset);
	    s_length = extract_field(buffer,&bufpos);
	    isinteger(s_length);
	}
	
	++nrecords;
	logfuseries.newRecord();
	timestamp.set(s_timestamp);
	oper.set(s_oper);
	path.set(s_path);
	offset.set(atoi(s_offset.c_str()));
	length.set(atoi(s_length.c_str()));
    }
    if (popened) {
	AssertAlways(pclose(infile) == 0,("Error on pclose: %s\n",strerror(errno)));
    } else {
	AssertAlways(fclose(infile) == 0,("Error on fclose: %s\n",strerror(errno)));
    }
    printf("%d records, %d extents; %lld bytes, %lld compressed, %.6gs seconds to pack\n",
	   nrecords, logfudsout.extents, logfudsout.unpacked_size, logfudsout.packed_size,logfudsout.pack_time);
    printf("  %.4gx compression ratio; %lld fixed data, %lld variable data\n",
	   (double)logfudsout.unpacked_size / (double)logfudsout.packed_size,
	   logfudsout.unpacked_fixed, logfudsout.unpacked_variable);
    printf("  extents-part-compression: ");
    if (logfudsout.compress_none > 0) printf("%d none, ",logfudsout.compress_none);
    if (logfudsout.compress_lzo > 0) printf("%d lzo, ",logfudsout.compress_lzo);
    if (logfudsout.compress_gzip > 0) printf("%d gzip, ",logfudsout.compress_gzip);
    if (logfudsout.compress_bz2 > 0) printf("%d bz2, ",logfudsout.compress_bz2);
    printf("\n  packed in %.6gs\n",logfudsout.pack_time);
}
