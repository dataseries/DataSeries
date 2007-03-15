/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <errno.h>
#include <stdio.h>

#include <Lintel/LintelAssert.H>

#include <DataSeries/DataSeriesFile.H>
#include <DataSeries/commonargs.H>

#include "cryptutil.H"

const std::string logfu_xml
(
 "<ExtentType name=\"Trace::LogFu::common\" >\n"
 "  <field type=\"variable32\" name=\"timestamp\" pack_unique=\"yes\" />\n"
 "  <field type=\"variable32\" name=\"oper\" pack_unique=\"yes\" />\n"
 "  <field type=\"variable32\" name=\"path\" opt_nullable=\"yes\" pack_unique=\"yes\" />\n"
 "  <field type=\"variable32\" name=\"oldpath\" opt_nullable=\"yes\" pack_unique=\"yes\" />\n"
 "  <field type=\"variable32\" name=\"newpath\" opt_nullable=\"yes\" pack_unique=\"yes\" />\n"
 "  <field type=\"variable32\" name=\"hostname\" opt_nullable=\"yes\" pack_unique=\"yes\" />\n"
 "  <field type=\"variable32\" name=\"message\" opt_nullable=\"yes\" pack_unique=\"yes\" />\n"
 "  <field type=\"int64\" name=\"offset\" opt_nullable=\"yes\" />\n"
 "  <field type=\"int64\" name=\"length\" opt_nullable=\"yes\" />\n"
 "  <field type=\"int32\" name=\"owner\" opt_nullable=\"yes\" />\n"
 "  <field type=\"int32\" name=\"group\" opt_nullable=\"yes\" />\n"
 "  <field type=\"int32\" name=\"mode\" opt_nullable=\"yes\" />\n"
 "  <field type=\"int32\" name=\"flags\" opt_nullable=\"yes\" />\n"
 "  <field type=\"int32\" name=\"dev\" opt_nullable=\"yes\" />\n"
 "  <field type=\"int32\" name=\"datasync\" opt_nullable=\"yes\" />\n"
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
	AssertAlways(isdigit(s[i]),("%s is supposed to be an integer\n",
	    s.c_str()));
    }
}

void
readString(FILE *in, std::string &ret)
{
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
    
    DataSeriesSink logfudsout(argv[2],packing_args.compress_modes,
        packing_args.compress_level);
    ExtentTypeLibrary library;
    ExtentType *logfutype = library.registerType(logfu_xml);
    ExtentSeries logfuseries(*logfutype);
    logfudsout.writeExtentLibrary(library);

    Variable32Field timestamp(logfuseries,"timestamp");
    Variable32Field oper(logfuseries,"oper");
    Variable32Field path(logfuseries,"path",Field::flag_nullable);
    Variable32Field oldpath(logfuseries,"oldpath",Field::flag_nullable);
    Variable32Field newpath(logfuseries,"newpath",Field::flag_nullable);
    Variable32Field hostname(logfuseries,"hostname",Field::flag_nullable);
    Variable32Field message(logfuseries,"message",Field::flag_nullable);
    Int64Field offset(logfuseries,"offset",Field::flag_nullable);
    Int64Field length(logfuseries,"length",Field::flag_nullable);
    Int32Field mode(logfuseries,"mode",Field::flag_nullable);
    Int32Field owner(logfuseries,"owner",Field::flag_nullable);
    Int32Field group(logfuseries,"group",Field::flag_nullable);
    Int32Field flags(logfuseries,"flags",Field::flag_nullable);
    Int32Field datasync(logfuseries,"datasync",Field::flag_nullable);
    Int32Field dev(logfuseries,"dev",Field::flag_nullable);

    prepareEncrypt("01234567890123456789", "01234567890123456789");

    Extent *logfuextent = new Extent(logfuseries);
    int nrecords = 0;
    int nread = 0;
    std::string buffer;
    while(1) {
	readString(infile,buffer);
	++nread;
	if ((int)(logfuextent->extentsize()+buffer.size()) >
	    packing_args.extent_size || feof(infile)) {
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
	std::string s_oldpath;
	std::string s_newpath;
	std::string s_hostname;
	std::string s_message;
	std::string s_offset;
	std::string s_length;
	std::string s_mode;
	std::string s_flags;
	std::string s_owner;
	std::string s_group;
	std::string s_datasync;
	std::string s_dev;

	if (s_oper == "generic") {
	    std::string s_zero = extract_field(buffer,&bufpos);
	    s_message = extract_to_end(buffer,&bufpos);
	} else if (s_oper == "start") {
	    s_hostname = extract_field(buffer,&bufpos);
	    s_offset = extract_field(buffer,&bufpos);
	} else if (s_oper == "stop") {
	    std::string s_zero = extract_field(buffer,&bufpos);
	} else if ((s_oper == "rename") || (s_oper == "symlink") || (s_oper == "link")) {
	    s_oldpath = encryptString(extract_field(buffer,&bufpos));
	    s_newpath = encryptString(extract_field(buffer,&bufpos));
	} else 
	    s_path = encryptString(extract_field(buffer,&bufpos));

	if ((s_oper == "readlink") || (s_oper == "truncate") || (s_oper == "ftruncate")) {
	    s_length = extract_field(buffer,&bufpos);
	    isinteger(s_offset);
	} else if ((s_oper == "readdir")) {
	    s_offset = extract_field(buffer,&bufpos);
	    isinteger(s_offset);
	} else if ((s_oper == "read") || (s_oper == "write")) {
	    s_length = extract_field(buffer,&bufpos);
	    isinteger(s_length);
	    s_offset = extract_field(buffer,&bufpos);
	    isinteger(s_offset);
	} else if ((s_oper == "mkdir") || (s_oper == "chmod") || (s_oper == "access") ||
	    (s_oper == "create")) {
	    s_mode = extract_field(buffer,&bufpos);
	    isinteger(s_mode);
	} else if ((s_oper == "open")) {
	    s_flags = extract_field(buffer,&bufpos);
	    isinteger(s_flags);
	} else if ((s_oper == "chown")) {
	    s_owner = extract_field(buffer,&bufpos);
	    isinteger(s_flags);
	} else if ((s_oper == "fsync")) {
	    s_datasync = extract_field(buffer,&bufpos);
	    isinteger(s_mode);
	    s_group = extract_field(buffer,&bufpos);
	    isinteger(s_group);
	} else if (s_oper == "mknod") {
	    s_mode = extract_field(buffer,&bufpos);
	    isinteger(s_mode);
	    s_dev = extract_field(buffer,&bufpos);
	    isinteger(s_dev);
	}
	
	++nrecords;
	logfuseries.newRecord();
	timestamp.set(s_timestamp);
	oper.set(s_oper);
	path.set(s_path);
	newpath.set(s_newpath);
	oldpath.set(s_oldpath);
	hostname.set(s_hostname);
	message.set(s_message);
	offset.set(atoi(s_offset.c_str()));
	length.set(atoi(s_length.c_str()));
	mode.set(atoi(s_mode.c_str()));
	owner.set(atoi(s_owner.c_str()));
	group.set(atoi(s_group.c_str()));
	flags.set(atoi(s_flags.c_str()));
	datasync.set(atoi(s_datasync.c_str()));
	dev.set(atoi(s_dev.c_str()));
    }
    if (popened) {
	AssertAlways(pclose(infile) == 0,("Error on pclose: %s\n",
	    strerror(errno)));
    } else {
	AssertAlways(fclose(infile) == 0,("Error on fclose: %s\n",
	    strerror(errno)));
    }
    printf("%d records, %d extents; %lld bytes, %lld compressed, %.6gs seconds to pack\n",
	   nrecords, logfudsout.extents, logfudsout.unpacked_size, logfudsout.packed_size,logfudsout.pack_time);
    printf("  %.4gx compression ratio; %lld fixed data, %lld variable data\n",
	   (double)logfudsout.unpacked_size / (double)logfudsout.packed_size,
	   logfudsout.unpacked_fixed, logfudsout.unpacked_variable);
    printf("  extents-part-compression: ");
    if (logfudsout.compress_none > 0)
        printf("%d none, ",logfudsout.compress_none);
    if (logfudsout.compress_lzo > 0)
        printf("%d lzo, ",logfudsout.compress_lzo);
    if (logfudsout.compress_gzip > 0)
        printf("%d gzip, ",logfudsout.compress_gzip);
    if (logfudsout.compress_bz2 > 0)
        printf("%d bz2, ",logfudsout.compress_bz2);
    printf("\n  packed in %.6gs\n",logfudsout.pack_time);
}
