/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/


#define _XOPEN_SOURCE
#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <math.h>

#include <Lintel/StringUtil.H>
#include <Lintel/Clock.H>

#include <DataSeries/DataSeriesFile.H>
#include <DataSeries/commonargs.H>
#include <DataSeries/DataSeriesModule.H>

#include <SRT/SRTTrace.H>
#include <SRT/SRTrecord.H>
#include <SRT/SRTTraceRaw.H>
#include <SRT/SRTTrace_Filter.H>

static const bool delta_enter_kernel = false;
static const bool scale_offset = false;

const std::string srt_ioflags(
// listed in the same order they are set in mi2srt::setIOflags
// flag_inval records are pruned on conversion
  "  <field type=\"bool\" name=\"flag_synchronous\"/>\n"
  "  <field type=\"bool\" name=\"flag_raw\"/>\n"
  "  <field type=\"bool\" name=\"flag_nocache\"/>\n"
  "  <field type=\"bool\" name=\"flag_call\"/>\n"
  "  <field type=\"bool\" name=\"flag_fsysio\"/>\n"
  "  <field type=\"bool\" name=\"flag_bufdata_invalid\"/>\n"
  "  <field type=\"bool\" name=\"flag_cache\"/>\n"
  "  <field type=\"bool\" name=\"flag_pftimeout\"/>\n"
  "  <field type=\"bool\" name=\"flag_writev\"/>\n"
  "  <field type=\"bool\" name=\"flag_rewrite\"/>\n"
  "  <field type=\"bool\" name=\"flag_delwrite\"/>\n"
  "  <field type=\"bool\" name=\"flag_async\"/>\n"
  "  <field type=\"bool\" name=\"flag_ndelay\"/>\n"
  "  <field type=\"bool\" name=\"flag_wanted\"/>\n"
  "  <field type=\"bool\" name=\"flag_end_of_data\"/>\n"
  "  <field type=\"bool\" name=\"flag_phys\"/>\n"
  "  <field type=\"bool\" name=\"flag_busy\"/>\n"
  "  <field type=\"bool\" name=\"flag_error\"/>\n"
  "  <field type=\"bool\" name=\"flag_done\"/>\n"
  "  <field type=\"bool\" name=\"is_read\"/>\n"
  "  <field type=\"bool\" name=\"flag_ordwrite\"/>\n"
  "  <field type=\"bool\" name=\"flag_merged\"/>\n"
  "  <field type=\"bool\" name=\"flag_merged_from\"/>\n"
  // the following flags may have been set by srt files that were derived from kitrace
  "  <field type=\"bool\" name=\"act_release\"/>\n"
  "  <field type=\"bool\" name=\"act_allocate\"/>\n"
  "  <field type=\"bool\" name=\"act_free\"/>\n"
  "  <field type=\"bool\" name=\"act_raw\"/>\n"
  "  <field type=\"bool\" name=\"act_flush\"/>\n"
  "  <field type=\"bool\" name=\"net_buf\"/>\n"
  );



const std::string srt_timefields(
  "  <field type=\"int64\" name=\"enter_driver\" comment=\"time in units of 2^-32 seconds since UNIX epoch, printed in close to microseconds\" pack_relative=\"enter_driver\"  print_divisor=\"4295\" />\n"
  "  <field type=\"int64\" name=\"leave_driver\" comment=\"time in units of 2^-32 seconds since UNIX epoch, printed in close to microseconds\" pack_relative=\"enter_driver\"  print_divisor=\"4295\" />\n"
  "  <field type=\"int64\" name=\"return_to_driver\" comment=\"time in units of 2^-32 seconds since UNIX epoch, printed in close to microseconds\" pack_relative=\"enter_driver\"  print_divisor=\"4295\" />\n"
  );


/*
const std::string srt_timefields(
  "  <field type=\"double\" name=\"enter_driver\" pack_scale=\"1e-6\" pack_relative=\"enter_driver\" opt_doublebase=\"%.15g\" />\n"
  "  <field type=\"double\" name=\"leave_driver\" pack_scale=\"1e-6\" pack_relative=\"enter_driver\" opt_doublebase=\"%.15g\" />\n"
  "  <field type=\"double\" name=\"return_to_driver\" pack_scale=\"1e-6\" pack_relative=\"enter_driver\" opt_doublebase=\"%.15g\" />\n"
  );
*/
const std::string srt_v3fields(
  "  <field type=\"int32\" name=\"bytes\"/>\n"
  "  <field type=\"int64\" name=\"disk_offset\" pack_relative=\"disk_offset\" />\n"
  "  <field type=\"byte\" name=\"device_major\" />\n"
  "  <field type=\"byte\" name=\"device_minor\" />\n"
  "  <field type=\"byte\" name=\"device_controller\" />\n"
  "  <field type=\"byte\" name=\"device_disk\" />\n"
  "  <field type=\"byte\" name=\"device_partition\" />\n"
  "  <field type=\"int32\" name=\"driver_type\"/>\n"
  "  <field type=\"int32\" name=\"cylinder_number\"/>\n"
  "  <field type=\"byte\" name=\"buffertype\"/>\n"
  "  <field type=\"variable32\" name=\"buffertype_text\" pack_unique=\"yes\" />\n"
  );

const std::string srt_v4fields(
  "  <field type=\"int32\" name=\"queue_length\"/>\n"
  );

const std::string srt_v5fields(
  "  <field type=\"int32\" name=\"pid\"/>\n"
  );

const std::string srt_v6fields(
  "  <field type=\"int32\" name=\"logical_volume_number\"/>\n"
  );
			  
const std::string srt_v7fields(
  "  <field type=\"int32\" name=\"bytes\"/>\n"
  "  <field type=\"int32\" name=\"machine_id\"/>\n"
  "  <field type=\"byte\" name=\"device_major\" />\n"
  "  <field type=\"byte\" name=\"device_minor\" />\n"
  "  <field type=\"byte\" name=\"device_controller\" />\n"
  "  <field type=\"byte\" name=\"device_disk\" />\n"
  "  <field type=\"byte\" name=\"device_partition\" />\n"
  "  <field type=\"int32\" name=\"driver_type\" opt_nullable=\"yes\"/>\n"
  "  <field type=\"int32\" name=\"thread_id\" opt_nullable=\"yes\"/>\n"
  "  <field type=\"int32\" name=\"queue_length\"/>\n"
  "  <field type=\"int32\" name=\"pid\"/>\n"
  "  <field type=\"int32\" name=\"logical_volume_number\"/>\n"
  "  <field type=\"int64\" name=\"disk_offset\" pack_relative=\"disk_offset\"/>\n"
  "  <field type=\"int64\" name=\"lv_offset\" pack_relative=\"lv_offset\" opt_nullable=\"yes\"/>\n"
  "  <field type=\"byte\" name=\"buffertype\"/>\n"
  );

int32 to_usec(double secs)
{
    return (int32)round(1e6 * secs);
}

bool debug_each_extent = false;

int
main(int argc, char *argv[])
{
    SRTTrace *tracestream;

    commonPackingArgs packing_args;
    getPackingArgs(&argc,argv,&packing_args);

    AssertAlways(argc == 3 || argc == 4,
		 ("Usage: %s in-file out-file [minor_version]'- allowed for stdin/stdout'\n",
		  argv[0]));
    if (strcmp(argv[1],"-")==0) {
	tracestream = new SRTTraceRaw(fileno(stdin));
    } else {
	tracestream = new SRTTraceRaw(argv+1,1);
    }
    AssertAlways(tracestream != NULL,("Unable to open %s for read",argv[1]));
    DataSeriesSink srtdsout(argv[2],packing_args.compress_modes,packing_args.compress_level);

    int trace_major = tracestream->version().major_num();
    int trace_minor = tracestream->version().minor_num();
    
    printf ("inferred trace version %d.%d\n", trace_major, trace_minor);
    
    if (argc == 4) {
	trace_minor = atoi(argv[3]);
	printf ("overriding minor with %d\n", trace_minor);
    }
    SRTrawRecord *raw_tr;
    Clock::Tfrac base_time, time_offset;
    SRTrecord *_tr;
    SRTio *tr;
    //do {
	raw_tr = tracestream->record();
	AssertAlways(raw_tr != NULL && tracestream->eof() == false &&
		tracestream->fail() == false,
		("error, no first record in the srt trace!\n"));
	time_offset = 0;
	_tr = new SRTrecord(raw_tr, 
		SRTrawTraceVersion(trace_major, trace_minor));
	AssertAlways(_tr->type() == SRTrecord::IO,
		     ("Only know how to handle I/O records\n"));
	tr = (SRTio *)_tr;
	if (tr->is_suspect()) {
	    printf ("skipping suspect IO record\n");
	}
	//} while (tr->is_suspect());
    {
	base_time = tr->tfrac_created();
	if ((base_time>>32) < 86400) { // Less than one day
	    base_time = 0;
#if 0
	    // At some point want to have auto-inferring of time in this case
	    const char *header = tracestream->header();
	    printf("HI %s\n", header);

	    vector<string> lines;
	    split(header, "\n", lines);
	    for(vector<string>::iterator i = lines.begin(); 
		i != lines.end(); ++i) {
		if (!prefixequal(*i, "tracedate = "))
		    continue;
		struct tm tm;
		tm.tm_yday = -1;
		printf("Warning, Inferring start time from ");
		strptime((*i).c_str() + 12, "%a %b %d %H:%M:%S %Y", &tm);
		AssertAlways(tm.tm_yday != -1, ("bad"));
		
	    }
	    exit(0); // strptime()
#endif
	} else {
	    // set the base time to the start of the year
	    Clock::Tfrac curtime = base_time;
	    printf("sizeof Tfrac %d curtime %lld base_time %lld\n", sizeof(Clock::Tfrac), curtime, base_time);
	    AssertAlways(curtime == base_time,
			 ("internal self check failed\n"));
	    /*
	    time_t foo = (time_t)curtime;
	    struct tm *gmt = gmtime(&foo);
	    curtime -= ((24 * gmt->tm_yday + gmt->tm_hour) * 60 + gmt->tm_min) * 60 + gmt->tm_sec;
	    base_time = curtime;
	    */
	    base_time = 0;
	    printf("adjusted basetime %lld\n", base_time);
	}
    }
    std::string srtheadertype_xml = "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Trace::BlockIO::SRTHeader";
    srtheadertype_xml.append("\" version=\"");
    char header_char_ver[4];
    header_char_ver[0] = (char)('0' + trace_major);
    header_char_ver[1] = '.';
    header_char_ver[2] = (char)('0' + trace_minor);
    header_char_ver[3] = '\0';
    srtheadertype_xml.append(header_char_ver);
    srtheadertype_xml.append("\" >\n");
    std::string srt_header = "  <field type=\"variable32\" name=\"header_text\" pack_unique=\"yes\" print_style=\"text\" />\n";
    srtheadertype_xml.append(srt_header);
    srtheadertype_xml.append("</ExtentType>\n");
    ExtentTypeLibrary library;
    ExtentType *srtheadertype = library.registerType(srtheadertype_xml);
    ExtentSeries srtheaderseries(*srtheadertype);
    OutputModule headeroutmodule(srtdsout,srtheaderseries,srtheadertype,packing_args.extent_size);

    std::string srttype_xml = "<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"Trace::BlockIO::HP-UX";
    srttype_xml.append("\" version=\"");
    char char_ver[4];
    char_ver[0] = (char)('0' + trace_major);
    char_ver[1] = '.';
    char_ver[2] = (char)('0' + trace_minor);
    char_ver[3] = '\0';
    srttype_xml.append(char_ver);
    srttype_xml.append("\" >\n");
    char *buf = new char[srt_timefields.size() + 200];
    sprintf(buf,srt_timefields.c_str(),base_time,base_time,base_time);
    srttype_xml.append(buf);
    printf("trace_minor %d\n", trace_minor);
    if (trace_minor < 7) {
	if (trace_minor == 0) {
	    //NEED VERSION 0 HERE
	    //srttype_xml.append(srt_v0fields);
	    printf("trace_minor got inside 0!%d\n", trace_minor);
	} else if (trace_minor >= 3) {
	    srttype_xml.append(srt_v3fields);
	    printf("trace_minor got inside 3!%d\n", trace_minor);
	} 
	if (trace_minor >= 4) {
	    srttype_xml.append(srt_v4fields);
	    printf("trace_minor got inside 4!%d\n", trace_minor);
	} 
	if (trace_minor >= 5) {
	    srttype_xml.append(srt_v5fields);
	    printf("trace_minor got inside 5!%d\n", trace_minor);
	} 
	if (trace_minor >= 6) {
	    srttype_xml.append(srt_v6fields);
	    printf("trace_minor got inside 6!%d\n", trace_minor);
	}
    } else if (trace_minor == 7) {
	srttype_xml.append(srt_v7fields);
    } else {
	AssertFatal(("missing support for srt trace version %d\n",trace_minor));
    }
    srttype_xml.append(srt_ioflags);
    srttype_xml.append("</ExtentType>\n");
    ExtentType *srttype = library.registerType(srttype_xml);
    ExtentSeries srtseries(*srttype);
    OutputModule outmodule(srtdsout,srtseries,srttype,packing_args.extent_size);
    srtdsout.writeExtentLibrary(library);

    Variable32Field header_text(srtheaderseries, "header_text", Field::flag_nullable);

    Int64Field enter_kernel(srtseries,"enter_driver");
    Int64Field leave_driver(srtseries,"leave_driver");
    Int64Field return_to_driver(srtseries,"return_to_driver");
    Int32Field bytes(srtseries,"bytes");
    Int64Field disk_offset(srtseries,"disk_offset");
    ByteField device_major(srtseries, "device_major");
    ByteField device_minor(srtseries, "device_minor");
    ByteField device_controller(srtseries, "device_controller");
    ByteField device_disk(srtseries, "device_disk");
    ByteField device_partition(srtseries, "device_partition");
    Int32Field driver_type(srtseries,"driver_type", Field::flag_nullable);
    BoolField flag_synchronous(srtseries,"flag_synchronous");
    BoolField flag_raw(srtseries,"flag_raw");
    BoolField flag_no_cache(srtseries,"flag_nocache");
    BoolField flag_call(srtseries,"flag_call");
    BoolField flag_filesystemIO(srtseries,"flag_fsysio");
    BoolField flag_bufdata_invalid(srtseries,"flag_bufdata_invalid");
    BoolField flag_cache(srtseries,"flag_cache");
    BoolField flag_power_failure_timeout(srtseries,"flag_pftimeout");
    BoolField flag_write_verification(srtseries,"flag_writev");
    BoolField flag_rewrite(srtseries,"flag_rewrite");
    BoolField flag_delwrite(srtseries,"flag_delwrite");
    BoolField flag_async(srtseries,"flag_async");
    BoolField flag_ndelay(srtseries,"flag_ndelay");
    BoolField flag_wanted(srtseries,"flag_wanted");
    BoolField flag_end_of_data(srtseries,"flag_end_of_data");
    BoolField flag_phys(srtseries,"flag_phys");
    BoolField flag_busy(srtseries,"flag_busy");
    BoolField flag_error(srtseries,"flag_error");
    BoolField flag_done(srtseries,"flag_done");
    BoolField is_read(srtseries,"is_read");
    BoolField flag_ord_write(srtseries,"flag_ordwrite");
    BoolField flag_merged(srtseries,"flag_merged");
    BoolField flag_merged_from(srtseries,"flag_merged_from");
    BoolField act_release(srtseries,"act_release");
    BoolField act_allocate(srtseries,"act_allocate");
    BoolField act_free(srtseries,"act_free");
    BoolField act_raw(srtseries,"act_raw");
    BoolField act_flush(srtseries,"act_flush");
    BoolField net_buf(srtseries,"net_buf");
    

    ByteField buffertype(srtseries,"buffertype");
    Variable32Field buffertype_text(srtseries, "buffertype_text");
    
    Int32Field *cylinder_number = NULL;
    if (trace_minor == 0) {
	//NEED VERSION 0 INIT HERE
    } else if (trace_minor >= 1 && trace_minor < 7) {
	cylinder_number = new Int32Field(srtseries,"cylinder_number");
    }
    Int32Field *queue_length = NULL;
    if (trace_minor >= 4) {
	queue_length = new Int32Field(srtseries,"queue_length");
    }
    Int32Field *pid = NULL;
    if (trace_minor >= 5) {
	pid = new Int32Field(srtseries,"pid");
    }
    Int32Field *logical_volume_number = NULL;
    if (trace_minor >= 6) {
	logical_volume_number = new Int32Field(srtseries,"logical_volume_number");
    }
    Int32Field *machine_id = NULL;
    Int32Field *thread_id = NULL;
    Int64Field *lv_offset = NULL;
    if (trace_minor >= 7) {
	machine_id = new Int32Field(srtseries,"machine_id");
	thread_id = new Int32Field(srtseries,"thread_id",Field::flag_nullable);
	lv_offset = new Int64Field(srtseries,"lv_offset",Field::flag_nullable);
    }

    //Write out SRT header info to the DS file and flush the extent.
    headeroutmodule.newRecord();
    //printf("HI %s\n", tracestream->header());
    
    header_text.set(tracestream->header());
    headeroutmodule.flushExtent();

    int nrecords = 0;
    while(1) {
	if (raw_tr == NULL || tracestream->eof() || tracestream->fail()) 
	    break;
	
	SRTrecord *_tr = new SRTrecord(raw_tr, 
				      SRTrawTraceVersion(trace_major, trace_minor));
	
	AssertAlways(_tr->type() == SRTrecord::IO,
		     ("Only know how to handle I/O records\n"));
	SRTio *tr = (SRTio *)_tr;
	AssertAlways(trace_minor == tr->get_version(), ("Version mismatch between header (minor version %d) and data (minor version %d).  Override header with data version to convert correctly!\n",trace_minor, tr->get_version()));
	++nrecords;
	AssertAlways(trace_minor < 7 || tr->noStart() == false,("?!"));
	outmodule.newRecord();
	AssertAlways(fabs(tr->created() *1e6 - round(tr->created()*1e6)) < 0.1,
		     ("bad created %.8f\n",tr->created()));
	enter_kernel.set(tr->tfrac_created()-base_time);
	Clock::Tfrac tmp = tr->tfrac_started() - tr->tfrac_created();
	/*
	printf("tmp is %f from started %f and created %f\n", tmp, tr->started(), tr->created());
	AssertAlways(fabs(tmp * 1e6 - round(tmp * 1e6)) < 0.1,
		     ("bad started %.8f\n",tr->started()));
	*/
	leave_driver.set(tr->tfrac_started()-base_time);
        tmp = tr->tfrac_finished() - tr->tfrac_created();
	/*
	AssertAlways(fabs(tmp * 1e6 - round(tmp * 1e6)) < 0.1,
		     ("bad finished %.8f\n",tr->started()));
	*/
	return_to_driver.set(tr->tfrac_finished()-base_time);
	bytes.set(tr->length());
	disk_offset.set(scale_offset ? (tr->offset() / 1024) : tr->offset());
	device_major.set(tr->device_number() >> 24 & 0xFF);
	device_minor.set(tr->device_number() >> 16 & 0xFF);
	device_controller.set(tr->device_number() >> 12 & 0xF);
	device_disk.set(tr->device_number() >> 8 & 0xF);
	device_partition.set(tr->device_number() & 0xFF);
	buffertype.set((char)tr->buffertype());
	buffertype_text.set(tr->buffertype_text());
	flag_synchronous.set(tr->is_synchronous());
	flag_raw.set(tr->is_raw());
	flag_no_cache.set(tr->is_no_cache());
	flag_call.set(tr->is_call());
	flag_filesystemIO.set(tr->is_filesystemIO());
	flag_bufdata_invalid.set(tr->is_invalid_info());
	flag_cache.set(tr->is_cache());
	flag_power_failure_timeout.set(tr->is_power_failure_timeout());
	flag_write_verification.set(tr->is_write_verification());
	flag_rewrite.set(tr->is_rewrite());
	flag_delwrite.set(tr->is_write_at_exit());
	flag_async.set(tr->is_asynchronous());
	flag_ndelay.set(tr->is_no_delay());
	flag_wanted.set(tr->is_wanted());
	flag_end_of_data.set(tr->is_end_of_data());
	flag_phys.set(tr->is_physical_io());
	flag_busy.set(tr->is_busy());
	flag_error.set(tr->is_error());
	flag_done.set(tr->is_transaction_complete());
	is_read.set(tr->is_read());
	flag_ord_write.set(tr->is_ord_write());
	flag_merged.set(tr->is_merged());
	flag_merged_from.set(tr->is_merged_from());

	act_release.set(tr->is_release());
	act_allocate.set(tr->is_allocate());
	act_free.set(tr->is_free());
	act_raw.set(tr->is_character_dev_io());
	act_flush.set(tr->is_flush());
	// The 1996 cello traces appear to have the flag set in some
	// records.  This seems to be a mis-transcode from KI, but the
	// Origin IO Flags header file from HP-UX from 1996 is lost.
	net_buf.set(tr->is_netbuf());

	AssertAlways(tr->is_DUXaccess() == false,("need more columns"));
	AssertAlways(tr->is_private() == false,("need more columns"));
	// is readahead is just a composite test of async && read && fsysio
	//	AssertAlways(tr->is_readahead() == false,("need more columns"));

	if (trace_minor >= 7 && tr->noDriver()) {
	    driver_type.setNull(true);
	} else {
	    driver_type.set(tr->driverType());
	}
	if (cylinder_number) {
	    cylinder_number->set(tr->cylno());
	}
	if (queue_length) {
	    AssertAlways(trace_minor < 7 || tr->noQueueLen() == false,("?!"));
	    queue_length->set(tr->qlen());
	}
	if (pid) {
	    AssertAlways(tr->noPid() == false,("?!"));
	    pid->set(tr->pid());
	}
	if (logical_volume_number) {
	    AssertAlways(trace_minor < 7 || tr->noLvDevNo() == false,("?!"));
	    logical_volume_number->set(tr->lvdevno());
	}
	if (machine_id) {
	    AssertAlways(tr->noMachineID() == false,("?!"));
	    machine_id->set(tr->machineID());
	}
	if (thread_id) {
	    if (tr->noThread()) {
		thread_id->setNull(true);
	    } else {
		thread_id->set(tr->thread());
	    }
	}
	if (lv_offset) {
	    if (tr->noLvOffset()) {
		lv_offset->setNull(true);
	    } else {
		lv_offset->set(tr->lv_offset());
	    }
	}
	delete _tr;
	raw_tr = tracestream->record();
    }
    outmodule.flushExtent();
    fprintf(stderr,"%d records, %d extents; %lld bytes, %lld compressed\n",
	    nrecords, srtdsout.extents, srtdsout.unpacked_size, srtdsout.packed_size);
    fprintf(stderr,"  %.4gx compression ratio; %lld fixed data, %lld variable data\n",
	    (double)srtdsout.unpacked_size / (double)srtdsout.packed_size,
	    srtdsout.unpacked_fixed, srtdsout.unpacked_variable);
    fprintf(stderr,"  extents-part-compression: ");
    if (srtdsout.compress_none > 0) fprintf(stderr,"%d none, ",srtdsout.compress_none);
    if (srtdsout.compress_lzo > 0) fprintf(stderr,"%d lzo, ",srtdsout.compress_lzo);
    if (srtdsout.compress_gzip > 0) fprintf(stderr,"%d gzip, ",srtdsout.compress_gzip);
    if (srtdsout.compress_bz2 > 0) fprintf(stderr,"%d bz2, ",srtdsout.compress_bz2);
    fprintf(stderr," packed in %.6gs\n",
	   srtdsout.pack_time);
    return 0;
}
