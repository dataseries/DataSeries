#include <sys/time.h>
#include <sys/resource.h>
#include <math.h>

#include <SRTTrace.H>
#include <SRTrecord.H>
#include <SRTTraceRaw.H>
#include <SRTTrace_Filter.H>

#include <DataSeriesFile.H>
#include <DataSeriesModule.H>

int
main(int argc, char *argv[])
{
    SRTTrace *tracestream;

    typedef ExtentType::int32 int32;
    typedef ExtentType::int64 int64;
    AssertAlways(argc == 3,
		 ("Usage: %s in-srt in-ds '- allowed for stdin'\n",
		  argv[0]));
    if (strcmp(argv[1],"-")==0) {
	tracestream = new SRTTraceRaw(fileno(stdin));
    } else {
	tracestream = new SRTTraceRaw(argv+1,1);
    }
    AssertAlways(tracestream != NULL,("Unable to open %s for read",argv[1]));

    SourceModule srtdsin_source; 
    srtdsin_source.addSource(argv[2]);
    FilterModule srtdsin(srtdsin_source,"I/O trace: SRT-V");

    int trace_major = tracestream->version().major_num();
    int trace_minor = tracestream->version().minor_num();
    
    ExtentSeries srtseries;

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

    DoubleField enter_kernel(srtseries,"enter_driver", DoubleField::flag_allownonzerobase);
    DoubleField leave_driver(srtseries,"leave_driver", DoubleField::flag_allownonzerobase);
    DoubleField return_to_driver(srtseries,"return_to_driver", DoubleField::flag_allownonzerobase);
    Int32Field bytes(srtseries,"bytes");
    Int64Field disk_offset(srtseries,"disk_offset");
    Int32Field device_number(srtseries,"device_number");
    Int32Field driver_type(srtseries,"driver_type", Field::flag_nullable);
    ByteField buffertype(srtseries,"buffertype");

    // could check for these things with hasColumn also, but that will be
    // implicitly checked by trying to create the fields
    Int32Field *cylinder_number = NULL;
    if (trace_minor >= 1 && trace_minor < 7) {
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
	thread_id = new Int32Field(srtseries,"thread_id", Field::flag_nullable);
	lv_offset = new Int64Field(srtseries,"lv_offset", Field::flag_nullable);
    }
    Extent *srtextent = srtdsin.getExtent();
    srtseries.setExtent(srtextent);

    int nrecords = 0;
    while(1) {
	SRTrawRecord *raw_tr = tracestream->record();
	if (srtseries.pos.morerecords() == false) {
	    delete srtextent;
	    srtextent = srtdsin.getExtent();
	    if (srtextent != NULL) {
		srtseries.setExtent(srtextent);
	    } else {
		srtseries.clearExtent();
	    }
	}
	if (raw_tr == NULL || tracestream->eof() || tracestream->fail() || srtextent == NULL) {
	    AssertAlways((raw_tr == NULL || tracestream->eof()) && 
			 srtextent == NULL,("traces ended at different places\n"));
	    break;
	}
	
	SRTrecord *_tr = new SRTrecord(raw_tr, 
				      SRTrawTraceVersion(trace_major, trace_minor));
	
	AssertAlways(_tr->type() == SRTrecord::IO,
		     ("Only know how to handle I/O records\n"));
	SRTio *tr = (SRTio *)_tr;
	++nrecords;
	AssertAlways(trace_minor < 7 || tr->noStart() == false,("?!"));
	AssertAlways(fabs(enter_kernel.absval() - tr->created()) < 5e-7,("bad compare\n"));
	AssertAlways(fabs(leave_driver.absval() - tr->started()) < 5e-7,("bad compare\n"));
	AssertAlways(fabs(return_to_driver.absval() - tr->finished()) < 5e-7,("bad compare\n"));
	AssertAlways(bytes.val() == (int32)tr->length(),("bad compare\n"));
	AssertAlways(disk_offset.val() == (int64)tr->offset(),("bad compare %d %lld %lld\n",nrecords,disk_offset.val(),(int64)tr->offset()));
	AssertAlways(device_number.val() == (int32)tr->device_number(),("bad compare\n"));
	AssertAlways(buffertype.val() == (int32)tr->buffertype(),("bad compare\n"));
	AssertAlways(tr->is_synchronous() == flag_synchronous.val(),("bad compare"));
	AssertAlways(tr->is_DUXaccess() == false,("bad compare"));
	AssertAlways(tr->is_netbuf() == false,("bad compare"));
	AssertAlways(tr->is_raw() == flag_raw.val(),("bad compare"));
	AssertAlways(tr->is_no_cache() == flag_no_cache.val(),("bad compare"));
	AssertAlways(tr->is_call() == flag_call.val(),("bad compare"));
	AssertAlways(tr->is_filesystemIO() == flag_filesystemIO.val(),("bad compare"));
	AssertAlways(tr->is_invalid_info() == flag_bufdata_invalid.val(),("bad compare"));
	AssertAlways(tr->is_cache() == flag_cache.val(),("bad compare"));
	AssertAlways(tr->is_power_failure_timeout() == flag_power_failure_timeout.val(),("bad compare"));
	AssertAlways(tr->is_write_verification() == flag_write_verification.val(),("bad compare"));
	AssertAlways(tr->is_private() == false,("bad compare"));
	AssertAlways(tr->is_rewrite() == flag_rewrite.val(),("bad compare"));
	AssertAlways(tr->is_ord_write() == flag_ord_write.val(),("bad compare"));
	AssertAlways(tr->is_write_at_exit() == flag_delwrite.val(),("bad compare"));
	AssertAlways(tr->is_asynchronous() == flag_async.val(),("bad compare"));
	AssertAlways(tr->is_no_delay() == flag_ndelay.val(),("bad compare"));
	AssertAlways(tr->is_wanted() == flag_wanted.val(),("bad compare"));
	AssertAlways(tr->is_end_of_data() == flag_end_of_data.val(),("bad compare"));
	AssertAlways(tr->is_physical_io() == flag_phys.val(),("bad compare"));
	AssertAlways(tr->is_busy() == flag_busy.val(),("bad compare"));
	AssertAlways(tr->is_error() == flag_error.val(),("bad compare"));
	AssertAlways(tr->is_transaction_complete() == flag_done.val(),("bad compare"));
	AssertAlways(tr->is_read() == is_read.val(),("bad compare"));
	// is_readahead is a composite test, so we don't propogate it as a flag
	// AssertAlways(tr->is_readahead() == false,("bad compare"));
	AssertAlways(tr->is_merged() == flag_merged.val(),("bad compare"));
	AssertAlways(tr->is_merged_from() == flag_merged_from.val(),("bad compare"));
	AssertAlways(tr->is_flush() == act_flush.val(),("bad compare"));
	AssertAlways(tr->is_release() == act_release.val(),("bad compare"));
	AssertAlways(tr->is_allocate() == act_allocate.val(),("bad compare"));
	AssertAlways(tr->is_free() == act_free.val(),("bad compare"));
	AssertAlways(tr->is_character_dev_io() == act_raw.val(),("bad compare"));

	if (trace_minor >= 7 && tr->noDriver()) {
	    AssertAlways(driver_type.isNull(),("bad compare\n"));
	} else {
	    AssertAlways(driver_type.val() == (int32)tr->driverType(),
			 ("bad compare\n"));
	}
	if (cylinder_number) {
	    AssertAlways(cylinder_number->val() == (int32)tr->cylno(),("bad compare\n"));
	}
	if (queue_length) {
	    AssertAlways(trace_minor < 7 || tr->noQueueLen() == false,("?!"));
	    AssertAlways(queue_length->val() == (int32)tr->qlen(),("bad compare"));
	}
	if (pid) {
	    AssertAlways(tr->noPid() == false,("?!"));
	    AssertAlways(pid->val() == (int32)tr->pid(),("bad compare"));
	}
	if (logical_volume_number) {
	    AssertAlways(trace_minor < 7 || tr->noLvDevNo() == false,("?!"));
	    AssertAlways(logical_volume_number->val() == (int32)tr->lvdevno(),("bad compare"));
	}
	if (machine_id) {
	    AssertAlways(tr->noMachineID() == false,("?!"));
	    AssertAlways(machine_id->val() == (int32)tr->machineID(),("bad compare"));
	}
	if (thread_id) {
	    if (tr->noThread()) {
		AssertAlways(thread_id->isNull(),("bad compare"));
	    } else {
		AssertAlways((ExtentType::int32)tr->thread() >= 0,
			     ("internal error\n"));
		AssertAlways(thread_id->val() == (int32)tr->thread(),("bad compare"));
	    }
	}
	if (lv_offset) {
	    if (tr->noLvOffset()) {
		AssertAlways(lv_offset->isNull(),("bad compare"));
	    } else {
		AssertAlways((ExtentType::int64)tr->lv_offset() >= 0,
			     ("internal error\n"));
		AssertAlways(lv_offset->val() == (int64)tr->lv_offset(),("bad compare"));
	    }
	}
	delete _tr;
	++srtseries.pos;
    }
    printf("%d records successfully compared\n",nrecords);
}
