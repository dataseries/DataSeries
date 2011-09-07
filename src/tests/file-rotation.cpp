/*
   (c) Copyright 2011, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/
#include <boost/format.hpp>

#include <Lintel/LintelLog.hpp>
#include <Lintel/ProgramOptions.hpp>

#include <DataSeries/DataSeriesSink.hpp>
#include <DataSeries/RotatingFileSink.hpp>

using namespace std;
using lintel::ProgramOption;
using namespace dataseries;
using boost::format;

ProgramOption<uint32_t> po_nthreads("nthreads", "number of threads", 5);
ProgramOption<double> po_execution_time("execution-time", "execution time in seconds", 5.0);
ProgramOption<double> po_extent_interval("extent-interval", "interval between extents (s)", 0.05);
ProgramOption<double> po_rotate_interval("rotate-interval", "rotate interval in seconds", 0.5);

const string extent_type_xml = 
"<ExtentType namespace=\"ssd.hpl.hp.com\" name=\"File-Rotation\" version=\"1.0\" >\n"
"  <field type=\"int32\" name=\"thread\" />\n"
"  <field type=\"int32\" name=\"count\" />\n"
"</ExtentType>\n";

void writeExtent(IExtentSink &output, const ExtentType &type, uint32_t thread,
                 uint32_t &count, uint32_t rows) {
    ExtentSeries s(new Extent(type));
    Int32Field f_thread(s, "thread");
    Int32Field f_count(s, "count");
    for (uint32_t i = 0; i < rows; ++i) {
        s.newRecord();
        f_thread.set(thread);
        f_count.set(count);
        ++count;
    }
    output.writeExtent(*s.getExtent(), NULL);
    delete s.getExtent();
    s.clearExtent();
}

void simpleFileRotation() {
    ExtentTypeLibrary library;
    const ExtentType &type = library.registerTypeR(extent_type_xml);

    DataSeriesSink output(Extent::compress_lzf);

    uint32_t count = 0;
    for (uint32_t i=0; i < 10; ++i) {
        LintelLog::info(format("simple round %d") % i);
        output.open(str(format("simple-fr-%d.ds") % i));
        output.writeExtentLibrary(library);
        writeExtent(output, type, i, count, i+5);
        output.close();
    }
}

void simpleRotatingFileSink() {
    RotatingFileSink rfs(Extent::compress_lzf);

    const ExtentType &type = rfs.registerType(extent_type_xml);
    
    uint32_t count = 0;
    for (uint32_t i=0; i < 10; ++i) {
        LintelLog::info(format("simple rfs %d") % i);
        writeExtent(rfs, type, i, count, i+5);
        rfs.changeFile(str(format("simple-rfs-%d.ds") % i));
        rfs.waitForCanChange();
        writeExtent(rfs, type, i, count, i+5);
    }
}

void *ptrExtentWriter(RotatingFileSink *rfs, const ExtentType *type, uint32_t thread_num) {
    Clock::Tfrac start = Clock::todTfrac();
    int64_t stop = start + Clock::secondsToTfrac(po_execution_time.get());

    Clock::Tfrac interval = Clock::secondsToTfrac(po_extent_interval.get());
    uint32_t count = 0;
    for(int64_t i = start; i < stop; i += interval) {
        int64_t now = Clock::todTfrac();
        int64_t delta = i - now;
        if (delta > 0) {
            uint32_t microseconds = Clock::TfracToMicroSec(delta);
            usleep(microseconds);
        }
        writeExtent(*rfs, *type, thread_num, count, 1);
    }
    LintelLog::info(format("thread %d wrote %d extents") % thread_num % count);
    return NULL;
}

void *ptrRotater(RotatingFileSink *rfs) {
    Clock::Tfrac stop = Clock::todTfrac() + Clock::secondsToTfrac(po_execution_time.get());

    uint32_t usleep_amt = static_cast<uint32_t>(round(1000.0 * 1000.0 * po_rotate_interval.get()));
    SINVARIANT(usleep_amt > 0 && usleep_amt < 1000000);
    uint32_t count = 0;
    while (Clock::todTfrac() < stop) {
        rfs->changeFile(str(format("ptr-%d.ds") % count));
        ++count;
        usleep(usleep_amt);
    }
    LintelLog::info(format("thread rotated %d times") % count);
    return NULL;
}


void periodicThreadedRotater() {
    RotatingFileSink rfs(Extent::compress_lzf);

    const ExtentType &type = rfs.registerType(extent_type_xml);
    
    SINVARIANT(po_extent_interval.get() > 0 && po_extent_interval.get() < 1);
    SINVARIANT(po_rotate_interval.get() > 0 && po_rotate_interval.get() < 1);
    vector<PThread *> threads;
    for (uint32_t i=0; i < po_nthreads.get(); ++i) {
        threads.push_back(new PThreadFunction(boost::bind(ptrExtentWriter, &rfs, &type, i)));
        threads.back()->start();
    }
    threads.push_back(new PThreadFunction(boost::bind(ptrRotater, &rfs)));
    threads.back()->start();
    for (vector<PThread *>::iterator i = threads.begin(); i != threads.end(); ++i) {
        (**i).join();
        delete *i;
    }
}

static uint32_t cbr_count;
Clock::Tfrac cbr_last_rotate;

void callbackRotate(RotatingFileSink *rfs, off64_t, Extent &) {
    Clock::Tfrac rotate_interval = Clock::secondsToTfrac(po_rotate_interval.get());
    Clock::Tfrac now = Clock::todTfrac();
    if (now > cbr_last_rotate + rotate_interval) {
        if (rfs->canChangeFile()) {
            rfs->changeFile(str(format("cbr-%d.ds") % cbr_count));
            ++cbr_count;
            cbr_last_rotate = now;
        } else {
            string new_filename(rfs->getNewFilename());
            if (new_filename == RotatingFileSink::closed_filename) {
                // ignore, we're being removed
            } else {
                LintelLog::warn(format("unable to rotate; last rotation (%s) incomplete")
                                % new_filename);
            }
        }
    }
}

void extentCallbackRotater() {
    RotatingFileSink rfs(Extent::compress_lzf);

    const ExtentType &type = rfs.registerType(extent_type_xml);
    rfs.setExtentWriteCallback(boost::bind(callbackRotate, &rfs, _1, _2));
    rfs.changeFile("cbr-0.ds");
    cbr_count = 1;
    cbr_last_rotate = Clock::todTfrac();

    SINVARIANT(po_extent_interval.get() > 0 && po_extent_interval.get() < 1);
    vector<PThread *> threads;
    for (uint32_t i=0; i < po_nthreads.get(); ++i) {
        threads.push_back(new PThreadFunction(boost::bind(ptrExtentWriter, &rfs, &type, i)));
        threads.back()->start();
    }
    for (vector<PThread *>::iterator i = threads.begin(); i != threads.end(); ++i) {
        (**i).join();
        delete *i;
    }
    rfs.close();
    LintelLog::info(format("rotated %d times") % cbr_count);
}

// Notes on parallel tests: If we have an epoch counter that starts at 0 and is incremented once
// after a call to changeFile, and we write that counter into the extent, then we can guarantee
// that the counters in the extent should be at least the same as the file count.  If we get a
// count before and after each rotate and writeExtent call then we could perform a more complicated
// calculation that determines which file an extent could possibly end up in based on the overlap
// of the range for the writeExtent with the range for the rotate.

int main(int argc, char *argv[]) {
    vector<string> mode = lintel::parseCommandLine(argc, argv, true);
    INVARIANT(mode.size() == 1, format("Usage: %s <mode>") % argv[0]);

    if (mode[0] == "simple") {
        simpleFileRotation();
    } else if (mode[0] == "simple-rotating") {
        simpleRotatingFileSink();
    } else if (mode[0] == "periodic-threaded-rotater") {
        periodicThreadedRotater();
    } else if (mode[0] == "extent-callback-rotater") {
        extentCallbackRotater();
    } else {
        FATAL_ERROR(format("unknown mode '%s'; see code") % mode[0]);
    }
}
