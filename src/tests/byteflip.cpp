// -*-C++-*-
/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    test program for DataSeries
*/

#include <stdio.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <zlib.h>
#if DATASERIES_ENABLE_LZO
#include <lzoconf.h>
#endif

#include <boost/bind.hpp>

#include <Lintel/HashTable.hpp>
#include <Lintel/MersenneTwisterRandom.hpp>
#include <Lintel/Clock.hpp>
#include <Lintel/Stats.hpp>

#include <DataSeries/cryptutil.hpp>
#include <DataSeries/Extent.hpp>
#include <DataSeries/ExtentField.hpp>
#include <DataSeries/DataSeriesModule.hpp>

using namespace std;
using boost::format;

#define REVERSE_INT32(Int32ValuE) \
    ((Int32ValuE >> 24) & 0xff) | ((Int32ValuE >> 8) & 0xff00) \
  | ((Int32ValuE << 8) & 0xff0000) | ((Int32ValuE << 24) & 0xff000000)

void doit_charflip(uint32_t *buf, unsigned buflen) {
    for(unsigned i=0;i<buflen;++i) {
	unsigned char *b = (unsigned char *)(buf+i);

	unsigned char f = b[0];
	b[0] = b[3];
	b[3] = f;
	unsigned char g = b[1];
	b[1] = b[2];
	b[2] = g;
    }
}

void doit_intshift1(uint32_t *buf, unsigned buflen) {
    for(unsigned i=0;i<buflen;++i) {
	buf[i] = REVERSE_INT32(buf[i]);
    }
}

void doit_intshift2(uint32_t *buf, unsigned buflen) {
    for(unsigned i=0;i<buflen;++i) {
	uint32_t v = buf[i];
	buf[i] = ((v >> 24) & 0xFF) | ((v>>8) & 0xFF00) |
	    ((v & 0xFF00) << 8) | ((v & 0xFF) << 24);
    }
}

void doit_intshift3(uint32_t *buf, unsigned buflen) {
    for(unsigned i=0;i<buflen;++i) {
	uint32_t v = buf[i];
	buf[i] = ((v >> 24) & 0xFF) + ((v>>8) & 0xFF00) +
	    ((v & 0xFF00) << 8) + ((v & 0xFF) << 24);
    }
}

void doit_intshift4(uint32_t *buf, unsigned buflen) {
    for(unsigned i=0;i<buflen;++i) {
	uint32_t v = buf[i];
	buf[i] = ((v >> 24) & 0xFF) | ((v>>8) & 0xFF00) |
	    ((v << 8) & 0xFF0000) | ((v << 24) & 0xFF000000);
    }
}

#if __GNUC__ >= 2 && (defined(__i386__) || defined(__x86_64__))
void doit_bswap_i486(uint32_t *buf, unsigned buflen) {
    for(unsigned i=0;i<buflen;++i) {
	register uint32_t tmp = buf[i];
	asm("bswap %0" : "=r" (tmp) : "0" (tmp));
	buf[i] = tmp;
    }
}
#endif

#ifdef bswap_32
void doit_bswap_32(uint32_t *buf, unsigned buflen) {
    for(unsigned i=0;i<buflen;++i) {
	buf[i] = bswap_32(buf[i]);
    }
}
#endif

struct ByteFlipTest {
    // reps + bufsize takes ~0.5 second for each test on a 2.8GhZ p4 Xeon
    static const unsigned bufsize = 500 * 1000;
    static const unsigned reps = 2 * 100;
    uint32_t buf[bufsize];
    uint32_t expected_sum;
    Stats flip4_time;
};

void onebytefliptest(ByteFlipTest &bft, const string &fnname, Stats &timing,
		     void (*fn)(uint32_t *, unsigned), bool first_run = false) {
    if (timing.count() >= 2 && timing.mean()-timing.conf95() 
	> (bft.flip4_time.mean() + bft.flip4_time.conf95())) {
	cout << format("   skipping %s, mean time %.6g-%.6g > flip4+conf95 %.6g + %.6g\n")
	    % fnname % timing.mean() % timing.conf95()
	    % bft.flip4_time.mean() % bft.flip4_time.conf95();
	return;
    }
    uint32_t sum = 0;
    struct rusage test_start, test_end;
    getrusage(RUSAGE_SELF,&test_start);
    for(unsigned i=0; i<bft.reps; ++i) {
	fn(bft.buf, bft.bufsize);
        for(unsigned j=i; j<bft.bufsize; j += bft.reps/2) { // add in each entry twice
	    sum += bft.buf[j];
	}
    }
    getrusage(RUSAGE_SELF,&test_end);

    double elapsed = (test_end.ru_utime.tv_sec - test_start.ru_utime.tv_sec) 
	+ (test_end.ru_utime.tv_usec - test_start.ru_utime.tv_usec)/1.0e6;
    if (first_run) {
	bft.expected_sum = sum;
    } 
    INVARIANT(sum == bft.expected_sum, format("bad %s byteswap? %d != %d")
	      % fnname % sum % bft.expected_sum);
    timing.add(elapsed);
    cout << format("   via %s: sum %d in %.6g seconds; mean %.6gs +- %.6gs\n")
	% fnname % sum % elapsed % timing.mean() % timing.stddev();
}

bool in_range(double point, double min, double max) {
    return point >= min && point <= max;
}

bool range_overlap(double a_min, double a_max, double b_min, double b_max) {
    return in_range(a_min, b_min, b_max) || in_range(a_max, b_min, b_max) ||
	in_range(b_min, a_min, a_max) || in_range(b_max, a_min, a_max);
}


double elapsed(struct rusage &start, struct rusage &end) {
    return end.ru_utime.tv_sec - start.ru_utime.tv_sec 
	+ (end.ru_utime.tv_usec - start.ru_utime.tv_usec)/1.0e6;
}

void test_byteflip() {
#if LINTEL_DEBUG
    // With debugging turned on, even with the functions marked
    // inline, it seems gcc still generates a function call, which
    // makes elapsed_flip4 much slower

    cout << "byteflip timing test is useless with DEBUG set\n";
    return;
#endif

    SINVARIANT(Extent::flip4bytes(0x12345678) == 0x78563412);
    SINVARIANT(Extent::flip4bytes(0xFEDCBA90) == 0x90BADCFE);
    static const unsigned rounds = 5;
    ByteFlipTest bft;

    for(unsigned i=0; i < bft.bufsize; ++i) {
	bft.buf[i] = MTRandom.randInt();
    }

    cout << "Initial execution:\n";
    onebytefliptest(bft, "Extent::flip4bytes", bft.flip4_time, Extent::run_flip4bytes, true);

    for (unsigned tries = 0; tries < 3; ++tries) {
	const unsigned max_runtimes = 10;
	Stats runtimes[max_runtimes];

	for(unsigned i = 0; i < (rounds * (tries + 1)); ++i) {
	    cout << format("\nRound %d:\n") % i;
	    onebytefliptest(bft, "character flip",  runtimes[0],  doit_charflip);
	    onebytefliptest(bft, "integer shift 1", runtimes[1], doit_intshift1);
	    onebytefliptest(bft, "integer shift 2", runtimes[2], doit_intshift2);
	    onebytefliptest(bft, "integer shift 3", runtimes[3], doit_intshift3);
	    onebytefliptest(bft, "integer shift 4", runtimes[4], doit_intshift4);
	    
#if __GNUC__ >= 2 && (defined(__i386__) || defined(__x86_64__))
	    onebytefliptest(bft, "bswap_i486", runtimes[5], doit_bswap_i486);
#endif
#ifdef bswap_32
	    onebytefliptest(bft, "bswap_32",   runtimes[6], doit_bswap_32);
#endif
	
	    onebytefliptest(bft, "Extent::flip4bytes", bft.flip4_time, Extent::run_flip4bytes);
	}

	unsigned bestidx = 0;
	for(unsigned i = 1; i < max_runtimes; ++i) {
	    if (runtimes[i].count() == 0)
		continue;
	    if (runtimes[i].mean() < runtimes[bestidx].mean()) 
		bestidx = i;
	}
	double bestmean = runtimes[bestidx].mean();
	double bestconf95 = runtimes[bestidx].conf95();
	// 0.005 means that if bestmean and flip4mean are within 1% of
	// each other, that will be considered acceptable.  Got a run
	// where the times differered by 6ms (~.1%), but had been stable
	// enough that the confidence intervals were too small to generate
	// an overlap.
	if (bestconf95 < bestmean * 0.005) {
	    bestconf95 = bestmean * 0.005;
	}
	double flip4mean = bft.flip4_time.mean();
	double flip4conf95 = bft.flip4_time.conf95();
	if (flip4conf95 < flip4mean * 0.005) {
	    flip4conf95 = flip4mean * 0.005;
	}
	if (range_overlap(flip4mean - flip4conf95, flip4mean + flip4conf95,
			  bestmean - bestconf95, bestmean + bestconf95)) {
	    cout << format("flip4 performance of (%.6g +- %.6g) verified to overlap with best time (%.6g +- %.6g)\n")
		% flip4mean % flip4conf95 % bestmean % bestconf95;
	    return;
	}
	

	cout << format("Error, flip4_time (%.6g +- %.6g) does not overlap with best time (%.6g +- %.6g); something is weird")
	    % flip4mean % flip4conf95 % bestmean % bestconf95;
	sleep(2); // Wait a little while before trying again
    }
    FATAL_ERROR("after multiple tries, unable to achieve overlap");
}

int main(int argc, char *argv[]) {
    Extent::setReadChecksFromEnv(true);

    test_byteflip();
}
