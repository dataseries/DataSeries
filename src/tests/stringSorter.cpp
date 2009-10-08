// -*-C++-*-
/*
   (c) Copyright 2009, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/*
  Program to explore performance charateristics of sorting.  This
  isn't exactly an indysort implementation, but it can sort a 4
  million row pennysort input at 782932 records/sec (78.2/MB sec)
*/

#include <stdlib.h>
#include <string>
#include <algorithm>
#include <boost/random.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <sys/stat.h>
#include <sys/time.h>
#include <iostream>
#include <sys/uio.h>

#include <Lintel/PriorityQueue.hpp>
#include <Lintel/ProgramOptions.hpp>


using lintel::ProgramOption;
using namespace std;
typedef struct buf {
    int32_t size;
    char * data;
} buf_t;

#define INDY_KEY_SIZE (10)
#define INDY_SIZE (100)

// If trick is defined, we hard-code that records are 100 long.  This
// is cheating, but it is an interesting performance point.  Code goes
// about 10% faster. (at least 840k records/sec )

#ifdef TRICK
typedef struct trick {
    char * iov_base;
} line_t;
#else
typedef struct iovec line_t;
#endif
typedef struct text {
    int32_t size;
    line_t * lines;
} text_t;

buf_t getArray(const char * fname) {
    struct stat s;
    stat(fname, &s);
    buf_t retVal;
    retVal.size = s.st_size+1;
    retVal.data = new char[s.st_size+1];

    FILE * f = fopen(fname, "rb");
    fread(retVal.data, s.st_size, 1, f);
    retVal.data[s.st_size] = '\n'; //newline terminate the whole thing.
    return retVal;
}

text_t getText(buf_t data) {
    int32_t count=1000;
    text_t retVal;
    //retVal.size = count;
    retVal.lines = (line_t *)malloc(count * sizeof(line_t));

    char * p = data.data;
    int32_t i;
    for (i = 0; p-data.data < data.size; i++) {
	if (count <= i) { 
	    count *=2;
	    retVal.lines = (line_t *)realloc(retVal.lines, count*sizeof(line_t));
	}
	retVal.lines[i].iov_base = p;
#ifdef TRICK
	p += strchr(p,'\n')-p+1;
#else
	retVal.lines[i].iov_len = strchr(p,'\n')-p+1;
	p += retVal.lines[i].iov_len; 
#endif
    }
    retVal.size = i-1;
    return retVal;
}

void destroyText(text_t  t) {
    free(t.lines);
}

void printText(text_t t) {
#ifndef TRICK
    writev(1, t.lines, t.size);
#endif
}

class LineComper {
public:
    bool operator()(const line_t & l,
		    const line_t & r) {
	int res = memcmp(l.iov_base, r.iov_base, 
#ifdef TRICK
			 INDY_KEY_SIZE);
#else
			 min(l.iov_len, r.iov_len));
#endif
	if (res==0) {
#ifdef TRICK
	    return false;
#else
	    return l.iov_len < r.iov_len;
#endif
	} else {
	    return res < 0;
	}
    }
};

class TextComper {
public:
    TextComper()  {
    }
    LineComper comp;
    bool operator()(const text_t  *l, const text_t  *r) {
	return comp(r->lines[0], l->lines[0]);
    }
};


ProgramOption<string> po_input("input", "File to sort", string("NO_DEFAULT"));

ProgramOption<int32_t> po_iters("iters", "Number of times to loop", 1);

ProgramOption<int32_t> po_chunksize("chunk-size", "Size of chunking", 6400);

ProgramOption<bool> po_suppress_output("suppress-output", "Don't print sorted result");

int main(int argc, char ** argv){
    lintel::parseCommandLine(argc, argv);

    INVARIANT(po_input.used(), "Must specify an input file!");

    buf_t buffer = getArray(po_input.get().c_str());
    buf_t outbuf;
    outbuf.data = 0;
    outbuf.size = 0;
    text_t t;
    t.lines = 0;
    t.size = 0;
    struct timeval start;
    struct timeval stop;
    double accum=0;
    int32_t do_chunking = po_chunksize.get();
    for(int32_t i = 0; i<po_iters.get(); i++) {
	cerr << "getting buffer" << endl;
	cerr.flush();
	t = getText(buffer);
#ifndef TRICK
	cerr << "a size is " << t.lines[0].iov_len <<  endl;
#endif
	cerr << "starting sort" << endl;
	cerr.flush();
	gettimeofday(&start,0);
	if (do_chunking==0) {
	    sort(t.lines, t.lines + t.size, LineComper());
	} else {
	    int32_t num_chunk = t.size / do_chunking + 1;
	    text_t * chunks = new text_t[num_chunk];
	    int32_t cur_chunk = 0;
	    int32_t c;
	    PriorityQueue <text_t *,  TextComper> qu;
	    for(c = do_chunking; c<t.size; c+=do_chunking) {
		chunks[cur_chunk].size = do_chunking;
		chunks[cur_chunk].lines = t.lines - do_chunking + c;
		sort(t.lines - do_chunking + c, t.lines + c, LineComper()); 
		qu.push(&chunks[cur_chunk]);
		cur_chunk++;
	    }
	    chunks[cur_chunk].size = t.size - c + do_chunking;
	    chunks[cur_chunk].lines = t.lines - do_chunking + c;
	    sort(t.lines - do_chunking + c, t.lines + t.size, LineComper());

	    qu.push(&chunks[cur_chunk]);
	    cur_chunk++;

	    /*text_t copy;
	    copy.size = t.size;
	    copy.lines = (line_t *)malloc(sizeof(line_t) * copy.size);*/
	    outbuf.size = buffer.size;
	    outbuf.data = new char[buffer.size];	    
	    char * p = outbuf.data;
	    for(c=0; c<t.size; c++) {
		text_t * top = qu.top();
#ifdef TRICK
		memcpy(p, top->lines->iov_base, INDY_SIZE);
		p += INDY_SIZE;
#else
		memcpy(p, top->lines->iov_base, top->lines->iov_len);
		p += top->lines->iov_len;
#endif
		top->lines++;
		top->size--;
		
		if (top->size) {
		    qu.replaceTop(top);
		} else {
		    qu.pop();
		} 
	    }
	    
	}

	gettimeofday(&stop,0);
	accum += stop.tv_sec - start.tv_sec;
	accum += (stop.tv_usec - start.tv_usec)/1000000.0;
	//destroyText(t);
    }
    cerr << "Used " << accum << " seconds total to sort" << endl;
    cerr.flush();
    if (!po_suppress_output.get()) {
	if (do_chunking==0) {
	    printText(t);
	} else {
	    write(1, outbuf.data, outbuf.size-1);
	}
    }
    return 0;
}
