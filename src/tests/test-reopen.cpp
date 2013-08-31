// -*-C++-*-
/*
  (c) Copyright 2012, Hewlett-Packard Development Company, LP

  See the file named COPYING for license details
*/

#include <fcntl.h>
#include <time.h>
#include <utime.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>

#include <iostream>

#include <Lintel/TestUtil.hpp>

#include <DataSeries/DataSeriesSource.hpp>

using namespace std;

void testReopen(string file) {
    // copy the file
    string cmd = "/bin/cp " + file + " test-reopen.ds";
    int ret = system(cmd.c_str());
    SINVARIANT(ret == 0);
    file = "test-reopen.ds";

    struct utimbuf file_time;
    file_time.actime = time(NULL);
    SINVARIANT(file_time.actime > 1000*1000*1000);
    file_time.actime -= 100;
    file_time.modtime = file_time.actime;
    ret = utime(file.c_str(), &file_time);
    SINVARIANT(ret == 0);


    // test we can open the file
    DataSeriesSource source(file);

    cout << "Open passed.\n";
    // test we can re-open the file
    source.closefile();
    file_time.actime += 50;
    file_time.modtime = file_time.actime;
    ret = utime(file.c_str(), &file_time);
    SINVARIANT(ret == 0);
    source.reopenfile(); // File is valid, reopen should succeed, and should re-read bits
    cout << "Valid re-open passed.\n";

    // corrupt the file
    int fd = open(file.c_str(), O_RDWR);
    SINVARIANT(fd >0);
    off_t ret_off = lseek(fd, -28, SEEK_END); 
    SINVARIANT(ret_off > 0);
    ret = write(fd, &fd, 4); // Corrupt if fd != ~0
    SINVARIANT(ret == 4);

    // test that opening a corrupt file with the same mtime succeeds
    source.closefile();
    ret = utime(file.c_str(), &file_time);
    SINVARIANT(ret == 0);
    source.reopenfile();
    cout << "Corrupt file, same mtime, reopen succeeded as expected.\n";

    // test that opening a corrupt file with a newer mtime fails
    file_time.modtime += 1;
    source.closefile();
    ret = utime(file.c_str(), &file_time);
    SINVARIANT(ret == 0);
    TEST_INVARIANT_MSG1(source.reopenfile(), "bad header for the tail of test-reopen.ds!");
    cout << "Corrupt file, newer mtime, reopen correctly failed.\n";

    // test that opening a corrupt file with an older mtime fails
    file_time.modtime -= 2;
    source.closefile();
    ret = utime(file.c_str(), &file_time);
    SINVARIANT(ret == 0);
    TEST_INVARIANT_MSG1(source.reopenfile(), "bad header for the tail of test-reopen.ds!");

    cout << "Corrupt file, older mtime, reopen correctly failed.\n";
}

int main(int argc, char *argv[]) {
    testReopen(argv[1]);
    return 0;
}
