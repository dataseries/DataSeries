// -*-C++-*-
/*
   (c) Copyright 2012, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/wait.h>

#include <iostream>

#include <DataSeries/DataSeriesSource.hpp>

void
doReopen(std::string file,
         bool resetModTime)
{
    // copy the file
    std::string cmd = "/bin/cp " + file + " test.ds";
    system(cmd.c_str());
    file = "test.ds";

    // sleep to ensure the modification we make falls into a different mtime
    sleep(2);

    // test that a corruption is not caught in the common re-open case
    DataSeriesSource source(file);

    // close the source
    source.closefile();

    // corrupt the file's tail
    int fd = open(file.c_str(), O_RDWR);
    lseek(fd, -4, SEEK_END);
    write(fd, &fd, 4);

    // reset mtime to zero?
    if (resetModTime) {
        struct timeval times[2];
        memset(times, 0, sizeof(times));
        futimes(fd, times);
    }

    close(fd);

    // re-open the file... checks will be made or not based on the mtime
    source.reopenfile();
}

int
main(int argc,
     char *argv[])
{
    // do it once resetting the mod time, shouldn't notice a problem
    std::cout << "Resetting mod time" << std::endl;
    doReopen(argv[1], true);
    std::cout << "SUCCESS: still alive!" << std::endl;

    // do it once without resettin the mod time, should notice the problem
    int child = fork();
    if (!child) {
        std::cout << "Not resetting mod time" << std::endl;
        doReopen(argv[1], false);
        std::cout << "ERROR: still alive!" << std::endl;
        exit(0);
    }

    int status;
    waitpid(child, &status, 0);

    // fail if the child succeeded
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        return -1;
    }
    return 0;
}
