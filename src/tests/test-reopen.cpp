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

using namespace std;

void doReopen(string file, bool reset_mod_time) {
    // copy the file
    string cmd = "/bin/cp " + file + " test-reopen.ds";
    int ret = system(cmd.c_str());
    SINVARIANT(ret == 0);
    file = "test-reopen.ds";

    if (!reset_mod_time) {
        // sleep to ensure the modification we make falls into a different mtime
        sleep(2);
    }

    // test that a corruption is not caught in the common re-open case
    DataSeriesSource source(file);

    // close the source
    source.closefile();

    // corrupt the file's tail
    int fd = open(file.c_str(), O_RDWR);
    SINVARIANT(fd >0);
    off_t ret_off = lseek(fd, -28, SEEK_END);
    SINVARIANT(ret_off > 0);
    ret = write(fd, &fd, 4);
    SINVARIANT(ret == 4);

    if (reset_mod_time) {
        // reset mtime to zero
        struct timeval times[2];
        memset(times, 0, sizeof(times));
        ret = futimes(fd, times);
        SINVARIANT(ret == 0);
    }

    close(fd);

    // TODO-craig: Use TestUtil.hpp to verify the error message here, and change the
    // exit check to verify that you exit(213) or some weird thing like that.  Right now it's
    // failing for a bogus reason (name_to_type.find(type->getName()) == name_to_type.end()), which
    // makes me think the reopen won't work correctly on a validly updated file.  Perhaps add a test
    // that you can re-open a file that just had it's mod-time pushed forward?

    // re-open the file... checks will be made or not based on the mtime
    source.reopenfile();
}

int main(int argc, char *argv[]) {
    // do it once resetting the mod time, avoiding the checks, so shouldn't notice a problem
    cout << "Resetting mod time\n";
    doReopen(argv[1], true);
    cout << "SUCCESS: still alive!\n";

    // do it once without resetting the mod time, should notice the problem
    int child = fork();
    SINVARIANT(child >= 0);
    if (!child) {
        cout << "Not resetting mod time\n";
        doReopen(argv[1], false);
        cout << "ERROR: still alive!\n";
        exit(0); // Claim successful exit since parent expects failure
    }

    int status;
    waitpid(child, &status, 0);

    if (WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT) {
        // child properly failed.
        return 0;
    } else {
        return 1;
    }
}
