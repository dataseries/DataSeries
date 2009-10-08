#include <stdlib.h>

#include <iostream>

#include <boost/format.hpp>

#include <Lintel/Clock.hpp>

using namespace std;

int main(int argc, char *argv[]) {
    uint64_t MB = atoll(argv[2]);
    char *buffer = new char[1000000]; // 1 MB buffer
    bzero(buffer, 1000000);
    while (true) {
        Clock::Tfrac start_clock = Clock::todTfrac();
        for (uint32_t i = 0; i < MB; ++i) {
            SINVARIANT(fwrite(buffer, 1000000, 1, stdout) == 1);
        }
        //fsync(fileno(stdout));
        Clock::Tfrac stop_clock = Clock::todTfrac();
        double s = Clock::TfracToDouble(stop_clock - start_clock);
        double throughput = MB/s;
        cerr << boost::format("*** %s ***\tT = %s MB/s ||| data = %s MB ||| t = %s s") % argv[1] % throughput % MB % s << endl;
    }
    return 0;
}
