#include <DataSeries/SubExtentPointer.hpp>

namespace dataseries {
    bool checkSubExtentPointerSizes(size_t sep_rowoffset) {
        return sep_rowoffset == sizeof(dataseries::SEP_RowOffset);
    }
}


