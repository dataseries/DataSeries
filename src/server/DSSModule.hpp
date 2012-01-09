// Common includes for server modules, rather than duplicating everywhere; not all of them need
// all this, but it's close

#include <boost/foreach.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <Lintel/LintelLog.hpp>

#include <DataSeries/GeneralField.hpp>
#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SequenceModule.hpp>

#include "GVVec.hpp"
#include "RenameCopier.hpp"
#include "ServerModules.hpp"
#include "ThrowError.hpp"

using namespace std;
using namespace dataseries;
using boost::format;
using boost::scoped_ptr;
using boost::shared_ptr;
