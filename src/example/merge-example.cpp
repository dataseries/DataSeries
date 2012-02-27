// -*-C++-*-
/*
   (c) Copyright 2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

// Note this is intended to be a simple example of how to merge
// multiple extents, it does not handle multiple data types.
// Nor does it let you map multiple things onto a single output
// name, which would make sense in the fake example setup below 
// where both the sizes should map onto the same output column.

#include <inttypes.h>

#include <boost/format.hpp>

#include <Lintel/HashMap.hpp>
#include <Lintel/MersenneTwisterRandom.hpp>
#include <Lintel/StatsQuantile.hpp>
#include <Lintel/StringUtil.hpp>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/DStoTextModule.hpp>
#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SequenceModule.hpp>

using namespace std;
using namespace boost;

/// Sample source class, generates data ab-initio; one column "time"
/// is generated from start time with each row separated by a uniform
/// random draw.  one column "rownum" is just incremented from
/// start_row to max_rowid-1 n columns are generated as random values
/// (col-#)

class RandomSource : public DataSeriesModule {
public:
    RandomSource(const string &type_name, uint32_t _max_rowid, uint32_t ncolumns,
		 uint32_t start_row, double start_time)
	: max_rowid(_max_rowid), cur_row(start_row), cur_time(start_time),
	  // Prepare our statically known fields (columns) in the constructor.
	  time(series, "time"), rownum(series, "rownum")
    { 
	vector<string> spec;

	// Since we don't know the extent name, or number of columns
	// we have to dynamically generate the output specification.

	spec.push_back((format("<ExtentType name=\"%s\">") % type_name).str());
	// Fake up all the columns as doubles even though rownum should
	// be int32/64 to make writing the merger easier.
	spec.push_back("  <field type=\"double\" name=\"time\" />");
	spec.push_back("  <field type=\"double\" name=\"rownum\" />");
	for(uint32_t i=0; i<ncolumns; ++i) {
	    spec.push_back((format("  <field type=\"double\" name=\"col-%d\" />") % i).str());
	    // Prepare the dynamically known fields at runtime.
	    columns.push_back(new DoubleField(series, (format("col-%d") % i).str()));
	}
	spec.push_back("</ExtentType>");

	// Now we can make the type and initialize the series.
	string type = join("",spec);
	output_type = &ExtentTypeLibrary::sharedExtentType(type);
	series.setType(*output_type);
    }

    virtual Extent::Ptr getSharedExtent() {
	if (cur_row == max_rowid) {
	    return Extent::Ptr(); // No more extents
	}

	INVARIANT(cur_row < max_rowid, "bad");

	// Pick a random number of rows to put into an extent; unless
	// the constants in main() are changed, this doesn't actually
	// matter.
	uint32_t extent_rows = 1000 + MTRandom.randInt(1000);
	if (cur_row + extent_rows > max_rowid) {
	    extent_rows = max_rowid - cur_row;
	}

	// Set the series' type.
	series.setType(output_type);
	// Make a new extent
        series.newExtent();

	// Generate all of the rows.
	for(uint32_t i = 0; i < extent_rows; ++i) {
	    series.newRecord();  // new row.

	    time.set(cur_time); // fixed column 1
	    rownum.set(cur_row); // fixed column 2
	    for(uint32_t j = 0; j < columns.size(); ++j) { // variable columns
		// multiply by 100 to get a bigger range
		columns[j]->set(100.0*MTRandom.randDoubleOpen()); 
	    }
	    // increment dependent things
	    cur_time += MTRandom.randDoubleOpen(); 
	    ++cur_row; 
	}
	return series.getSharedExtent();
    };
private:
    ExtentSeries series;

    uint32_t max_rowid, cur_row;
    double cur_time;
    DoubleField time;
    DoubleField rownum;
    vector<DoubleField *> columns;
    const ExtentType *output_type;
};

/// This class does the merge of multiple input extents mapping the
/// columns in the input extent into various columns in the output
/// extents and potentially pruning out the columns that are not
/// needed.  It only handles double fields right now.  This could be
/// fixed by the use of GeneralField/GeneralValue, or by handling the
/// different fields separately, e.g. having a vector for each of the
/// field types.  The latter is probably faster but more complicated.
/// Nor does it support merging output fields.  This could be
/// implemented just by excluding rows from the type specification if
/// they duplicate an earlier name after checking the types are the
/// same.

class Merger : public DataSeriesModule {
public:
    // No known upstream modules at creation time.
    Merger(const string &_type_name, const string &time_name)
	: type_name(_type_name), output_type(NULL), 
	  // Two fixed fields, one with a variable name.
	  orig_src(output_series, "orig_src"), time(output_series, time_name)
    { }

    // How do we map between source fields (columns) and destination ones.
    struct Mapping {
	Mapping(const string &src) : source_name(src), dest_name(src) {}
	Mapping(const string &src, const string &dest) 
	    : source_name(src), dest_name(dest) {}
	
	string source_name;
	string dest_name; 
    };

    // The complete description of an individual source
    struct Source {
	typedef vector<Source *>::iterator iter;
	DataSeriesModule &src;
	ExtentSeries series; // the series for this source.
	bool done; // Avoid repeatedly calling getExtent() on sources that are done.
	DoubleField time_field; // Fixed fields.
	vector<DoubleField *> srcs; // use internal series
	vector<DoubleField *> dests; // use output series
	Source(DataSeriesModule &_src, const string &time_field_name) 
	    : src(_src), done(false), time_field(series, time_field_name) { }
    };

    // This function specifies an input module for the merger, the
    // time field it should use for ordering, and the mapping of the
    // names.
    void addSource(DataSeriesModule &srcmod, const string &time_field, vector<Mapping> &mapping) {
	INVARIANT(output_type == NULL, "no addSource after first getExtent");
	
	Source *src = new Source(srcmod, time_field);
	for(vector<Mapping>::iterator i = mapping.begin(); 
	    i != mapping.end(); ++i) {
	    // All these fields are variable, so have to be constructed dynamically.
	    src->srcs.push_back(new DoubleField(src->series, i->source_name, Field::flag_nullable));
	    src->dests.push_back(new DoubleField(output_series, i->dest_name, Field::flag_nullable));
	}
	sources.push_back(src);
    }

    virtual Extent *getExtent() {
	if (output_type == NULL) {
	    vector<string> spec;
	    // As with the RandomSource, we don't know our spec so we have to construct it.
	    spec.push_back((format("<ExtentType name=\"%s\">\n") % type_name).str());
	    // Two fixed fields.
	    spec.push_back("  <field type=\"int32\" name=\"orig_src\" />\n");
	    spec.push_back((format("  <field type=\"double\" name=\"%s\" />\n") % time.getName()).str());
	    // All the variable fields from the various sources.
	    for(Source::iter i = sources.begin();
		i != sources.end(); ++i) {
		for(vector<DoubleField *>::iterator j = (**i).dests.begin();
		    j != (**i).dests.end(); ++j) {
		    spec.push_back((format("  <field type=\"double\" name=\"%s\" opt_nullable=\"yes\" />\n") 
				    % (**j).getName()).str());
		}
	    }
	    spec.push_back("</ExtentType>\n");
	    output_type = &ExtentTypeLibrary::sharedExtentType(join("",spec));
	    // For debugging, dump out the final spec.
	    cout << join("", spec);
	}
	
	// Make output extent and prepare to use it.
	Extent *out_extent = new Extent(*output_type);
       	output_series.setExtent(out_extent);

	// In practice a way to small number (10), but it forces
	// multiple extents with the constants used in main.
	const uint32_t max_rows_per_extent = 10;
	for(uint32_t rownum = 0; rownum < max_rows_per_extent; ++rownum) {
	    Source::iter least = sources.end();
	    
	    // Search for the source with the least input, more efficient if
	    // it used a priority queue.
	    for(Source::iter i = sources.begin(); i != sources.end(); ++i) {
		if ((**i).series.morerecords() == false) {
		    if ((**i).done) {
			continue; // definitely nothing in this source
		    }
		    // Might have more
		    Extent *e = (**i).src.getExtent();
		    if (e == NULL) {
			(**i).done = true; // nope, don't bother to ask again.
			continue;
		    }
		    (**i).series.setExtent(e);
		}
		if (least == sources.end()) {
		    least = i; // first source with a value, take it.
		} else if ((**i).time_field.val() < (**least).time_field.val()) {
		    least = i; // better than the previous least source, take it.
		}
	    }
	    if (least == sources.end()) { // didn't find any source
		if (rownum == 0) { // didn't make this row.
		    delete out_extent;
		    return NULL; // We're done.
		} else {
		    return out_extent; // We got a partial extent.
		}
	    }

	    output_series.newRecord(); // Now we know we have something so we can make a row.
	    // probably could move this up above and do it in the same
	    // loop as the search for least.

	    // Default all outputs to null
	    for(Source::iter i = sources.begin(); i != sources.end(); ++i) {
		for(vector<DoubleField *>::iterator j = (**i).dests.begin();
		    j != (**i).dests.end(); ++j) {
		    (**j).setNull();
		}
	    }
	    
	    // Set the fixed fields.
	    orig_src.set(least - sources.begin());
	    time.set((**least).time_field.val());
	    // Set the variable fields.
	    for(uint32_t j = 0; j < (**least).dests.size(); ++j) {
		if ((**least).srcs[j]->isNull()) {
		    (**least).dests[j]->setNull();
		} else {
		    (**least).dests[j]->set((**least).srcs[j]->val());
		}
	    }
	    // Advance to the next row in the least.
	    ++((**least).series);
	}
	// Give full extent to caller.
	return out_extent;
    }
private:
    ExtentSeries output_series;
    const string type_name;
    const ExtentType *output_type;
    vector<Source *> sources;
    Int32Field orig_src;
    DoubleField time;
};

// Not in Lintel, not clear if building a hashmap over doubles is even
// a good idea.
template <>
struct HashMap_hash<const double> {
    unsigned operator()(const double _a) const {
	BOOST_STATIC_ASSERT(sizeof(double) == sizeof(uint64_t));
	union {
	    uint64_t i;
	    double d;
	} v;
	v.d = _a;
	return lintel::BobJenkinsHashMixULL(v.i);
    }
};

/// This is a simple analysis, we are going to do self join in the
/// series, find matching open and close records and calculate the
/// latency between opens and closes, as well as the change in the
/// data and resource fork sizes between opens and closes.  The module
/// assumes that the input and output sources are numbered 0 and 1.
/// This could be changed to be an argument at the cost of changing a
/// switch to an if statement.  If we made that change, then we could
/// stack two analysis one that treated the inputs as (open, close) and
/// the other that treated the inputs as (close, open)
class OpenCloseAnalysis : public RowAnalysisModule {
public:
    OpenCloseAnalysis(DataSeriesModule &src)
	: RowAnalysisModule(src), 
	  // All our fields are static.
	  data_src(series, "orig_src"),
	  time(series, "time"),
	  open_id(series, "open_file_id", Field::flag_nullable),
	  close_id(series, "close_file_id", Field::flag_nullable),
	  open_datasize(series, "open_data_fork_KiB", Field::flag_nullable),
	  open_resourcesize(series, "open_resource_fork_KiB", Field::flag_nullable),
	  close_datasize(series, "close_data_fork_KiB", Field::flag_nullable),
	  close_resourcesize(series, "close_resource_fork_KiB", Field::flag_nullable),
	  opens_without_closes(0),
	  closes_without_opens(0),
	  rowcount(0)
    { }

    // We just want to handle all the rows separately, so we can use
    // the row analysis module.
    virtual void processRow() {
	++rowcount; 
	switch(data_src.val()) {
	case 0: // open
	    { 
		INVARIANT(!fileid_to_info.exists(open_id.val()), "already opened file??");
			  
		// Save the information about the open.
		fileid_to_info[open_id.val()] = finfo(time.val(), open_datasize.val(), close_datasize.val());
	    }
	    break;
	case 1: // close
	    {
		if (fileid_to_info.exists(close_id.val())) {
		    // Found the previous open, update all the statistics.
		    finfo &f = fileid_to_info[close_id.val()];
		    open_close_latency.add(time.val() - f.opentime);
		    datasize_change.add(close_datasize.val() - f.datasize);
		    resourcesize_change.add(close_resourcesize.val() - f.resourcesize);
		    fileid_to_info.remove(close_id.val());
		} else {
		    // Hmm, no open?
		    ++closes_without_opens;
		}
	    }
	    break;
	default:
	    FATAL_ERROR("??");
	}
    }

    virtual void completeProcessing() {
	// Everything here we never saw a close for.
	opens_without_closes = fileid_to_info.size();
    }

    virtual void printResult() {
	cout << "BEGIN Results for " << __PRETTY_FUNCTION__ << "\n";
	// My integer counting bits...
	cout << boost::format("processed %d rows\n") % rowcount;
	cout << boost::format("opens without closes: %d\n") % opens_without_closes;
	cout << boost::format("closes without opens: %d\n") % closes_without_opens;
	// My complicated statistics.
	cout << "open to close latency:\n";
	open_close_latency.printRome(2, cout);
	cout << "datasize change:\n";
	datasize_change.printRome(2, cout);
	cout << "resourcesize change:\n";
	resourcesize_change.printRome(2, cout);
	cout << "END Results for " << __PRETTY_FUNCTION__ << "\n";
    }

private:
    Int32Field data_src;
    DoubleField time, open_id, close_id;
    DoubleField open_datasize, open_resourcesize, close_datasize, close_resourcesize;

    struct finfo {
	double opentime, datasize, resourcesize;
	finfo(double a, double b, double c) 
	    : opentime(a), datasize(b), resourcesize(c) { }
	finfo() {} // needed by HashMap for some reason.
    };

    HashMap<double, finfo> fileid_to_info;

    // Example of generating different types of statistics.
    StatsQuantile open_close_latency;
    Stats datasize_change;
    Stats resourcesize_change;
    
    uint32_t opens_without_closes, closes_without_opens, rowcount;
};

int 
main()
{
    MTRandom.init(1972); // Fixed so everyone gets the same output.

    // We are going to fake this up as if we have a source or open
    // events and a source of close events.  We are pretending to have
    // traced on a macintosh and we happened to get the data and
    // resource fork sizes at both open and close.

    // Offset the ids and the times to give us a bit of control over
    // what we will see in the analysis.
    RandomSource a("src-a", 20, 3, 5, 0.0); // open's ids 5..20
    RandomSource b("src-b", 15, 2, 0, 10.0); // closes ids 0..15

    // Construct the merger
    Merger *m = new Merger("merged", "time");

    vector<Merger::Mapping> mapping;

    mapping.push_back(Merger::Mapping("rownum","open_file_id"));
    mapping.push_back(Merger::Mapping("col-0","open_data_fork_KiB"));
    // Note that we dropped a column from RandomSource a; we don't need it.
    mapping.push_back(Merger::Mapping("col-2","open_resource_fork_KiB"));
    
    m->addSource(a, "time", mapping);

    // Now build the mapping for the second source...
    mapping.clear();

    mapping.push_back(Merger::Mapping("rownum","close_file_id"));
    mapping.push_back(Merger::Mapping("col-0","close_data_fork_KiB"));
    mapping.push_back(Merger::Mapping("col-1","close_resource_fork_KiB"));

    m->addSource(b, "time", mapping);

    SequenceModule seq(m);
    // It's nice to see this series since it's random, but if you
    // comment out the next line it will be suppressed.  Normally
    // the list of modules in the sequence would be controlled by
    // command line arguments.
    seq.addModule(new DStoTextModule(seq.tail()));
    seq.addModule(new OpenCloseAnalysis(seq.tail()));

    seq.getAndDelete();
    
    RowAnalysisModule::printAllResults(seq);

    return 0;
}
