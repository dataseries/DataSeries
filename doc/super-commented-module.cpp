/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/


// This example is from the nfsdsanalysis code, and is
// hyper-documented.  A few unneeded bits were removed for simplicity.

// documentation is for the line following each chunk of documentation

// declare a new class (FileSizeByType), make it a subclass of
// NFSDSModule which means that it can fit into the general structure
// imposed by nfsdsanalysis.C.  The public means that functions defined 
// in NFSDSModule can be used by code external to FileSizeByType.
class FileSizeByType : public NFSDSModule { 
    // declare the functions that can be used by code external to FileSizeByType
public:
    // declare the way that you initialize a FileSizeByType class; it
    // takes one argument which is the source of the data it will process.
    FileSizeByType(DataSeriesModule &_source)
	// save the source as a class variable; create the extent
	// series (s) with a rule that says all extents we process
	// have to have the same XML type.  This line is boilerplate
	// in almost all of the modules.
	: source(_source), s(ExtentSeries::typeXMLIdentical),
	  // declare three variables that will store the values found
	  // in a particular row of the input series.  They need to
	  // know what data series they are part of (s), and what is
	  // the column name they should be extracting ("type",
	  // "file-size", or "filehandle") The fact that the variable
	  // names are similar to the column names is just a help to
	  // people.
	  type(s,"type"),filesize(s,"file-size"),filehandle(s,"filehandle")
    { 
    }
    // declare what should happen when a FileSizeByType instance is
    // deleted (freed), in particular, we have nothing to do.
    virtual ~FileSizeByType() { }

    // declare a hash table element data structure that will store the
    // type and the statistics about that type
    struct hteData {
	string type;
	// declare how we initialize this structure, setting type to a and file_size to NULL
	hteData(const string &a) : type(a),file_size(NULL) { }
	Stats *file_size;
    };

    // declare a hash table element hash function, it converts an
    // hteData into an unsigned integer for use in a Hash Table.
    class hteHash {
    public:
	// define the actual function which hashes a hteData structure
	// to get it's hash value.  In this case, very simple since
	// there is only a single variable that is used as the key of
	// the hash table.
	unsigned int operator()(const hteData &k) {
	    return HashTable_hashbytes(k.type.data(),k.type.size());
	}
    };

    // declare a hash table element comparison (equality) function, it
    // compares two hteData elements to determine if they are the
    // same.
    class hteEqual {
    public:
	// since we have only one part to our key, all we care is that it matches.
	bool operator()(const hteData &a, const hteData &b) {
	    return a.type == b.type;
	}
    };

    // declare the hash table that will store our statistics, it will
    // store data of the form hteData, hash hteData's with hteHash and
    // compare them with hteEqual.
    HashTable<hteData, hteHash, hteEqual> stats_table;

    // declare the function that will process each of the extents.
    // The functions form a pipeline from the source to the sink
    // wherein each instance gets it's data from the previous instance
    // in the pipeline, and returns it so that the next one can
    // process the data.
    virtual Extent *getExtent() {
	// get the extent for processing from our predecessor
	Extent *e = source.getExtent();
	// if it is null, we are done, so just return
	if (e == NULL) 
	    return NULL;
	// if we find an extent we didn't expect, skip it.
	if (e->type->name != "NFS trace: attr-ops")
	    return e;
	// loop over all of the rows in the extent.  In particular:
	// 1) set the series to be processing on this extent (s.setExtent(e));
        // 2) continue only if we have more records (s.pos.morerecords())
	// 3) increment our current position in the extent to get to the next row (++s.pos)
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    // construct an hteData with the appropriate type (hteData(type.stringval()))
	    // look it up in the hash table (stats_table.lookup(...))
	    // get the resulting entry (if any)
	    hteData *d = stats_table.lookup(hteData(type.stringval()));
	    // check if the type is already in the hash table.
	    if (d == NULL) {
		// construct a new hteData structure that we can put into the hash table
		hteData newd(type.stringval());
		// construct an instance for storing statistics associated with this data type
		newd.file_size = new Stats;
		// add the new hteData to the hash table, which returns a pointer to the copy 
		// that was actually placed into the hash table.
		d = stats_table.add(newd);
	    }
	    // add the filesize for this row (filesize.val()) to the cumulative statistics
	    // for this type (d->file_size->add(...))
	    d->file_size->add(filesize.val());
	}
	// return the extent so that the next module in the pipeline can process it.
	return e;
    }

    // define a class for comparing two hteData's to determine which one will be sorted earlier
    class sortByType {
    public:
	bool operator()(hteData *a, hteData *b) {
	    // compare based on the type of the thing, causing us to sort alphabetically
	    return a->type < b->type;
	}
    };

    // define the function that will print out our results; all of
    // these will be called by the main program at the end
    virtual void printResult() {
	// print out a header to make it easier to identify this set of results
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	// create a vector (resizeable array) of hteData entries
	vector<hteData *> vals;
	// iterate over every entry in the hash table; in particular:
	// 1) start at the beginning of the hash table (HashTable<...>::iterator i = stats_table.begin())
	// 2) continue unless we are at the end (i != stats_table.end())
	// 3) increment the current position after each iteration (++i)
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    // get the hteData structure associated with the current position (*i)
	    // get the address of that structure &(*i)
	    // add it to the end of the vector (vals.push_back(...));
	    vals.push_back(&(*i));
	}
	// sort the list of values from the beginning (vals.begin()) to the end (vals.end())
	// by the order defined in sortByType (alphabetically)
	sort(vals.begin(),vals.end(),sortByType());
	// iterate over the list of entries in the vector; same structure as iterating over
	// the hash table, but with a different iterator definition.
	for(vector<hteData *>::iterator i = vals.begin();
	    i != vals.end();++i) {
	    // extract out the value store in the current iterator position, alternately 
	    // we could have written (**i).<whatever> instead of j->
	    hteData *j = *i;
	    // print out the stuff that we want to print
	    printf("%10s %ld ents, %.2f MB total size, %.2f kB avg size, %.0f max bytes\n",
		   // convert the string type representation to a char *
		   j->type.c_str(),
		   // get the count from our accumulated statistics
		   j->file_size->count(),
		   // and the total, converted to MB
		   j->file_size->total() / (1024*1024.0),
		   // and so on
		   j->file_size->mean() / (1024.0),
		   j->file_size->max());
	}
	// print out a trailer to make it easier to identify this part of the data
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    // define the variables that are part of this class
    // our source module (& is a non-null pointer)
    DataSeriesModule &source;
    // our series of rows
    ExtentSeries s;
    // an accessor of the type stored in the row
    Variable32Field type;
    // the filesize
    Int64Field filesize;
    // and the filehandle
    Variable32Field filehandle;
};
