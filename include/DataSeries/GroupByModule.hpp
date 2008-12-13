// -*-C++-*-
/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Dataseries module for handling group-by type rollups
*/

#ifndef __GROUP_BY_MODULE_H
#define __GROUP_BY_MODULE_H

#include <Lintel/HashTable.hpp>
#include <DataSeries/RowAnalysisModule.hpp>

// TODO: start using this or drop it.

/** \brief Single series analysis handling different rows in different groups

  * One of the very common operations in an analysis is to group the
  * rows by the different values in one or more of the columns.  Then
  * an independent rollup is calculated across each of the different
  * groups.  The output for this class first lists the values in the
  * group, and then calls the individual rollup print operation to
  * print out the results for that row. 

  * TODO: perhaps we should generate output as a dataseries and then
  * run it through DStoTextModule to make printable output */

class GroupByModule : public RowAnalysisModule {
public:
    /** this is the virtual class that the user will define to perform
      * the analysis over each of the separate groups */
    class Analysis {
    public:
	Analysis(ExtentSeries &_s) : s(_s) { }
	virtual void doGroupRow() = 0;
	virtual void printResults() = 0;
    protected:
	ExtentSeries &s;
    };

    /** this is the virtual class that the user will provide to return
      * the analysis classes that they want. */
    class Factory {
    public:
	virtual Analysis *operator(ExtentSeries &s) = 0;
    };

    GroupByModule(DataSeriesModule &source,
		  Factory *f,
		  ExtentSeries::typeCompatibilityT type_compatibility = ExtentSeries::typeXMLIdentical);

    virtual void processRow();

    virtual void printResult();
private: 
    struct hteData {
	vector<GeneralValue> keys;
	Analysis *v;
    };
    struct hteHash {
    public:
	unsigned int operator()(const hteData &k) const {
	    unsigned int hash = 1972;
	    for(vector<GeneralValue>::iterator i = k.keys.begin();
		i != k.keys.end(); ++i) {
		hash = i->incrementalHash(hash);
	    }
	    return hash;
	}
    };

    struct hteEqual {
    public:
	unsigned int operator()(const hteData &a, const hteData &b) const {
	    if (a.size() != b.size())
		return false;
	    for(unsigned i = 0; i < a.size(); ++i) {
		if (a[i] != b[i])
		    return false;
	    }
	    return true;
	}
    };
    HashTable<hteData, hteHash, hteEqual> rollup;
};

#endif

