// -*-C++-*-
/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Data Series Modules for analyzing one row at a time
*/

#ifndef __ROW_ANALYSIS_MODULE_H
#define __ROW_ANALYSIS_MODULE_H

#include <DataSeries/DataSeriesModule.H>
#include <DataSeries/DSExpr.hpp>

class SequenceModule;

/** \brief Single series analysis handling each row in order.  

  * Based on the experience of building a lot of analysis modules, it
  * has become clear that one of the common usage models is to have an
  * analysis that operates over a single source, performs some
  * calculation over each row in the series, and at the very end a
  * function is called to print out the result of each of the
  * analysis.  This class moves the common operations into a parent
  * class, so that we can also add things like automatic select over
  * top of the analysis to further prune out values.  */

class RowAnalysisModule : public DataSeriesModule {
public:
    RowAnalysisModule(DataSeriesModule &source,
		      ExtentSeries::typeCompatibilityT type_compatibility = ExtentSeries::typeXMLIdentical);
    virtual ~RowAnalysisModule();
    
    virtual Extent *getExtent();

    /** this function will be called once after the first extent has been
	retrieved, but before processRow() has been called; it will not 
	be called if there were no extents to process */
    virtual void prepareForProcessing();

    /** this function will get called to process each row. */
    virtual void processRow() = 0;

    /** this function will get called once all data has been processed */
    virtual void completeProcessing();
    
    /** print your result to stdout. Default function prints nothing */
    virtual void printResult();

    /** \brief processRow is only called if where evaluates to true
     *
     * Sets an expression that controls whether or not the processRow
     * function will be called.  For each row the expression is
     * evaluated, and if it evaluates to true, then processRow will be
     * called. An empty expression is treated as true for all values.
     *
     * @param where the expression to evaluate
     */
    void setWhereExpr(const std::string &where_expression);

    /** \brief iterate across the sequence printing results if possible.
     * Tries to dynamically case each module in the sequence to a
     * RowAnalysisModule.  If it does not succeed, it ignores the
     * module.

     * @param expected_nonprintable if >= 0 aborts if # non-printable doesn't match expectation
     * @return the number of modules that could not be printed */
    static int printAllResults(SequenceModule &sequence,
			       int expected_nonprintable = -1);

    uint64_t processed_rows, ignored_rows;

protected:
    ExtentSeries series;
    DataSeriesModule &source;

private:
    bool prepared;
    std::string where_expr_str;
    DSExpr *where_expr;
};

#endif
