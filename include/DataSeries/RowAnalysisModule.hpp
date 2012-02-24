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

#include <DataSeries/DataSeriesModule.hpp>

class DSExpr;
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
    RowAnalysisModule(DataSeriesModule &source, ExtentSeries::typeCompatibilityT type_compatibility
                                                   = ExtentSeries::typeExact);
    virtual ~RowAnalysisModule();
    
    virtual Extent::Ptr getSharedExtent();

    // TODO: think about a firstExtentHook; primary (only?) use of
    // newExtentHook so far has been to handle the case of different
    // field names, and for that, you only need to hook on the first
    // Extent.

    // TODO: probably should redo the next two hooks; in practice, we
    // need 1 right at the beginning before any extents have been set,
    // but with the extent, a second right after that first extent is
    // set, and a third one that gets called after the extent is
    // set on every row.

    /** Called right after each extent is retrieved, but before the 
	extent is set in the series.  Serves two purposes: 1) infrequent
	statistics/processing; and 2) setting up fields with unknown 
	field names */
    virtual void newExtentHook(const Extent &e);


    /** This function will be called once, with the first extent that will be
	processed by the module.  The series will not be set when this function
	is called so that field names can be set.  This function is called
	before the newExtentHook is called on the same extent. */
    virtual void firstExtent(const Extent &e);

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
     * @param where_expression the expression to evaluate
     */
    void setWhereExpr(const std::string &where_expression);

    /** \brief iterate across the sequence printing results if possible.
     * Tries to dynamically case each module in the sequence to a
     * RowAnalysisModule.  If it does not succeed, it ignores the
     * module.

     * @param sequence What sequence should we find module in?
     * @param expected_nonprintable if >= 0 aborts if # non-printable doesn't match expectation
     * @return the number of modules that could not be printed */
    static int printAllResults(SequenceModule &sequence, int expected_nonprintable = -1);

    uint64_t processed_rows, ignored_rows;

protected:
    ExtentSeries series;
    DataSeriesModule &source;
    bool prepared;

    std::string where_expr_str;
    DSExpr *where_expr;
};

#endif
