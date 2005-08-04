/* -*-C++-*-
*******************************************************************************
*
* File:         RowAnalysisModule.H
* RCS:          $Header: /mount/cello/cvs/DataSeries/include/DataSeries/RowAnalysisModule.H,v 1.2 2005/02/15 01:18:34 anderse Exp $
* Description:  Data Series Modules for analyzing one row at a time
* Author:       Eric Anderson
* Created:      Mon Apr 26 16:22:42 2004
* Modified:     Sat Oct  2 18:47:47 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2004, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#ifndef __ROW_ANALYSIS_MODULE_H
#define __ROW_ANALYSIS_MODULE_H

#include <DataSeriesModule.H>

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

    /** this function will get called to process each row. */
    virtual void processRow() = 0;

    /** this function will get called once all data has been processed */
    virtual void completeProcessing();
    
    /** print your result to stdout. Default function prints nothing */
    virtual void printResult();

    /** \brief iterate across the sequence printing results if possible.
      * Tries to dynamically case each module in the sequence to a
      * RowAnalysisModule.  If it does not succeed, it ignores the
      * module.

      * @param expected_nonprintable if >= 0 aborts if # non-printable doesn't match expectation
      * @return the number of modules that could not be printed */
    static int printAllResults(SequenceModule &sequence,
			       int expected_nonprintable = -1);
protected:
    ExtentSeries series;
    DataSeriesModule &source;
};

#endif
