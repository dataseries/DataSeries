// -*-C++-*-
/*
   (c) Copyright 2008 Harvey Mudd College

   See the file named COPYING for license details
*/

#include <cmath>

#include <DataSeries/DataSeriesModule.hpp>
#include <DataSeries/ExtentField.hpp>
#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

/*  The core class that we will be using here is RowAnalysisModule,
    which provides iteration over a group of Extents.
    For this example, we will find the mean and standard deviation of
    the time taken by some operations. */
class ElapsedTimeStats : public RowAnalysisModule {
public:
    /*  Our constructor takes a DataSeriesModule from which to get
        Extents and passes it on to the base class constructor.
        Then, we use the inherited ExtentSeries called series to
        initialize the Fields.  An ExtentSeries is an iterator
        over the records of Extents.  Fields are connected to
        a particular ExtentSeries and provide access to the
        values of each record as the ExtentSeries points to it. */
    ElapsedTimeStats(DataSeriesModule& source)
        : RowAnalysisModule(source),
          time_begin(series, "time_begin"),
          time_end(series, "time_end") {}
    /*  There are a couple of functions that we need to override.
        The first is processRow.  This function will be called
        by RowAnalysisModule once for every row in the Extents
        being processed. */
    virtual void processRow() {
        ExtentType::int64 elapsed = time_end.val() - time_begin.val();
        sum += elapsed;
        sum_of_squares += elapsed * elapsed;
        ++count;
    }
    /*  The second function to override is printResult.  This function will be
        called at the end of the processing.  Even though we are calling it
        directly from main, it is a good idea to implement  so
        that some more complex things will work correctly.  See
        running_multiple_modules.cpp */
    virtual void printResult() {
        std::cout << "Average time per operation: " << sum / count << std::endl;
        double std_dev =
            std::sqrt((sum_of_squares - sum*(sum / count)) / (count - 1));
        std::cout << "std. dev. of operation time: " << std_dev << std::endl;
    }
private:
    Int64Field time_begin;
    Int64Field time_end;
    double sum;
    double sum_of_squares;
    long count;
};

/* Now, we're ready to actually run the analysis. */

int main(int argc, char *argv[]) {

    /*  The first thing to do is to specify which
        Extents are going to processed.  The TypeIndexModule
        class reads all the Extents of a single type
        from a group of files.  We construct one
        that processes Extents of the type "MyExtent"
        and load it up with the files passed on the
        command line. */

    TypeIndexModule source("MyExtent");
    for(int i = 1; i < argc; ++i) {
        source.addSource(argv[i]);
    }

    /*  We can now create our module, run it and
        output the result. */

    ElapsedTimeStats processor(source);
    
    processor.getAndDelete();
    processor.printResult();
}
