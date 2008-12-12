/**

@mainpage

@section Introduction

DataSeries is a trace file format and manipulation
library.  It is intended for storing structured serial data, so it is
similar to a append only SQL database in that it
stores data organized into the equivalent of tables.  It is different
in that the rows of the tables have an order that is preserved.  It is
also different in that DataSeries does not at this point have a query
language interface.  To access DataSeries data, the programmer will
need to write a module to process the data, and compile and link that
module in to a program.  Dataseries also explicitly exposes the
underlying storage of the data in files, although this can be
partially hidden through the use of DataSeries indices.

@section LibraryTutorial Library Tutorial

In this section, we present several small examples focused
at the common uses of library.

- @link writing_a_file.cpp Writing a file @endlink
- @link processing.cpp Processing files one record at a time @endlink
- @link running_multiple_modules.cpp Running multiple analyses in a single pass @endlink

*/

/**
@example writing_a_file.cpp
@brief This example demonstrates how to create a minimal DataSeries file from scratch
@example processing.cpp
@brief Demonstrates processing files one record at a time.
@example running_multiple_modules.cpp
@brief Demonstrates how to run multiple analyses in one pass over the input data.
*/
