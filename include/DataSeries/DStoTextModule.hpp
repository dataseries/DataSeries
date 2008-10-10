// -*-C++-*-
/*
   (c) Copyright 2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Module that converts a data series to text
*/

#ifndef __DSTOTEXTMODULE_H
#define __DSTOTEXTMODULE_H

#include <DataSeries/DataSeriesModule.hpp>

class DSExpr;
class GeneralField;

class DStoTextModule : public DataSeriesModule {
public:
    DStoTextModule(DataSeriesModule &source, FILE *text_dest = stdout);
    // measurements indicate that printing to ostreams is slightly
    // slower than stdio on linux/gcc2, and substantially slower (~4x)
    // on gcc3.  Therefore, this version is provided as an option to
    // allow printing to string buffers, but is not the default.
    DStoTextModule(DataSeriesModule &source, std::ostream &text_dest);
    virtual ~DStoTextModule();
    
    virtual Extent *getExtent(); // will print extent as a side effect.

    void setPrintSpec(const char *xmlText);

    /// After a call to this, the module owns the printSpec and will free it
    /// when done.
    void setHeader(const char *xmlText);
    void setFields(const char *xmlText);
    void addPrintField(const std::string &extenttype, 
		       const std::string &field);
    void setWhereExpr(const std::string &extenttype,
		      const std::string &where_expr_str);

    void skipIndex() {
	print_index = false;
    }

    bool printIndex() { 
	return print_index;
    }
  
    void skipExtentType() {
	print_extent_type = false;
    }

    void skipExtentFieldnames() {
	print_extent_fieldnames = false;
    }

    void enableCSV();
    void setSeparator(const std::string &s);    
    
    void setHeaderOnlyOnce();

    // need to keep around state because relative printing should be
    // done relative to the first row of the first extent, not the
    // first row of each extent.
    struct PerTypeState {
	PerTypeState();
	~PerTypeState();

	ExtentSeries series;
	std::map<std::string, xmlNodePtr> override_print_specs, print_specs;
	std::string header;
	std::vector<std::string> field_names;
	std::vector<GeneralField *> fields;
	std::string where_expr_str;
	DSExpr *where_expr;
    };

    uint64_t processed_rows, ignored_rows;

private:
    static xmlNodePtr parseXML(std::string xml, const std::string &roottype);

    void setPrintSpec(const std::string &extenttype,
		      const std::string &fieldname,
		      xmlNodePtr printSpec);
    void setHeader(const std::string &extenttype,
		   const std::string &header);

    void getExtentPrintSpecs(PerTypeState &state);

    // Also initializes state.fields if necessary.
    void getExtentPrintHeaders(PerTypeState &state);

    // Intiailizes state.where_expr if necessary.
    void getExtentParseWhereExpr(PerTypeState &state);
			       
    DataSeriesModule &source;
    std::ostream *stream_text_dest;
    FILE *text_dest;

    std::map<std::string, PerTypeState> type_to_state;
    std::vector<std::string> default_fields;

    bool print_index, print_extent_type, print_extent_fieldnames;
    bool csvEnabled;
    std::string separator; 
    bool header_only_once;
    bool header_printed;
};

#endif
