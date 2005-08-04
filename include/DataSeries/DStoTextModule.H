/* -*-C++-*-
*******************************************************************************
*
* File:         DStoTextModule.H
* RCS:          $Header: /mount/cello/cvs/DataSeries/include/DataSeries/DStoTextModule.H,v 1.4 2004/09/07 21:29:29 anderse Exp $
* Description:  Module that converts a data series to text
* Author:       Eric Anderson
* Created:      Fri Aug  8 20:36:11 2003
* Modified:     Sun Jul 11 01:22:47 2004 (Eric Anderson) anderse@hpl.hp.com
* Language:     C++
* Package:      N/A
* Status:       Experimental (Do Not Distribute)
*
* (C) Copyright 2003, Hewlett-Packard Laboratories, all rights reserved.
*
*******************************************************************************
*/

#ifndef __DSTOTEXTMODULE_H
#define __DSTOTEXTMODULE_H

#include <DataSeriesModule.H>
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
    void setPrintSpec(const std::string &extenttype,
		      const std::string &fieldname,
		      xmlNodePtr printSpec) {
	overridePrintSpecs[extenttype][fieldname] = printSpec;
    }
    void setHeader(const char *xmlText);
    void setHeader(const std::string &extenttype,
		   const std::string &header) {
	headers[extenttype] = header;
    }
    void setFields(const char *xmlText);
    void addPrintField(const std::string &extenttype, 
		       const std::string &field) {
	fieldLists[extenttype].push_back(field);
    }
    
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

    // this interface memory leaks the document;
    static xmlNodePtr parseXML(std::string xml, const std::string &roottype);
private:
    void getExtentPrintSpecs(std::map<std::string, xmlNodePtr> &printspecs,
			       ExtentSeries &es);

    void getExtentPrintHeaders(std::map<std::string, xmlNodePtr> &printspecs,
			       ExtentSeries &es, std::vector<GeneralField *> &fields);
			       
    DataSeriesModule &source;
    std::ostream *stream_text_dest;
    FILE *text_dest;

    std::map<std::string, std::map<std::string, xmlNodePtr> > overridePrintSpecs;
    std::map<std::string, std::string> headers;
    std::map<std::string, std::vector<std::string> > fieldLists;
    bool print_index, print_extent_type, print_extent_fieldnames;
    bool csvEnabled;
    std::string separator; 
};

#endif
