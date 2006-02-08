/*
   (c) Copyright 2003-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/


#include <ExtentSeries.H>
#include <Extent.H>
#include <ExtentField.H>

ExtentSeries::ExtentSeries(Extent *e, typeCompatibilityT _tc)
    : typeCompatibility(_tc)
{
    if (e == NULL) {
	type = NULL;
	my_extent = NULL;
    } else {
	type = e->type;
	AssertAlways(type != NULL,("bad initialization\n"));
	my_extent = e;
	pos.reset(e);
    }
}

void
ExtentSeries::setType(const ExtentType *_type)
{
    AssertAlways(_type != NULL,("NULL type not allowed in setSeriesType!\n"));
    switch(typeCompatibility)
	{
	case typeExact: 
	    AssertAlways(type == NULL,
			 ("Unable to change type with typeExact compatibility\nType 1:\n%s\nType 2:\n%s\n",
			  type->xmldesc.c_str(),_type->xmldesc.c_str()));
	    break;
	case typeXMLIdentical:
	    AssertAlways(type == NULL || type->xmldesc == _type->xmldesc,
			 ("Mismatch on XML descriptions with typeXMLIdentical compatibility; xml descriptions below\n%s\n%s",
			  type->xmldesc.c_str(),_type->xmldesc.c_str()));
	    break;
	case typeFieldMatch:
	    AssertFatal(("Unimplemented\n"));
	    break;
	case typeLoose:
	    break;
	default:
	    AssertFatal(("internal error\n"));
	}

    type = _type;
    for(std::vector<Field *>::iterator i = my_fields.begin();
	i != my_fields.end();++i) {
	AssertAlways(&(**i).dataseries == this,
		     ("Internal error\n"));
	(**i).newExtentType();
    }
}

void
ExtentSeries::setExtent(Extent *e)
{
    AssertAlways(e != NULL,("setExtent(NULL) invalid\n"));
    pos.reset(e);
    bool newtype = e->type != type;
    my_extent = e;
    if (newtype) {
	setType(e->type);
    }
}

void
ExtentSeries::clearExtent()
{
    my_extent = NULL;
    pos.cur_extent = NULL;
    pos.cur_pos = NULL;
    pos.recordsize = -1;
}

void
ExtentSeries::addField(Field &field)
{
    if (type != NULL) {
	field.newExtentType();
    }
    my_fields.push_back(&field);
}
