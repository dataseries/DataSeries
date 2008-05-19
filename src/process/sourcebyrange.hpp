/* -*-C++-*-
   (c) Copyright 2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    source by range; obsolete way of doing indexing used by the nfs analysis, should be replaced
*/

#ifndef __SOURCE_BY_RANGE_H
#define __SOURCE_BY_RANGE_H

bool isnumber(char *v);

void sourceByIndex(TypeIndexModule *source,char *index_filename,int start_secs, int end_secs);

#endif
