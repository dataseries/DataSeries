/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Expression lexer for DSStatGroupBy
*/

%option noyywrap
%option never-interactive
%option prefix="DSStatGroupBy_yy"

%{
#include "module/DSStatGroupByDefs.h"

void 
DSStatGroupByLex_setInputString(const char *str, int len)
{
    YY_BUFFER_STATE s = yy_scan_bytes(str,len);
    yy_switch_to_buffer(s);
}

%}

field [a-zA-Z_]([a-zA-Z0-9_]|(\\.))*

%%

{field} { DSStatGroupBy_yylval.text = yytext; return FIELD; }

\-      { return '-'; }
[ \t]   { /* ignore whitespace */ }
<<EOF>> { return END_OF_STRING; }
