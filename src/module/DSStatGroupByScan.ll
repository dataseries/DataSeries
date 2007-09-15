%{                                            /* -*- C++ -*- */
/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Expression lexer for DSStatGroupBy
*/

#include <cstdlib>
#include <errno.h>
#include <string>
#include <DSStatGroupByParse.hpp>
#include <Lintel/StringUtil.H>
#include <DataSeries/DSStatGroupByModule.H>

/* Work around an incompatibility in flex (at least versions
   2.5.31 through 2.5.33): it generates code that does
   not conform to C89.  See Debian bug 333231
   <http://bugs.debian.org/cgi-bin/bugreport.cgi?bug=333231>.  */
#undef yywrap
#define yywrap() 1

// Redefine yyterminate to return something of type token_type.
#define yyterminate() return token::END_OF_STRING
%}

%option noyywrap
%option never-interactive
%option nounput
%option batch
%option prefix="DSStatGroupByScan"

constant [0-9]+(\.[0-9]+)?
blank [ \t\n]
field [a-zA-Z_]([a-zA-Z0-9_]|(\\.))*

%{
#define YY_USER_ACTION  cur_column += (yyleng);
%}

%%

%{
typedef DSStatGroupBy::Parser::token_type token_type;
typedef DSStatGroupBy::Parser::token token;
static unsigned cur_column;
%}

{blank}+    { /* ignore whitespace */ }

[+\-*/\(\)]     return token_type(yytext[0]);
{constant} { yylval->constant = stringToDouble(yytext); 
             return token::CONSTANT; }
{field} { yylval->field = new std::string(yytext);
	  return token::FIELD; }

<<EOF>> { return token::END_OF_STRING; }
. { FATAL_ERROR(boost::format("invalid character '%c'") % *yytext); }

%%

void 
DSStatGroupByModule::setInputString(const std::string &str)
{
    YY_BUFFER_STATE s = yy_scan_bytes(str.data(),str.size());
    yy_switch_to_buffer(s);
}

