%{                                            /* -*- C++ -*- */
/*
   (c) Copyright 2004-2005, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Expression lexer for DSStatGroupBy
    To rebuild the scanner, you need to run make rebuild_DSStatGroupBy in the
    src subdirectory of your build directory.
*/

#include <cstdlib>
#include <errno.h>
#include <string>
#include <DSStatGroupByParse.hpp>
#include <Lintel/StringUtil.H>
#include <DataSeries/DSStatGroupByModule.H>

// Redefine yyterminate to return something of type token_type.
#define yyterminate() return token::END_OF_STRING
%}

%option noyywrap
%option never-interactive
%option nounput
%option batch
%option prefix="DSStatGroupByScan"
%option reentrant

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
DSStatGroupByModule::startScanning(const std::string &str)
{
    INVARIANT(scanner_state == NULL, "bad");
    yylex_init(&scanner_state);
    YY_BUFFER_STATE s = yy_scan_bytes(str.data(),str.size(), scanner_state);
    yy_switch_to_buffer(s, scanner_state);
}

void
DSStatGroupByModule::finishScanning()
{
    yylex_destroy(scanner_state);
    scanner_state = NULL;
}

