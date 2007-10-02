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

#include <Lintel/AssertBoost.H>
#include <Lintel/StringUtil.H>

#include <DataSeries/DSExpr.hpp>

#include <module/DSExprParse.hpp>

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
TfracToSeconds fn\.TfracToSeconds

%{
#define YY_USER_ACTION  cur_column += (yyleng);
%}

%%

%{
typedef DSExprImpl::Parser::token_type token_type;
typedef DSExprImpl::Parser::token token;
static unsigned cur_column;
%}

{blank}+    { /* ignore whitespace */ }

[+\-*/\(\)\>]     return token_type(yytext[0]);
{constant} { yylval->constant = stringToDouble(yytext); 
             return token::CONSTANT; }
{field} { yylval->field = new std::string(yytext);
	  return token::FIELD; }
{TfracToSeconds} { return token::FN_TfracToSeconds; }
<<EOF>> { return token::END_OF_STRING; }
. { FATAL_ERROR(boost::format("invalid character '%c'") % *yytext); }

%%

void 
DSExprImpl::Driver::startScanning(const std::string &str)
{
    INVARIANT(scanner_state == NULL, "bad");
    yylex_init(&scanner_state);
    YY_BUFFER_STATE s = yy_scan_bytes(str.data(),str.size(), scanner_state);
    yy_switch_to_buffer(s, scanner_state);
}

void
DSExprImpl::Driver::finishScanning()
{
    yylex_destroy(scanner_state);
    scanner_state = NULL;
}

