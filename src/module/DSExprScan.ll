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

#include <Lintel/AssertBoost.hpp>
#include <Lintel/StringUtil.hpp>

#include <module/DSExprImpl.hpp>

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

constant	[0-9]+(\.[0-9]+)?
blank		[ \t\n]
symbol		[a-zA-Z_]([a-zA-Z0-9_\.:]|(\\.))*
TfracToSeconds	"fn.TfracToSeconds"
gt		">"
lt		"<"
geq		">="
leq		"<="
eq		"=="
neq		"!="
rematch		"=~"
lor		"||"
land		"&&"
lnot		"!"
strliteral	\"([^\"]|\\.)*\"

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

[+\-*/\(\),]	{ return token_type(yytext[0]); }
{gt}	{ return token_type(token::GT); }
{lt}	{ return token_type(token::LT); }
{geq}	{ return token_type(token::GEQ); }
{leq}	{ return token_type(token::LEQ); }
{eq}	{ return token_type(token::EQ); }
{neq}	{ return token_type(token::NEQ); }
{rematch}	{ return token_type(token::REMATCH); }
{lor}	{ return token_type(token::LOR); }
{land}  { return token_type(token::LAND); }
{lnot}  { return token_type(token::LNOT); }
{constant}	{ yylval->constant = stringToDouble(yytext); 
                  return token::CONSTANT; }
{symbol} { yylval->symbol = new std::string(yytext);
	  return token::SYMBOL; }
{TfracToSeconds} { return token::FN_TfracToSeconds; }
{strliteral}	{ yylval->strliteral = new std::string(yytext);
                  return token_type(token::STRLITERAL); }
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

