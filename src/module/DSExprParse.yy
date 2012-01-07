/* -*- C++ -*-
   (c) Copyright 2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Expression parser for DataSeries
    To rebuild the parser, you need to run make rebuild_DSExpr in the
    src subdirectory of your build directory.
*/

%skeleton "lalr1.cc"
%require "2.1a"
%defines
%define "parser_class_name" "Parser"

%{
#include "DSExprImpl.hpp"
%}

%name-prefix="DSExprImpl"
%parse-param { DSExprImpl::Driver &driver }
%parse-param { void *scanner_state }
%error-verbose
%lex-param { void *scanner_state }

%union {
    double constant;
    DSExpr *expression;
    std::string *symbol;
    std::string *strliteral;
};

%{
YY_DECL;

#undef yylex
#define yylex DSExprScanlex

using namespace DSExprImpl;
%}

%file-prefix="yacc.DSExprParse"

%token END_OF_STRING 0
%token <symbol> SYMBOL 
%token <constant> CONSTANT
%token <strliteral> STRLITERAL
%token FN_TfracToSeconds
%token EQ
%token NEQ
%token REMATCH
%token GT
%token LT
%token GEQ
%token LEQ
%token LOR
%token LAND
%token LNOT

%type  <expression>     expr
%type  <expression>     rel_expr
%type  <expression>     bool_expr
%type  <expression>     top_level_expr

%%

%start complete_expr;

%left LOR;
%left LAND;
%left EQ NEQ;
%left LT GT LEQ GEQ;
%left '+' '-';
%left '*' '/';
%left ULNOT;
%left UMINUS;

complete_expr
	: top_level_expr END_OF_STRING { driver.expr = $1; }
        ;

top_level_expr
	: expr
	| bool_expr
	;

rel_expr
	: expr EQ expr { $$ = new ExprEq($1, $3); }
	| expr NEQ expr { $$ = new ExprNeq($1, $3); }
	| expr GT expr { $$ = new ExprGt($1, $3); }
	| expr LT expr { $$ = new ExprLt($1, $3); }
	| expr GEQ expr { $$ = new ExprGeq($1, $3); }
	| expr LEQ expr { $$ = new ExprLeq($1, $3); }
	| expr REMATCH expr { FATAL_ERROR("not implemented yet"); }
	;

bool_expr
	: bool_expr LOR bool_expr { $$ = new ExprLor($1, $3); }
	| bool_expr LAND bool_expr { $$ = new ExprLand($1, $3); }
	| LNOT bool_expr %prec ULNOT { $$ = new ExprLnot($2); }
	| '(' bool_expr ')'  { $$ = $2; }
	| rel_expr
	;

expr
	: expr '+' expr { $$ = new ExprAdd($1, $3); }
	| expr '-' expr { $$ = new ExprSubtract($1, $3); }
	| expr '*' expr { $$ = new ExprMultiply($1, $3); }
	| expr '/' expr { $$ = new ExprDivide($1, $3); }
	| '-' expr %prec UMINUS { $$ = new ExprMinus($2); }
	| '(' expr ')'  { $$ = $2; }
        | SYMBOL { $$ = driver.makeExprField(*$1); }
	| CONSTANT { $$ = new ExprNumericConstant($1); }
	| STRLITERAL { $$ = new ExprStrLiteral(*$1); }
	| SYMBOL '(' { driver.current_fnargs.clear(); } fnargs ')' 
	  { $$ = new ExprFunctionApplication(*$1, driver.current_fnargs); 
	    driver.current_fnargs.clear();
	  } 
	| FN_TfracToSeconds '(' expr ')' { $$ = new ExprFnTfracToSeconds($3); }
	;

fnargs
	: /* empty */ 
	| fnargs1
	;

fnargs1
	: expr { driver.current_fnargs.push_back($1); }
	| fnargs1 ',' expr { driver.current_fnargs.push_back($3); }
	;
