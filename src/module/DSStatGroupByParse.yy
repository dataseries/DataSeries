/* -*- C++ -*-
   (c) Copyright 2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

/** @file
    Expression parser for DSStatGroupBy
    To rebuild the parser, you need to run make rebuild_DSStatGroupBy in the
    src subdirectory of your build directory.
*/

%skeleton "lalr1.cc"
%require "2.1a"
%defines

%define "parser_class_name" "Parser"

%{
#include <string>
#include <DataSeries/DSStatGroupByModule.H>

#define YY_DECL \
  DSStatGroupBy::Parser::token_type \
  DSStatGroupByScanlex(DSStatGroupBy::Parser::semantic_type *yylval, void * yyscanner)
%}

%name-prefix="DSStatGroupBy"
%parse-param { DSStatGroupByModule &module }
%parse-param { void *scanner_state }
%error-verbose
%lex-param { void *scanner_state }

%union {
    double constant;
    DSStatGroupBy::Expr *expression;
    std::string *field;
};

%{

YY_DECL;

#undef yylex
#define yylex DSStatGroupByScanlex

namespace DSStatGroupBy {
    class ExprConstant : public Expr {
    public:
	ExprConstant(double v) : val(v) { }
	virtual double value() { return val; }
	// TODO: consider parsing the string as both a double and an
	// int64 to get better precision.
	virtual int64_t valueInt64() { return static_cast<int64_t>(val); }
    private:
	double val;
    };

    class ExprField : public Expr {
    public:
	ExprField(ExtentSeries &series, const std::string &fieldname)
	{ 
	    // Allow for almost arbitrary fieldnames through escaping...
	    if (fieldname.find('\\', 0) != std::string::npos) {
		std::string fixup;
		fixup.reserve(fieldname.size());
		for(unsigned i=0; i<fieldname.size(); ++i) {
		    if (fieldname[i] == '\\') {
			++i;
			INVARIANT(i < fieldname.size(), "missing escaped value");
		    }
		    fixup.push_back(fieldname[i]);
		}
		field = GeneralField::create(NULL, series, fixup);
	    } else {
		field = GeneralField::create(NULL, series, fieldname);
	    }
	}
	
	virtual ~ExprField() { 
	    delete field;
	};

	virtual double value() {
	    return field->valDouble();
	}
	virtual int64_t valueInt64() {
	    return GeneralValue(*field).valInt64();
	}
    private:
	GeneralField *field;
    };

    class ExprUnary : public Expr {
    public:
	ExprUnary(Expr *_subexpr)
	    : subexpr(_subexpr) { }
	virtual ~ExprUnary() {
	    delete subexpr;
	}
    protected:
	Expr *subexpr;
    };

    class ExprBinary : public Expr {
    public:
	ExprBinary(Expr *_left, Expr *_right)
	    : left(_left), right(_right) { }
	virtual ~ExprBinary() {
	    delete left; 
	    delete right;
	}
    protected:
	Expr *left, *right;
    };

    class ExprAdd : public ExprBinary {
    public:
	ExprAdd(Expr *left, Expr *right) : 
	    ExprBinary(left,right) { }
	virtual double value() { return left->value() + right->value(); }
	virtual int64_t valueInt64() { 
	    return left->valueInt64() + right->valueInt64(); 
	}
    };

    class ExprSubtract : public ExprBinary {
    public:
	ExprSubtract(Expr *left, Expr *right) : 
	    ExprBinary(left,right) { }
	virtual double value() { return left->value() - right->value(); }
	virtual int64_t valueInt64() { 
	    return left->valueInt64() - right->valueInt64(); 
	}
    };

    class ExprMultiply : public ExprBinary {
    public:
	ExprMultiply(Expr *left, Expr *right) : 
	    ExprBinary(left,right) { }
	virtual double value() { return left->value() * right->value(); }
	virtual int64_t valueInt64() { 
	    return left->valueInt64() * right->valueInt64(); 
	}
    };

    class ExprDivide : public ExprBinary {
    public:
	ExprDivide(Expr *left, Expr *right) : 
	    ExprBinary(left,right) { }
	virtual double value() { return left->value() / right->value(); }
	virtual int64_t valueInt64() { 
	    return left->valueInt64() / right->valueInt64(); 
	}
    };

    class ExprFnTfracToSeconds : public ExprUnary {
    public:
	ExprFnTfracToSeconds(Expr *subexpr) 
	    : ExprUnary(subexpr) 
	{ }
	virtual double value() {
	    return subexpr->valueInt64() / 4294967296.0;
	}
	virtual int64_t valueInt64() {
	    // TODO: Should we warn/error on this? we're dropping lots
	    // of precision here.  Also should we round or truncate as
	    // is currently implemented?
	    int64_t t = subexpr->valueInt64();
	    return Clock::TfracToSec(t);
	}
    };
}

using namespace DSStatGroupBy;
%}

%file-prefix="yacc.DSStatGroupBy"

%token            END_OF_STRING 0
%token <field>    FIELD 
%token <constant> CONSTANT
%token            FN_TfracToSeconds

%type  <expression>     expr

%%
%start complete_expr;

complete_expr: expr END_OF_STRING { module.expr = $1; } ;

%left '+' '-';
%left '*' '/';

expr: expr '+' expr { $$ = new ExprAdd($1, $3); }
    | expr '-' expr { $$ = new ExprSubtract($1, $3); }
    | expr '*' expr { $$ = new ExprMultiply($1, $3); }
    | expr '/' expr { $$ = new ExprDivide($1, $3); }
    | '(' expr ')'  { $$ = $2; }
    | FIELD { $$ = new ExprField(module.series, *$1); }
    | CONSTANT { $$ = new ExprConstant($1); }
    | FN_TfracToSeconds '(' expr ')' { $$ = new ExprFnTfracToSeconds($3); }
;

%%

void
DSStatGroupBy::Parser::error(const DSStatGroupBy::location &,
  	                     const std::string &)
{
	FATAL_ERROR("??");
}
