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
#include <string>
#include <Lintel/Double.hpp>
#include <DataSeries/DSExpr.hpp>

#define YY_DECL \
  DSExprImpl::Parser::token_type \
  DSExprScanlex(DSExprImpl::Parser::semantic_type *yylval, void * yyscanner)

namespace DSExprImpl {
    class Driver {
    public:
	// Implementation in DSExpr.cpp
	Driver(ExtentSeries &_series) 
	    : expr(NULL), series(_series), scanner_state(NULL) {}
	~Driver();

	void doit(const std::string &str);

	void startScanning(const std::string &str);
	void finishScanning();
	
	DSExpr *expr;
	ExtentSeries &series;
	void *scanner_state;
    };
};

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

#include <Lintel/Clock.hpp>

#include <DataSeries/GeneralField.hpp>

YY_DECL;

#undef yylex
#define yylex DSExprScanlex

namespace DSExprImpl {
    // TODO: make valGV to do general value calculations.

    class ExprNumericConstant : public DSExpr {
    public:
	// TODO: consider parsing the string as both a double and an
	// int64 to get better precision.
	ExprNumericConstant(double v) : val(v) {}

	virtual expr_type_t getType() {
	    return t_Numeric;
	}

	virtual double valDouble() { return val; }
	virtual int64_t valInt64() { return static_cast<int64_t>(val); }
	virtual bool valBool() { return val ? true : false; }
	virtual const std::string valString() {
	    FATAL_ERROR("no automatic coercion of numeric constants to string");
	}

    private:
	double val;
    };

    class ExprField : public DSExpr {
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

	virtual expr_type_t getType() {
	    if (field->getType() == ExtentType::ft_variable32) {
		return t_String;
	    } else {
		return t_Numeric;
	    }
	}

	virtual double valDouble() {
	    return field->valDouble();
	}
	virtual int64_t valInt64() {
	    return GeneralValue(*field).valInt64();
	}
	virtual bool valBool() {
	    return GeneralValue(*field).valBool();
	}
	virtual const std::string valString() {
	    return GeneralValue(*field).valString();
	}

    private:
	GeneralField *field;
    };

    class ExprStrLiteral : public DSExpr {
    public:
	ExprStrLiteral(const std::string &l)
	{
	    std::string fixup;
	    SINVARIANT(l.size() > 2);
	    SINVARIANT(l[0] == '\"');
	    SINVARIANT(l[l.size() - 1] == '\"');
	    fixup.reserve(l.size() - 2);
	    for (unsigned i = 1; i < l.size() - 1; ++i) {
		char cc = l[i];
		if (cc == '\\') {
		    ++i;
		    cc = l[i];
		    INVARIANT(i < l.size(), "missing escaped value");
		    switch (cc) {
		    case '\\':
			fixup.push_back('\\');
			break;
		    case '\"':
			fixup.push_back('\"');
			break;
		    case 'f':
			fixup.push_back('\f');
			break;
		    case 'r':
			fixup.push_back('\r');
			break;
		    case 'n':
			fixup.push_back('\n');
			break;
		    case 'b':
			fixup.push_back('\b');
			break;
		    case 't':
			fixup.push_back('\t');
			break;
		    case 'v':
			fixup.push_back('\v');
			break;
		    default:
			FATAL_ERROR("unknown escape");
		    }
		} else {
		    fixup.push_back(cc);
		}
	    }
	    s = fixup;
	}

	virtual ~ExprStrLiteral() {}

	virtual expr_type_t getType() {
	    return t_String;
	}

	virtual double valDouble() {
	    FATAL_ERROR("no automatic coercion of string literals to double");
	}
	virtual int64_t valInt64() {
	    FATAL_ERROR("no automatic coercion of string literals to integer");
	}
	virtual bool valBool() {
	    FATAL_ERROR("no automatic coercion of string literals to boolean");
	}
	virtual const std::string valString() {
	    return s;
	}
    private:
	std::string s;
    };

    class ExprUnary : public DSExpr {
    public:
	ExprUnary(DSExpr *_subexpr)
	    : subexpr(_subexpr) {}
	virtual ~ExprUnary() {
	    delete subexpr;
	}
    protected:
	DSExpr *subexpr;
    };

    class ExprBinary : public DSExpr {
    public:
	ExprBinary(DSExpr *_left, DSExpr *_right)
	    : left(_left), right(_right) {}
	virtual ~ExprBinary() {
	    delete left; 
	    delete right;
	}

	bool either_string() const
	{
	    return ((left->getType() == t_String) ||
		    (right->getType() == t_String));
	}

    protected:
	DSExpr *left, *right;
    };

    class ExprMinus : public ExprUnary {
    public:
	ExprMinus(DSExpr *subexpr)
	    : ExprUnary(subexpr) {}

	virtual expr_type_t getType() {
	    return t_Numeric;
	}

	virtual double valDouble() { 
	    return - subexpr->valDouble();
	}
	virtual int64_t valInt64() { 
	    return - subexpr->valInt64();
	}
	virtual bool valBool() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprAdd : public ExprBinary {
    public:
	ExprAdd(DSExpr *left, DSExpr *right) : 
	    ExprBinary(left,right) {}

	virtual expr_type_t getType() {
	    return t_Numeric;
	}

	virtual double valDouble() { 
	    return left->valDouble() + right->valDouble(); 
	}
	virtual int64_t valInt64() { 
	    // TODO: add check for overflow?
	    return left->valInt64()+ right->valInt64(); 
	}
	virtual bool valBool() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprSubtract : public ExprBinary {
    public:
	ExprSubtract(DSExpr *left, DSExpr *right) : 
	    ExprBinary(left,right) {}
	virtual expr_type_t getType() {
	    return t_Numeric;
	}
	virtual double valDouble() { 
	    return left->valDouble() - right->valDouble(); 
	}
	virtual int64_t valInt64() { 
	    return left->valInt64() - right->valInt64(); 
	}
	virtual bool valBool() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprMultiply : public ExprBinary {
    public:
	ExprMultiply(DSExpr *left, DSExpr *right) : 
	    ExprBinary(left,right) {}

	virtual expr_type_t getType() {
	    return t_Numeric;
	}

	virtual double valDouble() { 
	    return left->valDouble() * right->valDouble(); 
	}
	virtual int64_t valInt64() { 
	    return left->valInt64() * right->valInt64(); 
	}
	virtual bool valBool() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprDivide : public ExprBinary {
    public:
	ExprDivide(DSExpr *left, DSExpr *right) : 
	    ExprBinary(left,right) {}

	virtual expr_type_t getType() {
	    return t_Numeric;
	}

	virtual double valDouble() { 
	    return left->valDouble() / right->valDouble(); 
	}
	virtual int64_t valInt64() { 
	    return left->valInt64() / right->valInt64(); 
	}
	virtual bool valBool() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprEq : public ExprBinary {
    public:
	ExprEq(DSExpr *l, DSExpr *r)
	    : ExprBinary(l,r) {}
	virtual expr_type_t getType() {
	    return t_Bool;
	}
	virtual double valDouble() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual int64_t valInt64() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual bool valBool() {
	    if (either_string()) {
		return (left->valString() == right->valString());
	    } else {
		return Double::eq(left->valDouble(), right->valDouble());
	    }
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprNeq : public ExprBinary {
    public:
	ExprNeq(DSExpr *l, DSExpr *r)
	    : ExprBinary(l,r) {}

	virtual expr_type_t getType() {
	    return t_Bool;
	}

	virtual double valDouble() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual int64_t valInt64() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual bool valBool() {
	    if (either_string()) {
		return (left->valString() != right->valString());
	    } else {
		return !Double::eq(left->valDouble(), right->valDouble());
	    }
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprGt : public ExprBinary {
    public:
	ExprGt(DSExpr *l, DSExpr *r)
	    : ExprBinary(l,r) {}

	virtual expr_type_t getType() {
	    return t_Bool;
	}

	virtual double valDouble() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual int64_t valInt64() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual bool valBool() {
	    if (either_string()) {
		return (left->valString() > right->valString());
	    } else {
		return Double::gt(left->valDouble(), right->valDouble());
	    }
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprLt : public ExprBinary {
    public:
	ExprLt(DSExpr *l, DSExpr *r)
	    : ExprBinary(l,r) {}
	virtual expr_type_t getType() {
	    return t_Bool;
	}
	virtual double valDouble() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual int64_t valInt64() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual bool valBool() {
	    if (either_string()) {
		return (left->valString() < right->valString());
	    } else {
		return Double::lt(left->valDouble(), right->valDouble());
	    }
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprGeq : public ExprBinary {
    public:
	ExprGeq(DSExpr *l, DSExpr *r)
	    : ExprBinary(l,r) {}

	virtual expr_type_t getType() {
	    return t_Bool;
	}

	virtual double valDouble() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual int64_t valInt64() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual bool valBool() {
	    if (either_string()) {
		return (left->valString() >= right->valString());
	    } else {
		return Double::geq(left->valDouble(), right->valDouble());
	    }
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprLeq : public ExprBinary {
    public:
	ExprLeq(DSExpr *l, DSExpr *r)
	    : ExprBinary(l,r) {}

	virtual expr_type_t getType() {
	    return t_Bool;
	}

	virtual double valDouble() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual int64_t valInt64() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual bool valBool() {
	    if (either_string()) {
		return (left->valString() <= right->valString());
	    } else {
		return Double::leq(left->valDouble(), right->valDouble());
	    }
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprLor : public ExprBinary {
    public:
	ExprLor(DSExpr *l, DSExpr *r)
	    : ExprBinary(l,r) {}

	virtual expr_type_t getType() {
	    return t_Bool;
	}

	virtual double valDouble() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual int64_t valInt64() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual bool valBool() {
	    return (left->valBool() || right->valBool());
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprLand : public ExprBinary {
    public:
	ExprLand(DSExpr *l, DSExpr *r)
	    : ExprBinary(l,r) {}
	virtual expr_type_t getType() {
	    return t_Bool;
	}
	virtual double valDouble() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual int64_t valInt64() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual bool valBool() {
	    return (left->valBool() && right->valBool());
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprLnot : public ExprUnary {
    public:
	ExprLnot(DSExpr *subexpr)
	    : ExprUnary(subexpr) {}

	virtual expr_type_t getType() {
	    return t_Bool;
	}

	virtual double valDouble() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual int64_t valInt64() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual bool valBool() {
	    return !(subexpr->valBool());
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprFnTfracToSeconds : public ExprUnary {
    public:
	ExprFnTfracToSeconds(DSExpr *subexpr) 
	    : ExprUnary(subexpr) 
	{}

	virtual expr_type_t getType() {
	    return t_Numeric;
	}

	virtual double valDouble() {
	    return subexpr->valInt64() / 4294967296.0;
	}
	virtual int64_t valInt64() {
	    FATAL_ERROR("shouldn't try to get valInt64 using fn.TfracToSeconds, it drops too much precision");
	}
	virtual bool valBool() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual const std::string valString() {
	    FATAL_ERROR("no silent type switching");
	}
    };
}

using namespace DSExprImpl;
%}

%file-prefix="yacc.DSExprParse"

%token            END_OF_STRING 0
%token <symbol>    SYMBOL 
%token <constant> CONSTANT
%token            FN_TfracToSeconds
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
%token <strliteral> STRLITERAL

%type  <expression>     expr
%type  <expression>     bool_expr
%type  <expression>     rel_expr

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
	: expr END_OF_STRING { driver.expr = $1; } 
	| bool_expr END_OF_STRING { driver.expr = $1; } 
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
	| SYMBOL { $$ = new ExprField(driver.series, *$1); }
	| CONSTANT { $$ = new ExprNumericConstant($1); }
	| STRLITERAL { $$ = new ExprStrLiteral(*$1); }
	| SYMBOL '(' fnargs ')' { FATAL_ERROR("not implemented yet"); }
	| FN_TfracToSeconds '(' expr ')' { $$ = new ExprFnTfracToSeconds($3); }
	;

fnargs
	: expr
	| fnargs ',' expr
	;

%%

void
DSExprImpl::Parser::error(const DSExprImpl::location &,
			  const std::string &err)
{
    FATAL_ERROR(boost::format("error parsing: %s starting") % err);
}
