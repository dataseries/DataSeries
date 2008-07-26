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
#include <ios>
#include <string>
#include <Lintel/Double.hpp>
#include <DataSeries/DSExpr.hpp>
#include <boost/format.hpp>
#include <boost/foreach.hpp>

#define YY_DECL \
  DSExprImpl::Parser::token_type \
  DSExprScanlex(DSExprImpl::Parser::semantic_type *yylval, void * yyscanner)

namespace DSExprImpl {
    using namespace std;

    class Driver {
    public:
	// Implementation in DSExpr.cpp
	Driver(ExtentSeries &_series) 
	    : expr(NULL), series(_series), scanner_state(NULL) {}
	~Driver();

	void doit(const string &str);

	void startScanning(const string &str);
	void finishScanning();
	
	DSExpr *expr;
	ExtentSeries &series;
	void *scanner_state;
	DSExpr::List current_fnargs;
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
    using namespace std;
    using namespace boost;

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
	virtual const string valString() {
	    FATAL_ERROR("no automatic coercion of numeric constants to string");
	}

	virtual void dump(ostream &out) {
	    out << format("{NumericConstant: %1%}") % val;
	}

    private:
	double val;
    };

    class ExprField : public DSExpr {
    public:
	ExprField(ExtentSeries &series, const string &fieldname_)
	{ 
	    // Allow for almost arbitrary fieldnames through escaping...
	    if (fieldname_.find('\\', 0) != string::npos) {
		string fixup;
		fixup.reserve(fieldname_.size());
		for(unsigned i=0; i<fieldname_.size(); ++i) {
		    if (fieldname_[i] == '\\') {
			++i;
			INVARIANT(i < fieldname_.size(), "missing escaped value");
		    }
		    fixup.push_back(fieldname_[i]);
		}
		field = GeneralField::create(NULL, series, fixup);
		fieldname = fixup;
	    } else {
		field = GeneralField::create(NULL, series, fieldname_);
		fieldname = fieldname_;
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
	virtual const string valString() {
	    return GeneralValue(*field).valString();
	}

	virtual void dump(ostream &out) {
	    out << format("{Field: %1%}") % fieldname;
	}

    private:
	GeneralField *field;
	string fieldname;
    };

    class ExprStrLiteral : public DSExpr {
    public:
	ExprStrLiteral(const string &l)
	{
	    string fixup;
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
	virtual const string valString() {
	    return s;
	}

	virtual void dump(ostream &out) {
	    out << format("{StrLiteral: %1%}") % s;
	}

    private:
	string s;
    };

    class ExprUnary : public DSExpr {
    public:
	ExprUnary(DSExpr *_subexpr)
	    : subexpr(_subexpr) {}
	virtual ~ExprUnary() {
	    delete subexpr;
	}

	virtual void dump(ostream &out) {
	    out << opname();
	    out << " ";
	    subexpr->dump(out);
	}

	virtual string opname() const {
	    FATAL_ERROR("either override dump or defined opname");
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

	virtual void dump(ostream &out) {
	    left->dump(out);
	    out << " ";
	    out << opname();
	    out << " ";
	    right->dump(out);
	}

	virtual string opname() const {
	    FATAL_ERROR("either override dump or defined opname");
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string("-"); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string("+"); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string("-"); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string("*"); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string("/"); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string("=="); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string("!="); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string(">"); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string("<"); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string(">="); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string("<="); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string("||"); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string("&&"); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual string opname() const { return string("!"); }
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
	virtual const string valString() {
	    FATAL_ERROR("no silent type switching");
	}

	virtual void dump(ostream &out) {
	    out << "fn.TfracToSeconds(";
	    subexpr->dump(out);
	    out << ")";
	}
    };

    class ExprFunctionApplication : public DSExpr {
    public:
	ExprFunctionApplication(const string &fnname, const DSExpr::List &fnargs) 
	    : name(fnname), args(fnargs)
	{}
	virtual ~ExprFunctionApplication() {
	    BOOST_FOREACH(DSExpr *e, args) {
		delete e;
	    }
	    args.clear();
	}

	virtual expr_type_t getType() {
	    return t_Unknown;
	}

	virtual double valDouble() {
	    return 0.0;
	}
	virtual int64_t valInt64() {
	    return 0;
	}
	virtual bool valBool() {
	    return false;
	}
	virtual const string valString() {
	    return "";
	}

	virtual void dump(ostream &out) {
	    out << name;
	    out << "(";
	    out << " ";
	    bool first = true;
	    BOOST_FOREACH(DSExpr *e, args) {
		if (first) {
		    first = false;
		} else {
		    out << ", ";
		}
		e->dump(out);
	    }
	    out << " ";
	    out << ")";
	}

    private:
	const string name;
        DSExpr::List args;
    };
}

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
	| SYMBOL { $$ = new ExprField(driver.series, *$1); }
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

%%

void
DSExprImpl::Parser::error(const DSExprImpl::location &,
			  const std::string &err)
{
    FATAL_ERROR(boost::format("error parsing: %s starting") % err);
}
