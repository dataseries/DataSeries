#ifndef _DSExprImpl_Hpp_
#define _DSExprImpl_Hpp_

#include <iosfwd>
#include <string>

#include <boost/function.hpp>

#include <Lintel/Double.hpp>

#include <DataSeries/DSExpr.hpp>
#include <DataSeries/GeneralField.hpp>

#define YY_DECL \
  DSExprImpl::Parser::token_type \
  DSExprScanlex(DSExprImpl::Parser::semantic_type *yylval, void * yyscanner)

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

    virtual void dump(ostream &out);

private:
    double val;
};

class ExprField : public DSExpr {
public:
    ExprField(ExtentSeries &series, const string &fieldname);
	
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

    virtual void dump(ostream &out);

private:
    GeneralField *field;
    string fieldname;
};

class ExprStrLiteral : public DSExpr {
public:
    ExprStrLiteral(const string &l);

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

    virtual void dump(ostream &out);

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

    virtual void dump(ostream &out);

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

    virtual void dump(ostream &out);

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
    virtual ~ExprFunctionApplication();

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

    virtual void dump(ostream &out);

private:
    const string name;
    DSExpr::List args;
};

class Driver {
public:
    typedef DSExprParser::FieldNameToSeries FieldNameToSeries;

    Driver(ExtentSeries &series) 
	: expr(NULL), series(&series), field_name_to_series(), scanner_state(NULL), 
          current_fnargs() 
    { }

    Driver(const FieldNameToSeries &field_name_to_series)
	: expr(NULL), series(NULL), field_name_to_series(field_name_to_series), 
          scanner_state(NULL), current_fnargs() 
    { 
        SINVARIANT(!!field_name_to_series);
    }

    ExprField *makeExprField(const string &field_name) {
        if (series == NULL) {
            ExtentSeries *tmp = field_name_to_series(field_name);
            INVARIANT(tmp != NULL, format("field_name '%s' is not defined") % field_name);
            return new ExprField(*tmp, field_name);
        } else {
            return new ExprField(*series, field_name); 
        }
    }
    ~Driver();

    void doit(const string &str);

    void startScanning(const string &str);
    void finishScanning();
	
    DSExpr *expr;
    ExtentSeries *series;
    const FieldNameToSeries field_name_to_series;
    void *scanner_state;
    DSExpr::List current_fnargs;
};

};

#endif
