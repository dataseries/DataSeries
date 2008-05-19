/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton implementation for Bison LALR(1) parsers in C++

   Copyright (C) 2002, 2003, 2004, 2005, 2006 Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

// Take the name prefix into account.
#define yylex   DSExprImpllex

#include "DSExprParse.hpp"

/* User implementation prologue.  */
#line 60 "module/DSExprParse.yy"


#include <Lintel/Clock.hpp>

#include <DataSeries/GeneralField.hpp>

YY_DECL;

#undef yylex
#define yylex DSExprScanlex

namespace DSExprImpl {
    // TODO: make valGV to do general value calculations.

    class ExprConstant : public DSExpr {
    public:
	ExprConstant(double v) : val(v) { }
	virtual double valDouble() { return val; }
	// TODO: consider parsing the string as both a double and an
	// int64 to get better precision.
	virtual int64_t valInt64() { return static_cast<int64_t>(val); }
	virtual bool valBool() { return val ? true : false; }
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

	virtual double valDouble() {
	    return field->valDouble();
	}
	virtual int64_t valInt64() {
	    return GeneralValue(*field).valInt64();
	}
	virtual bool valBool() {
	    return GeneralValue(*field).valBool();
	}
    private:
	GeneralField *field;
    };

    class ExprUnary : public DSExpr {
    public:
	ExprUnary(DSExpr *_subexpr)
	    : subexpr(_subexpr) { }
	virtual ~ExprUnary() {
	    delete subexpr;
	}
    protected:
	DSExpr *subexpr;
    };

    class ExprBinary : public DSExpr {
    public:
	ExprBinary(DSExpr *_left, DSExpr *_right)
	    : left(_left), right(_right) { }
	virtual ~ExprBinary() {
	    delete left; 
	    delete right;
	}
    protected:
	DSExpr *left, *right;
    };

    class ExprAdd : public ExprBinary {
    public:
	ExprAdd(DSExpr *left, DSExpr *right) : 
	    ExprBinary(left,right) { }
	virtual double valDouble() { 
	    return left->valDouble() + right->valDouble(); 
	}
	virtual int64_t valInt64() { 
	    // TODO: add check for overflow?
	    return left->valInt64() + right->valInt64(); 
	}
	virtual bool valBool() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprSubtract : public ExprBinary {
    public:
	ExprSubtract(DSExpr *left, DSExpr *right) : 
	    ExprBinary(left,right) { }
	virtual double valDouble() { 
	    return left->valDouble() - right->valDouble(); 
	}
	virtual int64_t valInt64() { 
	    return left->valInt64() - right->valInt64(); 
	}
	virtual bool valBool() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprMultiply : public ExprBinary {
    public:
	ExprMultiply(DSExpr *left, DSExpr *right) : 
	    ExprBinary(left,right) { }
	virtual double valDouble() { 
	    return left->valDouble() * right->valDouble(); 
	}
	virtual int64_t valInt64() { 
	    return left->valInt64() * right->valInt64(); 
	}
	virtual bool valBool() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprDivide : public ExprBinary {
    public:
	ExprDivide(DSExpr *left, DSExpr *right) : 
	    ExprBinary(left,right) { }
	virtual double valDouble() { 
	    return left->valDouble() / right->valDouble(); 
	}
	virtual int64_t valInt64() { 
	    return left->valInt64() / right->valInt64(); 
	}
	virtual bool valBool() {
	    FATAL_ERROR("no silent type switching");
	}
    };

    class ExprStrictlyGreaterThan : public ExprBinary {
    public:
	ExprStrictlyGreaterThan(DSExpr *l, DSExpr *r)
	    : ExprBinary(l,r) { }
	virtual double valDouble() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual int64_t valInt64() {
	    FATAL_ERROR("no silent type switching");
	}
	virtual bool valBool() {
	    return left->valDouble() > right->valDouble();
	}
    };

    class ExprFnTfracToSeconds : public ExprUnary {
    public:
	ExprFnTfracToSeconds(DSExpr *subexpr) 
	    : ExprUnary(subexpr) 
	{ }
	virtual double valDouble() {
	    return subexpr->valInt64() / 4294967296.0;
	}
	virtual int64_t valInt64() {
	    FATAL_ERROR("shouldn't try to get valInt64 using fn.TfracToSeconds, it drops too much precision");
	}
	virtual bool valBool() {
	    FATAL_ERROR("no silent type switching");
	}
    };
}

using namespace DSExprImpl;


/* Line 317 of lalr1.cc.  */
#line 227 "module/DSExprParse.cpp"

#ifndef YY_
# if YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* FIXME: INFRINGES ON USER NAME SPACE */
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#define YYUSE(e) ((void) (e))

/* A pseudo ostream that takes yydebug_ into account.  */
# define YYCDEBUG							\
  for (bool yydebugcond_ = yydebug_; yydebugcond_; yydebugcond_ = false)	\
    (*yycdebug_)

/* Enable debugging if requested.  */
#if YYDEBUG

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)	\
do {							\
  if (yydebug_)						\
    {							\
      *yycdebug_ << Title << ' ';			\
      yy_symbol_print_ ((Type), (Value), (Location));	\
      *yycdebug_ << std::endl;				\
    }							\
} while (false)

# define YY_REDUCE_PRINT(Rule)		\
do {					\
  if (yydebug_)				\
    yy_reduce_print_ (Rule);		\
} while (false)

# define YY_STACK_PRINT()		\
do {					\
  if (yydebug_)				\
    yystack_print_ ();			\
} while (false)

#else /* !YYDEBUG */

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_REDUCE_PRINT(Rule)
# define YY_STACK_PRINT()

#endif /* !YYDEBUG */

#define YYACCEPT	goto yyacceptlab
#define YYABORT		goto yyabortlab
#define YYERROR		goto yyerrorlab

namespace DSExprImpl
{
#if YYERROR_VERBOSE

  /* Return YYSTR after stripping away unnecessary quotes and
     backslashes, so that it's suitable for yyerror.  The heuristic is
     that double-quoting is unnecessary unless the string contains an
     apostrophe, a comma, or backslash (other than backslash-backslash).
     YYSTR is taken from yytname.  */
  std::string
  Parser::yytnamerr_ (const char *yystr)
  {
    if (*yystr == '"')
      {
        std::string yyr = "";
        char const *yyp = yystr;

        for (;;)
          switch (*++yyp)
            {
            case '\'':
            case ',':
              goto do_not_strip_quotes;

            case '\\':
              if (*++yyp != '\\')
                goto do_not_strip_quotes;
              /* Fall through.  */
            default:
              yyr += *yyp;
              break;

            case '"':
              return yyr;
            }
      do_not_strip_quotes: ;
      }

    return yystr;
  }

#endif

  /// Build a parser object.
  Parser::Parser (DSExprImpl::Driver &driver_yyarg, void *scanner_state_yyarg)
    : yydebug_ (false),
      yycdebug_ (&std::cerr),
      driver (driver_yyarg),
      scanner_state (scanner_state_yyarg)
  {
  }

  Parser::~Parser ()
  {
  }

#if YYDEBUG
  /*--------------------------------.
  | Print this symbol on YYOUTPUT.  |
  `--------------------------------*/

  inline void
  Parser::yy_symbol_value_print_ (int yytype,
			   const semantic_type* yyvaluep, const location_type* yylocationp)
  {
    YYUSE (yylocationp);
    YYUSE (yyvaluep);
    switch (yytype)
      {
         default:
	  break;
      }
  }


  void
  Parser::yy_symbol_print_ (int yytype,
			   const semantic_type* yyvaluep, const location_type* yylocationp)
  {
    *yycdebug_ << (yytype < yyntokens_ ? "token" : "nterm")
	       << ' ' << yytname_[yytype] << " ("
	       << *yylocationp << ": ";
    yy_symbol_value_print_ (yytype, yyvaluep, yylocationp);
    *yycdebug_ << ')';
  }
#endif /* ! YYDEBUG */

  void
  Parser::yydestruct_ (const char* yymsg,
			   int yytype, semantic_type* yyvaluep, location_type* yylocationp)
  {
    YYUSE (yylocationp);
    YYUSE (yymsg);
    YYUSE (yyvaluep);

    YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

    switch (yytype)
      {
  
	default:
	  break;
      }
  }

  void
  Parser::yypop_ (unsigned int n)
  {
    yystate_stack_.pop (n);
    yysemantic_stack_.pop (n);
    yylocation_stack_.pop (n);
  }

  std::ostream&
  Parser::debug_stream () const
  {
    return *yycdebug_;
  }

  void
  Parser::set_debug_stream (std::ostream& o)
  {
    yycdebug_ = &o;
  }


  Parser::debug_level_type
  Parser::debug_level () const
  {
    return yydebug_;
  }

  void
  Parser::set_debug_level (debug_level_type l)
  {
    yydebug_ = l;
  }


  int
  Parser::parse ()
  {
    /// Look-ahead and look-ahead in internal form.
    int yychar = yyempty_;
    int yytoken = 0;

    /* State.  */
    int yyn;
    int yylen = 0;
    int yystate = 0;

    /* Error handling.  */
    int yynerrs_ = 0;
    int yyerrstatus_ = 0;

    /// Semantic value of the look-ahead.
    semantic_type yylval;
    /// Location of the look-ahead.
    location_type yylloc;
    /// The locations where the error started and ended.
    location yyerror_range[2];

    /// $$.
    semantic_type yyval;
    /// @$.
    location_type yyloc;

    int yyresult;

    YYCDEBUG << "Starting parse" << std::endl;


    /* Initialize the stacks.  The initial state will be pushed in
       yynewstate, since the latter expects the semantical and the
       location values to have been already stored, initialize these
       stacks with a primary value.  */
    yystate_stack_ = state_stack_type (0);
    yysemantic_stack_ = semantic_stack_type (0);
    yylocation_stack_ = location_stack_type (0);
    yysemantic_stack_.push (yylval);
    yylocation_stack_.push (yylloc);

    /* New state.  */
  yynewstate:
    yystate_stack_.push (yystate);
    YYCDEBUG << "Entering state " << yystate << std::endl;
    goto yybackup;

    /* Backup.  */
  yybackup:

    /* Try to take a decision without look-ahead.  */
    yyn = yypact_[yystate];
    if (yyn == yypact_ninf_)
      goto yydefault;

    /* Read a look-ahead token.  */
    if (yychar == yyempty_)
      {
	YYCDEBUG << "Reading a token: ";
	yychar = yylex (&yylval, scanner_state);
      }


    /* Convert token to internal form.  */
    if (yychar <= yyeof_)
      {
	yychar = yytoken = yyeof_;
	YYCDEBUG << "Now at end of input." << std::endl;
      }
    else
      {
	yytoken = yytranslate_ (yychar);
	YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
      }

    /* If the proper action on seeing token YYTOKEN is to reduce or to
       detect an error, take that action.  */
    yyn += yytoken;
    if (yyn < 0 || yylast_ < yyn || yycheck_[yyn] != yytoken)
      goto yydefault;

    /* Reduce or error.  */
    yyn = yytable_[yyn];
    if (yyn <= 0)
      {
	if (yyn == 0 || yyn == yytable_ninf_)
	goto yyerrlab;
	yyn = -yyn;
	goto yyreduce;
      }

    /* Accept?  */
    if (yyn == yyfinal_)
      goto yyacceptlab;

    /* Shift the look-ahead token.  */
    YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

    /* Discard the token being shifted unless it is eof.  */
    if (yychar != yyeof_)
      yychar = yyempty_;

    yysemantic_stack_.push (yylval);
    yylocation_stack_.push (yylloc);

    /* Count tokens shifted since error; after three, turn off error
       status.  */
    if (yyerrstatus_)
      --yyerrstatus_;

    yystate = yyn;
    goto yynewstate;

  /*-----------------------------------------------------------.
  | yydefault -- do the default action for the current state.  |
  `-----------------------------------------------------------*/
  yydefault:
    yyn = yydefact_[yystate];
    if (yyn == 0)
      goto yyerrlab;
    goto yyreduce;

  /*-----------------------------.
  | yyreduce -- Do a reduction.  |
  `-----------------------------*/
  yyreduce:
    yylen = yyr2_[yyn];
    /* If YYLEN is nonzero, implement the default value of the action:
       `$$ = $1'.  Otherwise, use the top of the stack.

       Otherwise, the following line sets YYVAL to garbage.
       This behavior is undocumented and Bison
       users should not rely upon it.  */
    if (yylen)
      yyval = yysemantic_stack_[yylen - 1];
    else
      yyval = yysemantic_stack_[0];

    {
      slice<location_type, location_stack_type> slice (yylocation_stack_, yylen);
      YYLLOC_DEFAULT (yyloc, slice, yylen);
    }
    YY_REDUCE_PRINT (yyn);
    switch (yyn)
      {
	  case 2:
#line 256 "module/DSExprParse.yy"
    { driver.expr = (yysemantic_stack_[(2) - (1)].expression); ;}
    break;

  case 3:
#line 257 "module/DSExprParse.yy"
    { driver.expr = (yysemantic_stack_[(2) - (1)].expression); ;}
    break;

  case 4:
#line 259 "module/DSExprParse.yy"
    { (yyval.expression) = new ExprStrictlyGreaterThan((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); ;}
    break;

  case 5:
#line 264 "module/DSExprParse.yy"
    { (yyval.expression) = new ExprAdd((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); ;}
    break;

  case 6:
#line 265 "module/DSExprParse.yy"
    { (yyval.expression) = new ExprSubtract((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); ;}
    break;

  case 7:
#line 266 "module/DSExprParse.yy"
    { (yyval.expression) = new ExprMultiply((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); ;}
    break;

  case 8:
#line 267 "module/DSExprParse.yy"
    { (yyval.expression) = new ExprDivide((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); ;}
    break;

  case 9:
#line 268 "module/DSExprParse.yy"
    { (yyval.expression) = (yysemantic_stack_[(3) - (2)].expression); ;}
    break;

  case 10:
#line 269 "module/DSExprParse.yy"
    { (yyval.expression) = new ExprField(driver.series, *(yysemantic_stack_[(1) - (1)].field)); ;}
    break;

  case 11:
#line 270 "module/DSExprParse.yy"
    { (yyval.expression) = new ExprConstant((yysemantic_stack_[(1) - (1)].constant)); ;}
    break;

  case 12:
#line 271 "module/DSExprParse.yy"
    { (yyval.expression) = new ExprFnTfracToSeconds((yysemantic_stack_[(4) - (3)].expression)); ;}
    break;


    /* Line 675 of lalr1.cc.  */
#line 629 "module/DSExprParse.cpp"
	default: break;
      }
    YY_SYMBOL_PRINT ("-> $$ =", yyr1_[yyn], &yyval, &yyloc);

    yypop_ (yylen);
    yylen = 0;
    YY_STACK_PRINT ();

    yysemantic_stack_.push (yyval);
    yylocation_stack_.push (yyloc);

    /* Shift the result of the reduction.  */
    yyn = yyr1_[yyn];
    yystate = yypgoto_[yyn - yyntokens_] + yystate_stack_[0];
    if (0 <= yystate && yystate <= yylast_
	&& yycheck_[yystate] == yystate_stack_[0])
      yystate = yytable_[yystate];
    else
      yystate = yydefgoto_[yyn - yyntokens_];
    goto yynewstate;

  /*------------------------------------.
  | yyerrlab -- here on detecting error |
  `------------------------------------*/
  yyerrlab:
    /* If not already recovering from an error, report this error.  */
    if (!yyerrstatus_)
      {
	++yynerrs_;
	error (yylloc, yysyntax_error_ (yystate, yytoken));
      }

    yyerror_range[0] = yylloc;
    if (yyerrstatus_ == 3)
      {
	/* If just tried and failed to reuse look-ahead token after an
	 error, discard it.  */

	if (yychar <= yyeof_)
	  {
	  /* Return failure if at end of input.  */
	  if (yychar == yyeof_)
	    YYABORT;
	  }
	else
	  {
	    yydestruct_ ("Error: discarding", yytoken, &yylval, &yylloc);
	    yychar = yyempty_;
	  }
      }

    /* Else will try to reuse look-ahead token after shifting the error
       token.  */
    goto yyerrlab1;


  /*---------------------------------------------------.
  | yyerrorlab -- error raised explicitly by YYERROR.  |
  `---------------------------------------------------*/
  yyerrorlab:

    /* Pacify compilers like GCC when the user code never invokes
       YYERROR and the label yyerrorlab therefore never appears in user
       code.  */
    if (false)
      goto yyerrorlab;

    yyerror_range[0] = yylocation_stack_[yylen - 1];
    /* Do not reclaim the symbols of the rule which action triggered
       this YYERROR.  */
    yypop_ (yylen);
    yylen = 0;
    yystate = yystate_stack_[0];
    goto yyerrlab1;

  /*-------------------------------------------------------------.
  | yyerrlab1 -- common code for both syntax error and YYERROR.  |
  `-------------------------------------------------------------*/
  yyerrlab1:
    yyerrstatus_ = 3;	/* Each real token shifted decrements this.  */

    for (;;)
      {
	yyn = yypact_[yystate];
	if (yyn != yypact_ninf_)
	{
	  yyn += yyterror_;
	  if (0 <= yyn && yyn <= yylast_ && yycheck_[yyn] == yyterror_)
	    {
	      yyn = yytable_[yyn];
	      if (0 < yyn)
		break;
	    }
	}

	/* Pop the current state because it cannot handle the error token.  */
	if (yystate_stack_.height () == 1)
	YYABORT;

	yyerror_range[0] = yylocation_stack_[0];
	yydestruct_ ("Error: popping",
		     yystos_[yystate],
		     &yysemantic_stack_[0], &yylocation_stack_[0]);
	yypop_ ();
	yystate = yystate_stack_[0];
	YY_STACK_PRINT ();
      }

    if (yyn == yyfinal_)
      goto yyacceptlab;

    yyerror_range[1] = yylloc;
    // Using YYLLOC is tempting, but would change the location of
    // the look-ahead.  YYLOC is available though.
    YYLLOC_DEFAULT (yyloc, (yyerror_range - 1), 2);
    yysemantic_stack_.push (yylval);
    yylocation_stack_.push (yyloc);

    /* Shift the error token.  */
    YY_SYMBOL_PRINT ("Shifting", yystos_[yyn],
		   &yysemantic_stack_[0], &yylocation_stack_[0]);

    yystate = yyn;
    goto yynewstate;

    /* Accept.  */
  yyacceptlab:
    yyresult = 0;
    goto yyreturn;

    /* Abort.  */
  yyabortlab:
    yyresult = 1;
    goto yyreturn;

  yyreturn:
    if (yychar != yyeof_ && yychar != yyempty_)
      yydestruct_ ("Cleanup: discarding lookahead", yytoken, &yylval, &yylloc);

    /* Do not reclaim the symbols of the rule which action triggered
       this YYABORT or YYACCEPT.  */
    yypop_ (yylen);
    while (yystate_stack_.height () != 1)
      {
	yydestruct_ ("Cleanup: popping",
		   yystos_[yystate_stack_[0]],
		   &yysemantic_stack_[0],
		   &yylocation_stack_[0]);
	yypop_ ();
      }

    return yyresult;
  }

  // Generate an error message.
  std::string
  Parser::yysyntax_error_ (int yystate, int tok)
  {
    std::string res;
    YYUSE (yystate);
#if YYERROR_VERBOSE
    int yyn = yypact_[yystate];
    if (yypact_ninf_ < yyn && yyn <= yylast_)
      {
	/* Start YYX at -YYN if negative to avoid negative indexes in
	   YYCHECK.  */
	int yyxbegin = yyn < 0 ? -yyn : 0;

	/* Stay within bounds of both yycheck and yytname.  */
	int yychecklim = yylast_ - yyn + 1;
	int yyxend = yychecklim < yyntokens_ ? yychecklim : yyntokens_;
	int count = 0;
	for (int x = yyxbegin; x < yyxend; ++x)
	  if (yycheck_[x + yyn] == x && x != yyterror_)
	    ++count;

	// FIXME: This method of building the message is not compatible
	// with internationalization.  It should work like yacc.c does it.
	// That is, first build a string that looks like this:
	// "syntax error, unexpected %s or %s or %s"
	// Then, invoke YY_ on this string.
	// Finally, use the string as a format to output
	// yytname_[tok], etc.
	// Until this gets fixed, this message appears in English only.
	res = "syntax error, unexpected ";
	res += yytnamerr_ (yytname_[tok]);
	if (count < 5)
	  {
	    count = 0;
	    for (int x = yyxbegin; x < yyxend; ++x)
	      if (yycheck_[x + yyn] == x && x != yyterror_)
		{
		  res += (!count++) ? ", expecting " : " or ";
		  res += yytnamerr_ (yytname_[x]);
		}
	  }
      }
    else
#endif
      res = YY_("syntax error");
    return res;
  }


  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
  const signed char Parser::yypact_ninf_ = -9;
  const signed char
  Parser::yypact_[] =
  {
        16,    -9,    -9,    -6,    16,     3,     6,     8,    16,    21,
      -9,    -9,    -9,    16,    16,    16,    16,    16,    27,    -9,
      15,    -8,    -8,    -9,    -9,    -9
  };

  /* YYDEFACT[S] -- default rule to reduce with in state S when YYTABLE
     doesn't specify something else to do.  Zero means the default is an
     error.  */
  const unsigned char
  Parser::yydefact_[] =
  {
         0,    10,    11,     0,     0,     0,     0,     0,     0,     0,
       1,     3,     2,     0,     0,     0,     0,     0,     0,     9,
       4,     5,     6,     7,     8,    12
  };

  /* YYPGOTO[NTERM-NUM].  */
  const signed char
  Parser::yypgoto_[] =
  {
        -9,    -9,    -9,    -4
  };

  /* YYDEFGOTO[NTERM-NUM].  */
  const signed char
  Parser::yydefgoto_[] =
  {
        -1,     5,     6,     7
  };

  /* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule which
     number is the opposite.  If zero, do what YYDEFACT says.  */
  const signed char Parser::yytable_ninf_ = -1;
  const unsigned char
  Parser::yytable_[] =
  {
         9,    16,    17,    10,    18,     8,    11,     0,    12,    20,
      21,    22,    23,    24,    13,    14,    15,    16,    17,     1,
       2,     3,    14,    15,    16,    17,     0,     4,    14,    15,
      16,    17,     0,    19,    14,    15,    16,    17,     0,    25
  };

  /* YYCHECK.  */
  const signed char
  Parser::yycheck_[] =
  {
         4,     9,    10,     0,     8,    11,     0,    -1,     0,    13,
      14,    15,    16,    17,     6,     7,     8,     9,    10,     3,
       4,     5,     7,     8,     9,    10,    -1,    11,     7,     8,
       9,    10,    -1,    12,     7,     8,     9,    10,    -1,    12
  };

  /* STOS_[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
  const unsigned char
  Parser::yystos_[] =
  {
         0,     3,     4,     5,    11,    14,    15,    16,    11,    16,
       0,     0,     0,     6,     7,     8,     9,    10,    16,    12,
      16,    16,    16,    16,    16,    12
  };

#if YYDEBUG
  /* TOKEN_NUMBER_[YYLEX-NUM] -- Internal symbol number corresponding
     to YYLEX-NUM.  */
  const unsigned short int
  Parser::yytoken_number_[] =
  {
         0,   256,   257,   258,   259,   260,    62,    43,    45,    42,
      47,    40,    41
  };
#endif

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
  const unsigned char
  Parser::yyr1_[] =
  {
         0,    13,    14,    14,    15,    16,    16,    16,    16,    16,
      16,    16,    16
  };

  /* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
  const unsigned char
  Parser::yyr2_[] =
  {
         0,     2,     2,     2,     3,     3,     3,     3,     3,     3,
       1,     1,     4
  };

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
  /* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
     First, the terminals, then, starting at \a yyntokens_, nonterminals.  */
  const char*
  const Parser::yytname_[] =
  {
    "END_OF_STRING", "error", "$undefined", "FIELD", "CONSTANT",
  "FN_TfracToSeconds", "'>'", "'+'", "'-'", "'*'", "'/'", "'('", "')'",
  "$accept", "complete_expr", "bool_expr", "expr", 0
  };
#endif

#if YYDEBUG
  /* YYRHS -- A `-1'-separated list of the rules' RHS.  */
  const Parser::rhs_number_type
  Parser::yyrhs_[] =
  {
        14,     0,    -1,    16,     0,    -1,    15,     0,    -1,    16,
       6,    16,    -1,    16,     7,    16,    -1,    16,     8,    16,
      -1,    16,     9,    16,    -1,    16,    10,    16,    -1,    11,
      16,    12,    -1,     3,    -1,     4,    -1,     5,    11,    16,
      12,    -1
  };

  /* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
     YYRHS.  */
  const unsigned char
  Parser::yyprhs_[] =
  {
         0,     0,     3,     6,     9,    13,    17,    21,    25,    29,
      33,    35,    37
  };

  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
  const unsigned short int
  Parser::yyrline_[] =
  {
         0,   256,   256,   257,   259,   264,   265,   266,   267,   268,
     269,   270,   271
  };

  // Print the state stack on the debug stream.
  void
  Parser::yystack_print_ ()
  {
    *yycdebug_ << "Stack now";
    for (state_stack_type::const_iterator i = yystate_stack_.begin ();
	 i != yystate_stack_.end (); ++i)
      *yycdebug_ << ' ' << *i;
    *yycdebug_ << std::endl;
  }

  // Report on the debug stream that the rule \a yyrule is going to be reduced.
  void
  Parser::yy_reduce_print_ (int yyrule)
  {
    unsigned int yylno = yyrline_[yyrule];
    int yynrhs = yyr2_[yyrule];
    /* Print the symbols being reduced, and their result.  */
    *yycdebug_ << "Reducing stack by rule " << yyrule - 1
	       << " (line " << yylno << "), ";
    /* The symbols being reduced.  */
    for (int yyi = 0; yyi < yynrhs; yyi++)
      YY_SYMBOL_PRINT ("   $" << yyi + 1 << " =",
		       yyrhs_[yyprhs_[yyrule] + yyi],
		       &(yysemantic_stack_[(yynrhs) - (yyi + 1)]),
		       &(yylocation_stack_[(yynrhs) - (yyi + 1)]));
  }
#endif // YYDEBUG

  /* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
  Parser::token_number_type
  Parser::yytranslate_ (int t)
  {
    static
    const token_number_type
    translate_table[] =
    {
           0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      11,    12,     9,     7,     2,     8,     2,    10,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     6,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5
    };
    if ((unsigned int) t <= yyuser_token_number_max_)
      return translate_table[t];
    else
      return yyundef_token_;
  }

  const int Parser::yyeof_ = 0;
  const int Parser::yylast_ = 39;
  const int Parser::yynnts_ = 4;
  const int Parser::yyempty_ = -2;
  const int Parser::yyfinal_ = 10;
  const int Parser::yyterror_ = 1;
  const int Parser::yyerrcode_ = 256;
  const int Parser::yyntokens_ = 13;

  const unsigned int Parser::yyuser_token_number_max_ = 260;
  const Parser::token_number_type Parser::yyundef_token_ = 2;

} // namespace DSExprImpl

#line 274 "module/DSExprParse.yy"


void
DSExprImpl::Parser::error(const DSExprImpl::location &,
			  const std::string &err)
{
	FATAL_ERROR(boost::format("error parsing: %s") % err);
}

