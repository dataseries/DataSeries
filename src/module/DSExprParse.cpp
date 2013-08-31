
/* A Bison parser, made by GNU Bison 2.4.1.  */

/* Skeleton implementation for Bison LALR(1) parsers in C++
   
   Copyright (C) 2002, 2003, 2004, 2005, 2006, 2007, 2008 Free Software
   Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

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

/* First part of user declarations.  */

/* Line 311 of lalr1.cc  */
#line 18 "module/DSExprParse.yy"

#include "DSExprImpl.hpp"


/* Line 311 of lalr1.cc  */
#line 48 "module/DSExprParse.cpp"


#include "DSExprParse.hpp"

/* User implementation prologue.  */

/* Line 317 of lalr1.cc  */
#line 35 "module/DSExprParse.yy"

YY_DECL;

#undef yylex
#define yylex DSExprScanlex

using namespace DSExprImpl;


/* Line 317 of lalr1.cc  */
#line 67 "module/DSExprParse.cpp"

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

/* Enable debugging if requested.  */
#if YYDEBUG

/* A pseudo ostream that takes yydebug_ into account.  */
# define YYCDEBUG if (yydebug_) (*yycdebug_)

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)          \
    do {                                                        \
        if (yydebug_)                                           \
        {                                                       \
            *yycdebug_ << Title << ' ';                         \
            yy_symbol_print_ ((Type), (Value), (Location));     \
            *yycdebug_ << std::endl;                            \
        }                                                       \
    } while (false)

# define YY_REDUCE_PRINT(Rule)                  \
    do {                                        \
        if (yydebug_)                           \
            yy_reduce_print_ (Rule);            \
    } while (false)

# define YY_STACK_PRINT()                       \
    do {                                        \
        if (yydebug_)                           \
            yystack_print_ ();                  \
    } while (false)

#else /* !YYDEBUG */

# define YYCDEBUG if (false) std::cerr
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_REDUCE_PRINT(Rule)
# define YY_STACK_PRINT()

#endif /* !YYDEBUG */

#define yyerrok         (yyerrstatus_ = 0)
#define yyclearin       (yychar = yyempty_)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYRECOVERING()  (!!yyerrstatus_)


/* Line 380 of lalr1.cc  */
#line 1 "[Bison:b4_percent_define_default]"

namespace DSExprImpl {

    /* Line 380 of lalr1.cc  */
#line 136 "module/DSExprParse.cpp"
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
            :
#if YYDEBUG
            yydebug_ (false),
            yycdebug_ (&std::cerr),
#endif
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
#endif

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

#if YYDEBUG
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
#endif

    int
    Parser::parse ()
    {
        /// Lookahead and lookahead in internal form.
        int yychar = yyempty_;
        int yytoken = 0;

        /* State.  */
        int yyn;
        int yylen = 0;
        int yystate = 0;

        /* Error handling.  */
        int yynerrs_ = 0;
        int yyerrstatus_ = 0;

        /// Semantic value of the lookahead.
        semantic_type yylval;
        /// Location of the lookahead.
        location_type yylloc;
        /// The locations where the error started and ended.
        location_type yyerror_range[2];

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

        /* Accept?  */
        if (yystate == yyfinal_)
            goto yyacceptlab;

        goto yybackup;

        /* Backup.  */
  yybackup:

        /* Try to take a decision without lookahead.  */
        yyn = yypact_[yystate];
        if (yyn == yypact_ninf_)
            goto yydefault;

        /* Read a lookahead token.  */
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

        /* Shift the lookahead token.  */
        YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

        /* Discard the token being shifted.  */
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

                /* Line 678 of lalr1.cc  */
#line 81 "module/DSExprParse.yy"
                { driver.expr = (yysemantic_stack_[(2) - (1)].expression); }
                break;

            case 5:

                /* Line 678 of lalr1.cc  */
#line 90 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprEq((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); }
                break;

            case 6:

                /* Line 678 of lalr1.cc  */
#line 91 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprNeq((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); }
                break;

            case 7:

                /* Line 678 of lalr1.cc  */
#line 92 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprGt((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); }
                break;

            case 8:

                /* Line 678 of lalr1.cc  */
#line 93 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprLt((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); }
                break;

            case 9:

                /* Line 678 of lalr1.cc  */
#line 94 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprGeq((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); }
                break;

            case 10:

                /* Line 678 of lalr1.cc  */
#line 95 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprLeq((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); }
                break;

            case 11:

                /* Line 678 of lalr1.cc  */
#line 96 "module/DSExprParse.yy"
                { FATAL_ERROR("not implemented yet"); }
                break;

            case 12:

                /* Line 678 of lalr1.cc  */
#line 100 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprLor((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); }
                break;

            case 13:

                /* Line 678 of lalr1.cc  */
#line 101 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprLand((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); }
                break;

            case 14:

                /* Line 678 of lalr1.cc  */
#line 102 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprLnot((yysemantic_stack_[(2) - (2)].expression)); }
                break;

            case 15:

                /* Line 678 of lalr1.cc  */
#line 103 "module/DSExprParse.yy"
                { (yyval.expression) = (yysemantic_stack_[(3) - (2)].expression); }
                break;

            case 17:

                /* Line 678 of lalr1.cc  */
#line 108 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprAdd((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); }
                break;

            case 18:

                /* Line 678 of lalr1.cc  */
#line 109 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprSubtract((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); }
                break;

            case 19:

                /* Line 678 of lalr1.cc  */
#line 110 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprMultiply((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); }
                break;

            case 20:

                /* Line 678 of lalr1.cc  */
#line 111 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprDivide((yysemantic_stack_[(3) - (1)].expression), (yysemantic_stack_[(3) - (3)].expression)); }
                break;

            case 21:

                /* Line 678 of lalr1.cc  */
#line 112 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprMinus((yysemantic_stack_[(2) - (2)].expression)); }
                break;

            case 22:

                /* Line 678 of lalr1.cc  */
#line 113 "module/DSExprParse.yy"
                { (yyval.expression) = (yysemantic_stack_[(3) - (2)].expression); }
                break;

            case 23:

                /* Line 678 of lalr1.cc  */
#line 114 "module/DSExprParse.yy"
                { (yyval.expression) = driver.makeExprField(*(yysemantic_stack_[(1) - (1)].symbol)); }
                break;

            case 24:

                /* Line 678 of lalr1.cc  */
#line 115 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprNumericConstant((yysemantic_stack_[(1) - (1)].constant)); }
                break;

            case 25:

                /* Line 678 of lalr1.cc  */
#line 116 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprStrLiteral(*(yysemantic_stack_[(1) - (1)].strliteral)); }
                break;

            case 26:

                /* Line 678 of lalr1.cc  */
#line 117 "module/DSExprParse.yy"
                { driver.current_fnargs.clear(); }
                break;

            case 27:

                /* Line 678 of lalr1.cc  */
#line 118 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprFunctionApplication(*(yysemantic_stack_[(5) - (1)].symbol), driver.current_fnargs); 
                    driver.current_fnargs.clear();
                }
                break;

            case 28:

                /* Line 678 of lalr1.cc  */
#line 121 "module/DSExprParse.yy"
                { (yyval.expression) = new ExprFnTfracToSeconds((yysemantic_stack_[(4) - (3)].expression)); }
                break;

            case 31:

                /* Line 678 of lalr1.cc  */
#line 130 "module/DSExprParse.yy"
                { driver.current_fnargs.push_back((yysemantic_stack_[(1) - (1)].expression)); }
                break;

            case 32:

                /* Line 678 of lalr1.cc  */
#line 131 "module/DSExprParse.yy"
                { driver.current_fnargs.push_back((yysemantic_stack_[(3) - (3)].expression)); }
                break;



                /* Line 678 of lalr1.cc  */
#line 612 "module/DSExprParse.cpp"
            default:
                break;
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
            /* If just tried and failed to reuse lookahead token after an
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

        /* Else will try to reuse lookahead token after shifting the error
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
        yyerrstatus_ = 3;   /* Each real token shifted decrements this.  */

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

        yyerror_range[1] = yylloc;
        // Using YYLLOC is tempting, but would change the location of
        // the lookahead.  YYLOC is available though.
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
        if (yychar != yyempty_)
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
    const signed char Parser::yypact_ninf_ = -19;
    const signed char
    Parser::yypact_[] =
    {
        34,   -18,   -19,   -19,    -8,    34,    -2,    34,    24,    41,
        -19,     5,    73,   -19,    -2,   -19,    73,    -2,   -19,    -6,
        55,   -19,   -19,    34,    34,    -2,    -2,    -2,    -2,    -2,
        -2,    -2,    -2,    -2,    -2,    -2,    -2,    29,    77,   -19,
        -19,    28,   -19,    -7,    -7,    -7,    -7,    -7,    -7,    -7,
        3,     3,   -19,   -19,    -7,    21,    26,   -19,   -19,    -2,
        -7
    };

    /* YYDEFACT[S] -- default rule to reduce with in state S when YYTABLE
       doesn't specify something else to do.  Zero means the default is an
       error.  */
    const unsigned char
    Parser::yydefact_[] =
    {
        0,    23,    24,    25,     0,     0,     0,     0,     0,     0,
        16,     4,     3,    26,     0,    14,     0,     0,    21,     0,
        0,     1,     2,     0,     0,     0,     0,     0,     0,     0,
        0,     0,     0,     0,     0,     0,    29,     0,     0,    15,
        22,    12,    13,     5,     6,    11,     7,     8,     9,    10,
        17,    18,    19,    20,    31,     0,    30,    28,    27,     0,
        32
    };

    /* YYPGOTO[NTERM-NUM].  */
    const signed char
    Parser::yypgoto_[] =
    {
        -19,   -19,   -19,   -19,    37,     0,   -19,   -19,   -19
    };

    /* YYDEFGOTO[NTERM-NUM].  */
    const signed char
    Parser::yydefgoto_[] =
    {
        -1,     8,     9,    10,    11,    16,    36,    55,    56
    };

    /* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
       positive, shift that token.  If negative, reduce the rule which
       number is the opposite.  If zero, do what YYDEFACT says.  */
    const signed char Parser::yytable_ninf_ = -1;
    const unsigned char
    Parser::yytable_[] =
    {
        12,     1,     2,     3,     4,    13,    18,    20,    23,    24,
        32,    33,    34,    35,    37,    14,     6,    38,    39,    23,
        24,    17,    34,    35,    21,    43,    44,    45,    46,    47,
        48,    49,    50,    51,    52,    53,    54,     1,     2,     3,
        4,    22,    15,    24,    19,    58,    32,    33,    34,    35,
        5,    59,     6,    57,     0,     0,     0,     7,     0,    60,
        41,    42,    25,    26,    27,    28,    29,    30,    31,     0,
        0,     0,    32,    33,    34,    35,     0,     0,     0,    40,
        25,    26,    27,    28,    29,    30,    31,     0,     0,     0,
        32,    33,    34,    35,    32,    33,    34,    35,     0,     0,
        0,    40
    };

    /* YYCHECK.  */
    const signed char
    Parser::yycheck_[] =
    {
        0,     3,     4,     5,     6,    23,     6,     7,    14,    15,
        17,    18,    19,    20,    14,    23,    18,    17,    24,    14,
        15,    23,    19,    20,     0,    25,    26,    27,    28,    29,
        30,    31,    32,    33,    34,    35,    36,     3,     4,     5,
        6,     0,     5,    15,     7,    24,    17,    18,    19,    20,
        16,    25,    18,    24,    -1,    -1,    -1,    23,    -1,    59,
        23,    24,     7,     8,     9,    10,    11,    12,    13,    -1,
        -1,    -1,    17,    18,    19,    20,    -1,    -1,    -1,    24,
        7,     8,     9,    10,    11,    12,    13,    -1,    -1,    -1,
        17,    18,    19,    20,    17,    18,    19,    20,    -1,    -1,
        -1,    24
    };

    /* STOS_[STATE-NUM] -- The (internal number of the) accessing
       symbol of state STATE-NUM.  */
    const unsigned char
    Parser::yystos_[] =
    {
        0,     3,     4,     5,     6,    16,    18,    23,    27,    28,
        29,    30,    31,    23,    23,    30,    31,    23,    31,    30,
        31,     0,     0,    14,    15,     7,     8,     9,    10,    11,
        12,    13,    17,    18,    19,    20,    32,    31,    31,    24,
        24,    30,    30,    31,    31,    31,    31,    31,    31,    31,
        31,    31,    31,    31,    31,    33,    34,    24,    24,    25,
        31
    };

#if YYDEBUG
    /* TOKEN_NUMBER_[YYLEX-NUM] -- Internal symbol number corresponding
       to YYLEX-NUM.  */
    const unsigned short int
    Parser::yytoken_number_[] =
    {
        0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
        265,   266,   267,   268,   269,   270,   271,    43,    45,    42,
        47,   272,   273,    40,    41,    44
    };
#endif

    /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
    const unsigned char
    Parser::yyr1_[] =
    {
        0,    26,    27,    28,    28,    29,    29,    29,    29,    29,
        29,    29,    30,    30,    30,    30,    30,    31,    31,    31,
        31,    31,    31,    31,    31,    31,    32,    31,    31,    33,
        33,    34,    34
    };

    /* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
    const unsigned char
    Parser::yyr2_[] =
    {
        0,     2,     2,     1,     1,     3,     3,     3,     3,     3,
        3,     3,     3,     3,     2,     3,     1,     3,     3,     3,
        3,     2,     3,     1,     1,     1,     0,     5,     4,     0,
        1,     1,     3
    };

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
    /* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
       First, the terminals, then, starting at \a yyntokens_, nonterminals.  */
    const char*
    const Parser::yytname_[] =
    {
        "END_OF_STRING", "error", "$undefined", "SYMBOL", "CONSTANT",
        "STRLITERAL", "FN_TfracToSeconds", "EQ", "NEQ", "REMATCH", "GT", "LT",
        "GEQ", "LEQ", "LOR", "LAND", "LNOT", "'+'", "'-'", "'*'", "'/'", "ULNOT",
        "UMINUS", "'('", "')'", "','", "$accept", "complete_expr",
        "top_level_expr", "rel_expr", "bool_expr", "expr", "$@1", "fnargs",
        "fnargs1", 0
    };
#endif

#if YYDEBUG
    /* YYRHS -- A `-1'-separated list of the rules' RHS.  */
    const Parser::rhs_number_type
    Parser::yyrhs_[] =
    {
        27,     0,    -1,    28,     0,    -1,    31,    -1,    30,    -1,
        31,     7,    31,    -1,    31,     8,    31,    -1,    31,    10,
        31,    -1,    31,    11,    31,    -1,    31,    12,    31,    -1,
        31,    13,    31,    -1,    31,     9,    31,    -1,    30,    14,
        30,    -1,    30,    15,    30,    -1,    16,    30,    -1,    23,
        30,    24,    -1,    29,    -1,    31,    17,    31,    -1,    31,
        18,    31,    -1,    31,    19,    31,    -1,    31,    20,    31,
        -1,    18,    31,    -1,    23,    31,    24,    -1,     3,    -1,
        4,    -1,     5,    -1,    -1,     3,    23,    32,    33,    24,
        -1,     6,    23,    31,    24,    -1,    -1,    34,    -1,    31,
        -1,    34,    25,    31,    -1
    };

    /* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
       YYRHS.  */
    const unsigned char
    Parser::yyprhs_[] =
    {
        0,     0,     3,     6,     8,    10,    14,    18,    22,    26,
        30,    34,    38,    42,    46,    49,    53,    55,    59,    63,
        67,    71,    74,    78,    80,    82,    84,    85,    91,    96,
        97,    99,   101
    };

    /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
    const unsigned char
    Parser::yyrline_[] =
    {
        0,    81,    81,    85,    86,    90,    91,    92,    93,    94,
        95,    96,   100,   101,   102,   103,   104,   108,   109,   110,
        111,   112,   113,   114,   115,   116,   117,   117,   121,   124,
        126,   130,   131
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
                   << " (line " << yylno << "):" << std::endl;
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
                    23,    24,    19,    17,    25,    18,     2,    20,     2,     2,
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
                    2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
                    2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
                    2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
                    5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
                    15,    16,    21,    22
                };
        if ((unsigned int) t <= yyuser_token_number_max_)
            return translate_table[t];
        else
            return yyundef_token_;
    }

    const int Parser::yyeof_ = 0;
    const int Parser::yylast_ = 101;
    const int Parser::yynnts_ = 9;
    const int Parser::yyempty_ = -2;
    const int Parser::yyfinal_ = 21;
    const int Parser::yyterror_ = 1;
    const int Parser::yyerrcode_ = 256;
    const int Parser::yyntokens_ = 26;

    const unsigned int Parser::yyuser_token_number_max_ = 273;
    const Parser::token_number_type Parser::yyundef_token_ = 2;


    /* Line 1054 of lalr1.cc  */
#line 1 "[Bison:b4_percent_define_default]"

} // DSExprImpl

/* Line 1054 of lalr1.cc  */
#line 1087 "module/DSExprParse.cpp"


