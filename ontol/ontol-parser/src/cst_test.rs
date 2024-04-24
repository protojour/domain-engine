use indoc::indoc;
use pretty_assertions::assert_eq;

use crate::{
    cst::{
        grammar::{expr_pattern, ontol, pattern_with_expr},
        parser::CstParser,
    },
    lexer::LexedSource,
};

fn parser(src: &str) -> CstParser {
    let (lexed, _errors) = LexedSource::lex(src);
    CstParser::from_lexed_source(src, lexed)
}

fn parse_fmt_ok(src: &str, parse_fn: impl Fn(&mut CstParser)) -> String {
    let mut parser = parser(src);
    parse_fn(&mut parser);
    let (tree, errors) = parser.finish();

    if !errors.is_empty() {
        println!("ERRORS: {errors:?}");
    }

    format!("{}", tree)
}

fn parse_fmt_err(src: &str, parse_fn: impl Fn(&mut CstParser)) -> String {
    let mut parser = parser(src);
    parse_fn(&mut parser);
    let (tree, errors) = parser.finish();

    assert!(errors.len() > 0);

    format!("{}", tree)
}

mod test_pattern_lookahead {
    use crate::cst::grammar::{lookahead_detect_pattern, DetectedPattern};

    use super::parser;

    #[test]
    fn test_anonymous_unit_struct() {
        let src = "()";
        assert_eq!(
            lookahead_detect_pattern(&parser(src)),
            DetectedPattern::Struct
        )
    }

    #[test]
    fn test_named_empty_struct() {
        let src = "foo()";
        assert_eq!(
            lookahead_detect_pattern(&parser(src)),
            DetectedPattern::Struct
        )
    }

    #[test]
    fn test_named_struct_binding() {
        let src = "foo(bar)";
        assert_eq!(
            lookahead_detect_pattern(&parser(src)),
            DetectedPattern::Struct
        )
    }

    #[test]
    fn anonymous_struct_params() {
        let src = "('sort': ord)";
        assert_eq!(
            lookahead_detect_pattern(&parser(src)),
            DetectedPattern::Struct
        )
    }

    #[test]
    fn test_unit_expr() {
        let src = "(bar)";
        assert_eq!(
            lookahead_detect_pattern(&parser(src)),
            DetectedPattern::Expr
        )
    }

    #[test]
    fn test_binary_expr() {
        let src = "(1 + 2)";
        assert_eq!(
            lookahead_detect_pattern(&parser(src)),
            DetectedPattern::Expr
        )
    }
}

#[test]
fn test_use_statement() {
    let src = indoc! {
        "
        /// comment
        use 'a' as b
        "
    };
    assert_eq!(
        indoc! {"
            Ontol
                UseStatement
                    DocComment
                    Whitespace
                    KwUse
                    Whitespace
                    Location
                        SingleQuoteText
                    Whitespace
                    Sym
                    Whitespace
                    IdentPath
                        Sym
                    Whitespace
            "
        },
        parse_fmt_ok(src, ontol),
    );
}

#[test]
fn test_def_statement_empty() {
    let src = indoc! {
        "
        def a (
            // empty
        )
        "
    };
    assert_eq!(
        indoc! {"
            Ontol
                DefStatement
                    KwDef
                    Whitespace
                    IdentPath
                        Sym
                    Whitespace
                    DefBody
                        ParenOpen
                        Whitespace
                        Comment
                        Whitespace
                        ParenClose
                    Whitespace
            "
        },
        parse_fmt_ok(src, ontol),
    );
}

#[test]
fn test_def_statement() {
    let src = indoc! {
        "
        /// comment
        def a (
            def b ()
        )
        "
    };
    assert_eq!(
        indoc! {"
            Ontol
                DefStatement
                    DocComment
                    Whitespace
                    KwDef
                    Whitespace
                    IdentPath
                        Sym
                    Whitespace
                    DefBody
                        ParenOpen
                        DefStatement
                            Whitespace
                            KwDef
                            Whitespace
                            IdentPath
                                Sym
                            Whitespace
                            DefBody
                                ParenOpen
                                ParenClose
                            Whitespace
                        ParenClose
                    Whitespace
            "
        },
        parse_fmt_ok(src, ontol),
    );
}

#[test]
fn test_def_statement_error1() {
    let src = "def";
    assert_eq!(
        indoc! {"
            Ontol
                DefStatement
                    KwDef
                    IdentPath
                        Error
                    DefBody
                        Error
                        Error
            "
        },
        parse_fmt_err(src, ontol),
    );
}

#[test]
fn test_rel1() {
    let src = indoc! {
        "
        /// comment
        rel {.} 'id'[rel .gen: auto]?|id: text
        "
    };
    assert_eq!(
        indoc! {"
            Ontol
                RelStatement
                    DocComment
                    Whitespace
                    KwRel
                    Whitespace
                    RelSubject
                        SetTypeRef
                            CurlyOpen
                            This
                                Dot
                            CurlyClose
                    RelFwdSet
                        Relation
                            Whitespace
                            UnitTypeRef
                                Literal
                                    SingleQuoteText
                            RelParams
                                SquareOpen
                                RelStatement
                                    KwRel
                                    Whitespace
                                    RelSubject
                                        UnitTypeRef
                                            This
                                                Dot
                                    RelFwdSet
                                        Relation
                                            UnitTypeRef
                                                IdentPath
                                                    Sym
                                            PropCardinality
                                    Colon
                                    Whitespace
                                    RelObject
                                        UnitTypeRef
                                            IdentPath
                                                Sym
                                SquareClose
                            PropCardinality
                                Question
                        Pipe
                        Relation
                            UnitTypeRef
                                IdentPath
                                    Sym
                            PropCardinality
                    Colon
                    Whitespace
                    RelObject
                        UnitTypeRef
                            IdentPath
                                Sym
                                Whitespace
            "
        },
        parse_fmt_ok(src, ontol),
    );
}

#[test]
fn test_rel2() {
    let src = "rel .'a'::'b'? {c}";
    assert_eq!(
        indoc! {"
            Ontol
                RelStatement
                    KwRel
                    Whitespace
                    RelSubject
                        UnitTypeRef
                            This
                                Dot
                    RelFwdSet
                        Relation
                            UnitTypeRef
                                Literal
                                    SingleQuoteText
                            PropCardinality
                    Colon
                    Colon
                    RelBackwdSet
                        IdentPath
                            SingleQuoteText
                        PropCardinality
                            Question
                        Whitespace
                    RelObject
                        SetTypeRef
                            CurlyOpen
                            IdentPath
                                Sym
                            CurlyClose
            "
        },
        parse_fmt_ok(src, ontol),
    );
}

#[test]
fn test_map1() {
    let src = indoc! {
        "
        /// comment
        map foo(
            ('a': x, 'b'?: y),
            named (
                'b': y
            )
        )
        "
    };
    assert_eq!(
        indoc! {"
            Ontol
                MapStatement
                    DocComment
                    Whitespace
                    KwMap
                    Whitespace
                    IdentPath
                        Sym
                    ParenOpen
                    Whitespace
                    MapArm
                        PatStruct
                            ParenOpen
                            StructParamAttrProp
                                UnitTypeRef
                                    Literal
                                        SingleQuoteText
                                Colon
                                Whitespace
                                PatAtom
                                    Sym
                            Comma
                            Whitespace
                            StructParamAttrProp
                                UnitTypeRef
                                    Literal
                                        SingleQuoteText
                                PropCardinality
                                    Question
                                Colon
                                Whitespace
                                PatAtom
                                    Sym
                            ParenClose
                    Comma
                    Whitespace
                    MapArm
                        PatStruct
                            IdentPath
                                Sym
                                Whitespace
                            ParenOpen
                            Whitespace
                            StructParamAttrProp
                                UnitTypeRef
                                    Literal
                                        SingleQuoteText
                                Colon
                                Whitespace
                                PatAtom
                                    Sym
                                Whitespace
                            ParenClose
                    Whitespace
                    ParenClose
                    Whitespace
            "
        },
        parse_fmt_ok(src, ontol),
    );
}

#[test]
fn test_map_unit_structs() {
    let src = "map foo(foo(), bar(x))";
    assert_eq!(
        indoc! {"
            Ontol
                MapStatement
                    KwMap
                    Whitespace
                    IdentPath
                        Sym
                    ParenOpen
                    MapArm
                        PatStruct
                            IdentPath
                                Sym
                            ParenOpen
                            ParenClose
                    Comma
                    Whitespace
                    MapArm
                        PatStruct
                            IdentPath
                                Sym
                            ParenOpen
                            StructParamAttrUnit
                                PatAtom
                                    Sym
                            ParenClose
                    ParenClose
            "
        },
        parse_fmt_ok(src, ontol),
    );
}

#[test]
fn test_map_set() {
    let src = indoc! {
        "
        /// comment
        map foo(
            {..@match foo(a) },
            { a, ..b }
        )
        "
    };
    assert_eq!(
        indoc! {"
            Ontol
                MapStatement
                    DocComment
                    Whitespace
                    KwMap
                    Whitespace
                    IdentPath
                        Sym
                    ParenOpen
                    Whitespace
                    MapArm
                        PatSet
                            CurlyOpen
                            SetElement
                                Spread
                                    DotDot
                                PatStruct
                                    Modifier
                                    Whitespace
                                    IdentPath
                                        Sym
                                    ParenOpen
                                    StructParamAttrUnit
                                        PatAtom
                                            Sym
                                    ParenClose
                            Whitespace
                            CurlyClose
                    Comma
                    Whitespace
                    MapArm
                        PatSet
                            CurlyOpen
                            Whitespace
                            SetElement
                                PatAtom
                                    Sym
                            Comma
                            Whitespace
                            SetElement
                                Spread
                                    DotDot
                                PatAtom
                                    Sym
                                Whitespace
                            CurlyClose
                    Whitespace
                    ParenClose
                    Whitespace
            "
        },
        parse_fmt_ok(src, ontol),
    );
}

#[test]
fn test_pattern_complex1() {
    let src = "@match bar('baz': @in { ..x })";
    assert_eq!(
        indoc! {"
            PatStruct
                Modifier
                Whitespace
                IdentPath
                    Sym
                ParenOpen
                StructParamAttrProp
                    UnitTypeRef
                        Literal
                            SingleQuoteText
                    Colon
                    PatSet
                        Whitespace
                        Modifier
                        Whitespace
                        CurlyOpen
                        Whitespace
                        SetElement
                            Spread
                                DotDot
                            PatAtom
                                Sym
                            Whitespace
                        CurlyClose
                ParenClose
            "
        },
        parse_fmt_ok(src, pattern_with_expr),
    );
}

#[test]
fn test_pattern_dot_ident_struct() {
    let src = "foo.bar('baz': x)";
    assert_eq!(
        indoc! {"
            PatStruct
                IdentPath
                    Sym
                    Dot
                    Sym
                ParenOpen
                StructParamAttrProp
                    UnitTypeRef
                        Literal
                            SingleQuoteText
                    Colon
                    Whitespace
                    PatAtom
                        Sym
                ParenClose
            "
        },
        parse_fmt_ok(src, pattern_with_expr),
    );
}

#[test]
fn test_pattern_struct_in_struct() {
    let src = "a(b('c': d))";
    assert_eq!(
        indoc! {"
            PatStruct
                IdentPath
                    Sym
                ParenOpen
                StructParamAttrUnit
                    PatStruct
                        IdentPath
                            Sym
                        ParenOpen
                        StructParamAttrProp
                            UnitTypeRef
                                Literal
                                    SingleQuoteText
                            Colon
                            Whitespace
                            PatAtom
                                Sym
                        ParenClose
                ParenClose
            "
        },
        parse_fmt_ok(src, pattern_with_expr),
    );
}

#[test]
fn test_pattern_atom() {
    assert_eq!(
        indoc! {"
            PatAtom
                Sym
            "
        },
        parse_fmt_ok("a", expr_pattern::entry),
    );
    assert_eq!(
        indoc! {"
            PatAtom
                SingleQuoteText
            "
        },
        parse_fmt_ok("'a'", expr_pattern::entry),
    );
}

#[test]
fn test_expr_binary1() {
    assert_eq!(
        indoc! {"
            PatBinary
                PatAtom
                    Number
                Plus
                PatAtom
                    Number
            "
        },
        parse_fmt_ok("1+2", expr_pattern::entry),
    );
}

#[test]
fn test_expr_binary2() {
    assert_eq!(
        indoc! {"
            PatBinary
                PatAtom
                    Number
                Plus
                PatBinary
                    PatAtom
                        Number
                    Plus
                    PatAtom
                        Number
            "
        },
        parse_fmt_ok("1+2+3", expr_pattern::entry),
    );
}

#[test]
fn test_expr_binary3() {
    assert_eq!(
        indoc! {"
            PatBinary
                PatBinary
                    PatAtom
                        Number
                    Star
                    PatAtom
                        Number
                Plus
                PatAtom
                    Number
            "
        },
        parse_fmt_ok("1*2+3", expr_pattern::entry),
    );
}

#[test]
fn test_expr_binary4() {
    assert_eq!(
        indoc! {"
            PatBinary
                PatAtom
                    Number
                Plus
                PatBinary
                    PatAtom
                        Number
                    Star
                    PatAtom
                        Number
            "
        },
        parse_fmt_ok("1+2*3", expr_pattern::entry),
    );
}

#[test]
fn test_expr_binary5() {
    assert_eq!(
        indoc! {"
            PatBinary
                PatAtom
                    Number
                Plus
                PatBinary
                    PatBinary
                        PatAtom
                            Number
                        Star
                        PatAtom
                            Number
                    Plus
                    PatAtom
                        Number
            "
        },
        parse_fmt_ok("1+2*3+5", expr_pattern::entry),
    );
}

#[test]
fn test_expr_binary6() {
    assert_eq!(
        indoc! {"
            PatBinary
                PatBinary
                    PatAtom
                        Number
                    Star
                    PatAtom
                        Number
                Plus
                PatBinary
                    PatAtom
                        Number
                    Star
                    PatAtom
                        Number
            "
        },
        parse_fmt_ok("1*2+3*5", expr_pattern::entry),
    );
}

#[test]
fn test_expr_binary7() {
    assert_eq!(
        indoc! {"
            Whitespace
            PatBinary
                PatAtom
                    Number
                Whitespace
                Star
                PatBinary
                    Whitespace
                    ParenOpen
                    Whitespace
                    PatBinary
                        PatAtom
                            Number
                        Whitespace
                        Plus
                        Whitespace
                        PatAtom
                            Number
                        Whitespace
                    ParenClose
                    Whitespace
                    Star
                    Whitespace
                    PatAtom
                        Number
                    Whitespace
            "
        },
        parse_fmt_ok(" 1 * ( 2 + 3 ) * 5 ", expr_pattern::entry),
    );
}
