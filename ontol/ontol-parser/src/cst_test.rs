use indoc::indoc;
use pretty_assertions::assert_eq;

use crate::cst::{grammar::expr_pattern, parser::CstParser};

fn test_parse_fmt(input: &str, parse_fn: impl Fn(&mut CstParser)) -> String {
    let mut parser = CstParser::from_input(input);
    parse_fn(&mut parser);

    format!("{}", parser.finish())
}

#[test]
fn test_pattern_atom() {
    assert_eq!(
        indoc! {"
            ExprPatternAtom
                Sym
            "
        },
        test_parse_fmt("a", expr_pattern),
    );
    assert_eq!(
        indoc! {"
            ExprPatternAtom
                SingleQuoteText
            "
        },
        test_parse_fmt("'a'", expr_pattern),
    );
}

#[test]
fn test_expr_binary1() {
    assert_eq!(
        indoc! {"
            ExprPatternBinary
                ExprPatternAtom
                    Number
                Plus
                ExprPatternAtom
                    Number
            "
        },
        test_parse_fmt("1+2", expr_pattern),
    );
}

#[test]
fn test_expr_binary2() {
    assert_eq!(
        indoc! {"
            ExprPatternBinary
                ExprPatternAtom
                    Number
                Plus
                ExprPatternBinary
                    ExprPatternAtom
                        Number
                    Plus
                    ExprPatternAtom
                        Number
            "
        },
        test_parse_fmt("1+2+3", expr_pattern),
    );
}

#[test]
fn test_expr_binary3() {
    assert_eq!(
        indoc! {"
            ExprPatternBinary
                ExprPatternBinary
                    ExprPatternAtom
                        Number
                    Star
                    ExprPatternAtom
                        Number
                Plus
                ExprPatternAtom
                    Number
            "
        },
        test_parse_fmt("1*2+3", expr_pattern),
    );
}

#[test]
fn test_expr_binary4() {
    assert_eq!(
        indoc! {"
            ExprPatternBinary
                ExprPatternAtom
                    Number
                Plus
                ExprPatternBinary
                    ExprPatternAtom
                        Number
                    Star
                    ExprPatternAtom
                        Number
            "
        },
        test_parse_fmt("1+2*3", expr_pattern),
    );
}

#[test]
fn test_expr_binary5() {
    assert_eq!(
        indoc! {"
            ExprPatternBinary
                ExprPatternAtom
                    Number
                Plus
                ExprPatternBinary
                    ExprPatternBinary
                        ExprPatternAtom
                            Number
                        Star
                        ExprPatternAtom
                            Number
                    Plus
                    ExprPatternAtom
                        Number
            "
        },
        test_parse_fmt("1+2*3+5", expr_pattern),
    );
}

#[test]
fn test_expr_binary6() {
    assert_eq!(
        indoc! {"
            ExprPatternBinary
                ExprPatternBinary
                    ExprPatternAtom
                        Number
                    Star
                    ExprPatternAtom
                        Number
                Plus
                ExprPatternBinary
                    ExprPatternAtom
                        Number
                    Star
                    ExprPatternAtom
                        Number
            "
        },
        test_parse_fmt("1*2+3*5", expr_pattern),
    );
}

#[test]
fn test_expr_binary7() {
    assert_eq!(
        indoc! {"
            Whitespace
            ExprPatternBinary
                ExprPatternAtom
                    Number
                Whitespace
                Star
                ExprPatternBinary
                    Whitespace
                    ParenOpen
                    Whitespace
                    ExprPatternBinary
                        ExprPatternAtom
                            Number
                        Whitespace
                        Plus
                        Whitespace
                        ExprPatternAtom
                            Number
                        Whitespace
                    ParenClose
                    Whitespace
                    Star
                    Whitespace
                    ExprPatternAtom
                        Number
                    Whitespace
            "
        },
        test_parse_fmt(" 1 * ( 2 + 3 ) * 5 ", expr_pattern),
    );
}
