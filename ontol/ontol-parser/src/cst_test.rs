use crate::{cst::parser::CstParser, lexer::LexedSource};

fn parser(src: &str) -> CstParser {
    let (lexed, _errors) = LexedSource::lex(src);
    CstParser::from_lexed_source(src, lexed)
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
