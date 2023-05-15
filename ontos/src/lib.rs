pub mod ast;
// pub mod lower;
pub mod parse;

mod display;
mod visitor;

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn parse_print(src: &str) -> String {
        let (ast, _) = parse::parse(src).unwrap();
        let mut out = String::new();
        use std::fmt::Write;
        write!(out, "{ast}").unwrap();
        out
    }

    #[test]
    fn test_unit() {
        let src = "#u";
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_fn_call() {
        let src = "(+ #0 2)";
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_destruct1() {
        let src = "
(destruct #0
    (destruct-prop foo
        ((#_ #2) #u)
        (() #u)
    )
)";
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_destruct2() {
        let src = "
(destruct #0
    (destruct-prop foo
        ((#_ #2)
            (destruct #2
                (destruct-prop bar
                    ((#_ #3) #u)
                    (() #u)
                )
            )
        )
        (() #u)
    )
)";
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_construct() {
        let src = "
(construct
    (construct-prop foo #u #u)
    (construct-prop foo #u
        (construct)
    )
    (construct-prop foo
        (construct)
        #u
    )
)";
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_mixed() {
        let src = "(+ 1
    (construct)
)";
        assert_eq!(src, parse_print(src));
    }
}
