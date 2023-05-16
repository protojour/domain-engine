use kind::NodeKind;

// pub mod lower;
pub mod kind;
pub mod parse;

mod display;
mod visitor;

pub trait Lang: Sized + Copy {
    type Node<'a>: Sized + Node<'a, Self>;

    fn make_node<'a>(&self, kind: NodeKind<'a, Self>) -> Self::Node<'a>;
}

pub trait Node<'a, L: Lang> {
    fn kind(&self) -> &NodeKind<'a, L>;
}

#[cfg(test)]
mod tests {
    use crate::parse::Parser;

    use super::*;
    use indoc::indoc;
    use pretty_assertions::assert_eq;

    #[derive(Clone, Copy)]
    struct TestLang;

    impl Lang for TestLang {
        type Node<'a> = NodeKind<'a, Self>;

        fn make_node<'a>(&self, kind: NodeKind<'a, Self>) -> Self::Node<'a> {
            kind
        }
    }

    fn parse_print(src: &str) -> String {
        let parser = Parser::new(TestLang);
        let (node, _) = parser.parse(src).unwrap();
        let mut out = String::new();
        use std::fmt::Write;
        write!(out, "{node}").unwrap();
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
        let src = indoc! {"
            (destruct #0
                (match-prop #0 foo
                    ((#_ #2) #u)
                    (() #u)
                )
            )"
        };
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_destruct2() {
        let src = indoc! {"
            (destruct #0
                (match-prop #0 foo
                    ((#_ #2)
                        (destruct #2
                            (match-prop #2 bar
                                ((#_ #3) #u)
                                (() #u)
                            )
                        )
                    )
                    (() #u)
                )
            )"
        };
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_struct() {
        let src = indoc! {"
            (struct (#0)
                (prop #0 foo #u #u)
                (prop #0 foo #u
                    (struct (#1))
                )
                (prop #0 foo
                    (struct (#2))
                    #u
                )
            )"
        };
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_mixed() {
        let src = indoc! {"
            (+ 1
                (struct (#0))
            )"
        };
        assert_eq!(src, parse_print(src));
    }
}
