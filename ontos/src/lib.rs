use std::fmt::Debug;

use kind::NodeKind;

pub mod display;
pub mod kind;
pub mod parse;
pub mod visitor;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Variable(pub u32);

impl Debug for Variable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let var = &self.0;
        write!(f, "Variable({var})")
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Binder(pub Variable);

#[derive(Clone, Copy, Debug)]
pub struct Label(pub u32);

pub trait Lang: Sized + Copy {
    type Node<'a>: Sized + Node<'a, Self>;

    fn make_node<'a>(&self, kind: NodeKind<'a, Self>) -> Self::Node<'a>;
}

pub trait Node<'a, L: Lang> {
    fn kind(&self) -> &NodeKind<'a, L>;
    fn kind_mut(&mut self) -> &mut NodeKind<'a, L>;
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
        let src = "(+ $0 2)";
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_match_prop1() {
        let src = indoc! {"
            (match-prop $0 s:10:10
                (($_ $2) #u)
                (() #u)
            )"
        };
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_match_prop2() {
        let src = indoc! {"
            (match-prop $0 s:0:0
                (($_ $2)
                    (match-prop $2 s:0:1
                        (($_ $3) #u)
                        (() #u)
                    )
                )
                (() #u)
            )"
        };
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_struct() {
        let src = indoc! {"
            (struct ($0)
                (prop $0 s:0:0
                    (#u #u)
                )
                (prop $0 s:0:0
                    (#u
                        (struct ($1))
                    )
                )
                (prop $0 s:0:0
                    (
                        (struct ($2))
                        #u
                    )
                )
            )"
        };
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_mixed() {
        let src = indoc! {"
            (+ 1
                (struct ($0))
            )"
        };
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_seq() {
        let src = indoc! {"
            (struct ($0)
                (prop $0 s:0:0
                    (#u
                        (seq ($1) $1)
                    )
                )
            )"
        };
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_map_seq() {
        let src = indoc! {"
            (struct ($0)
                (match-prop $1 s:0:0
                    (($_ $2)
                        (map-seq $2 ($3) $3)
                    )
                )
            )"
        };
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_let() {
        let src = indoc! {"
            (let ($0 (+ 1 2))
                (prop $1 s:0:0
                    (#u $0)
                )
            )"
        };
        assert_eq!(src, parse_print(src));
    }
}
