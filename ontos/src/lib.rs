use std::fmt::{Debug, Display};

use kind::NodeKind;

use crate::display::AsAlpha;

pub mod display;
pub mod kind;
pub mod parse;
pub mod visitor;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Variable(pub u32);

impl Debug for Variable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Variable({})", AsAlpha(self.0))
    }
}

impl Display for Variable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}", AsAlpha(self.0))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Binder(pub Variable);

#[derive(Clone, Copy)]
pub struct Label(pub u32);

impl Debug for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Label({})", AsAlpha(self.0))
    }
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "@{}", AsAlpha(self.0))
    }
}

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
    fn test_big_var() {
        let src = "$abc";
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_fn_call() {
        let src = "(+ $a 2)";
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_match_prop1() {
        let src = indoc! {"
            (match-prop $a S:10:10
                (($_ $b) #u)
                (() #u)
            )"
        };
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_match_prop2() {
        let src = indoc! {"
            (match-prop $a S:0:0
                (($_ $b)
                    (match-prop $b S:0:1
                        (($_ $c) #u)
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
            (struct ($a)
                (prop $a S:0:0
                    (#u #u)
                )
                (prop $a S:0:0
                    (#u
                        (struct ($b))
                    )
                )
                (prop $a S:0:0
                    (
                        (struct ($c))
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
                (struct ($a))
            )"
        };
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_seq() {
        let src = indoc! {"
            (struct ($a)
                (prop $a S:0:0
                    (#u
                        (seq (@c) $b)
                    )
                )
            )"
        };
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_map_seq() {
        let src = indoc! {"
            (struct ($a)
                (match-prop $b S:0:0
                    (($_ $c)
                        (map-seq $c ($d) $d)
                    )
                )
            )"
        };
        assert_eq!(src, parse_print(src));
    }

    #[test]
    fn test_let() {
        let src = indoc! {"
            (let ($a (+ 1 2))
                (prop $b S:0:0
                    (#u $a)
                )
            )"
        };
        assert_eq!(src, parse_print(src));
    }
}
