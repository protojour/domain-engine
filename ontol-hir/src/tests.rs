use crate::parse::Parser;

use super::*;
use indoc::indoc;
use pretty_assertions::assert_eq;

#[derive(Clone, Copy)]
struct TestLang;

impl Lang for TestLang {
    type Node<'a> = Kind<'a, Self>;

    fn make_node<'a>(&self, kind: Kind<'a, Self>) -> Self::Node<'a> {
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
                    (seq (@c) #u $b)
                )
            )
        )"
    };
    assert_eq!(src, parse_print(src));
}

#[test]
fn test_seq_prop() {
    let src = indoc! {"
        (struct ($a)
            (prop $a S:0:0
                (seq (@c)
                    #u
                    $b
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
                    (gen $c ($d $e $f) $d)
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
