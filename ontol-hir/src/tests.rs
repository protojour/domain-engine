use crate::parse::Parser;

use super::*;
use indoc::indoc;
use pretty_assertions::assert_eq;

#[derive(Clone, Copy)]
struct TestLang;

impl Lang for TestLang {
    type Data<'a, H> = H where H: Clone;

    fn default_data<'a, H: Clone>(&self, hir: H) -> Self::Data<'a, H> {
        hir
    }

    fn as_hir<'m, 'a, H: Clone>(data: &'m Self::Data<'a, H>) -> &'m H {
        data
    }
}

fn parse_print(src: &str) -> String {
    let parser = Parser::new(TestLang);
    let (node, _) = parser.parse_root(src).unwrap();
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
fn test_decl_seq() {
    let src = indoc! {"
        (struct ($a)
            (prop $a S:0:0
                (#u
                    (decl-seq (@c) #u $b)
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
                    (iter #u $b)
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
                    (sequence ($d)
                        (for-each $c ($e $f) $d)
                    )
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

#[test]
fn test_regex() {
    let src = indoc! {"
        (regex def@0:0 () ((1 $a)))"
    };
    assert_eq!(src, parse_print(src));
}

#[test]
fn test_match_regex() {
    let src = indoc! {"
        (match-regex $a def@0:0
            (((1 $b) (2 $c)) $b)
        )"
    };
    assert_eq!(src, parse_print(src));
}
