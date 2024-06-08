use std::mem::size_of;

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

    fn wrap<'a, H: Clone>(_data: &Self::Data<'a, H>, hir: H) -> Self::Data<'a, H> {
        hir
    }

    fn as_hir<'m, H: Clone>(data: &'m Self::Data<'_, H>) -> &'m H {
        data
    }
}

/// Kind with unit metadata
type TestKind<'a> = Kind<'a, TestLang>;

#[test]
fn assert_size() {
    assert_eq!(48, size_of::<TestKind>());
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
fn test_struct() {
    let src = indoc! {"
        (struct ($a)
            (prop- $a R:0:0
                #u
            )
            (prop- $a R:0:0
                [#u
                    (struct ($b))
                ]
            )
            (prop- $a R:0:0
                [
                    (struct ($c))
                    #u
                ]
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
fn test_set() {
    let src = indoc! {"
        (struct ($a)
            (prop- $a R:0:0
                [#u
                    (set
                        (.. @c #u $b)
                    )
                ]
            )
        )"
    };
    assert_eq!(src, parse_print(src));
}

#[test]
fn test_set_in_prop() {
    let src = indoc! {"
        (struct ($a)
            (prop- $a R:0:0
                [#u
                    (set
                        (.. @c #u $b)
                    )
                ]
            )
        )"
    };
    assert_eq!(src, parse_print(src));
}

#[test]
fn test_map_seq() {
    let src = indoc! {"
        (block
            (let-prop $c ($b R:0:0))
            (let-prop [$f $g] ($b R:0:0))
            (struct ($a)
                (prop- $a R:0:0
                    [#u
                        (make-seq ($d)
                            (for-each $c ($e $f) $d)
                        )
                    ]
                )
            )
        )"
    };
    assert_eq!(src, parse_print(src));
}

#[test]
fn test_with() {
    let src = indoc! {"
        (with ($a (+ 1 2))
            (prop- $b R:0:0
                [$a #u]
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
