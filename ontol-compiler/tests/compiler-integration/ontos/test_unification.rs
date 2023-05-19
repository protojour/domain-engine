use indoc::indoc;
use ontol_compiler::typed_ontos::lang::{OntosNode, TypedOntos};
use ontol_compiler::typed_ontos::unify::unify;
use ontos::parse::Parser;
use pretty_assertions::assert_eq;
use test_log::test;

fn parse_typed(src: &str) -> OntosNode<'static> {
    Parser::new(TypedOntos).parse(src).unwrap().0
}

fn test_unify(source: &str, target: &str) -> String {
    let node = unify(parse_typed(source), parse_typed(target));
    let mut output = String::new();
    use std::fmt::Write;
    write!(&mut output, "{node}").unwrap();
    output
}

#[test]
fn test_unify_struct_tmp() {
    let output = test_unify(
        "
        (struct (#1)
            (prop #1 a #u #0)
        )
        ",
        "
        (struct (#2)
            (prop #2 a #u #0)
        )
        ",
    );
    let expected = indoc! {
        "(struct (#2))"
    };
    assert_eq!(expected, output);
}
