use std::{fs, path::PathBuf};

use crate::cst::{grammar, tree::SyntaxMarker};

use super::*;

#[rstest::rstest]
fn cst(#[files("test-cases/cst/*.test")] path: PathBuf) {
    let contents = fs::read_to_string(path.clone()).unwrap();

    let mut ontol_src = String::new();
    let mut cst_expect = String::new();

    let mut lines = contents.lines();

    let mut grammar: Option<&str> = None;

    for line in lines.by_ref() {
        if line.starts_with("//@") {
            grammar = Some(line.strip_prefix("//@ grammar=").expect("missing grammar"));
            break;
        } else {
            ontol_src.push_str(line);
            ontol_src.push('\n');
        }
    }

    for line in lines {
        cst_expect.push_str(line);
        cst_expect.push('\n');
    }

    let mut test_token_count = false;

    let parse_fn = match grammar {
        Some("ontol") => {
            test_token_count = true;
            grammar::ontol
        }
        Some("pattern") => grammar::pattern_with_expr,
        Some("expr") => grammar::expr_pattern::entry,
        Some(other) => {
            panic!("unrecognized grammar `{other}`");
        }
        None => panic!("missing //@ line"),
    };

    let (lexed, _errors) = cst_lex(&ontol_src);
    let mut parser = CstParser::from_lexed_source(&ontol_src, lexed);
    parse_fn(&mut parser);

    let (tree, _errors) = parser.finish();

    if test_token_count {
        let token_marker_count = tree
            .markers()
            .iter()
            .filter(|m| {
                matches!(
                    m,
                    SyntaxMarker::Token { .. } | SyntaxMarker::Ignorable { .. }
                )
            })
            .count();

        assert_eq!(token_marker_count, tree.lex().tokens().len());
    }

    pretty_assertions::assert_eq!(
        cst_expect,
        format!("{}", tree.debug_tree(&ontol_src)),
        "test file `{path}` did not match expectation",
        path = path.to_str().unwrap()
    );
}
