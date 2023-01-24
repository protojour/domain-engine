use crate::TestCompile;

#[test]
fn lex_error() {
    "( ;; ERROR parse error".fail()
}

#[test]
fn parse_erorr() {
    "() ;; ERROR parse error".fail()
}
