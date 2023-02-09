use crate::TestCompile;

#[test]
fn test_string_patterns() {
    r#"
    (type! uuid)
    (rel! (uuid) _ (string))

    (type! my_id)
    (rel! (my_id) _ (re! "my/" (uuid))) ;; ERROR parse error: expected end of list

    ; now how does the (uuid) above end up as a property?
    ; (rel! (my_id) uuid _)
    "#
    .compile_fail()
}
