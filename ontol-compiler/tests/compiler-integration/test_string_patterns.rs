use crate::TestCompile;

#[test]
#[ignore = "figure out index relations"]
fn test_string_patterns() {
    "
    ; (type! hex)
    ; (rel! (hex) {} (re! 'a-zA-Z0-9'))

    (type! uuid)
    (rel! uuid {_} string)
    ; (rel! uuid {  0..07 } hex)
    ; (rel! uuid {     08 } '-')
    ; (rel! uuid {  9..12 } hex)
    ; (rel! uuid {     13 } '-')
    ; (rel! uuid { 14..17 } hex)
    ; (rel! uuid {     18 } '-')
    ; (rel! uuid { 19..22 } hex)
    ; (rel! uuid {     23 } '-')
    ; (rel! uuid { 24..25 } hex)

    (type! my_id)
    (rel! my_id {} string)
    (rel! my_id { 0 } 'my/')
    (rel! my_id { 1 } uuid)

    ; FIXME: named sequence elements, if 1 expands to `1 @1`
    ; (rel! my_id {uuid @1} uuid)
    "
    .compile_fail()
}

#[test]
fn regex_named_group_as_relation() {
    r#"
    (type! lol)
    (rel! lol {} (re! "abc(?<named>.)")) ;; ERROR parse error: invalid type
    "#
    .compile_fail()
}

#[test]
#[ignore]
fn transducers() {
    r#"
    (auto!
        []
        { int }
        { int }
        position
    )

    (auto!
        []
        { position }
        { position }
        { position* }
        position-list
    )
    
    (auto!
        position-list
        { position }
        { position }
        { position* }
        position-ring
    )
    "#
    .compile_fail();
}
