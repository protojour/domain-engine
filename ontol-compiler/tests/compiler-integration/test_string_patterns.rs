use crate::TestCompile;

#[test]
fn simple_string_pattern() {
    "
    (type! foo)
    (rel! '' { 'foo' } foo)
    "
    .compile_ok(|env| {})
}

#[test]
#[ignore = "figure out index relations"]
fn test_string_patterns() {
    "
    (type! hex)
    (rel! '' { /a-zA-Z0-9/ } hex)

    (type! uuid)
    (rel!
        ''
        { hex{8} }
        { '-' }
        { hex{4} }
        { '-' }
        { hex{4} }
        { '-' }
        { hex{4} }
        { '-' }
        { hex{12} }
        uuid
    )

    (type! my_id)
    (rel! '' { 'my/' } { uuid } my_id)
    "
    .compile_fail()
}

#[test]
fn regex_named_group_as_relation() {
    "
    (type! lol)
    (rel! _ { /abc(?<named>.)/ } lol) ;; ERROR parse error: expected end of list
    "
    .compile_fail()
}

#[test]
#[ignore]
fn automata() {
    r#"
    (rel!
        []
        { int }
        { int }
        position
    )

    (rel!
        []
        { position }
        { position }
        { position* }
        position-list
    )
    
    (rel!
        position-list
        { position }
        { position }
        { position* }
        position-ring
    )
    "#
    .compile_fail();
}
