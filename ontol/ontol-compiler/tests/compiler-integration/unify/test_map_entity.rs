use ontol_macros::test;
use ontol_test_utils::{src_name, TestCompile, TestPackages};
use serde_json::json;

#[test]
fn should_map_inherent_capturing_pattern_id() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'id': (fmt '' => 'foo/' => uuid => .)
    )
    def bar (
        rel. 'id': (fmt '' => 'bar/' => uuid => .)
    )

    map(
        foo('id': id),
        bar('id': id),
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({ "id": "foo/67e55044-10b1-426f-9247-bb680e5fe0c8" }),
            json!({ "id": "bar/67e55044-10b1-426f-9247-bb680e5fe0c8" }),
        );
    });
}

#[test]
fn test_extract_rel_params() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    sym { id }

    def a1_id(fmt '' => 'a1/' => uuid => .)
    def a2_id(fmt '' => 'a2/' => uuid => .)
    def b1_id(fmt '' => 'b1/' => uuid => .)
    def b2_id(fmt '' => 'b2/' => uuid => .)

    def a2(
        rel. id: a2_id
        rel* 'foo': text
    )
    def b2(
        rel. id: b2_id
        rel* 'foo': text
        rel* 'bar': text
    )

    def a_edge (
        rel* 'bar': text
    )

    def a1(
        rel. id: a1_id
        rel* 'foreign'[rel* is: a_edge]: a2
    )
    def b1(
        rel. id: b1_id
        rel* 'foreign': b2
    )

    map(
        a1(
            id: root_id,
            'foreign'['bar': b]: a2(id: foreign_id, 'foo': f)
        ),
        b1(
            id: root_id,
            'foreign': b2(
                id: foreign_id,
                'foo': f,
                'bar': b,
            )
        )
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("a1", "b1"),
            json!({
                "id": "a1/11111111-1111-1111-1111-111111111111",
                "foreign": {
                    "id": "a2/22222222-2222-2222-2222-222222222222",
                    "foo": "FOO",
                    "_edge": {
                        "bar": "BAR"
                    }
                }
            }),
            json!({
                "id": "b1/11111111-1111-1111-1111-111111111111",
                "foreign": {
                    "id": "b2/22222222-2222-2222-2222-222222222222",
                    "foo": "FOO",
                    "bar": "BAR",
                }
            }),
        );

        test.mapper().assert_map_eq(
            ("b1", "a1"),
            json!({
                "id": "b1/11111111-1111-1111-1111-111111111111",
                "foreign": {
                    "id": "b2/22222222-2222-2222-2222-222222222222",
                    "foo": "FOO",
                    "bar": "BAR",
                }
            }),
            json!({
                "id": "a1/11111111-1111-1111-1111-111111111111",
                "foreign": {
                    "id": "a2/22222222-2222-2222-2222-222222222222",
                    "foo": "FOO",
                    "_edge": {
                        "bar": "BAR"
                    }
                }
            }),
        );
    });
}

#[test]
fn test_rel_params_implicit_map() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def a_id (fmt '' => 'a/' => serial => .)
    def b_id (fmt '' => 'b/' => serial => .)
    def a_inner_id (fmt '' => 'a_inner/' => serial => .)
    def b_inner_id (fmt '' => 'b_inner/' => serial => .)

    def a_inner (
        rel. 'id': a_inner_id
        rel* 'a_prop': text
    )
    def b_inner (
        rel. 'id': b_inner_id
        rel* 'b_prop': text
    )

    def a_edge (rel* 'aa': text)
    def b_edge (rel* 'bb': text)

    def a (
        rel. 'id': a_id
        rel* 'foreign'[rel* is: a_edge]: a_inner
    )
    def b (
        rel. 'id': b_id
        rel* 'foreign'[rel* is: b_edge]: b_inner
    )

    map(
        a_inner('id': id, 'a_prop': x),
        b_inner('id': id, 'b_prop': x),
    )
    map(
        a_edge('aa': x),
        b_edge('bb': x),
    )
    map(
        a('id': id, 'foreign': x),
        b('id': id, 'foreign': x),
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("a", "b"),
            json!({
                "id": "a/1",
                "foreign": {
                    "id": "a_inner/2",
                    "a_prop": "PROP",
                    "_edge": {
                        "aa": "EDGE"
                    }
                }
            }),
            json!({
                "id": "b/1",
                "foreign": {
                    "id": "b_inner/2",
                    "b_prop": "PROP",
                    "_edge": {
                        "bb": "EDGE"
                    }
                }
            }),
        );
    });
}

#[test]
fn test_map_relation_sequence_default_fallback() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo_inner (rel. 'foo_id': (rel* is: text))
    def bar_inner (rel. 'bar_id': (rel* is: text))
    rel {foo_inner} 'bars'::'foos'? {bar_inner}

    def bar (
        rel. 'id': (rel* is: text)
        rel* 'foos': {text}
    )

    map(
        @match bar_inner(
            'bar_id': id,
            'foos': { ..foo_inner('foo_id': foo) },
        ),
        bar(
            'id': id,
            'foos': {..foo}
        ),
    )
    "
    .compile_then(|test| {
        // The point of this test is to show that a
        // "foos" in input is not needed to produce a "foo" in output,
        // since the input "foo" is an inverted/object relation sequence property:
        test.mapper().assert_map_eq(
            ("bar_inner", "bar"),
            json!({ "bar_id": "B" }),
            json!({ "id": "B", "foos": [] }),
        );
        test.mapper().assert_map_eq(
            ("bar_inner", "bar"),
            json!({ "bar_id": "B", "foos": [{ "foo_id": "F" }]}),
            json!({ "id": "B", "foos": ["F"] }),
        );
    });
}

#[test]
fn test_map_generate_edge() {
    TestPackages::with_static_sources([
        (
            src_name("outer"),
            "
            use 'inner' as inner

            def foo (
                rel. 'id': (rel* is: text)
                rel* 'bars'[rel* 'field': text]: {bar}
            )
            def bar (rel* 'id': (rel* is: text))

            map(
                foo(
                    'id': foo_id,
                    'bars': {
                        ..['field': field] bar('id': bar_id)
                    }
                ),
                inner.foo(
                    'id': foo_id,
                    'bars': {
                        ..inner.bar(
                            'id': bar_id,
                            'field': field
                        )
                    }
                ),
            )
            ",
        ),
        (
            src_name("inner"),
            "
            domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
            def foo (
                rel. 'id': (rel* is: text)
                rel* 'bars': {bar}
            )

            def bar (
                rel. 'id': (rel* is: text)
                rel* 'field': text
            )
            ",
        ),
    ])
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("inner.foo", "foo"),
            json!({
                "id": "1",
                "bars": [{ "id": "2", "field": "F" }]
            }),
            json!({
                "id": "1",
                "bars": [{
                    "id": "2",
                    "_edge": { "field": "F" }
                }]
            }),
        );
    });
}

const WORK: &str = "
def worker_id (fmt '' => 'worker/' => uuid => .)
def tech_id (fmt '' => 'tech/' => uuid => .)

def worker ()
def technology ()

def worker (
    rel. 'ID': worker_id
    rel* 'name': text

    rel* 'technologies': {technology}
)

def technology (
    rel. 'ID': tech_id
    rel* 'name': text
)
";

const DEV: &str = "
def lang_id (fmt '' => uuid => .)
def dev_id (fmt '' => uuid => .)

def language ()
def developer ()

def language (
    rel. 'id': lang_id

    rel* 'name': text
    rel* 'developers': {developer}
)

def developer (
    rel. 'id': dev_id
    rel* 'name': text
)
";

#[test]
fn test_map_invert() {
    TestPackages::with_static_sources([
        (
            src_name("entry"),
            "
            use 'work' as work
            use 'dev' as dev

            map(
                work.worker(
                    'ID': p_id,
                    'name': p_name,
                    'technologies': {..work.technology(
                        'ID': tech_id,
                        'name': tech_name,
                    )}
                ),
                dev.language(
                    'id': tech_id,
                    'name': tech_name,
                    'developers': {..dev.developer( // ERROR TODO: Incompatible aggregation group// ERROR TODO: unable to loop
                        'id': p_id,
                        'name': p_name,
                    )}
                )
            )
            ",
        ),
        (src_name("work"), WORK),
        (src_name("dev"), DEV),
    ])
    .compile_fail();
}
