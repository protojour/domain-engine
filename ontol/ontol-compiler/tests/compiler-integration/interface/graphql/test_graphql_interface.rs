use assert_matches::assert_matches;
use ontol_examples::{artist_and_instrument, edge_entity_union, findings};
use ontol_macros::test;
use ontol_runtime::{
    debug::OntolDebug,
    interface::{
        graphql::{
            data::{FieldKind, NativeScalarKind, ObjectInterface, ObjectKind},
            schema::QueryLevel,
        },
        serde::operator::SerdeOperator,
    },
};
use ontol_test_utils::{
    default_file_url, default_short_name, expect_eq, file_url,
    test_extensions::{
        graphql::{ObjectDataExt, TypeDataExt, UnitTypeRefExt},
        serde::SerdeOperatorExt,
    },
    TestCompile, TestPackages,
};

use super::schema_test;

#[test]
fn test_domain_docs_as_query_docs() {
    "
    /// Domain docs
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()

    def foo (
        rel. 'id': (fmt '' => text => .)
        rel* 'prop': i64
    )
    "
    .compile_then(|test| {
        let (_schema, test) = schema_test(&test, default_short_name());
        let ontology = test.test.ontology();
        let query_data = test.query_object_data();
        let ObjectKind::Query { domain_def_id } = &query_data.kind else {
            panic!();
        };

        let docs = &ontology[ontology.get_def_docs(*domain_def_id).unwrap()];
        assert_eq!(docs, "Domain docs");
    });
}

#[test]
fn test_graphql_small_range_number_becomes_int() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def myint (
        rel* is: integer
        rel* min: 0
        rel* max: 100
    )
    def foo (
        rel. 'id': (fmt '' => text => .)
        rel* 'prop': myint
    )
    "
    .compile_then(|test| {
        let (_schema, test) = schema_test(&test, default_short_name());
        let foo_type = test.type_data("foo", QueryLevel::Node);
        let foo_object = foo_type.object_data();

        let prop_field = foo_object.fields.get("prop").unwrap();
        assert_matches!(prop_field.kind, FieldKind::Property { .. });

        let native = prop_field.field_type.unit.native_scalar();
        assert_matches!(native.kind, NativeScalarKind::Int(_));
    });
}

#[test]
fn test_graphql_i64_custom_scalar() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'id': (fmt '' => text => .)
        rel* 'prop': i64
    )
    "
    .compile_then(|test| {
        let (schema, test) = schema_test(&test, default_short_name());
        let ontology = test.test.ontology();
        let foo_type = test.type_data("foo", QueryLevel::Node);
        let foo_object = foo_type.object_data();

        let prop_field = foo_object.fields.get("prop").unwrap();
        assert_matches!(prop_field.kind, FieldKind::Property { .. });

        let prop_type_data = schema.type_data(prop_field.field_type.unit.addr());

        expect_eq!(
            actual = &ontology[prop_type_data.typename],
            expected = "_ontol_i64"
        );
        let _i64_scalar_data = prop_type_data.custom_scalar();
    });
}

#[test]
fn test_graphql_default_scalar() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'id': (fmt '' => text => .)
        rel* 'default'[rel* default := '']: text
    )
    "
    .compile_then(|test| {
        let (_schema, test) = schema_test(&test, default_short_name());
        let foo_type = test.type_data("foo", QueryLevel::Node);
        let foo_object = foo_type.object_data();

        let prop_field = foo_object.fields.get("default").unwrap();
        assert_matches!(prop_field.kind, FieldKind::Property { .. });

        let native = prop_field.field_type.unit.native_scalar();
        assert_matches!(native.kind, NativeScalarKind::String);
    });
}

#[test]
fn test_graphql_scalar_array() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'id': (fmt '' => text => .)
        rel* 'tags': {text}
    )
    "
    .compile_then(|test| {
        let (_schema, test) = schema_test(&test, default_short_name());
        let foo_type = test.type_data("foo", QueryLevel::Node);
        let foo_object = foo_type.object_data();

        let prop_field = foo_object.fields.get("tags").unwrap();
        assert_matches!(prop_field.kind, FieldKind::Property { .. });

        let native = prop_field.field_type.unit.native_scalar();
        assert_matches!(native.kind, NativeScalarKind::String);

        let operator = &test.test.ontology()[native.operator_addr];
        if !matches!(operator, SerdeOperator::RelationList(_)) {
            panic!("{:?} was not RelationSequence", operator.debug(&()));
        }
    });
}

#[test]
fn test_graphql_serde_renaming() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'id': (fmt '' => text => .)
        rel* 'must-rewrite': text
        rel* 'must_rewrite': text
    )
    "
    .compile_then(|test| {
        let (_schema, schema_test) = schema_test(&test, default_short_name());
        let foo_node = schema_test
            .type_data("foo", QueryLevel::Node)
            .object_data()
            .node_data();

        let fields: Vec<_> = test.ontology()[foo_node.operator_addr]
            .struct_op()
            .properties
            .iter()
            .map(|(key, _)| key.arc_str())
            .collect();

        expect_eq!(
            actual = fields.as_slice(),
            expected = &["id", "must_rewrite", "must_rewrite_", "_edge"]
        );
    });
}

#[test]
fn test_query_map_empty_input_becomes_hidden_arg() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def entity (
        rel. 'id': (rel* is: text)
    )
    def empty ()

    map my_query(
        (),
        empty(),
    )
    "
    .compile_then(|test| {
        let (schema, _test) = schema_test(&test, default_short_name());

        let query = schema.type_data(schema.query).object_data();
        let my_query = query.fields.get("my_query").unwrap();
        let FieldKind::MapFind(field) = &my_query.kind else {
            panic!("Incorrect field kind");
        };

        assert!(field.input_arg.hidden);
    });
}

#[test]
fn test_graphql_artist_and_instrument() {
    let test = artist_and_instrument().1.compile();
    let (schema, test) = schema_test(&test, default_short_name());
    let ontology = test.test.ontology();
    let query_object = test.query_object_data();

    let artists_field = query_object.fields.get("artists").unwrap();
    let artist_connection = schema.type_data(artists_field.field_type.unit.addr());

    expect_eq!(
        actual = &ontology[artist_connection.typename],
        expected = "artistConnection"
    );

    let edges_field = artist_connection.object_data().fields.get("edges").unwrap();
    let artist_edge = schema.type_data(edges_field.field_type.unit.addr());

    expect_eq!(
        actual = &ontology[artist_edge.typename],
        expected = "artistEdge"
    );

    let node_field = artist_edge.object_data().fields.get("node").unwrap();
    let artist = schema.type_data(node_field.field_type.unit.addr());

    expect_eq!(actual = &ontology[artist.typename], expected = "artist");
    expect_eq!(
        actual = artist
            .input_typename
            .map(|constant| &test.test.ontology()[constant]),
        expected = Some("artistInput")
    );
}

#[test]
fn test_imperfect_mapping_mutation() {
    let test = TestPackages::with_static_sources([
        (
            default_file_url(),
            "
            use 'lower' as lower

            def upper (
                rel. 'id': (rel* is: text)
                rel* 'x': text
            )

            map(
                upper(
                    'id': id,
                    'x': x,
                ),
                @match lower.lower(
                    'id': id,
                    'a': x,
                )
            )
            ",
        ),
        (
            file_url("lower"),
            "
            domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
            def lower (
                rel. 'id': (rel* is: text)
                rel* 'a': text
                rel* 'b': text
            )
            ",
        ),
    ])
    .compile();
    let (_schema, test) = schema_test(&test, default_short_name());
    let mutation_object = test.mutation_object_data();
    let upper_field = mutation_object.fields.get("upper").unwrap();
    let FieldKind::EntityMutation(field) = &upper_field.kind else {
        panic!()
    };

    // The outer domain cannot create entities of the inner domain because
    // of the imperfect mapping in that direction.
    assert!(field.create_arg.is_none());
    assert!(field.update_arg.is_some());
    assert!(field.delete_arg.is_some());
}

#[test]
fn incompatible_edge_types_are_distinct() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def edge_type (
        rel* 'edge_field': text
    )

    arc link {
        (s) targets: (t) type: edge_type
    }

    def source (
        rel. 'id': (rel* is: text)
        rel* link.targets: {target}
    )
    def target (
        rel. 'id': (rel* is: text)
    )

    map targets (
        (),
        target {
            ..@match target()
        }
    )
    "
    .compile_then(|test| {
        let (schema, test) = schema_test(&test, default_short_name());
        let ontology = test.test.ontology();

        let query = schema.type_data(schema.query).object_data();

        if true {
            let targets_query = query.fields.get("targets").unwrap();
            let targets_query_connection = schema.type_data(targets_query.field_type.unit.addr());

            assert_eq!(
                &ontology[targets_query_connection.typename],
                "targetConnection"
            );

            let targets_query_edge = schema.type_data(
                targets_query_connection
                    .object_data()
                    .fields
                    .get("edges")
                    .unwrap()
                    .field_type
                    .unit
                    .addr(),
            );
            assert_eq!(&ontology[targets_query_edge.typename], "targetEdge");
            assert!(!targets_query_edge
                .fields()
                .unwrap()
                .contains_key("edge_field"));
        }

        {
            let source = test.type_data("source", QueryLevel::Node).object_data();
            let targets_field = source.fields.get("targets").unwrap();

            let targets_connection = schema.type_data(targets_field.field_type.unit.addr());
            assert_eq!(
                &ontology[targets_connection.typename],
                "edge_typetargetConnection"
            );

            let targets_edge = schema.type_data(
                targets_connection
                    .object_data()
                    .fields
                    .get("edges")
                    .unwrap()
                    .field_type
                    .unit
                    .addr(),
            );

            assert_eq!(&ontology[targets_edge.typename], "edge_typetargetEdge");
            assert!(targets_edge.fields().unwrap().contains_key("edge_field"));
        }
    });
}

#[test]
fn graphql_flattened_union() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def kind ()

    def foo (
        rel. 'id': (rel* is: text)
        rel* kind: (
            rel* is?: bar
            rel* is?: qux
        )
    )

    def bar (
        rel* 'kind': 'bar'
        rel* 'data': text
        rel* 'bar': i64
    )
    def qux (
        rel* 'kind': 'qux'
        rel* 'data': i64
        rel* 'qux': text
    )
    "
    .compile_then(|test| {
        let (_schema, schema_test) = schema_test(&test, default_short_name());
        let foo_interface = schema_test.type_data("foo", QueryLevel::Node).object_data();

        let ObjectInterface::Interface = &foo_interface.interface else {
            panic!("concrete")
        };

        assert_eq!(foo_interface.field_names(), vec!["id", "kind"]);

        let implementors = schema_test.implementors();
        assert_eq!(implementors.len(), 2);
        let bar = &implementors[0];
        let qux = &implementors[1];

        assert_eq!(&test.ontology()[bar.typename], "foo_kind_bar");
        assert_eq!(
            bar.object_data().field_names(),
            vec!["id", "kind", "data", "bar"]
        );
        assert_eq!(&test.ontology()[qux.typename], "foo_kind_qux");
        assert_eq!(
            qux.object_data().field_names(),
            vec!["id", "kind", "data", "qux"]
        );
    });
}

#[test]
fn graphql_flattened_union_pascal_casing() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def Kind ()

    def Foo (
        rel. 'id': (rel* is: text)
        rel* Kind: (
            rel* is?: Bar
            rel* is?: Qux
        )
    )

    def Bar (rel* 'kind': 'bar')
    def Qux (rel* 'kind': 'qux')
    "
    .compile_then(|test| {
        let (_schema, schema_test) = schema_test(&test, default_short_name());
        let foo_interface = schema_test.type_data("Foo", QueryLevel::Node).object_data();

        assert_eq!(foo_interface.field_names(), vec!["id", "kind"]);

        let implementors = schema_test.implementors();
        let bar = &implementors[0];
        let qux = &implementors[1];

        assert_eq!(&test.ontology()[bar.typename], "FooKindBar");
        assert_eq!(&test.ontology()[qux.typename], "FooKindQux");
    });
}

#[test]
fn test_edge_entity_union() {
    let test = edge_entity_union().1.compile();
    let (schema, test) = schema_test(&test, default_short_name());
    let ontology = test.test.ontology();

    {
        let link = test.type_data("link", QueryLevel::Node).object_data();

        assert_eq!(&["id", "from", "to"], link.field_names().as_slice());
        let id_field = link.fields.get("id").unwrap();
        let to_field = link.fields.get("to").unwrap();

        expect_eq!(
            actual = id_field.field_type.unit.native_scalar().kind,
            expected = NativeScalarKind::ID
        );
        expect_eq!(
            actual = to_field.field_type.unit.native_scalar().kind,
            expected = NativeScalarKind::ID
        );
    }

    {
        let foo = test.type_data("foo", QueryLevel::Node).object_data();

        assert_eq!(&["id", "related_to"], foo.field_names().as_slice());
        let related_to = foo.fields.get("related_to").unwrap();
        let related_to_connection = schema.type_data(related_to.field_type.unit.addr());

        expect_eq!(
            actual = &ontology[related_to_connection.typename],
            expected = "baz_or_quxConnection"
        );
    }
}

#[test]
fn test_vertex() {
    let test = findings().1.compile();
    let (schema, test) = schema_test(&test, default_short_name());

    let finding_session = test
        .type_data("FindingSession", QueryLevel::Node)
        .object_data();
    let findings_field = finding_session.fields.get("findings").unwrap();
    let findings_connection = schema
        .type_data(findings_field.field_type.unit.addr())
        .object_data();
    let findings_nodes = findings_connection.fields.get("nodes").unwrap();

    expect_eq!(
        actual = findings_nodes.field_type.unit.native_scalar().kind,
        expected = NativeScalarKind::ID
    );
}
