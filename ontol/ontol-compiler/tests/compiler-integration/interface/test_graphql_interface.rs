use assert_matches::assert_matches;
use ontol_macros::test;
use ontol_runtime::{
    debug::NoFmt,
    interface::{
        graphql::{
            data::{
                FieldKind, NativeScalarKind, ObjectData, ObjectInterface, ObjectKind, TypeData,
                TypeKind,
            },
            schema::{GraphqlSchema, QueryLevel},
        },
        serde::operator::SerdeOperator,
    },
    ontology::config::DataStoreConfig,
};
use ontol_test_utils::{
    examples::ARTIST_AND_INSTRUMENT,
    expect_eq, src_name,
    test_extensions::{
        graphql::{ObjectDataExt, TypeDataExt, UnitTypeRefExt},
        serde::SerdeOperatorExt,
    },
    OntolTest, SrcName, TestCompile, TestPackages,
};

#[test]
fn test_domain_docs_as_query_docs() {
    "
    /// Domain docs
    domain foo ()

    def foo (
        rel .id: (fmt '' => text => .)
        rel .'prop': i64
    )
    "
    .compile_then(|test| {
        let (_schema, test) = schema_test(&test, SrcName::default());
        let ontology = test.test.ontology();
        let query_data = test.query_object_data();
        let ObjectKind::Query { domain_def_id } = &query_data.kind else {
            panic!();
        };

        let docs = &ontology[ontology.get_docs(*domain_def_id).unwrap()];
        assert_eq!(docs, "Domain docs");
    });
}

#[test]
fn test_graphql_small_range_number_becomes_int() {
    "
    def myint (
        rel .is: integer
        rel .min: 0
        rel .max: 100
    )
    def foo (
        rel .id: (fmt '' => text => .)
        rel .'prop': myint
    )
    "
    .compile_then(|test| {
        let (_schema, test) = schema_test(&test, SrcName::default());
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
    def foo (
        rel .id: (fmt '' => text => .)
        rel .'prop': i64
    )
    "
    .compile_then(|test| {
        let (schema, test) = schema_test(&test, SrcName::default());
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
    def foo (
        rel .id: (fmt '' => text => .)
        rel .'default'[rel .default := '']: text
    )
    "
    .compile_then(|test| {
        let (_schema, test) = schema_test(&test, SrcName::default());
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
    def foo (
        rel .id: (fmt '' => text => .)
        rel .'tags': {text}
    )
    "
    .compile_then(|test| {
        let (_schema, test) = schema_test(&test, SrcName::default());
        let foo_type = test.type_data("foo", QueryLevel::Node);
        let foo_object = foo_type.object_data();

        let prop_field = foo_object.fields.get("tags").unwrap();
        assert_matches!(prop_field.kind, FieldKind::Property { .. });

        let native = prop_field.field_type.unit.native_scalar();
        assert_matches!(native.kind, NativeScalarKind::String);

        let operator = &test.test.ontology()[native.operator_addr];
        if !matches!(operator, SerdeOperator::RelationList(_)) {
            panic!("{:?} was not RelationSequence", NoFmt(operator));
        }
    });
}

#[test]
fn test_graphql_serde_renaming() {
    "
    def foo (
        rel .id: (fmt '' => text => .)
        rel .'must-rewrite': text
        rel .'must_rewrite': text
    )
    "
    .compile_then(|test| {
        let (_schema, schema_test) = schema_test(&test, SrcName::default());
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
            expected = &["must_rewrite", "must_rewrite_", "_edge"]
        );
    });
}

#[test]
fn test_query_map_empty_input_becomes_hidden_arg() {
    "
    def entity (
        rel .'id'|id: (rel .is: text)
    )
    def empty ()

    map my_query(
        (),
        empty(),
    )
    "
    .compile_then(|test| {
        let (schema, _test) = schema_test(&test, SrcName::default());

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
    let test = ARTIST_AND_INSTRUMENT.1.compile();
    let (schema, test) = schema_test(&test, SrcName::default());
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
fn test_no_datastore_yields_empty_mutation() {
    let test = "
    def foo (
        rel .'id'|id: (rel .is: text)
        rel .'x': text
    )
    "
    .compile();
    let (_schema, test) = schema_test(&test, SrcName::default());
    assert!(test.mutation_object_data().fields.is_empty());
}

#[test]
fn test_imperfect_mapping_mutation() {
    let test = TestPackages::with_static_sources([
        (
            SrcName::default(),
            "
            use 'lower' as lower

            def upper (
                rel .'id'|id: (rel .is: text)
                rel .'x': text
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
            src_name("lower"),
            "
            def lower (
                rel .'id'|id: (rel .is: text)
                rel .'a': text
                rel .'b': text
            )
            ",
        ),
    ])
    .with_data_store(src_name("lower"), DataStoreConfig::Default)
    .compile();
    let (_schema, test) = schema_test(&test, SrcName::default());
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
    def edge_type (
        rel .'edge_field': text
    )

    def source (
        rel .'id'|id: (rel .is: text)
        rel .'targets'[rel .is: edge_type]: {target}
    )
    def target (
        rel .'id'|id: (rel .is: text)
    )

    map targets (
        (),
        target {
            ..@match target()
        }
    )
    "
    .compile_then(|test| {
        let (schema, test) = schema_test(&test, SrcName::default());
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
                "_anon1_10targetConnection"
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

            assert_eq!(&ontology[targets_edge.typename], "_anon1_10targetEdge");
            assert!(targets_edge.fields().unwrap().contains_key("edge_field"));
        }
    });
}

#[test]
fn graphql_flattened_union() {
    "
    def kind ()

    def foo (
        rel .'id'|id: (rel .is: text)
        rel .kind: (
            rel .is?: bar
            rel .is?: qux
        )
    )

    def bar (
        rel .'kind': 'bar'
        rel .'data': text
        rel .'bar': i64
    )
    def qux (
        rel .'kind': 'qux'
        rel .'data': i64
        rel .'qux': text
    )
    "
    .compile_then(|test| {
        let (_schema, schema_test) = schema_test(&test, SrcName::default());
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
    def Kind ()

    def Foo (
        rel .'id'|id: (rel .is: text)
        rel .Kind: (
            rel .is?: Bar
            rel .is?: Qux
        )
    )

    def Bar (rel .'kind': 'bar')
    def Qux (rel .'kind': 'qux')
    "
    .compile_then(|test| {
        let (_schema, schema_test) = schema_test(&test, SrcName::default());
        let foo_interface = schema_test.type_data("Foo", QueryLevel::Node).object_data();

        assert_eq!(foo_interface.field_names(), vec!["id", "kind"]);

        let implementors = schema_test.implementors();
        let bar = &implementors[0];
        let qux = &implementors[1];

        assert_eq!(&test.ontology()[bar.typename], "FooKindBar");
        assert_eq!(&test.ontology()[qux.typename], "FooKindQux");
    });
}

struct SchemaTest<'o> {
    test: &'o OntolTest,
    schema: &'o GraphqlSchema,
}

impl<'o> SchemaTest<'o> {
    fn query_object_data(&self) -> &ObjectData {
        self.schema.type_data(self.schema.query).object_data()
    }

    fn mutation_object_data(&self) -> &ObjectData {
        self.schema.type_data(self.schema.mutation).object_data()
    }

    // fn mutation_object_data(&self) -> &ObjectData {
    //     object_data(self.schema.type_data(self.schema.query))
    // }

    fn type_data(&self, type_name: &str, query_level: QueryLevel) -> &TypeData {
        let [binding] = self.test.bind([type_name]);
        self.schema
            .type_data_by_key((binding.def_id(), query_level))
            .unwrap()
    }

    fn implementors(&self) -> Vec<&TypeData> {
        self
            .schema
            .types
            .iter()
            .filter(|type_data| {
                let TypeKind::Object(object_data) = &type_data.kind else {
                    return false;
                };
                matches!(&object_data.interface, ObjectInterface::Implements(interfaces) if !interfaces.is_empty())
            })
            .collect()
    }
}

fn schema_test(
    test: &OntolTest,
    source_name: impl Into<SrcName>,
) -> (&GraphqlSchema, SchemaTest<'_>) {
    let schema = test.graphql_schema(source_name);
    (schema, SchemaTest { test, schema })
}
