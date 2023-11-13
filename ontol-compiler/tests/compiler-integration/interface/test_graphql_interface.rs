use assert_matches::assert_matches;
use ontol_runtime::{
    interface::graphql::{
        data::{FieldKind, NativeScalarKind, ObjectData, TypeData},
        schema::{GraphqlSchema, QueryLevel},
    },
    interface::serde::operator::SerdeOperator,
};
use ontol_test_utils::{
    examples::ARTIST_AND_INSTRUMENT,
    expect_eq,
    test_extensions::{
        graphql::{ObjectDataExt, TypeDataExt, UnitTypeRefExt},
        serde::SerdeOperatorExt,
    },
    OntolTest, TestCompile, ROOT_SRC_NAME,
};
use test_log::test;

#[test]
fn test_graphql_small_range_number_becomes_int() {
    "
    def myint {
        rel .is: integer
        rel .min: 0
        rel .max: 100
    }
    def foo {
        rel .id: { fmt '' => text => . }
        rel .'prop': myint
    }
    "
    .compile_then(|test| {
        let (_schema, test) = schema_test(&test, ROOT_SRC_NAME);
        let foo_type = test.type_data("foo", QueryLevel::Node);
        let foo_object = foo_type.object_data();

        let prop_field = foo_object.fields.get("prop").unwrap();
        assert_matches!(prop_field.kind, FieldKind::Property(_));

        let native = prop_field.field_type.unit.native_scalar();
        assert_matches!(native.kind, NativeScalarKind::Int(_));
    });
}

#[test]
fn test_graphql_i64_custom_scalar() {
    "
    def foo {
        rel .id: { fmt '' => text => . }
        rel .'prop': i64
    }
    "
    .compile_then(|test| {
        let (schema, test) = schema_test(&test, ROOT_SRC_NAME);
        let foo_type = test.type_data("foo", QueryLevel::Node);
        let foo_object = foo_type.object_data();

        let prop_field = foo_object.fields.get("prop").unwrap();
        assert_matches!(prop_field.kind, FieldKind::Property(_));

        let prop_type_data = schema.type_data(prop_field.field_type.unit.addr());

        expect_eq!(actual = prop_type_data.typename, expected = "_ontol_i64");
        let _i64_scalar_data = prop_type_data.custom_scalar();
    });
}

#[test]
fn test_graphql_default_scalar() {
    "
    def foo {
        rel .id: { fmt '' => text => . }
        rel .'default'(rel .default := ''): text
    }
    "
    .compile_then(|test| {
        let (_schema, test) = schema_test(&test, ROOT_SRC_NAME);
        let foo_type = test.type_data("foo", QueryLevel::Node);
        let foo_object = foo_type.object_data();

        let prop_field = foo_object.fields.get("default").unwrap();
        assert_matches!(prop_field.kind, FieldKind::Property(_));

        let native = prop_field.field_type.unit.native_scalar();
        assert_matches!(native.kind, NativeScalarKind::String);
    });
}

#[test]
fn test_graphql_scalar_array() {
    "
    def foo {
        rel .id: { fmt '' => text => . }
        rel .'tags': [text]
    }
    "
    .compile_then(|test| {
        let (_schema, test) = schema_test(&test, ROOT_SRC_NAME);
        let foo_type = test.type_data("foo", QueryLevel::Node);
        let foo_object = foo_type.object_data();

        let prop_field = foo_object.fields.get("tags").unwrap();
        assert_matches!(prop_field.kind, FieldKind::Property(_));

        let native = prop_field.field_type.unit.native_scalar();
        assert_matches!(native.kind, NativeScalarKind::String);

        let operator = test.test.ontology.get_serde_operator(native.operator_addr);
        assert_matches!(operator, SerdeOperator::RelationSequence(_));
    });
}

#[test]
fn test_graphql_serde_renaming() {
    "
    def foo {
        rel .id: { fmt '' => text => . }
        rel .'must-rewrite': text
        rel .'must_rewrite': text
    }
    "
    .compile_then(|test| {
        let (_schema, schema_test) = schema_test(&test, ROOT_SRC_NAME);
        let foo_node = schema_test
            .type_data("foo", QueryLevel::Node)
            .object_data()
            .node_data();

        let fields: Vec<_> = test
            .ontology
            .get_serde_operator(foo_node.operator_addr)
            .struct_op()
            .properties
            .keys()
            .collect();

        expect_eq!(
            actual = fields.as_slice(),
            expected = &["must_rewrite", "must_rewrite_"]
        );
    });
}

#[test]
fn test_query_map_empty_input_becomes_hidden_arg() {
    "
    def entity {
        rel .'id'|id: { rel .is: text }
    }
    def empty {}

    map my_query {
        {}
        empty {}
    }
    "
    .compile_then(|test| {
        let (schema, _test) = schema_test(&test, ROOT_SRC_NAME);

        let query = schema.type_data(schema.query).object_data();
        let my_query = query.fields.get("my_query").unwrap();
        let FieldKind::MapFind { input_arg, .. } = &my_query.kind else {
            panic!("Incorrect field kind");
        };

        assert!(input_arg.hidden);
    });
}

#[test]
fn test_graphql_artist_and_instrument() {
    let test = ARTIST_AND_INSTRUMENT.1.compile();
    let (schema, test) = schema_test(&test, ROOT_SRC_NAME);
    let query_object = test.query_object_data();

    let artists_field = query_object.fields.get("artists").unwrap();
    let artist_connection = schema.type_data(artists_field.field_type.unit.addr());

    expect_eq!(
        actual = artist_connection.typename,
        expected = "artistConnection"
    );

    let edges_field = artist_connection.object_data().fields.get("edges").unwrap();
    let artist_edge = schema.type_data(edges_field.field_type.unit.addr());

    expect_eq!(actual = artist_edge.typename, expected = "artistEdge");

    let node_field = artist_edge.object_data().fields.get("node").unwrap();
    let artist = schema.type_data(node_field.field_type.unit.addr());

    expect_eq!(actual = artist.typename, expected = "artist");
    expect_eq!(
        actual = artist.input_typename.as_deref(),
        expected = Some("artistInput")
    );
}

struct SchemaTest<'o> {
    test: &'o OntolTest,
    schema: &'o GraphqlSchema,
}

impl<'o> SchemaTest<'o> {
    fn query_object_data(&self) -> &ObjectData {
        self.schema.type_data(self.schema.query).object_data()
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
}

fn schema_test<'o>(test: &'o OntolTest, source_name: &str) -> (&'o GraphqlSchema, SchemaTest<'o>) {
    let schema = test.graphql_schema(source_name);
    (schema, SchemaTest { test, schema })
}
