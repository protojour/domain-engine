use assert_matches::assert_matches;
use ontol_runtime::graphql::{
    data::{FieldKind, ObjectData, ScalarData, TypeData, TypeIndex, TypeKind, UnitTypeRef},
    schema::GraphqlSchema,
    QueryLevel,
};
use ontol_test_utils::{
    examples::ARTIST_AND_INSTRUMENT, expect_eq, OntolTest, TestCompile, ROOT_SRC_NAME,
};
use test_log::test;

#[test]
fn test_graphql_basic_schema() {
    "
    pub def foo_id { fmt '' => text => . }
    pub def foo {
        rel foo_id identifies: .
        rel .'prop': i64
    }
    "
    .compile_ok(|test| {
        let (schema, test) = schema_test(&test, ROOT_SRC_NAME);
        let foo_node = test.type_data("foo", QueryLevel::Node);
        let foo_object = object_data(foo_node);

        let prop_field = foo_object.fields.get("prop").unwrap();
        assert_matches!(prop_field.kind, FieldKind::Property(_));

        let prop_type_data = schema.type_data(type_ref_index(&prop_field.field_type.unit));

        expect_eq!(actual = prop_type_data.typename, expected = "i64");
        let _i64_scalar_data = custom_scalar(prop_type_data);
    });
}

#[test]
fn test_graphql_artist_and_instrument() {
    ARTIST_AND_INSTRUMENT.1.compile_ok(|test| {
        let (schema, test) = schema_test(&test, ROOT_SRC_NAME);
        let query_object = test.query_object_data();

        let artist_list_field = query_object.fields.get("artistList").unwrap();
        let artist_connection =
            schema.type_data(type_ref_index(&artist_list_field.field_type.unit));

        expect_eq!(
            actual = artist_connection.typename,
            expected = "artistConnection"
        );

        let edges_field = object_data(artist_connection).fields.get("edges").unwrap();
        let artist_edge = schema.type_data(type_ref_index(&edges_field.field_type.unit));

        expect_eq!(actual = artist_edge.typename, expected = "artistEdge");

        let node_field = object_data(artist_edge).fields.get("node").unwrap();
        let artist = schema.type_data(type_ref_index(&node_field.field_type.unit));

        expect_eq!(actual = artist.typename, expected = "artist");
        expect_eq!(
            actual = artist.input_typename.as_deref(),
            expected = Some("artistInput")
        );
    });
}

struct SchemaTest<'o> {
    test: &'o OntolTest,
    schema: &'o GraphqlSchema,
}

impl<'o> SchemaTest<'o> {
    fn query_object_data(&self) -> &ObjectData {
        object_data(self.schema.type_data(self.schema.query))
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

fn object_data(type_data: &TypeData) -> &ObjectData {
    let TypeKind::Object(object_data) = &type_data.kind else {
        panic!()
    };
    object_data
}

fn custom_scalar(type_data: &TypeData) -> &ScalarData {
    let TypeKind::CustomScalar(scalar_data) = &type_data.kind else {
        panic!()
    };
    scalar_data
}

fn type_ref_index(type_ref: &UnitTypeRef) -> TypeIndex {
    let UnitTypeRef::Indexed(index) = type_ref else {
        panic!();
    };
    *index
}
