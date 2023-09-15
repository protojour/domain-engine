use assert_matches::assert_matches;
use ontol_runtime::graphql::{
    data::{
        FieldKind, NativeScalarKind, NativeScalarRef, ObjectData, ScalarData, TypeData, TypeIndex,
        TypeKind, UnitTypeRef,
    },
    schema::{GraphqlSchema, QueryLevel},
};
use ontol_test_utils::{
    examples::ARTIST_AND_INSTRUMENT, expect_eq, OntolTest, TestCompile, ROOT_SRC_NAME,
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
    pub def foo {
        rel .id: { fmt '' => text => . }
        rel .'prop': myint
    }
    "
    .compile_ok(|test| {
        let (_schema, test) = schema_test(&test, ROOT_SRC_NAME);
        let foo_node = test.type_data("foo", QueryLevel::Node);
        let foo_object = foo_node.object_data();

        let prop_field = foo_object.fields.get("prop").unwrap();
        assert_matches!(prop_field.kind, FieldKind::Property(_));

        let native = prop_field.field_type.unit.native_scalar();
        assert_matches!(native.kind, NativeScalarKind::Int(_));
    });
}

#[test]
fn test_graphql_i64_custom_scalar() {
    "
    pub def foo {
        rel .id: { fmt '' => text => . }
        rel .'prop': i64
    }
    "
    .compile_ok(|test| {
        let (schema, test) = schema_test(&test, ROOT_SRC_NAME);
        let foo_node = test.type_data("foo", QueryLevel::Node);
        let foo_object = foo_node.object_data();

        let prop_field = foo_object.fields.get("prop").unwrap();
        assert_matches!(prop_field.kind, FieldKind::Property(_));

        let prop_type_data = schema.type_data(prop_field.field_type.unit.indexed());

        expect_eq!(actual = prop_type_data.typename, expected = "i64");
        let _i64_scalar_data = prop_type_data.custom_scalar();
    });
}

#[test]
fn test_graphql_artist_and_instrument() {
    ARTIST_AND_INSTRUMENT.1.compile_ok(|test| {
        let (schema, test) = schema_test(&test, ROOT_SRC_NAME);
        let query_object = test.query_object_data();

        let artist_list_field = query_object.fields.get("artistList").unwrap();
        let artist_connection = schema.type_data(artist_list_field.field_type.unit.indexed());

        expect_eq!(
            actual = artist_connection.typename,
            expected = "artistConnection"
        );

        let edges_field = artist_connection.object_data().fields.get("edges").unwrap();
        let artist_edge = schema.type_data(edges_field.field_type.unit.indexed());

        expect_eq!(actual = artist_edge.typename, expected = "artistEdge");

        let node_field = artist_edge.object_data().fields.get("node").unwrap();
        let artist = schema.type_data(node_field.field_type.unit.indexed());

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

trait TypeDataExt {
    fn object_data(&self) -> &ObjectData;
    fn custom_scalar(&self) -> &ScalarData;
}

trait UnitTypeRefExt {
    fn indexed(&self) -> TypeIndex;
    fn native_scalar(&self) -> &NativeScalarRef;
}

impl TypeDataExt for TypeData {
    #[track_caller]
    fn object_data(&self) -> &ObjectData {
        let TypeKind::Object(object_data) = &self.kind else {
            panic!("not an object");
        };
        object_data
    }

    #[track_caller]
    fn custom_scalar(&self) -> &ScalarData {
        let TypeKind::CustomScalar(scalar_data) = &self.kind else {
            panic!("not an custom_scalar type");
        };
        scalar_data
    }
}

impl UnitTypeRefExt for UnitTypeRef {
    #[track_caller]
    fn indexed(&self) -> TypeIndex {
        let UnitTypeRef::Indexed(index) = self else {
            panic!("not an indexed type: {self:?}");
        };
        *index
    }

    fn native_scalar(&self) -> &NativeScalarRef {
        let UnitTypeRef::NativeScalar(native_scalar_ref) = self else {
            panic!("not a native scalar: {self:?}");
        };
        native_scalar_ref
    }
}
