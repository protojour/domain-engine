use ontol_runtime::interface::graphql::{
    data::{ObjectData, ObjectInterface, TypeData, TypeKind},
    schema::{GraphqlSchema, QueryLevel},
};
use ontol_test_utils::{OntolTest, test_extensions::graphql::TypeDataExt};

mod test_graphql_interface;

struct SchemaTest<'o> {
    test: &'o OntolTest,
    schema: &'o GraphqlSchema,
}

impl SchemaTest<'_> {
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

fn schema_test<'o>(
    test: &'o OntolTest,
    domain_short_name: &str,
) -> (&'o GraphqlSchema, SchemaTest<'o>) {
    let schema = test.graphql_schema(domain_short_name);
    (schema, SchemaTest { test, schema })
}
