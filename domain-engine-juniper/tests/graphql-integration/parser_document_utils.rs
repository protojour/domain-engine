use graphql_parser::schema::{Definition, Document, InputObjectType, TypeDefinition};

#[derive(Eq, PartialEq, Debug)]
pub struct Nullable(pub bool);

pub fn find_input_object_type<'d, 'a>(
    parser_document: &'d Document<'a, &'a str>,
    name: &str,
) -> Option<&'d InputObjectType<'a, &'a str>> {
    parser_document
        .definitions
        .iter()
        .find_map(|definition| match definition {
            Definition::TypeDefinition(TypeDefinition::InputObject(input_object))
                if input_object.name == name =>
            {
                Some(input_object)
            }
            _ => None,
        })
}
