use graphql_parser::schema::{
    Definition, Document, Field, InputObjectType, ObjectType, TypeDefinition,
};

#[derive(Eq, PartialEq, Debug)]
pub struct Nullable(pub bool);

pub fn find_object_type<'d, 'a>(
    parser_document: &'d Document<'a, &'a str>,
    name: &str,
) -> Option<&'d ObjectType<'a, &'a str>> {
    parser_document
        .definitions
        .iter()
        .find_map(|definition| match definition {
            Definition::TypeDefinition(TypeDefinition::Object(object)) if object.name == name => {
                Some(object)
            }
            _ => None,
        })
}

pub fn find_object_field<'a>(
    object: &'a ObjectType<'a, &'a str>,
    name: &str,
) -> Option<&'a Field<'a, &'a str>> {
    object.fields.iter().find(|field| field.name == name)
}

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
