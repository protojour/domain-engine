use graphql_parser::schema::{
    Definition, Document, Field, InputObjectType, InputValue, ObjectType, TypeDefinition,
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct FieldInfo {
    pub name: String,
    pub field_type: String,
}

impl From<(&str, &str)> for FieldInfo {
    fn from(value: (&str, &str)) -> Self {
        FieldInfo {
            name: value.0.to_string(),
            field_type: value.1.to_string(),
        }
    }
}

impl<'a> From<&InputValue<'a, &'a str>> for FieldInfo {
    fn from(value: &InputValue<'a, &'a str>) -> Self {
        FieldInfo {
            name: value.name.to_string(),
            field_type: value.value_type.to_string(),
        }
    }
}

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
