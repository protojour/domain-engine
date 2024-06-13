use crate::juniper;

use super::{gql_domain::TypeInfo, Ctx};

pub struct DefDictionaryEntry {
    pub name: String,

    pub definitions: Vec<TypeInfo>,
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl DefDictionaryEntry {
    fn name(&self) -> &str {
        &self.name
    }
}
