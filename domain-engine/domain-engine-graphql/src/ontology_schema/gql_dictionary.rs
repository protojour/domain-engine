use crate::juniper;

use super::{gql_domain::Def, Ctx};

pub struct DefDictionaryEntry {
    pub name: String,

    pub definitions: Vec<Def>,
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl DefDictionaryEntry {
    fn name(&self) -> &str {
        &self.name
    }

    fn definitions(&self) -> Vec<Def> {
        self.definitions.clone()
    }
}
