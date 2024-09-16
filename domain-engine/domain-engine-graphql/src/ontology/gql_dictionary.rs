use crate::{gql_scalar::GqlScalar, juniper};

use super::{gql_def::Def, OntologyCtx};

pub struct DefDictionaryEntry {
    pub name: String,

    pub definitions: Vec<Def>,
}

#[juniper::graphql_object]
#[graphql(context = OntologyCtx, scalar = GqlScalar)]
impl DefDictionaryEntry {
    fn name(&self) -> &str {
        &self.name
    }

    fn definitions(&self) -> Vec<Def> {
        self.definitions.clone()
    }
}
