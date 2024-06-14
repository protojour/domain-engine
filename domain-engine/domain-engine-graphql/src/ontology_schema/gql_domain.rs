use ontol_runtime::{ontology::domain::DefKind, PackageId};

use crate::juniper;

use super::{gql_def, Ctx};

pub struct Domain {
    pub id: PackageId,
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl Domain {
    fn name(&self, ctx: &Ctx) -> String {
        let domain = ctx.find_domain(self.id).unwrap();
        String::from(&ctx[domain.unique_name()])
    }
    fn entities(&self, ctx: &Ctx) -> Vec<gql_def::Entity> {
        let domain = ctx.find_domain(self.id).unwrap();
        let mut entities = vec![];
        for def in domain.defs() {
            if matches!(def.kind, DefKind::Entity(_)) {
                entities.push(gql_def::Entity { id: def.id })
            }
        }
        entities
    }
    fn defs(&self, ctx: &Ctx, kind: Option<gql_def::DefKind>) -> Vec<gql_def::Def> {
        let defs = ctx
            .find_domain(self.id)
            .unwrap()
            .defs()
            .map(|def| gql_def::Def { id: def.id });
        if let Some(kind) = kind {
            defs.filter(|def| def.kind(ctx) == kind).collect()
        } else {
            defs.collect()
        }
    }
    fn maps(&self, ctx: &Ctx) -> Vec<gql_def::NamedMap> {
        ctx.iter_named_downmaps()
            .filter(|(package_id, ..)| package_id == &self.id)
            .map(|(_, name, _)| gql_def::NamedMap { name })
            .collect()
    }
}
