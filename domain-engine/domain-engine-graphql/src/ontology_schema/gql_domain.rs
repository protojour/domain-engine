use ontol_runtime::{
    ontology::domain::{self, DefKind},
    PackageId,
};

use crate::juniper;

use super::{gql_def, Ctx};

pub struct Domain {
    pub pkg_id: PackageId,
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl Domain {
    fn id(&self, ctx: &Ctx) -> juniper::ID {
        self.data(ctx).domain_id().ulid.to_string().into()
    }

    fn id_stable(&self, ctx: &Ctx) -> bool {
        self.data(ctx).domain_id().stable
    }

    fn package_id(&self) -> String {
        format!("{:?}", self.pkg_id)
    }

    fn name(&self, ctx: &Ctx) -> String {
        ctx[self.data(ctx).unique_name()].to_string()
    }

    fn entities(&self, ctx: &Ctx) -> Vec<gql_def::Entity> {
        let mut entities = vec![];
        for def in self.data(ctx).defs() {
            if matches!(def.kind, DefKind::Entity(_)) {
                entities.push(gql_def::Entity { id: def.id })
            }
        }
        entities
    }

    fn defs(&self, ctx: &Ctx, kind: Option<gql_def::DefKind>) -> Vec<gql_def::Def> {
        let defs = self.data(ctx).defs().map(|def| gql_def::Def { id: def.id });
        if let Some(kind) = kind {
            defs.filter(|def| def.kind(ctx) == kind).collect()
        } else {
            defs.collect()
        }
    }

    fn maps(&self, ctx: &Ctx) -> Vec<gql_def::NamedMap> {
        ctx.iter_named_downmaps()
            .filter(|(package_id, ..)| package_id == &self.pkg_id)
            .map(|(_, name, _)| gql_def::NamedMap { name })
            .collect()
    }
}

impl Domain {
    fn data<'c>(&self, ctx: &'c Ctx) -> &'c domain::Domain {
        ctx.find_domain(self.pkg_id).unwrap()
    }
}
