use ontol_runtime::{
    ontology::domain::{self, DefKind},
    PackageId,
};

use crate::juniper;

use super::{gql_def, OntologyCtx};

pub struct Domain {
    pub pkg_id: PackageId,
}

#[juniper::graphql_object]
#[graphql(context = OntologyCtx)]
impl Domain {
    fn id(&self, ctx: &OntologyCtx) -> juniper::ID {
        self.data(ctx).domain_id().ulid.to_string().into()
    }

    fn id_stable(&self, ctx: &OntologyCtx) -> bool {
        self.data(ctx).domain_id().stable
    }

    fn package_id(&self) -> String {
        format!("{:?}", self.pkg_id)
    }

    fn name(&self, ctx: &OntologyCtx) -> String {
        ctx[self.data(ctx).unique_name()].to_string()
    }

    fn doc(&self, ctx: &OntologyCtx) -> Option<String> {
        let text_constant = ctx.get_def_docs(self.data(ctx).def_id())?;
        Some(ctx[text_constant].to_string())
    }

    fn entities(&self, ctx: &OntologyCtx) -> Vec<gql_def::Entity> {
        let mut entities = vec![];
        for def in self.data(ctx).defs() {
            if matches!(def.kind, DefKind::Entity(_)) {
                entities.push(gql_def::Entity { id: def.id })
            }
        }
        entities
    }

    fn defs(&self, ctx: &OntologyCtx, kind: Option<gql_def::DefKind>) -> Vec<gql_def::Def> {
        let defs = self.data(ctx).defs().map(|def| gql_def::Def { id: def.id });
        if let Some(kind) = kind {
            defs.filter(|def| def.kind(ctx) == kind).collect()
        } else {
            defs.collect()
        }
    }

    fn maps(&self, ctx: &OntologyCtx) -> Vec<gql_def::NamedMap> {
        ctx.iter_named_downmaps()
            .filter(|(package_id, ..)| package_id == &self.pkg_id)
            .map(|(_, name, _)| gql_def::NamedMap { name })
            .collect()
    }
}

impl Domain {
    fn data<'c>(&self, ctx: &'c OntologyCtx) -> &'c domain::Domain {
        ctx.find_domain(self.pkg_id).unwrap()
    }
}
