use ontol_runtime::{
    ontology::domain::{self, DefKind},
    DomainIndex,
};

use crate::{gql_scalar::GqlScalar, juniper};

use super::{gql_def, OntologyCtx};

pub struct Domain {
    pub domain_index: DomainIndex,
}

#[juniper::graphql_object]
#[graphql(context = OntologyCtx, scalar = GqlScalar)]
impl Domain {
    fn id(&self, ctx: &OntologyCtx) -> juniper::ID {
        self.data(ctx).domain_id().ulid.to_string().into()
    }

    fn id_stable(&self, ctx: &OntologyCtx) -> bool {
        self.data(ctx).domain_id().stable
    }

    fn index(&self) -> String {
        format!("{:?}", self.domain_index)
    }

    fn name(&self, ctx: &OntologyCtx) -> String {
        ctx[self.data(ctx).unique_name()].to_string()
    }

    fn doc(&self, ctx: &OntologyCtx) -> Option<String> {
        Some(ctx.get_def_docs(self.data(ctx).def_id())?.to_string())
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

    fn defs(
        &self,
        kind: Option<gql_def::DefKind>,
        ident: Option<String>,
        ctx: &OntologyCtx,
    ) -> Vec<gql_def::Def> {
        let defs = self.data(ctx).defs().filter_map(|def| {
            if let Some(def_ident) = def.ident() {
                if let Some(ident_filter) = ident.as_ref() {
                    let def_ident = &ctx[def_ident];
                    if def_ident == ident_filter {
                        Some(gql_def::Def { id: def.id })
                    } else {
                        None
                    }
                } else {
                    Some(gql_def::Def { id: def.id })
                }
            } else {
                None
            }
        });
        if let Some(kind) = kind {
            defs.filter(|def| def.kind(ctx) == kind).collect()
        } else {
            defs.collect()
        }
    }

    fn maps(&self, ctx: &OntologyCtx) -> Vec<gql_def::NamedMap> {
        ctx.iter_named_downmaps()
            .filter(|(domain_index, ..)| domain_index == &self.domain_index)
            .map(|(_, name, _)| gql_def::NamedMap { name })
            .collect()
    }
}

impl Domain {
    fn data<'c>(&self, ctx: &'c OntologyCtx) -> &'c domain::Domain {
        ctx.domain_by_index(self.domain_index).unwrap()
    }
}
