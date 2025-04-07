use std::{borrow::Cow, collections::HashMap};

use heck::{AsLowerCamelCase, AsSnakeCase};
use itertools::Itertools;
use ontol_core::tag::DomainIndex;
use ontol_runtime::{DefId, ontology::aspects::DefsAspect};

use crate::{
    def::{DefKind, Defs},
    strings::StringCtx,
};

#[derive(Default)]
pub struct GraphqlNamespace<'o> {
    rewrites: HashMap<String, String>,
    domain_disambiguation: Option<DomainDisambiguation<'o>>,
}

pub struct DomainDisambiguation<'o> {
    pub root_domain: DomainIndex,
    pub ontology_defs: &'o DefsAspect,
}

impl<'o> GraphqlNamespace<'o> {
    pub fn with_domain_disambiguation(domain_disambiguation: DomainDisambiguation<'o>) -> Self {
        Self {
            rewrites: Default::default(),
            domain_disambiguation: Some(domain_disambiguation),
        }
    }

    pub fn unique_literal(&mut self, literal: &str) -> String {
        let mut string: String = literal.into();
        while self.rewrites.contains_key(&string) {
            string += "_";
        }
        self.rewrite(&string).into()
    }

    pub fn typename(&mut self, def: DefId, ctx: NameCtx) -> String {
        self.concat(&[&Typename(def, ctx)], ctx)
    }

    pub fn connection(&mut self, rel_def: Option<DefId>, def: DefId, ctx: NameCtx) -> String {
        if let Some(rel_def) = rel_def {
            self.concat(
                &[&Typename(rel_def, ctx), &Typename(def, ctx), &"Connection"],
                ctx,
            )
        } else {
            self.concat(&[&Typename(def, ctx), &"Connection"], ctx)
        }
    }

    pub fn mutation_result(&mut self, def: DefId, ctx: NameCtx) -> String {
        self.concat(&[&Typename(def, ctx), &"Mutation"], ctx)
    }

    pub fn edge(&mut self, rel_def: Option<DefId>, def: DefId, ctx: NameCtx) -> String {
        if let Some(rel_def) = rel_def {
            self.concat(
                &[&Typename(rel_def, ctx), &Typename(def, ctx), &"Edge"],
                ctx,
            )
        } else {
            self.concat(&[&Typename(def, ctx), &"Edge"], ctx)
        }
    }

    pub fn input(&mut self, def: DefId, ctx: NameCtx) -> String {
        self.concat(&[&Typename(def, ctx), &"Input"], ctx)
    }

    pub fn partial_input(&mut self, def: DefId, ctx: NameCtx) -> String {
        self.concat(&[&Typename(def, ctx), &"PartialInput"], ctx)
    }

    pub fn union_input(&mut self, def: DefId, ctx: NameCtx) -> String {
        self.concat(&[&Typename(def, ctx), &"UnionInput"], ctx)
    }

    pub fn union_partial_input(&mut self, def: DefId, ctx: NameCtx) -> String {
        self.concat(&[&Typename(def, ctx), &"UnionPartialInput"], ctx)
    }

    pub fn edge_input(&mut self, rel_def: Option<DefId>, def: DefId, ctx: NameCtx) -> String {
        if let Some(rel_def) = rel_def {
            self.concat(
                &[&Typename(rel_def, ctx), &Typename(def, ctx), &"EdgeInput"],
                ctx,
            )
        } else {
            self.concat(&[&Typename(def, ctx), &"EdgeInput"], ctx)
        }
    }

    pub fn patch_edges_input(
        &mut self,
        rel_def: Option<DefId>,
        def: DefId,
        ctx: NameCtx,
    ) -> String {
        if let Some(rel_def) = rel_def {
            self.concat(
                &[
                    &Typename(rel_def, ctx),
                    &Typename(def, ctx),
                    &"PatchEdgesInput",
                ],
                ctx,
            )
        } else {
            self.concat(&[&Typename(def, ctx), &"PatchEdgesInput"], ctx)
        }
    }

    fn concat(&mut self, elements: &[&dyn ProcessName], ctx: NameCtx) -> String {
        let mut output: String = "".into();

        if let Some(domain_disambiguation) = &self.domain_disambiguation {
            if let Some(def_id) = elements.iter().find_map(|elem| elem.def_id()) {
                let domain_index = def_id.0;
                if domain_index != domain_disambiguation.root_domain {
                    output.push('_');
                    if let Some(domain) = domain_disambiguation
                        .ontology_defs
                        .domain_by_index(domain_index)
                    {
                        let domain_name =
                            adapt_graphql_identifier(&ctx.str_ctx[domain.unique_name()])
                                .into_adapted();
                        output.push_str(&domain_name);
                    } else {
                        output.push_str(&format!("domain{}", domain_index.index()));
                    }
                    output.push('_');
                }
            }
        }

        for element in elements {
            output.push_str(element.process(self));
        }
        output
    }

    fn rewrite(&mut self, typename: &str) -> &str {
        if self.rewrites.contains_key(typename) {
            self.rewrites.get(typename).unwrap()
        } else {
            // FIXME: Better naming conflict resolution:
            let rewritten: String = if typename == "Query" {
                "Query_".into()
            } else if typename == "Mutation" {
                "Mutation_".into()
            } else {
                adapt_graphql_identifier(typename).into_adapted()
            };

            self.rewrites.insert(typename.into(), rewritten);
            self.rewrites.get(typename).unwrap()
        }
    }
}

trait ProcessName {
    fn def_id(&self) -> Option<DefId>;

    fn process<'n>(&self, namespace: &'n mut GraphqlNamespace) -> &'n str;
}

#[derive(Clone, Copy)]
pub struct NameCtx<'c, 'm> {
    pub str_ctx: &'c StringCtx<'m>,
    pub defs: &'c Defs<'m>,
}

pub struct Typename<'a, 'm>(DefId, NameCtx<'a, 'm>);

impl ProcessName for Typename<'_, '_> {
    fn def_id(&self) -> Option<DefId> {
        Some(self.0)
    }

    fn process<'n>(&self, namespace: &'n mut GraphqlNamespace) -> &'n str {
        let def_id = self.0;
        let ctx = self.1;

        match ctx.defs.def_kind(def_id) {
            DefKind::InlineUnion(members) => {
                let names = members
                    .iter()
                    .map(|member_id| format_name_pre_rewrite(def_id, ctx.defs.def_kind(*member_id)))
                    .collect_vec();

                let mut concat = String::new();
                let mut iter = names.iter().peekable();

                while let Some(cur) = iter.next() {
                    concat.push_str(cur);

                    if let Some(next) = iter.peek() {
                        if cur.chars().next().is_some_and(char::is_uppercase) {
                            concat.push_str("Or");
                        } else {
                            concat.push_str("_or");
                        }

                        if !next.chars().next().is_some_and(char::is_uppercase) {
                            concat.push('_');
                        }
                    }
                }

                namespace.rewrite(&concat)
            }
            kind => {
                let name = format_name_pre_rewrite(def_id, kind);
                namespace.rewrite(&name)
            }
        }
    }
}

fn format_name_pre_rewrite<'k>(def_id: DefId, kind: &'k DefKind) -> Cow<'k, str> {
    match kind.opt_identifier() {
        Some(name) => name,
        None => Cow::Owned(format!("_anon{}_{}", def_id.0.index(), def_id.1)),
    }
}

impl ProcessName for &'static str {
    fn def_id(&self) -> Option<DefId> {
        None
    }

    fn process<'n>(&self, _: &'n mut GraphqlNamespace) -> &'n str {
        self
    }
}

pub enum GqlAdaptedIdent<'a> {
    Valid(&'a str),
    Adapted(String),
}

impl GqlAdaptedIdent<'_> {
    fn into_adapted(self) -> String {
        match self {
            Self::Valid(valid) => valid.into(),
            Self::Adapted(adapted) => adapted,
        }
    }
}

pub fn adapt_graphql_identifier(input: &str) -> GqlAdaptedIdent {
    if is_valid_graphql_identifier(input) {
        GqlAdaptedIdent::Valid(input)
    } else if input.contains('-') {
        GqlAdaptedIdent::Adapted(format!("{}", AsSnakeCase(input)))
    } else {
        GqlAdaptedIdent::Adapted(format!("{}", AsLowerCamelCase(input)))
    }
}

// copied from juniper, where this is not public API
pub fn is_valid_graphql_identifier(input: &str) -> bool {
    for (i, c) in input.chars().enumerate() {
        let is_valid = c.is_ascii_alphabetic() || c == '_' || (i > 0 && c.is_ascii_digit());
        if !is_valid {
            return false;
        }
    }
    !input.is_empty()
}
