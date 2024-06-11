use std::collections::HashMap;

use heck::{AsLowerCamelCase, AsSnakeCase};
use ontol_runtime::{
    ontology::{domain::TypeInfo, Ontology},
    DefId, PackageId,
};

use crate::strings::StringCtx;

#[derive(Default)]
pub struct GraphqlNamespace<'o> {
    rewrites: HashMap<String, String>,
    domain_disambiguation: Option<DomainDisambiguation<'o>>,
}

pub struct DomainDisambiguation<'o> {
    pub root_domain: PackageId,
    pub ontology: &'o Ontology,
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

    pub fn typename(&mut self, type_info: &TypeInfo, strings: &StringCtx) -> String {
        self.concat(&[&Typename(type_info, strings)], strings)
    }

    pub fn connection(
        &mut self,
        rel_type_info: Option<&TypeInfo>,
        type_info: &TypeInfo,
        strings: &StringCtx,
    ) -> String {
        if let Some(rel_type_info) = rel_type_info {
            self.concat(
                &[
                    &Typename(rel_type_info, strings),
                    &Typename(type_info, strings),
                    &"Connection",
                ],
                strings,
            )
        } else {
            self.concat(&[&Typename(type_info, strings), &"Connection"], strings)
        }
    }

    pub fn mutation_result(&mut self, type_info: &TypeInfo, strings: &StringCtx) -> String {
        self.concat(&[&Typename(type_info, strings), &"Mutation"], strings)
    }

    pub fn edge(
        &mut self,
        rel_type_info: Option<&TypeInfo>,
        type_info: &TypeInfo,
        strings: &StringCtx,
    ) -> String {
        if let Some(rel_type_info) = rel_type_info {
            self.concat(
                &[
                    &Typename(rel_type_info, strings),
                    &Typename(type_info, strings),
                    &"Edge",
                ],
                strings,
            )
        } else {
            self.concat(&[&Typename(type_info, strings), &"Edge"], strings)
        }
    }

    pub fn input(&mut self, type_info: &TypeInfo, strings: &StringCtx) -> String {
        self.concat(&[&Typename(type_info, strings), &"Input"], strings)
    }

    pub fn partial_input(&mut self, type_info: &TypeInfo, strings: &StringCtx) -> String {
        self.concat(&[&Typename(type_info, strings), &"PartialInput"], strings)
    }

    pub fn union_input(&mut self, type_info: &TypeInfo, strings: &StringCtx) -> String {
        self.concat(&[&Typename(type_info, strings), &"UnionInput"], strings)
    }

    pub fn union_partial_input(&mut self, type_info: &TypeInfo, strings: &StringCtx) -> String {
        self.concat(
            &[&Typename(type_info, strings), &"UnionPartialInput"],
            strings,
        )
    }

    pub fn edge_input(
        &mut self,
        rel_type_info: Option<&TypeInfo>,
        type_info: &TypeInfo,
        strings: &StringCtx,
    ) -> String {
        if let Some(rel_type_info) = rel_type_info {
            self.concat(
                &[
                    &Typename(rel_type_info, strings),
                    &Typename(type_info, strings),
                    &"EdgeInput",
                ],
                strings,
            )
        } else {
            self.concat(&[&Typename(type_info, strings), &"EdgeInput"], strings)
        }
    }

    pub fn patch_edges_input(
        &mut self,
        rel_type_info: Option<&TypeInfo>,
        type_info: &TypeInfo,
        strings: &StringCtx,
    ) -> String {
        if let Some(rel_type_info) = rel_type_info {
            self.concat(
                &[
                    &Typename(rel_type_info, strings),
                    &Typename(type_info, strings),
                    &"PatchEdgesInput",
                ],
                strings,
            )
        } else {
            self.concat(
                &[&Typename(type_info, strings), &"PatchEdgesInput"],
                strings,
            )
        }
    }

    fn concat(&mut self, elements: &[&dyn ProcessName], strings: &StringCtx) -> String {
        let mut output: String = "".into();

        if let Some(domain_disambiguation) = &self.domain_disambiguation {
            if let Some(def_id) = elements.iter().find_map(|elem| elem.def_id()) {
                let package_id = def_id.0;
                if package_id != domain_disambiguation.root_domain {
                    output.push('_');
                    if let Some(domain) = domain_disambiguation.ontology.find_domain(package_id) {
                        let domain_name =
                            adapt_graphql_identifier(&strings[domain.unique_name()]).into_adapted();
                        output.push_str(&domain_name);
                    } else {
                        output.push_str(&format!("domain{}", package_id.id()));
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

struct Typename<'a, 'm>(&'a TypeInfo, &'a StringCtx<'m>);

impl<'a, 'm> ProcessName for Typename<'a, 'm> {
    fn def_id(&self) -> Option<DefId> {
        Some(self.0.def_id)
    }

    fn process<'n>(&self, namespace: &'n mut GraphqlNamespace) -> &'n str {
        let type_info = self.0;
        match type_info.name() {
            Some(name) => {
                let strings = self.1;
                namespace.rewrite(&strings[name])
            }
            None => namespace.rewrite(&format!(
                "_anon{}_{}",
                type_info.def_id.0.id(),
                type_info.def_id.1
            )),
        }
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

impl<'a> GqlAdaptedIdent<'a> {
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
